"""Scheduled in-process watcher.

Runs SGO odds + Kalshi ticks on APScheduler's AsyncIOScheduler, sharing
the FastAPI event loop. The first warmup tick is fired with
asyncio.create_task(...) BEFORE yield so the HTTP server comes up
immediately and Railway's healthcheck passes even on a cold deploy.

Public exports:
    scheduled_watcher        — async context manager; main.py uses this
                               in the FastAPI lifespan.
    run_slate_watcher_once   — one-shot tick used by POST /api/slates/refresh.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.services import kalshi as kalshi_svc
from app.services import sgo_odds as sgo_svc

logger = logging.getLogger(__name__)


async def _tick_job() -> None:
    """First-boot warmup: SGO + Kalshi once so the UI has data immediately.
    Runs as a fire-and-forget asyncio.task AFTER lifespan yields, so a
    slow external API doesn't block the healthcheck."""
    settings = get_settings()
    try:
        if settings.sgo_api_key:
            logger.info("Boot warmup: SGO odds tick starting")
            await sgo_svc.fetch_tick()
            logger.info("Boot warmup: SGO odds tick done")
        else:
            logger.info("Boot warmup: SGO key missing, skipping SGO tick")
    except Exception as e:
        logger.exception("Boot warmup SGO tick failed: %s", e)

    try:
        if settings.kalshi_key_id and settings.kalshi_private_key:
            logger.info("Boot warmup: Kalshi tick starting")
            await kalshi_svc.fetch_tick()
            logger.info("Boot warmup: Kalshi tick done")
        else:
            logger.info("Boot warmup: Kalshi creds missing, skipping Kalshi tick")
    except Exception as e:
        logger.exception("Boot warmup Kalshi tick failed: %s", e)


async def _sgo_job() -> None:
    try:
        await sgo_svc.fetch_tick()
    except Exception as e:
        logger.exception("SGO tick failed: %s", e)


async def _kalshi_job() -> None:
    try:
        await kalshi_svc.fetch_tick()
    except Exception as e:
        logger.exception("Kalshi tick failed: %s", e)


async def run_slate_watcher_once() -> dict:
    """Manual one-shot trigger for /api/slates/refresh.

    Runs both SGO and Kalshi ticks once and returns a summary. Errors
    in either tick are logged and surfaced in the result dict but do
    not raise — the route handler should still return 200.
    """
    settings = get_settings()
    result: dict = {"sgo": None, "kalshi": None}

    try:
        if settings.sgo_api_key:
            await sgo_svc.fetch_tick()
            result["sgo"] = "ok"
        else:
            result["sgo"] = "skipped (no SGO_API_KEY)"
    except Exception as e:
        logger.exception("Manual SGO tick failed: %s", e)
        result["sgo"] = f"error: {e}"

    try:
        if settings.kalshi_key_id and settings.kalshi_private_key:
            await kalshi_svc.fetch_tick()
            result["kalshi"] = "ok"
        else:
            result["kalshi"] = "skipped (no Kalshi creds)"
    except Exception as e:
        logger.exception("Manual Kalshi tick failed: %s", e)
        result["kalshi"] = f"error: {e}"

    return result


@asynccontextmanager
async def scheduled_watcher():
    """Start the scheduler, run jobs in-process, shut down cleanly on exit."""
    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        _sgo_job,
        trigger=IntervalTrigger(minutes=15),
        id="sgo_odds_tick",
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _kalshi_job,
        trigger=IntervalTrigger(minutes=10),
        id="kalshi_tick",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info("Scheduler started: SGO=15min, Kalshi=10min")

    # Fire-and-forget warmup so /health is up immediately.
    asyncio.create_task(_tick_job())

    try:
        yield
    finally:
        logger.info("Scheduler shutting down")
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
