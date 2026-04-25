"""Scheduled in-process watcher (v6.2.1).

SGO odds + Kalshi ticks on APScheduler's AsyncIOScheduler, sharing the
FastAPI event loop. The first warmup tick is fired with
asyncio.create_task(...) BEFORE yield so the HTTP server comes up
immediately and Railway's healthcheck passes even on a cold deploy.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.services.kalshi import run_kalshi_tick
from app.services.sgo_odds import run_sgo_tick

logger = logging.getLogger(__name__)


async def _tick_job() -> None:
    """First-boot warmup: SGO + Kalshi once so the UI has data immediately."""
    settings = get_settings()
    try:
        if settings.sgo_api_key:
            logger.info("Boot warmup: SGO odds tick starting")
            await run_sgo_tick()
            logger.info("Boot warmup: SGO odds tick done")
        else:
            logger.info("Boot warmup: SGO key missing, skipping SGO tick")
    except Exception as e:
        logger.exception("Boot warmup SGO tick failed: %s", e)

    try:
        if settings.kalshi_key_id and settings.kalshi_private_key:
            logger.info("Boot warmup: Kalshi tick starting")
            await run_kalshi_tick()
            logger.info("Boot warmup: Kalshi tick done")
        else:
            logger.info("Boot warmup: Kalshi creds missing, skipping Kalshi tick")
    except Exception as e:
        logger.exception("Boot warmup Kalshi tick failed: %s", e)


async def _sgo_job() -> None:
    try:
        await run_sgo_tick()
    except Exception as e:
        logger.exception("SGO tick failed: %s", e)


async def _kalshi_job() -> None:
    try:
        await run_kalshi_tick()
    except Exception as e:
        logger.exception("Kalshi tick failed: %s", e)


@asynccontextmanager
async def scheduled_watcher():
    """Start the scheduler, run jobs in-process, and shut down cleanly on exit."""
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
