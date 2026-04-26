"""Scheduled in-process watcher.

Runs three jobs on APScheduler's AsyncIOScheduler (shares the FastAPI loop):

  * SGO odds tick     — every 15 min, primary tennis odds
  * Kalshi tick       — every 10 min, ML probabilities for every active match
  * DK auto-ingest    — once daily at 11:00 UTC; pulls Featured Classic
                        draft groups for each enabled sport. Gated by
                        DK_AUTO_INGEST_ENABLED env var (default false).

Each job uses max_instances=1 + coalesce=True so a slow tick can't stack.

Boot sequence is non-blocking:
  1. Add jobs (NO next_run_time=now — that would block startup)
  2. scheduler.start() (registers timers, doesn't run anything)
  3. asyncio.create_task(_tick_job()) — fire-and-forget warmup
  4. yield → FastAPI starts serving /health immediately

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
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.services import kalshi as kalshi_svc
from app.services import sgo_odds as sgo_svc

logger = logging.getLogger(__name__)


async def _tick_job() -> None:
    """First-boot warmup: SGO + Kalshi once so the UI has data immediately."""
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


async def _dk_auto_ingest_job() -> None:
    """Daily 11:00 UTC: pull Featured Classic from DK lobby for each sport.

    Imported lazily so the watcher boots cleanly even if dk_auto_ingest
    has any import-time issues — those would surface only when this job
    actually runs, not at app startup.
    """
    settings = get_settings()
    if not settings.dk_auto_ingest_enabled:
        logger.info("DK auto-ingest skipped (DK_AUTO_INGEST_ENABLED=false)")
        return
    try:
        from app.services.dk_auto_ingest import run_dk_auto_ingest_tick
        result = await run_dk_auto_ingest_tick()
        logger.info("DK auto-ingest result: %s", result)
    except Exception as e:
        logger.exception("DK auto-ingest job failed: %s", e)


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
    settings = get_settings()
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

    # DK auto-ingest daily at 11:00 UTC (~7am ET / 4am PT — well before
    # any tennis slate locks). Gated internally on DK_AUTO_INGEST_ENABLED.
    scheduler.add_job(
        _dk_auto_ingest_job,
        trigger=CronTrigger(hour=11, minute=0, timezone="UTC"),
        id="dk_auto_ingest_daily",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info(
        "Scheduler started: SGO=15min, Kalshi=10min, DK auto-ingest=11:00 UTC daily (enabled=%s)",
        settings.dk_auto_ingest_enabled,
    )

    # Fire-and-forget warmup so /health is up immediately.
    asyncio.create_task(_tick_job())

    try:
        yield
    finally:
        logger.info("Scheduler shutting down")
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
