"""Scheduled in-process watcher.

Runs three jobs on APScheduler's AsyncIOScheduler (shares the FastAPI loop):

  * SGO odds tick   — every 15 min, all active slates, primary tennis odds
  * Kalshi tick     — every 10 min, ML probabilities for every active match
  * DK auto-ingest  — once daily at 11:00 UTC, pulls Featured Classic
                      draft groups for each enabled sport

Each job uses max_instances=1 + coalesce=True so a slow tick can't stack.
The whole scheduler is wrapped in an async context manager so FastAPI's
lifespan can start/stop it cleanly.

Boot sequence is non-blocking:
  1. Add jobs (NO next_run_time=now — that would block startup)
  2. scheduler.start() (registers the timers, doesn't run anything)
  3. asyncio.create_task(_tick_job()) — fire-and-forget warmup
  4. yield → FastAPI starts serving /health immediately

This guarantees Railway's healthcheck succeeds even if SGO/Kalshi
external calls take 30+ seconds on a cold deploy.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.services.dk_auto_ingest import run_dk_auto_ingest
from app.services.kalshi import run_kalshi_tick
from app.services.sgo_odds import run_sgo_tick

logger = logging.getLogger(__name__)


async def _tick_job() -> None:
    """First-boot warmup: kick off odds + kalshi once so the UI has data
    immediately rather than waiting for the next scheduled interval.

    Runs as a fire-and-forget task AFTER the FastAPI lifespan yields,
    so a slow external API doesn't block the healthcheck.
    """
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


async def _dk_auto_ingest_job() -> None:
    settings = get_settings()
    if not settings.dk_auto_ingest_enabled:
        logger.info("DK auto-ingest skipped (DK_AUTO_INGEST_ENABLED=false)")
        return
    try:
        for sport in settings.sports_list:
            logger.info("DK auto-ingest starting for sport=%s", sport)
            result = await run_dk_auto_ingest(sport=sport)
            logger.info("DK auto-ingest result for %s: %s", sport, result)
    except Exception as e:
        logger.exception("DK auto-ingest failed: %s", e)


@asynccontextmanager
async def scheduled_watcher():
    """Start the scheduler, run jobs in-process, and shut down cleanly on exit."""
    settings = get_settings()
    scheduler = AsyncIOScheduler(timezone="UTC")

    # SGO every 15 min — first run after the interval elapses, NOT immediately.
    # Immediate refresh is handled by _tick_job() below as a fire-and-forget task.
    scheduler.add_job(
        _sgo_job,
        trigger=IntervalTrigger(minutes=15),
        id="sgo_odds_tick",
        max_instances=1,
        coalesce=True,
    )

    # Kalshi every 10 min — same pattern.
    scheduler.add_job(
        _kalshi_job,
        trigger=IntervalTrigger(minutes=10),
        id="kalshi_tick",
        max_instances=1,
        coalesce=True,
    )

    # DK auto-ingest daily at 11:00 UTC (~7am ET / 4am PT — well before any slate locks)
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

    # Fire-and-forget the first warmup tick so /health responds before
    # the (potentially slow) external API calls finish. This must run
    # AFTER scheduler.start() and BEFORE yield.
    asyncio.create_task(_tick_job())

    try:
        yield
    finally:
        logger.info("Scheduler shutting down")
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
