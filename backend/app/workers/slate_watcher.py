"""Watcher: schedules SGO odds, PrizePicks (direct), Kalshi, and DK auto-ingest.

Exports both `scheduled_watcher` (the name main.py imports) and
`watcher_lifespan` (alias) so the lifespan integration works either way.

4 jobs:
  - sgo_odds_tick       (every 15 min) — Pinnacle ML/games-won/spreads
  - prizepicks_direct   (every 15 min) — Aces/DFs/Breaks/Games/Sets/Fantasy Score
                                          from api.prizepicks.com
  - kalshi_tick         (every 10 min) — kalshi_prob_a/_b (sole win-% source)
  - dk_auto_ingest      (cron 11:00 UTC daily) — Featured slate only,
                          gated by DK_AUTO_INGEST_ENABLED env var
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.services import sgo_odds as sgo_svc
from app.services import prizepicks_direct as pp_direct_svc
from app.services import kalshi as kalshi_svc
from app.services import dk_auto_ingest as dk_auto_svc

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


# ─────────────────────────────────────────────────────────────────────
# Per-job wrappers
# ─────────────────────────────────────────────────────────────────────

async def _sgo_odds_tick():
    try:
        result = await sgo_svc.fetch_tick("TEN")
        logger.info("sgo_odds tick result: %s", result)
    except Exception as e:
        logger.exception("sgo_odds tick failed: %s", e)


async def _prizepicks_tick():
    try:
        result = await pp_direct_svc.fetch_tick("TEN")
        logger.info("prizepicks_direct tick result: %s", result)
    except Exception as e:
        logger.exception("prizepicks_direct tick failed: %s", e)


async def _kalshi_tick():
    try:
        result = await kalshi_svc.fetch_tick()
        logger.info("kalshi tick result: %s", result)
    except Exception as e:
        logger.exception("kalshi tick failed: %s", e)


async def _dk_auto_ingest_tick():
    if os.getenv("DK_AUTO_INGEST_ENABLED", "false").lower() != "true":
        logger.info("dk_auto_ingest skipped: DK_AUTO_INGEST_ENABLED!=true")
        return
    try:
        result = await dk_auto_svc.run_daily_ingest(sport="tennis")
        logger.info("dk_auto_ingest result: %s", result)
    except Exception as e:
        logger.exception("dk_auto_ingest failed: %s", e)


# ─────────────────────────────────────────────────────────────────────
# Boot warmup — sequential to avoid Supabase pool pressure
# ─────────────────────────────────────────────────────────────────────

async def _warmup():
    logger.info("watcher warmup: starting initial tick of each job")
    await _sgo_odds_tick()
    await _prizepicks_tick()
    await _kalshi_tick()
    logger.info("watcher warmup: complete")


# ─────────────────────────────────────────────────────────────────────
# Lifespan integration — exposed as both `scheduled_watcher`
# (what main.py imports) and `watcher_lifespan` (alias for back-compat).
# ─────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def scheduled_watcher(app=None):
    """Start the in-process scheduler. Safe to use as a FastAPI lifespan
    or as a standalone async context manager (the `app` argument is
    optional so callers can do `async with scheduled_watcher():`).
    """
    global _scheduler
    _scheduler = AsyncIOScheduler(timezone="UTC")

    _scheduler.add_job(
        _sgo_odds_tick,
        "interval",
        minutes=15,
        id="sgo_odds_tick",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        _prizepicks_tick,
        "interval",
        minutes=15,
        id="prizepicks_direct_tick",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        _kalshi_tick,
        "interval",
        minutes=10,
        id="kalshi_tick",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        _dk_auto_ingest_tick,
        CronTrigger(hour=11, minute=0, timezone="UTC"),
        id="dk_auto_ingest_daily",
        max_instances=1,
        coalesce=True,
    )

    _scheduler.start()
    logger.info(
        "watcher started: sgo_odds(15m), prizepicks_direct(15m), "
        "kalshi(10m), dk_auto_ingest(cron 11:00 UTC)"
    )

    asyncio.create_task(_warmup())

    try:
        yield
    finally:
        if _scheduler:
            _scheduler.shutdown(wait=False)
            _scheduler = None
            logger.info("watcher stopped")


# Back-compat alias (some routes / scripts import this name)
watcher_lifespan = scheduled_watcher


# ─────────────────────────────────────────────────────────────────────
# Manual one-shot — used by /api/slates/refresh
# ─────────────────────────────────────────────────────────────────────

async def run_slate_watcher_once() -> dict:
    out: dict = {}
    try:
        await sgo_svc.fetch_tick("TEN")
        out["sgo"] = "ok"
    except Exception as e:
        logger.exception("on-demand sgo failed: %s", e)
        out["sgo"] = f"error: {e!r}"
    try:
        await pp_direct_svc.fetch_tick("TEN")
        out["sgo_pp"] = "ok"
    except Exception as e:
        logger.exception("on-demand pp_direct failed: %s", e)
        out["sgo_pp"] = f"error: {e!r}"
    try:
        await kalshi_svc.fetch_tick()
        out["kalshi"] = "ok"
    except Exception as e:
        logger.exception("on-demand kalshi failed: %s", e)
        out["kalshi"] = f"error: {e!r}"
    return out
