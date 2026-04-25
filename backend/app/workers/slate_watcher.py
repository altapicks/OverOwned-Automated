"""Slate watcher job — v6.0.

Runs on a schedule (default 15min). Responsibilities (v6.0 simplified):
  1. Tick The Odds API for tennis (fills moneylines + games-won lines as
     fallback for slates that don't have those values from the manual upload)
  2. Tick Kalshi for tennis (live market probabilities → live Leverage Tracker)
  3. Snapshot closing_odds for any slate past lock_time

DK scraping, slate auto-creation, and slate classification have been
REMOVED in v6.0. Slates are now created exclusively via the manual upload
endpoint at POST /api/admin/slates/upload. This eliminates the UTC-rollover
ghost-slate problem and ambiguity around "which slate is today".

The watcher only ATTACHES odds to manually-created matches — it never
creates slates or matches itself.
"""
from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings
from app.db import get_client
from app.services import kalshi, odds_api
from app.services.dk_client import SPORT_CODE_MAP

logger = logging.getLogger(__name__)


async def run_slate_watcher_once() -> dict:
    """One polling cycle. Safe to call from a cron, a scheduler, or manually."""
    s = get_settings()
    results: dict = {"sports": {}}

    # ── Odds ingestion (per sport, gated to tennis with upcoming matches) ──
    for sport_code in s.sports_list:
        sport_name = SPORT_CODE_MAP.get(sport_code, sport_code.lower())
        if sport_name != "tennis":
            continue
        try:
            odds_summary = await odds_api.fetch_tick(sport_code)
            logger.info("The Odds API tick: %s", odds_summary)
            results["sports"].setdefault(sport_name, {})["odds_api"] = odds_summary
        except Exception as e:
            logger.exception("Odds API tick failed: %s", e)
            results["sports"].setdefault(sport_name, {})["odds_api_error"] = str(e)

        try:
            kalshi_summary = await kalshi.fetch_tick()
            logger.info("Kalshi tick: %s", kalshi_summary)
            results["sports"].setdefault(sport_name, {})["kalshi"] = kalshi_summary
        except Exception as e:
            logger.exception("Kalshi tick failed: %s", e)
            results["sports"].setdefault(sport_name, {})["kalshi_error"] = str(e)

    # ── Closing odds snapshot ──────────────────────────────────────────
    # For any slate whose lock_time has passed, copy match.odds → match.closing_odds
    # (one-time per match). Frontend reads closing_odds everywhere except the
    # Live Leverage Tracker, which keeps using live odds.
    try:
        snap_summary = await _snapshot_closing_odds()
        logger.info("closing_odds snapshot: %s", snap_summary)
        results["closing_odds_snapshot"] = snap_summary
    except Exception as e:
        logger.exception("closing_odds snapshot failed: %s", e)
        results["closing_odds_snapshot_error"] = str(e)

    return results


async def _snapshot_closing_odds() -> dict:
    """Snapshot matches.odds into matches.closing_odds for every match whose
    slate has passed lock_time and whose closing_odds is still empty.

    Idempotent: after the first successful run for a slate, closing_odds is
    non-empty so this is a no-op for those rows on subsequent cycles.
    """
    db = get_client()
    now_iso = _utc_now_iso()
    slates = (
        db.table("slates")
        .select("id, lock_time")
        .not_.is_("lock_time", "null")
        .lte("lock_time", now_iso)
        .execute()
        .data
        or []
    )
    if not slates:
        return {"snapshotted": 0, "slates_checked": 0}

    total_snapped = 0
    for slate in slates:
        matches = (
            db.table("matches")
            .select("id, odds, closing_odds")
            .eq("slate_id", slate["id"])
            .execute()
            .data
            or []
        )
        for m in matches:
            closing = m.get("closing_odds") or {}
            if closing and isinstance(closing, dict) and len(closing) > 0:
                continue
            odds = m.get("odds") or {}
            if not isinstance(odds, dict) or len(odds) == 0:
                continue
            db.table("matches").update({"closing_odds": odds}).eq("id", m["id"]).execute()
            total_snapped += 1
    return {"snapshotted": total_snapped, "slates_checked": len(slates)}


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


@asynccontextmanager
async def scheduled_watcher():
    """Long-running scheduler. Use this in the worker process entrypoint."""
    s = get_settings()
    scheduler = AsyncIOScheduler()
    trigger = IntervalTrigger(minutes=s.dk_poll_interval_minutes)

    async def _job():
        try:
            logger.info("Running slate watcher cycle")
            r = await run_slate_watcher_once()
            logger.info("Slate watcher done: %s", r)
        except Exception as e:
            logger.exception("Slate watcher cycle failed: %s", e)

    scheduler.add_job(_job, trigger=trigger, id="slate_watcher", max_instances=1, coalesce=True)
    scheduler.start()
    logger.info(
        "Slate watcher started (interval=%d min, sports=%s) — v6.0 (no DK scraping)",
        s.dk_poll_interval_minutes,
        s.sports_list,
    )
    await _job()
    try:
        yield scheduler
    finally:
        scheduler.shutdown(wait=False)


async def _main():
    logging.basicConfig(
        level=get_settings().log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    stop = asyncio.Event()

    def _handle_signal():
        logger.info("Received shutdown signal")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    async with scheduled_watcher():
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(_main())
