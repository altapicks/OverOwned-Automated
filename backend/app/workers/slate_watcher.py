"""Slate watcher job — v6.1 (SGO).

Runs on a schedule (default 15min). Responsibilities:

1. Tick SGO odds for tennis (Pinnacle ML, total sets, set spread,
   per-player game O/U → matches.odds.sgo + promoted flat keys)
2. Tick SGO PrizePicks props for tennis (Fantasy Score, Aces, Break Points
   → prizepicks_lines via deactivate-then-insert)
3. Tick Kalshi for tennis (live match-winner probabilities → matches.odds.kalshi
   + kalshi_prob_a/_b — UNCHANGED, the source of truth for win %)
4. Snapshot closing_odds for any slate past lock_time

Manual CSV upload at POST /api/admin/slates/upload remains the slate-creation
path AND the last-resort override layer — sync_slate_contents preserves all
matches.odds.* market sources on re-upload, only replacing posted_lines.
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
from app.services import kalshi, sgo_odds, sgo_prizepicks
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

        # SGO sportsbook odds (Pinnacle ML, total sets, set spread, per-player games)
        try:
            sgo_summary = await sgo_odds.fetch_tick(sport_code)
            logger.info("SGO odds tick: %s", sgo_summary)
            results["sports"].setdefault(sport_name, {})["sgo_odds"] = sgo_summary
        except Exception as e:
            logger.exception("SGO odds tick failed: %s", e)
            results["sports"].setdefault(sport_name, {})["sgo_odds_error"] = str(e)

        # SGO PrizePicks props (Fantasy Score, Aces, Break Points)
        try:
            pp_summary = await sgo_prizepicks.fetch_tick(sport_code)
            logger.info("SGO PrizePicks tick: %s", pp_summary)
            results["sports"].setdefault(sport_name, {})["sgo_prizepicks"] = pp_summary
        except Exception as e:
            logger.exception("SGO PrizePicks tick failed: %s", e)
            results["sports"].setdefault(sport_name, {})["sgo_prizepicks_error"] = str(e)

        # Kalshi win-% (UNCHANGED — source of truth for win probability)
        try:
            kalshi_summary = await kalshi.fetch_tick()
            logger.info("Kalshi tick: %s", kalshi_summary)
            results["sports"].setdefault(sport_name, {})["kalshi"] = kalshi_summary
        except Exception as e:
            logger.exception("Kalshi tick failed: %s", e)
            results["sports"].setdefault(sport_name, {})["kalshi_error"] = str(e)

    # ── Closing odds snapshot ──────────────────────────────────────────
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
    slate has passed lock_time and whose closing_odds is still empty."""
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
        "Slate watcher started (interval=%d min, sports=%s) — v6.1 (SGO)",
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
