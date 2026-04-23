"""Slate watcher job.

Runs on a schedule (default 15min). For each configured sport:
  1. Hit DK lobby for draft groups
  2. For each draft group, call the slate builder
  3. Log results to ingestion_log

Run this as a separate process from the API. On Railway we have two
services: `api` (uvicorn) and `worker` (this script), both reading from
the same Supabase.
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
from app.services import notifier
from app.services.dk_client import DraftKingsClient, SPORT_CODE_MAP
from app.services.slate_builder import ingest_draft_group

logger = logging.getLogger(__name__)


async def run_slate_watcher_once() -> dict:
    """One polling cycle. Safe to call from a cron, a scheduler, or manually."""
    s = get_settings()
    results = {"sports": {}}
    async with DraftKingsClient() as dk:
        for sport_code in s.sports_list:
            sport_name = SPORT_CODE_MAP.get(sport_code, sport_code.lower())
            try:
                groups = await dk.list_draft_groups(sport_code)
                sport_summary = {"draft_groups": len(groups), "ingested": []}
                for dg in groups:
                    try:
                        summary = await ingest_draft_group(dk, dg, sport_name)
                        sport_summary["ingested"].append(summary)
                    except Exception as e:
                        logger.exception(
                            "Failed to ingest draft group %s", dg.draft_group_id
                        )
                        await notifier.notify_error(
                            "slate_watcher",
                            f"Draft group {dg.draft_group_id}: {e}",
                            {"sport": sport_name, "dgid": dg.draft_group_id},
                        )
                        _log_error("dk_draftables", sport_name, str(e), {"dgid": dg.draft_group_id})
                results["sports"][sport_name] = sport_summary
            except Exception as e:
                logger.exception("Failed to list draft groups for %s", sport_code)
                await notifier.notify_error(
                    "slate_watcher",
                    f"Lobby pull failed for {sport_code}: {e}",
                    {"sport": sport_name},
                )
                _log_error("dk_lobby", sport_name, str(e), {"sport_code": sport_code})
    return results


def _log_error(source: str, sport: str, msg: str, context: dict) -> None:
    try:
        get_client().table("ingestion_log").insert(
            {
                "source": source,
                "sport": sport,
                "status": "error",
                "error_message": msg[:500],
                "context": context,
            }
        ).execute()
    except Exception as e:
        logger.error("Failed to write ingestion_log error row: %s", e)


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
        "Slate watcher started (interval=%d min, sports=%s)",
        s.dk_poll_interval_minutes,
        s.sports_list,
    )
    # Run once immediately on startup
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
            pass  # Windows

    async with scheduled_watcher():
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(_main())
