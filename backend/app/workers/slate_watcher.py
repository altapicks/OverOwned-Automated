"""Slate watcher job.

Runs on a schedule (default 15min). For each configured sport:
  1. Hit DK lobby for draft groups
  2. Fetch draftables for each (needed for classification)
  3. Classify each slate: Classic | Showdown | Other
  4. Decide which to ingest per classifier rules + env config
  5. Ingest via slate_builder, skip the rest (log to skipped_draft_groups)
  6. Notify Discord with appropriate flavor

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
from app.services.slate_classifier import (
    SlateType,
    classify_slate,
    pick_slates_to_ingest,
)

logger = logging.getLogger(__name__)


async def run_slate_watcher_once() -> dict:
    """One polling cycle. Safe to call from a cron, a scheduler, or manually."""
    s = get_settings()
    allowed_types = {
        SlateType(t) for t in s.slate_types_list if t in {"classic", "showdown", "other"}
    }
    results = {"sports": {}}

    async with DraftKingsClient() as dk:
        for sport_code in s.sports_list:
            sport_name = SPORT_CODE_MAP.get(sport_code, sport_code.lower())
            try:
                groups = await dk.list_draft_groups(sport_code)
            except Exception as e:
                logger.exception("Failed to list draft groups for %s", sport_code)
                await notifier.notify_error(
                    "slate_watcher",
                    f"Lobby pull failed for {sport_code}: {e}",
                    {"sport": sport_name},
                )
                _log_error("dk_lobby", sport_name, str(e), {"sport_code": sport_code})
                continue

            # Fetch draftables and classify each group
            classified = []
            for dg in groups:
                try:
                    draftables, _ = await dk.get_draftables(dg.draft_group_id)
                except Exception as e:
                    logger.exception(
                        "Failed to fetch draftables for %s", dg.draft_group_id
                    )
                    _log_error(
                        "dk_draftables",
                        sport_name,
                        str(e),
                        {"dgid": dg.draft_group_id},
                    )
                    continue
                slate_type = classify_slate(dg, draftables)
                classified.append((dg, draftables, slate_type))
                logger.info(
                    "Classified dgid=%d contest_type=%r label=%r → %s",
                    dg.draft_group_id,
                    dg.contest_type,
                    dg.slate_label,
                    slate_type.value,
                )

            # Decide which to ingest
            to_ingest = pick_slates_to_ingest(
                classified,
                allowed_types=allowed_types,
                fallback_to_showdown=s.dk_fallback_to_showdown,
            )
            ingested_dgids = {t[0].draft_group_id for t in to_ingest}

            # Log the skipped ones
            for dg, _draftables, slate_type in classified:
                if dg.draft_group_id in ingested_dgids:
                    continue
                reason = (
                    "classified_other"
                    if slate_type == SlateType.OTHER
                    else "showdown_without_fallback"
                    if slate_type == SlateType.SHOWDOWN
                    else "disallowed_type"
                )
                _log_skipped(dg, sport_name, slate_type.value, reason)

            # Ingest
            sport_summary = {
                "draft_groups_found": len(groups),
                "classified": [
                    {"dgid": dg.draft_group_id, "type": t.value}
                    for dg, _, t in classified
                ],
                "ingested": [],
                "skipped": len(classified) - len(to_ingest),
            }
            for dg, draftables, slate_type, is_fallback in to_ingest:
                try:
                    summary = await ingest_draft_group(
                        dk,
                        dg,
                        sport_name,
                        pre_fetched_draftables=draftables,
                        slate_type=slate_type.value,
                        is_fallback=is_fallback,
                    )
                    sport_summary["ingested"].append(summary)
                except Exception as e:
                    logger.exception(
                        "Failed to ingest draft group %s", dg.draft_group_id
                    )
                    await notifier.notify_error(
                        "slate_watcher",
                        f"Ingest failed dgid={dg.draft_group_id}: {e}",
                        {"sport": sport_name, "dgid": dg.draft_group_id},
                    )
                    _log_error(
                        "dk_ingest",
                        sport_name,
                        str(e),
                        {"dgid": dg.draft_group_id},
                    )

            results["sports"][sport_name] = sport_summary

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


def _log_skipped(dg, sport: str, classification: str, reason: str) -> None:
    try:
        get_client().table("skipped_draft_groups").upsert(
            {
                "dk_draft_group_id": dg.draft_group_id,
                "sport": sport,
                "contest_type": dg.contest_type,
                "slate_label": dg.slate_label,
                "classification": classification,
                "reason": reason,
                "context": {
                    "lock_time": dg.lock_time.isoformat() if dg.lock_time else None,
                    "salary_cap": dg.salary_cap,
                },
            },
            on_conflict="dk_draft_group_id",
        ).execute()
    except Exception as e:
        logger.error("Failed to write skipped_draft_groups row: %s", e)


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
        "Slate watcher started (interval=%d min, sports=%s, types=%s, fallback=%s)",
        s.dk_poll_interval_minutes,
        s.sports_list,
        s.slate_types_list,
        s.dk_fallback_to_showdown,
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
