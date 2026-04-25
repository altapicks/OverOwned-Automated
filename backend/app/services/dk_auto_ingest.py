"""DraftKings auto-ingest service — v6.2.

Daily orchestration shim that:
  1. Calls DK lobby for the configured sport
  2. Filters DraftGroups to (DraftGroupTag == "Featured" AND ContestTypeId == 106)
     → kills the empty 372 placeholder and the Short Slate
  3. If multiple Featured Classic groups exist, picks the one with the highest
     GameCount (ties broken by earliest StartDate)
  4. Fetches its draftables and pipes them to slate_builder.ingest_draft_group
     with slate_type='classic', is_fallback=False

Manual CSV upload via /api/admin/slates/upload remains the override layer
on top of this — sync_slate_contents preserves matches.odds.* sources on
re-upload, only replacing posted_lines.
"""

from __future__ import annotations

import logging
from typing import Optional

from app.config import get_settings
from app.services.dk_client import (
    CONTEST_TYPE_ID_MAP,
    DraftKingsClient,
    SPORT_CODE_MAP,
    _parse_dk_datetime,
)
from app.services.slate_builder import ingest_draft_group
from app.models import DKDraftGroup

logger = logging.getLogger(__name__)

# Filter rule — confirmed from live DK 2026 lobby on 2026-04-25:
#   DraftGroupTag == "Featured" AND ContestTypeId == 106
# 106 = Classic. 372 = Showdown/alt-format (also Featured, but NOT what we want).
FEATURED_TAG = "Featured"
CLASSIC_CONTEST_TYPE_ID = 106


def _is_featured_classic(dg: dict) -> bool:
    tag = (dg.get("DraftGroupTag") or "").strip()
    ctid = dg.get("ContestTypeId")
    return tag == FEATURED_TAG and ctid == CLASSIC_CONTEST_TYPE_ID


def _pick_best_featured(candidates: list[dict]) -> Optional[dict]:
    """If DK ever lists multiple Featured Classics for a day (rare — happens
    around Grand Slam transitions when both AM and PM main slates run),
    pick the one with the most games. Tie-break: earliest StartDate."""
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    def _key(g: dict):
        gc = int(g.get("GameCount") or 0)
        sd = g.get("StartDate") or ""
        # max GameCount, then min StartDate (so we negate sd by sorting ascending)
        return (-gc, sd)

    candidates_sorted = sorted(candidates, key=_key)
    pick = candidates_sorted[0]
    logger.info(
        "Multiple Featured Classics found (n=%d); picked dgid=%d games=%s start=%s",
        len(candidates),
        pick.get("DraftGroupId"),
        pick.get("GameCount"),
        pick.get("StartDate"),
    )
    return pick


def _to_draft_group_model(dg: dict, sport_code: str) -> DKDraftGroup:
    """Build a DKDraftGroup the same way dk_client.list_draft_groups does,
    so slate_builder gets a familiar object. We do this from the raw dict
    because we already filtered on raw fields and want to skip a redundant
    list_draft_groups call."""
    tag = (dg.get("DraftGroupTag") or "").strip()
    suffix = (dg.get("ContestStartTimeSuffix") or "").strip()
    slate_label = " ".join(s for s in (tag, suffix) if s) or None

    ctid = dg.get("ContestTypeId")
    contest_type = CONTEST_TYPE_ID_MAP.get(ctid, "Classic") if isinstance(ctid, int) else "Classic"

    return DKDraftGroup(
        draft_group_id=int(dg["DraftGroupId"]),
        sport=SPORT_CODE_MAP.get(sport_code, sport_code.lower()),
        contest_type=contest_type,
        slate_label=slate_label,
        lock_time=_parse_dk_datetime(dg.get("StartDate") or dg.get("StartDateEst")),
        salary_cap=int(dg.get("SalaryCap") or 50000),
    )


async def fetch_featured_slate(sport_code: str = "TEN") -> dict:
    """Find today's Featured Classic on DK and ingest it.

    Returns a summary dict suitable for ingestion_log / admin endpoint response.
    """
    sport_name = SPORT_CODE_MAP.get(sport_code, sport_code.lower())

    async with DraftKingsClient() as dk:
        raw_groups = await dk.list_draft_groups_raw(sport_code)
        if not raw_groups:
            return {
                "status": "no_draft_groups",
                "sport": sport_name,
                "candidates": 0,
            }

        featured = [g for g in raw_groups if _is_featured_classic(g)]
        logger.info(
            "DK auto-ingest: sport=%s total_groups=%d featured_classic=%d",
            sport_code,
            len(raw_groups),
            len(featured),
        )

        if not featured:
            return {
                "status": "no_featured_classic",
                "sport": sport_name,
                "total_groups": len(raw_groups),
                "tags_seen": sorted({(g.get("DraftGroupTag") or "(none)") for g in raw_groups}),
                "contest_type_ids_seen": sorted({g.get("ContestTypeId") for g in raw_groups if g.get("ContestTypeId") is not None}),
            }

        pick = _pick_best_featured(featured)
        if not pick:
            return {"status": "no_pick", "sport": sport_name}

        dg_model = _to_draft_group_model(pick, sport_code)
        logger.info(
            "DK auto-ingest: ingesting dgid=%d label=%r games=%s start=%s",
            dg_model.draft_group_id,
            dg_model.slate_label,
            pick.get("GameCount"),
            pick.get("StartDate"),
        )

        # Fetch draftables and hand off to the existing slate_builder.
        # ingest_draft_group is idempotent on dk_draft_group_id, so
        # repeat calls just refresh the slate.
        draftables, _competitions = await dk.get_draftables(dg_model.draft_group_id)
        if not draftables:
            return {
                "status": "empty_draftables",
                "sport": sport_name,
                "draft_group_id": dg_model.draft_group_id,
            }

        summary = await ingest_draft_group(
            dk_client=dk,
            draft_group=dg_model,
            sport=sport_name,
            pre_fetched_draftables=draftables,
            slate_type="classic",
            is_fallback=False,
        )

    return {
        "status": "ok",
        "sport": sport_name,
        "draft_group_id": dg_model.draft_group_id,
        "slate_label": dg_model.slate_label,
        "lock_time": dg_model.lock_time.isoformat() if dg_model.lock_time else None,
        "ingest": summary,
    }


async def run_dk_auto_ingest_tick() -> dict:
    """Scheduler entrypoint. Runs once per day, gated by config flag."""
    s = get_settings()
    if not s.dk_auto_ingest_enabled:
        return {"status": "disabled"}

    results: dict = {"sports": {}}
    for sport_code in s.sports_list:
        sport_name = SPORT_CODE_MAP.get(sport_code, sport_code.lower())
        if sport_name != "tennis":
            # Auto-ingest is tennis-only for now; other sports stay manual.
            continue
        try:
            r = await fetch_featured_slate(sport_code)
            results["sports"][sport_name] = r
        except Exception as e:
            logger.exception("DK auto-ingest failed for %s: %s", sport_code, e)
            results["sports"][sport_name] = {"status": "error", "error": str(e)}
    return results
