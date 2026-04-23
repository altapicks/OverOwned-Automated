"""Slate builder.

Runs per draft group:
  1. Fetch draftables from DK
  2. Normalize each player name against the players master table
  3. Group players into matches using DK's competition_id
  4. Upsert slate, slate_players, and matches rows to Supabase
  5. Emit Discord notifications for new slates and unmatched names

Idempotent: re-running on the same draft_group_id updates cleanly.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Optional

from app.db import get_client
from app.models import DKDraftable, DKDraftGroup
from app.services import notifier
from app.services.dk_client import DraftKingsClient
from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)


async def ingest_draft_group(
    dk_client: DraftKingsClient,
    draft_group: DKDraftGroup,
    sport: str,
    pre_fetched_draftables: list | None = None,
    slate_type: str = "classic",
    is_fallback: bool = False,
) -> dict:
    """Pull a draft group's draftables and upsert the whole slate.

    Returns a summary dict with counts suitable for ingestion_log.

    Args:
        pre_fetched_draftables: if provided, skip the DK fetch (the watcher
            already fetched them for classification). Saves a redundant call.
        slate_type: 'classic' or 'showdown' — set by the classifier.
        is_fallback: True when this is a Showdown ingested because no Classic
            was available for the day.
    """
    t0 = time.perf_counter()
    if pre_fetched_draftables is not None:
        draftables = pre_fetched_draftables
    else:
        draftables, _competitions = await dk_client.get_draftables(draft_group.draft_group_id)
    if not draftables:
        return {"status": "empty", "players": 0, "matches": 0}

    normalizer = PlayerNormalizer(sport=sport)
    db = get_client()

    # ── 1. Upsert slate ──────────────────────────────────────────────
    slate_date = _infer_slate_date(draftables)
    slate_row = {
        "sport": sport,
        "dk_draft_group_id": draft_group.draft_group_id,
        "slate_date": slate_date.isoformat(),
        "slate_label": draft_group.slate_label,
        "contest_type": slate_type,  # 'classic' | 'showdown' — from classifier
        "salary_cap": draft_group.salary_cap,
        "lock_time": draft_group.lock_time.isoformat() if draft_group.lock_time else None,
        "is_fallback": is_fallback,
    }
    existing = (
        db.table("slates")
        .select("id, first_seen_at")
        .eq("dk_draft_group_id", draft_group.draft_group_id)
        .execute()
    )
    is_new_slate = not existing.data
    if is_new_slate:
        result = db.table("slates").insert(slate_row).execute()
        slate_id = result.data[0]["id"]
    else:
        slate_id = existing.data[0]["id"]
        db.table("slates").update(
            {**slate_row, "last_synced_at": "now()"}
        ).eq("id", slate_id).execute()

    # ── 2. Resolve and upsert players ────────────────────────────────
    # For showdown (tennis can have it), the same player appears multiple
    # times with different roster_positions. Collapse to one slate_player
    # row per (player, roster_position) but track all DK IDs + salaries.
    canonical_map: dict[int, tuple[str, DKDraftable]] = {}  # dk_player_id → (canonical_id, draftable)
    unmatched_pings: list[tuple[str, str, float]] = []  # (raw_name, best_guess, score)

    for d in draftables:
        match_ctx = {
            "dk_player_id": d.dk_player_id,
            "salary": d.salary,
            "competition": d.competition_name,
            "draft_group_id": draft_group.draft_group_id,
        }
        res = normalizer.resolve(d.display_name, source="dk", context=match_ctx)
        if not res.canonical_id:
            # Couldn't create or match — skip this draftable but note it
            unmatched_pings.append((d.display_name, "", 0))
            continue
        canonical_map[d.dk_player_id] = (res.canonical_id, d)
        if not res.auto_resolved:
            unmatched_pings.append(
                (d.display_name, res.display_name, res.score)
            )

    # ── 3. Build matches from competition_id grouping ────────────────
    comp_to_players: dict[int, list[tuple[str, DKDraftable]]] = {}
    for _dkid, (cid, d) in canonical_map.items():
        if d.competition_id is None:
            continue
        comp_to_players.setdefault(d.competition_id, []).append((cid, d))

    # Dedupe: in showdown each player shows up 2-3 times per comp. One entry per canonical_id.
    match_records: list[dict] = []
    canonical_id_to_match_id: dict[str, str] = {}

    for comp_id, entries in comp_to_players.items():
        unique_by_cid: dict[str, DKDraftable] = {}
        for cid, d in entries:
            # keep the classic-position version if present, else whichever
            if cid not in unique_by_cid or unique_by_cid[cid].roster_position == "CPT":
                unique_by_cid[cid] = d
        player_ids = list(unique_by_cid.keys())
        if len(player_ids) != 2:
            # Tennis matches are always 1v1; anything else is a data anomaly we skip
            logger.warning(
                "Skipping competition %s with %d players (expected 2)",
                comp_id,
                len(player_ids),
            )
            continue
        a_cid, b_cid = player_ids
        a_d, b_d = unique_by_cid[a_cid], unique_by_cid[b_cid]

        match_row = {
            "slate_id": slate_id,
            "player_a_id": a_cid,
            "player_b_id": b_cid,
            "tournament": _extract_tournament(a_d.competition_name),
            "start_time": a_d.start_time.isoformat() if a_d.start_time else None,
            "dk_competition_id": comp_id,
        }
        # Upsert match on (slate_id, player_a_id, player_b_id)
        existing_match = (
            db.table("matches")
            .select("id")
            .eq("slate_id", slate_id)
            .eq("player_a_id", a_cid)
            .eq("player_b_id", b_cid)
            .execute()
        )
        if existing_match.data:
            match_id = existing_match.data[0]["id"]
            db.table("matches").update(match_row).eq("id", match_id).execute()
        else:
            match_id = db.table("matches").insert(match_row).execute().data[0]["id"]
        match_records.append({"id": match_id, **match_row})
        canonical_id_to_match_id[a_cid] = match_id
        canonical_id_to_match_id[b_cid] = match_id

    # ── 4. Upsert slate_players ──────────────────────────────────────
    slate_player_rows = []
    for _dkid, (cid, d) in canonical_map.items():
        slate_player_rows.append(
            {
                "slate_id": slate_id,
                "player_id": cid,
                "dk_player_id": d.dk_player_id,
                "dk_display_name": d.display_name,
                "salary": d.salary,
                "avg_ppg": d.avg_ppg,
                "roster_position": d.roster_position or "P",
                "match_id": canonical_id_to_match_id.get(cid),
            }
        )
    if slate_player_rows:
        db.table("slate_players").upsert(
            slate_player_rows, on_conflict="slate_id,player_id,roster_position"
        ).execute()

    # ── 5. Notifications ─────────────────────────────────────────────
    if is_new_slate:
        await notifier.notify_new_slate(
            sport=sport,
            slate_date=slate_date.isoformat(),
            slate_label=draft_group.slate_label,
            draft_group_id=draft_group.draft_group_id,
            player_count=len(canonical_map),
            match_count=len(match_records),
            lock_time=draft_group.lock_time.isoformat() if draft_group.lock_time else None,
            slate_type=slate_type,
            is_fallback=is_fallback,
        )

    for raw_name, best_guess, score in unmatched_pings[:5]:  # cap to avoid spam
        await notifier.notify_unmatched(sport, "dk", raw_name, best_guess, score)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    summary = {
        "status": "ok",
        "is_new_slate": is_new_slate,
        "slate_id": slate_id,
        "draft_group_id": draft_group.draft_group_id,
        "players": len(canonical_map),
        "matches": len(match_records),
        "unmatched": len(unmatched_pings),
        "duration_ms": elapsed_ms,
    }
    db.table("ingestion_log").insert(
        {
            "source": "dk_draftables",
            "sport": sport,
            "status": "ok",
            "items_processed": len(canonical_map),
            "duration_ms": elapsed_ms,
            "context": summary,
        }
    ).execute()
    return summary


def _infer_slate_date(draftables: list[DKDraftable]) -> date:
    """Use the earliest competition start_time as the slate date."""
    times = [d.start_time for d in draftables if d.start_time]
    if not times:
        return date.today()
    return min(times).date()


def _extract_tournament(competition_name: Optional[str]) -> Optional[str]:
    """DK's competition names are just 'Player A vs Player B'. Tournament
    has to come from elsewhere eventually — for now we leave it null and
    let the user / a future tournament-lookup service fill it in."""
    return None
