"""Slate builder.

Runs per draft group:
1. Fetch draftables from DK
2. Normalize each player name against the players master table
3. Group players into matches using DK's competition_id
4. Read-then-write slate, slate_players, and matches rows to Supabase
   (avoids supabase-py .upsert() 21000 errors by mirroring the
   manual_slate_ingest.py pattern that's proven to work)
5. Emit Discord notifications for new slates and unmatched names

Idempotent: re-running on the same draft_group_id updates cleanly.

Schema notes (from manual_slate_ingest.py — verified against current DB):
- slate_players has NO surrogate `id` column. PK is composite
  (slate_id, player_id, roster_position). Updates/deletes use
  composite filter, not a single id.
- matches DOES have an `id` column. Updates/deletes use it.
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
    """Pull a draft group's draftables and write the whole slate.

    Returns a summary dict with counts suitable for ingestion_log.
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
        "contest_type": slate_type,
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

    # ── 2. Resolve player names → canonical_ids ──────────────────────
    canonical_map: dict[int, tuple[str, DKDraftable]] = {}
    unmatched_pings: list[tuple[str, str, float]] = []

    for d in draftables:
        match_ctx = {
            "dk_player_id": d.dk_player_id,
            "salary": d.salary,
            "competition": d.competition_name,
            "draft_group_id": draft_group.draft_group_id,
        }
        res = normalizer.resolve(d.display_name, source="dk", context=match_ctx)
        if not res.canonical_id:
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

    match_records: list[dict] = []
    canonical_id_to_match_id: dict[str, str] = {}
    for comp_id, entries in comp_to_players.items():
        unique_by_cid: dict[str, DKDraftable] = {}
        for cid, d in entries:
            if cid not in unique_by_cid or unique_by_cid[cid].roster_position == "CPT":
                unique_by_cid[cid] = d
        player_ids = list(unique_by_cid.keys())
        if len(player_ids) != 2:
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

    # ── 4. Slate_players: read-then-update-or-insert ─────────────────
    # MIRRORS manual_slate_ingest.sync_slate_contents to avoid the
    # supabase-py .upsert(on_conflict=...) 21000 bug. The bug surfaces
    # when slate_players has secondary unique indexes beyond the
    # composite PK; even a perfectly-deduped batch can collide on
    # those secondary keys. The read-then-write dance sidesteps it
    # entirely by issuing per-player UPDATE or INSERT statements.

    # 4a. Build per-canonical-id payload, deduped by composite PK
    # (slate_id, player_id, roster_position). Two DK ids collapsing
    # onto one canonical_id keep the higher-salary listing.
    deduped: dict[tuple, dict] = {}
    duplicates_dropped = 0
    for _dkid, (cid, d) in canonical_map.items():
        roster_pos = d.roster_position or "P"
        key = (slate_id, cid, roster_pos)
        row = {
            "slate_id": slate_id,
            "player_id": cid,
            "dk_player_id": d.dk_player_id,
            "dk_display_name": d.display_name,
            "salary": d.salary,
            "avg_ppg": d.avg_ppg,
            "roster_position": roster_pos,
            "match_id": canonical_id_to_match_id.get(cid),
        }
        if key in deduped:
            duplicates_dropped += 1
            existing_row = deduped[key]
            if (row.get("salary") or 0) > (existing_row.get("salary") or 0):
                logger.warning(
                    "slate_players dedupe: replacing dk_id=%s sal=%s with dk_id=%s sal=%s for player_id=%s rp=%s",
                    existing_row.get("dk_player_id"), existing_row.get("salary"),
                    row.get("dk_player_id"), row.get("salary"),
                    cid, roster_pos,
                )
                deduped[key] = row
            else:
                logger.warning(
                    "slate_players dedupe: dropping dk_id=%s sal=%s (kept dk_id=%s sal=%s) for player_id=%s rp=%s",
                    row.get("dk_player_id"), row.get("salary"),
                    existing_row.get("dk_player_id"), existing_row.get("salary"),
                    cid, roster_pos,
                )
        else:
            deduped[key] = row

    final_rows = list(deduped.values())

    # 4b. Read existing slate_players for this slate, keyed by
    # (player_id, roster_position) so we can decide UPDATE vs INSERT.
    existing_sp = (
        db.table("slate_players")
        .select("player_id, roster_position")
        .eq("slate_id", slate_id)
        .execute()
        .data or []
    )
    existing_keys: set[tuple[str, str]] = {
        (sp["player_id"], sp["roster_position"]) for sp in existing_sp
    }

    sp_inserts: list[dict] = []
    sp_updates_done = 0
    for row in final_rows:
        cid = row["player_id"]
        roster_pos = row["roster_position"]
        if (cid, roster_pos) in existing_keys:
            # UPDATE via composite filter (no surrogate id on slate_players)
            update_payload = {
                "dk_player_id": row["dk_player_id"],
                "dk_display_name": row["dk_display_name"],
                "salary": row["salary"],
                "avg_ppg": row["avg_ppg"],
                "match_id": row["match_id"],
            }
            db.table("slate_players").update(update_payload).eq(
                "slate_id", slate_id
            ).eq("player_id", cid).eq("roster_position", roster_pos).execute()
            sp_updates_done += 1
        else:
            sp_inserts.append(row)

    if sp_inserts:
        # Guard against a possible secondary unique index on
        # (slate_id, dk_player_id) by deduping the insert batch on
        # that key too. Already-deduped on PK; this is belt-and-braces.
        seen_dk: dict[int, dict] = {}
        for r in sp_inserts:
            dk_id = r.get("dk_player_id")
            if dk_id is None:
                seen_dk[id(r)] = r  # use object id as fallback unique
                continue
            if dk_id in seen_dk:
                duplicates_dropped += 1
                logger.warning(
                    "slate_players insert dedupe by dk_player_id=%s: dropping %s",
                    dk_id, r.get("dk_display_name"),
                )
                continue
            seen_dk[dk_id] = r
        sp_inserts = list(seen_dk.values())

        # Insert one-at-a-time to isolate any row that still violates
        # a constraint we don't know about. Slow path (~30 inserts)
        # but bulletproof against single-bad-row blocking the slate.
        sp_insert_errors: list[str] = []
        sp_inserted_count = 0
        for r in sp_inserts:
            try:
                db.table("slate_players").insert(r).execute()
                sp_inserted_count += 1
            except Exception as e:
                logger.exception(
                    "slate_players insert failed for player_id=%s dk_id=%s: %s",
                    r.get("player_id"), r.get("dk_player_id"), e,
                )
                sp_insert_errors.append(
                    f"{r.get('dk_display_name')} (dk_id={r.get('dk_player_id')}): {e}"
                )
    else:
        sp_inserted_count = 0
        sp_insert_errors = []

    # ── 5. Notifications ─────────────────────────────────────────────
    if is_new_slate:
        await notifier.notify_new_slate(
            sport=sport,
            slate_date=slate_date.isoformat(),
            slate_label=draft_group.slate_label,
            draft_group_id=draft_group.draft_group_id,
            player_count=sp_inserted_count + sp_updates_done,
            match_count=len(match_records),
            lock_time=draft_group.lock_time.isoformat() if draft_group.lock_time else None,
            slate_type=slate_type,
            is_fallback=is_fallback,
        )

    for raw_name, best_guess, score in unmatched_pings[:5]:
        await notifier.notify_unmatched(sport, "dk", raw_name, best_guess, score)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    summary = {
        "status": "ok",
        "is_new_slate": is_new_slate,
        "slate_id": slate_id,
        "draft_group_id": draft_group.draft_group_id,
        "players": sp_inserted_count + sp_updates_done,
        "players_inserted": sp_inserted_count,
        "players_updated": sp_updates_done,
        "duplicates_dropped": duplicates_dropped,
        "insert_errors": sp_insert_errors,
        "matches": len(match_records),
        "unmatched": len(unmatched_pings),
        "duration_ms": elapsed_ms,
    }
    db.table("ingestion_log").insert(
        {
            "source": "dk_draftables",
            "sport": sport,
            "status": "ok",
            "items_processed": sp_inserted_count + sp_updates_done,
            "duration_ms": elapsed_ms,
            "context": summary,
        }
    ).execute()
    return summary


def _infer_slate_date(draftables: list[DKDraftable]) -> date:
    times = [d.start_time for d in draftables if d.start_time]
    if not times:
        return date.today()
    return min(times).date()


def _extract_tournament(competition_name: Optional[str]) -> Optional[str]:
    return None
