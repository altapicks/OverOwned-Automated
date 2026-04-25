"""Manual slate ingestion service — v6.0b.

Replaces the DK-scraping ingest. The operator (admin) uploads a single CSV
per slate that contains everything the app needs:
  - Player identity (name, opponent for match pairing)
  - DK player ID + salary (for lineup builder + DK upload CSV export)
  - sim_own (per-player projected field ownership, 0-100)
  - start_time per match (per-row; fallback to slate lock_time)
  - Stat-prop lines: aces, dfs, breaks, gw, gl, sw, sl (Underdog-style)
  - PrizePicks Fantasy Score line per player

v6.0b: sync_slate_contents is non-destructive to live market data.
Re-uploads preserve matches.odds.kalshi, matches.odds.the_odds_api, and
matches.closing_odds. Only columns owned by the CSV (player IDs, tournament,
surface, best_of, start_time, status, posted_lines) are touched. Orphaned
matches/slate_players (present before but missing from the new CSV) are
deleted, and the counts are surfaced in the result so the operator notices
if they uploaded the wrong file.

Schema notes (verified against current DB):
  - slate_players has NO surrogate `id` column. Primary key is composite
    (slate_id, player_id, roster_position). Updates and deletes use the
    composite filter, not a single id.
  - matches DOES have an `id` column. Updates/deletes use it.

Kalshi continues to attach live odds on its 15-min cycle. Odds API
continues as a fallback for moneylines and games-won lines.

The route handler in app/routes/admin_slate.py wraps this with HTTP + auth
+ multipart parsing.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from typing import Any, Optional

from app.db import get_client

logger = logging.getLogger(__name__)


# CSV columns. Required + optional. Unknown columns are ignored.
REQUIRED_COLS = {"name", "opponent", "dk_id", "salary"}
OPTIONAL_COLS = {
    "sim_own",
    "start_time",
    "aces_line",
    "dfs_line",
    "breaks_line",
    "gw_line",
    "gl_line",
    "sw_line",
    "sl_line",
    "pp_fs_line",
}

# Map CSV column → posted_lines key (Underdog stat-prop schema).
# These get nested under matches.odds.posted_lines.{a|b}.{key}
STAT_LINE_MAP = {
    "aces_line": "aces",
    "dfs_line": "dfs",
    "breaks_line": "breaks",
    "gw_line": "games_won",
    "gl_line": "games_lost",
    "sw_line": "sets_won",
    "sl_line": "sets_lost",
}


# ─────────────────────────────────────────────────────────────────────
# CSV parsing + validation
# ─────────────────────────────────────────────────────────────────────


def parse_csv(text: str) -> tuple[list[dict], list[str]]:
    """Parse CSV text into a list of normalized row dicts. Returns (rows, errors)."""
    errors: list[str] = []
    rows: list[dict] = []

    # Strip BOM if present (Excel default for "Save as CSV UTF-8")
    if text.startswith("\ufeff"):
        text = text[1:]

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], ["CSV has no header row"]

    headers = {h.strip().lower() for h in reader.fieldnames if h}
    missing = REQUIRED_COLS - headers
    if missing:
        errors.append(f"Missing required columns: {', '.join(sorted(missing))}")
        return [], errors

    for i, raw in enumerate(reader, start=2):  # +2 because header is row 1
        if not any((v or "").strip() for v in raw.values()):
            continue
        normalized = {(k or "").strip().lower(): (v or "").strip() for k, v in raw.items()}
        try:
            rows.append(_normalize_row(normalized, i))
        except ValueError as e:
            errors.append(f"Row {i}: {e}")
    return rows, errors


def _normalize_row(raw: dict, row_num: int) -> dict:
    """Coerce types + validate one row. Raises ValueError on invalid input."""
    name = raw.get("name", "")
    if not name:
        raise ValueError("name is empty")
    opponent = raw.get("opponent", "")
    if not opponent:
        raise ValueError(f"opponent is empty for {name}")

    try:
        dk_id = int(raw["dk_id"])
    except (KeyError, ValueError):
        raise ValueError(f"dk_id must be an integer for {name}")

    try:
        salary = int(raw["salary"])
    except (KeyError, ValueError):
        raise ValueError(f"salary must be an integer for {name}")

    out: dict[str, Any] = {
        "name": name,
        "opponent": opponent,
        "dk_id": dk_id,
        "salary": salary,
    }

    # sim_own: 0-100, default 0 (no Monte Carlo fallback per spec)
    sim_raw = raw.get("sim_own", "")
    if sim_raw == "":
        out["sim_own"] = 0.0
    else:
        try:
            sim_val = float(sim_raw)
        except ValueError:
            raise ValueError(f"sim_own must be a number for {name}")
        if not 0 <= sim_val <= 100:
            raise ValueError(f"sim_own must be 0-100 for {name}, got {sim_val}")
        out["sim_own"] = sim_val

    out["start_time"] = raw.get("start_time", "") or None

    for csv_key in STAT_LINE_MAP:
        raw_val = raw.get(csv_key, "")
        if raw_val == "":
            out[csv_key] = None
        else:
            try:
                out[csv_key] = float(raw_val)
            except ValueError:
                raise ValueError(f"{csv_key} must be a number for {name}, got '{raw_val}'")

    pp_fs_raw = raw.get("pp_fs_line", "")
    if pp_fs_raw == "":
        out["pp_fs_line"] = None
    else:
        try:
            out["pp_fs_line"] = float(pp_fs_raw)
        except ValueError:
            raise ValueError(f"pp_fs_line must be a number for {name}, got '{pp_fs_raw}'")

    return out


# ─────────────────────────────────────────────────────────────────────
# Name resolution — match CSV names against players.canonical_id
# ─────────────────────────────────────────────────────────────────────


def resolve_names(rows: list[dict]) -> tuple[dict[str, str], list[str]]:
    """For each unique name in rows + opponents, find the canonical_id."""
    db = get_client()
    all_names = set()
    for r in rows:
        all_names.add(r["name"])
        all_names.add(r["opponent"])
    if not all_names:
        return {}, []

    candidates = (
        db.table("players")
        .select("canonical_id, display_name, aliases")
        .execute()
        .data
        or []
    )

    def _norm(s: str) -> str:
        return _strip_accents(s).lower().strip()

    lookup: dict[str, str] = {}
    for c in candidates:
        lookup[_norm(c["display_name"])] = c["canonical_id"]
        # v6.0c: aliases is stored as jsonb. Existing data uses an OBJECT
        # keyed by source (e.g. {"dk": "Tommy Paul"}), so iterating over the
        # raw value yields KEYS ("dk") not VALUES — which made the alias
        # lookup dead code. Handle both shapes for forward/backward compat.
        aliases_raw = c.get("aliases") or {}
        if isinstance(aliases_raw, dict):
            alias_values = aliases_raw.values()
        elif isinstance(aliases_raw, list):
            alias_values = aliases_raw
        else:
            alias_values = []
        for alias in alias_values:
            if isinstance(alias, str) and alias:
                lookup[_norm(alias)] = c["canonical_id"]

    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    for name in all_names:
        cid = lookup.get(_norm(name))
        if cid:
            resolved[name] = cid
        else:
            unresolved.append(name)
    return resolved, sorted(set(unresolved))


def _strip_accents(s: str) -> str:
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


# ─────────────────────────────────────────────────────────────────────
# Match pairing
# ─────────────────────────────────────────────────────────────────────


def pair_matches(
    rows: list[dict], resolved: dict[str, str]
) -> tuple[list[dict], list[str]]:
    """Pair player rows into matches via the opponent column."""
    warnings: list[str] = []
    by_name = {r["name"]: r for r in rows}

    seen_pairs: set[frozenset[str]] = set()
    matches: list[dict] = []
    for r in rows:
        a_name = r["name"]
        b_name = r["opponent"]
        if a_name == b_name:
            warnings.append(f"{a_name} listed as their own opponent")
            continue
        if a_name not in resolved or b_name not in resolved:
            continue
        if b_name not in by_name:
            warnings.append(f"{a_name} → opponent '{b_name}' has no row in CSV")
            continue
        if by_name[b_name]["opponent"] != a_name:
            warnings.append(
                f"Asymmetric: {a_name}→{b_name} but {b_name}→"
                f"{by_name[b_name]['opponent']}"
            )
            continue

        pair_key = frozenset([resolved[a_name], resolved[b_name]])
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        a_cid = resolved[a_name]
        b_cid = resolved[b_name]
        if a_cid > b_cid:
            a_name, b_name = b_name, a_name
            a_cid, b_cid = b_cid, a_cid

        matches.append(
            {
                "player_a_id": a_cid,
                "player_b_id": b_cid,
                "player_a_row": by_name[a_name],
                "player_b_row": by_name[b_name],
            }
        )
    return matches, warnings


# ─────────────────────────────────────────────────────────────────────
# DB write — slate upsert
# ─────────────────────────────────────────────────────────────────────


def _synthetic_draft_group_id() -> int:
    """Manual slates need a unique value for slates.dk_draft_group_id. We use
    a negative epoch-ms timestamp — guaranteed unique, distinguishable from
    real DK IDs (which are positive)."""
    return -int(time.time() * 1000)


def upsert_slate(
    *,
    sport: str,
    slate_date: str,
    tournament: str,
    surface: Optional[str],
    lock_time: Optional[str],
    slate_label: Optional[str] = None,
) -> str:
    """Find-or-create the slate row for (sport, slate_date). Returns slate_id.

    Explicit-allowlist update: only touches slate_label and lock_time.
    Preserves first_seen_at, id, dk_draft_group_id, and any future columns.
    """
    db = get_client()
    existing = (
        db.table("slates")
        .select("id")
        .eq("sport", sport)
        .eq("slate_date", slate_date)
        .eq("status", "active")
        .order("first_seen_at", desc=True)
        .limit(1)
        .execute()
        .data
    )

    if existing:
        slate_id = existing[0]["id"]
        update_payload = {
            "slate_label": slate_label or tournament,
            "lock_time": lock_time,
        }
        db.table("slates").update(update_payload).eq("id", slate_id).execute()
        logger.info("Updated existing slate %s for %s %s", slate_id, sport, slate_date)
        return slate_id

    insert_payload = {
        "sport": sport,
        "slate_date": slate_date,
        "slate_label": slate_label or tournament,
        "contest_type": "classic",
        "salary_cap": 50000,
        "lock_time": lock_time,
        "status": "active",
        "dk_draft_group_id": _synthetic_draft_group_id(),
    }
    inserted = db.table("slates").insert(insert_payload).execute().data
    slate_id = inserted[0]["id"]
    logger.info("Created new manual slate %s for %s %s", slate_id, sport, slate_date)
    return slate_id


# ─────────────────────────────────────────────────────────────────────
# DB write — sync_slate_contents (Option A: read-merge-write)
# ─────────────────────────────────────────────────────────────────────


def _merge_odds_preserving_market_data(
    existing_odds: Optional[dict], new_posted_lines: dict
) -> dict:
    """Shallow merge: replace posted_lines wholesale, preserve all other keys.

    existing_odds may contain kalshi, the_odds_api, moneyline, etc.
    new_posted_lines is what this CSV upload produced.

    Semantics:
      - New CSV produced posted_lines → replace block wholesale
      - New CSV produced no posted_lines BUT old odds had one → drop the block
        (CSV is authoritative for whether posted_lines exist)
      - All other top-level keys → untouched
    """
    merged = dict(existing_odds or {})
    if new_posted_lines:
        merged["posted_lines"] = new_posted_lines
    elif "posted_lines" in merged:
        del merged["posted_lines"]
    return merged


def _build_match_payload_from_pair(
    m: dict, tournament: str, surface: Optional[str]
) -> tuple[dict, dict]:
    """Build the CSV-owned columns for a match + its posted_lines block."""
    a_row = m["player_a_row"]
    b_row = m["player_b_row"]
    start_time = a_row.get("start_time") or b_row.get("start_time")

    posted_a: dict[str, float] = {}
    posted_b: dict[str, float] = {}
    for csv_key, posted_key in STAT_LINE_MAP.items():
        if a_row.get(csv_key) is not None:
            posted_a[posted_key] = a_row[csv_key]
        if b_row.get(csv_key) is not None:
            posted_b[posted_key] = b_row[csv_key]
    posted_lines: dict = {}
    if posted_a:
        posted_lines["a"] = posted_a
    if posted_b:
        posted_lines["b"] = posted_b

    csv_cols = {
        "player_a_id": m["player_a_id"],
        "player_b_id": m["player_b_id"],
        "tournament": tournament,
        "surface": surface,
        "best_of": 3,
        "start_time": start_time,
        "status": "scheduled",
    }
    return csv_cols, posted_lines


def sync_slate_contents(
    *,
    slate_id: str,
    matches: list[dict],
    tournament: str,
    surface: Optional[str],
    lock_time: Optional[str],
) -> dict:
    """Non-destructively sync matches + slate_players for this slate.

    Read-merge-write — preserves live market data on re-upload:
      - matches.odds.kalshi/the_odds_api: UNTOUCHED
      - matches.odds.posted_lines:        REPLACED WHOLESALE from CSV
      - matches.closing_odds/opening_odds: UNTOUCHED (separate columns)
      - CSV-owned cols on matches:        refreshed from CSV

    slate_players composite key: (slate_id, player_id, roster_position).
    Tennis classic = 'P'. Future showdown work would key by all three.

    Orphans (in DB but not in new CSV) are DELETED. Counts surfaced in
    the result so the operator notices if the wrong CSV was uploaded.
    """
    db = get_client()

    # ── 1. Read existing matches + their odds (for the merge) ───────
    existing_matches = (
        db.table("matches")
        .select("id, player_a_id, player_b_id, odds")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )
    existing_by_pair: dict[frozenset, dict] = {
        frozenset([m["player_a_id"], m["player_b_id"]]): m
        for m in existing_matches
    }

    # ── 2. Partition new matches into UPDATEs vs INSERTs ────────────
    new_pairs: set[frozenset] = set()
    to_insert: list[dict] = []
    updates_done = 0

    for m in matches:
        pair_key = frozenset([m["player_a_id"], m["player_b_id"]])
        new_pairs.add(pair_key)
        csv_cols, posted_lines = _build_match_payload_from_pair(m, tournament, surface)

        existing = existing_by_pair.get(pair_key)
        if existing:
            merged_odds = _merge_odds_preserving_market_data(
                existing.get("odds"), posted_lines
            )
            update_payload = {**csv_cols, "odds": merged_odds}
            db.table("matches").update(update_payload).eq("id", existing["id"]).execute()
            updates_done += 1
        else:
            insert_payload = {
                "slate_id": slate_id,
                **csv_cols,
                "odds": {"posted_lines": posted_lines} if posted_lines else {},
            }
            to_insert.append(insert_payload)

    if to_insert:
        db.table("matches").insert(to_insert).execute()

    # ── 3. Delete orphaned matches ──────────────────────────────────
    orphan_match_ids = [
        existing_by_pair[pk]["id"]
        for pk in existing_by_pair
        if pk not in new_pairs
    ]
    matches_orphaned = len(orphan_match_ids)
    if orphan_match_ids:
        logger.warning(
            "Orphan matches on slate %s: %d match(es) present in DB but "
            "missing from new CSV — deleting. If unexpected, verify the "
            "correct CSV was uploaded.",
            slate_id, matches_orphaned,
        )
        # Clear FK refs from slate_players first (match_id references match)
        db.table("slate_players").update({"match_id": None}).in_(
            "match_id", orphan_match_ids
        ).execute()
        db.table("matches").delete().in_("id", orphan_match_ids).execute()

    # ── 4. Re-fetch matches for fresh IDs (slate_players need match_id) ──
    fresh_matches = (
        db.table("matches")
        .select("id, player_a_id, player_b_id")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )
    match_id_by_pair: dict[frozenset, str] = {
        frozenset([m["player_a_id"], m["player_b_id"]]): m["id"]
        for m in fresh_matches
    }

    # ── 5. Read existing slate_players (composite-PK aware) ─────────
    existing_sp = (
        db.table("slate_players")
        .select("player_id, roster_position")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )
    existing_sp_player_ids: set[str] = {sp["player_id"] for sp in existing_sp}

    # ── 6. Sync slate_players: UPDATE existing, INSERT new ──────────
    new_player_ids: set[str] = set()
    sp_to_insert: list[dict] = []
    sp_updates_done = 0

    for m in matches:
        match_id = match_id_by_pair.get(
            frozenset([m["player_a_id"], m["player_b_id"]])
        )
        for side_row, cid in [
            (m["player_a_row"], m["player_a_id"]),
            (m["player_b_row"], m["player_b_id"]),
        ]:
            new_player_ids.add(cid)
            payload_cols = {
                "dk_player_id": side_row["dk_id"],
                "dk_player_id_override": side_row["dk_id"],
                "dk_display_name": side_row["name"],
                "salary": side_row["salary"],
                "avg_ppg": 0,
                "match_id": match_id,
                "ss_pool_own": side_row["sim_own"],
            }
            if cid in existing_sp_player_ids:
                # UPDATE via composite filter
                db.table("slate_players").update(payload_cols).eq(
                    "slate_id", slate_id
                ).eq("player_id", cid).eq("roster_position", "P").execute()
                sp_updates_done += 1
            else:
                # INSERT — include the PK columns
                sp_to_insert.append(
                    {
                        "slate_id": slate_id,
                        "player_id": cid,
                        "roster_position": "P",
                        **payload_cols,
                    }
                )

    if sp_to_insert:
        db.table("slate_players").insert(sp_to_insert).execute()

    # ── 7. Delete orphaned slate_players ────────────────────────────
    orphan_player_ids = [
        pid for pid in existing_sp_player_ids if pid not in new_player_ids
    ]
    sp_orphaned = len(orphan_player_ids)
    if orphan_player_ids:
        logger.warning(
            "Orphan slate_players on slate %s: %d player(s) missing from "
            "new CSV — deleting.",
            slate_id, sp_orphaned,
        )
        db.table("slate_players").delete().eq("slate_id", slate_id).in_(
            "player_id", orphan_player_ids
        ).execute()

    return {
        "matches_total": len(matches),
        "matches_updated": updates_done,
        "matches_inserted": len(to_insert),
        "matches_orphaned_and_deleted": matches_orphaned,
        "slate_players_total": len(new_player_ids),
        "slate_players_updated": sp_updates_done,
        "slate_players_inserted": len(sp_to_insert),
        "slate_players_orphaned_and_deleted": sp_orphaned,
    }


# ─────────────────────────────────────────────────────────────────────
# PrizePicks FS lines — deactivate-then-upsert pattern
# ─────────────────────────────────────────────────────────────────────


def upsert_pp_fs_lines(
    *, slate_id: str, rows: list[dict], resolved: dict[str, str]
) -> int:
    """Insert prizepicks_lines rows for any row with a non-null pp_fs_line.

    v6.0c: previously used .upsert(on_conflict=...) but the matching unique
    index is PARTIAL (WHERE is_active = true) and PostgREST rejects on_conflict
    for partial indexes. We deactivate prior active rows first (so no conflict
    can occur on the partial-active uniqueness), then plain-insert the new set.

    Inactive rows accumulate in the DB but are filtered out by every read path
    (the partial index keeps only the latest set queryable as 'active').

    v6.0e: explicit observability — supabase-py has been silently completing
    .execute() without writing rows. Instead of trusting len(pp_rows) as the
    written count, capture the actual response and verify the row count matches.
    Loud failure with full context if anything is off.
    """
    db = get_client()

    deactivate_resp = db.table("prizepicks_lines").update({"is_active": False}).eq(
        "slate_id", slate_id
    ).eq("stat_type", "Fantasy Score").execute()
    logger.info(
        "PP FS deactivate: slate=%s rows_affected=%s",
        slate_id,
        len(getattr(deactivate_resp, "data", []) or []),
    )

    pp_rows = []
    skipped_no_fs = 0
    skipped_unresolved = 0
    for r in rows:
        if r.get("pp_fs_line") is None:
            skipped_no_fs += 1
            continue
        cid = resolved.get(r["name"])
        if not cid:
            skipped_unresolved += 1
            logger.warning(
                "PP FS skipped row: name=%r had pp_fs_line but no resolved cid. "
                "resolved keys sample: %r",
                r.get("name"), list(resolved.keys())[:5],
            )
            continue
        pp_rows.append(
            {
                "slate_id": slate_id,
                "player_id": cid,
                "raw_player_name": r["name"],
                "stat_type": "Fantasy Score",
                "current_line": r["pp_fs_line"],
                "league": "tennis",
                "is_active": True,
            }
        )

    logger.info(
        "PP FS insert prep: slate=%s pp_rows_to_insert=%d skipped_no_fs=%d "
        "skipped_unresolved=%d total_input_rows=%d",
        slate_id, len(pp_rows), skipped_no_fs, skipped_unresolved, len(rows),
    )

    if not pp_rows:
        logger.warning(
            "PP FS insert skipped: pp_rows is empty (slate=%s, input rows=%d)",
            slate_id, len(rows),
        )
        return 0

    # Log a sample row for debugging shape issues
    logger.info("PP FS insert sample payload[0]: %r", pp_rows[0])

    try:
        insert_resp = db.table("prizepicks_lines").insert(pp_rows).execute()
    except Exception as exc:
        logger.exception(
            "PP FS insert raised exception: slate=%s pp_rows=%d err=%r",
            slate_id, len(pp_rows), exc,
        )
        raise

    written_data = getattr(insert_resp, "data", None) or []
    written_count = len(written_data)

    logger.info(
        "PP FS insert result: slate=%s tried=%d written=%d response_type=%s",
        slate_id, len(pp_rows), written_count, type(insert_resp).__name__,
    )

    if written_count != len(pp_rows):
        logger.error(
            "PP FS insert count mismatch! tried=%d written=%d. "
            "Full response: %r",
            len(pp_rows), written_count, insert_resp,
        )
        raise RuntimeError(
            f"PP FS lines insert mismatch: tried {len(pp_rows)} rows, "
            f"wrote {written_count}. Slate {slate_id}. "
            f"Check Railway logs for full response."
        )

    return written_count


# ─────────────────────────────────────────────────────────────────────
# Dry-run orphan preview (read-only)
# ─────────────────────────────────────────────────────────────────────


def _preview_orphans(
    *, sport: str, slate_date: str, new_matches: list[dict]
) -> dict:
    """Read-only peek: if an active slate already exists for (sport, slate_date),
    how many matches/slate_players would be orphaned by this upload?

    Always returns a dict — never None, never raises. Used by dry_run.
    """
    db = get_client()
    try:
        existing = (
            db.table("slates")
            .select("id")
            .eq("sport", sport)
            .eq("slate_date", slate_date)
            .eq("status", "active")
            .order("first_seen_at", desc=True)
            .limit(1)
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.warning("Orphan preview slate-lookup failed: %s", e)
        return {
            "orphan_preview_matches": 0,
            "orphan_preview_slate_players": 0,
            "orphan_preview_available": False,
        }

    if not existing:
        return {
            "orphan_preview_matches": 0,
            "orphan_preview_slate_players": 0,
            "orphan_preview_available": True,
        }

    slate_id = existing[0]["id"]
    new_pairs: set[frozenset] = {
        frozenset([m["player_a_id"], m["player_b_id"]]) for m in new_matches
    }
    new_player_ids: set[str] = (
        {m["player_a_id"] for m in new_matches}
        | {m["player_b_id"] for m in new_matches}
    )

    try:
        existing_matches = (
            db.table("matches")
            .select("player_a_id, player_b_id")
            .eq("slate_id", slate_id)
            .execute()
            .data
            or []
        )
        existing_sp = (
            db.table("slate_players")
            .select("player_id")
            .eq("slate_id", slate_id)
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.warning("Orphan preview content-lookup failed: %s", e)
        return {
            "orphan_preview_matches": 0,
            "orphan_preview_slate_players": 0,
            "orphan_preview_available": False,
        }

    existing_pairs = {
        frozenset([m["player_a_id"], m["player_b_id"]]) for m in existing_matches
    }
    existing_player_ids = {sp["player_id"] for sp in existing_sp}

    return {
        "orphan_preview_matches": len(existing_pairs - new_pairs),
        "orphan_preview_slate_players": len(existing_player_ids - new_player_ids),
        "orphan_preview_available": True,
    }


# ─────────────────────────────────────────────────────────────────────
# Top-level entry point
# ─────────────────────────────────────────────────────────────────────


def ingest_manual_slate(
    *,
    csv_text: str,
    sport: str,
    slate_date: str,
    tournament: str,
    surface: Optional[str],
    lock_time: Optional[str],
    dry_run: bool = False,
) -> dict:
    """End-to-end manual slate ingestion."""
    result: dict = {
        "ok": False,
        "warnings": [],
        "errors": [],
        "unmatched_names": [],
        "summary": {},
    }

    rows, parse_errs = parse_csv(csv_text)
    if parse_errs:
        result["errors"].extend(parse_errs)
        return result
    if not rows:
        result["errors"].append("CSV had no data rows")
        return result

    resolved, unresolved = resolve_names(rows)
    if unresolved:
        result["unmatched_names"] = unresolved
        result["warnings"].append(
            f"{len(unresolved)} unmatched name(s) — players must exist in the players table"
        )

    matches, pair_warnings = pair_matches(rows, resolved)
    result["warnings"].extend(pair_warnings)

    paired_players = {m["player_a_id"] for m in matches} | {
        m["player_b_id"] for m in matches
    }
    result["summary"] = {
        "rows_parsed": len(rows),
        "matches_paired": len(matches),
        "players_paired": len(paired_players),
        "unmatched_count": len(unresolved),
        "fs_lines": sum(1 for r in rows if r.get("pp_fs_line") is not None),
        "sim_own_total": round(sum(r["sim_own"] for r in rows), 1),
    }

    if unresolved:
        result["errors"].append(
            "Cannot publish: unmatched names must be resolved first. "
            "Add aliases or correct spelling and re-upload."
        )
        return result
    if not matches:
        result["errors"].append("No valid matches paired — cannot publish empty slate.")
        return result

    if dry_run:
        result["summary"].update(
            _preview_orphans(
                sport=sport, slate_date=slate_date, new_matches=matches
            )
        )
        result["ok"] = True
        return result

    slate_id = upsert_slate(
        sport=sport,
        slate_date=slate_date,
        tournament=tournament,
        surface=surface,
        lock_time=lock_time,
    )
    write_summary = sync_slate_contents(
        slate_id=slate_id,
        matches=matches,
        tournament=tournament,
        surface=surface,
        lock_time=lock_time,
    )
    pp_count = upsert_pp_fs_lines(slate_id=slate_id, rows=rows, resolved=resolved)

    result["ok"] = True
    result["slate_id"] = slate_id
    result["summary"].update(write_summary)
    result["summary"]["pp_fs_lines_written"] = pp_count
    return result
