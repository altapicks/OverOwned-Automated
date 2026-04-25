"""Manual slate ingestion service — v6.0.

Replaces the DK-scraping ingest. The operator (admin) uploads a single
CSV per slate that contains everything the app needs:

  - Player identity (name, opponent for match pairing)
  - DK player ID + salary (for lineup builder + DK upload CSV export)
  - sim_own (per-player projected field ownership, 0-100)
  - start_time per match (per-row; fallback to slate lock_time)
  - Stat-prop lines: aces, dfs, breaks, gw, gl, sw, sl (Underdog-style)
  - PrizePicks Fantasy Score line per player

Idempotent: re-uploading the same {sport, slate_date} updates the
existing slate IN PLACE — keeps the slate_id stable so attached
PrizePicks lines and contest_ownership rows survive corrections.

Kalshi continues to attach live odds to the manual-created matches on
its 15-min cycle. Odds API continues as a fallback for moneylines and
games-won lines if those columns are empty in the CSV.

Inputs: parsed CSV rows + slate header (tournament, surface, lock_time,
sport, slate_date).

Outputs: a result object with:
  - slate_id (created or updated)
  - counts of matches / players / lines processed
  - warnings: unmatched names, unpaired players, schema issues

The route handler in app/routes/admin_slate.py wraps this with HTTP +
auth + multipart parsing.
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
        # Skip blank lines (all values empty / whitespace)
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

    # sim_own: optional, 0-100, default 0 (per spec — no Monte Carlo fallback)
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

    # start_time: optional, ISO 8601 or "9:00 AM"-like (parsed leniently downstream)
    out["start_time"] = raw.get("start_time", "") or None

    # Stat prop lines: optional. Each is a number or empty.
    for csv_key in STAT_LINE_MAP:
        raw_val = raw.get(csv_key, "")
        if raw_val == "":
            out[csv_key] = None
        else:
            try:
                out[csv_key] = float(raw_val)
            except ValueError:
                raise ValueError(f"{csv_key} must be a number for {name}, got '{raw_val}'")

    # PrizePicks Fantasy Score line — separate table
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
    """For each unique name in rows + opponents, find the canonical_id.

    Returns (name → canonical_id map, list of unresolved names).
    """
    db = get_client()
    all_names = set()
    for r in rows:
        all_names.add(r["name"])
        all_names.add(r["opponent"])

    if not all_names:
        return {}, []

    # Pull all candidate players. For ~thousands of rows in a typical
    # season-spanning players table this is fine; if it grows we can
    # scope by sport.
    candidates = (
        db.table("players")
        .select("canonical_id, display_name, aliases")
        .execute()
        .data
        or []
    )

    # Build a normalized lookup: lowercase + accent-stripped → canonical_id.
    # Aliases get the same treatment so common alt-names work without
    # editing display_name.
    def _norm(s: str) -> str:
        return _strip_accents(s).lower().strip()

    lookup: dict[str, str] = {}
    for c in candidates:
        lookup[_norm(c["display_name"])] = c["canonical_id"]
        for alias in c.get("aliases") or []:
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
    """Best-effort accent stripping — handles common Latin diacritics
    without pulling in unicodedata's full Unicode normalization (which
    behaves identically here for our use case)."""
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
    """Pair player rows into matches via the opponent column.

    A valid match has both sides present in the CSV with reciprocal opponent
    references (Paul→Etcheverry AND Etcheverry→Paul). Asymmetric or missing
    pairings produce warnings; only valid pairs become matches.

    Returns (list of match dicts, list of warnings).
    """
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
            # Already reported in unresolved list; skip
            continue
        if b_name not in by_name:
            warnings.append(f"{a_name} → opponent '{b_name}' has no row in CSV")
            continue
        # Reciprocity check
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

        # Pick "a" deterministically: alphabetically lower canonical_id
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
# DB write — upsert slate + matches + slate_players + lines
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

    Re-upload semantics: same (sport, slate_date) → updates the existing
    row in place (preserves slate_id so attached PP lines + contest
    ownership survive). New (sport, slate_date) → creates a new row.
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

    payload = {
        "sport": sport,
        "slate_date": slate_date,
        "slate_label": slate_label or tournament,
        "contest_type": "classic",
        "salary_cap": 50000,
        "lock_time": lock_time,
        "status": "active",
        "last_synced_at": "now()",
    }

    if existing:
        slate_id = existing[0]["id"]
        # Drop the now() literal — supabase-py needs proper ISO; let DB default handle it
        payload.pop("last_synced_at", None)
        db.table("slates").update(payload).eq("id", slate_id).execute()
        logger.info("Updated existing slate %s for %s %s", slate_id, sport, slate_date)
        return slate_id

    # New slate — also set dk_draft_group_id (synthetic) and first_seen_at
    payload["dk_draft_group_id"] = _synthetic_draft_group_id()
    inserted = db.table("slates").insert(payload).execute().data
    slate_id = inserted[0]["id"]
    logger.info("Created new manual slate %s for %s %s", slate_id, sport, slate_date)
    return slate_id


def replace_slate_contents(
    *,
    slate_id: str,
    matches: list[dict],
    tournament: str,
    surface: Optional[str],
    lock_time: Optional[str],
) -> dict:
    """Wipe and re-create matches, slate_players, and stat lines for this slate.

    Idempotent: produces the same DB state regardless of prior contents.
    Preserves anything keyed by slate_id but stored in OTHER tables that
    we don't manage here (contest_ownership, prizepicks_lines for FS — see
    upsert_pp_fs_lines for those).
    """
    db = get_client()

    # ── 1. Wipe existing matches and slate_players for this slate ────
    # FK from slate_players(match_id) to matches(id) is ON DELETE not set, so
    # delete order matters: slate_players first, then matches.
    db.table("slate_players").delete().eq("slate_id", slate_id).execute()
    db.table("matches").delete().eq("slate_id", slate_id).execute()

    # ── 2. Re-insert matches ─────────────────────────────────────────
    match_rows = []
    for m in matches:
        a_row = m["player_a_row"]
        b_row = m["player_b_row"]
        # Pick start_time: prefer player A's, fall back to B's, then null.
        start_time = a_row.get("start_time") or b_row.get("start_time")
        # Build posted_lines — nest stat lines under "a" and "b"
        posted_a: dict[str, float] = {}
        posted_b: dict[str, float] = {}
        for csv_key, posted_key in STAT_LINE_MAP.items():
            if a_row.get(csv_key) is not None:
                posted_a[posted_key] = a_row[csv_key]
            if b_row.get(csv_key) is not None:
                posted_b[posted_key] = b_row[csv_key]
        posted_lines = {}
        if posted_a:
            posted_lines["a"] = posted_a
        if posted_b:
            posted_lines["b"] = posted_b
        odds_payload = {"posted_lines": posted_lines} if posted_lines else {}

        match_rows.append(
            {
                "slate_id": slate_id,
                "player_a_id": m["player_a_id"],
                "player_b_id": m["player_b_id"],
                "tournament": tournament,
                "surface": surface,
                "best_of": 3,
                "start_time": start_time,
                "status": "scheduled",
                "odds": odds_payload,
            }
        )
    if match_rows:
        db.table("matches").insert(match_rows).execute()

    # Pull back inserted matches to get their generated IDs (for slate_players.match_id)
    fresh_matches = (
        db.table("matches")
        .select("id, player_a_id, player_b_id")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )
    match_id_by_pair = {
        frozenset([m["player_a_id"], m["player_b_id"]]): m["id"] for m in fresh_matches
    }

    # ── 3. Re-insert slate_players ───────────────────────────────────
    sp_rows = []
    for m in matches:
        match_id = match_id_by_pair.get(
            frozenset([m["player_a_id"], m["player_b_id"]])
        )
        for side_row, cid in [
            (m["player_a_row"], m["player_a_id"]),
            (m["player_b_row"], m["player_b_id"]),
        ]:
            sp_rows.append(
                {
                    "slate_id": slate_id,
                    "player_id": cid,
                    "dk_player_id": side_row["dk_id"],
                    "dk_player_id_override": side_row["dk_id"],  # bind to manual ID
                    "dk_display_name": side_row["name"],
                    "salary": side_row["salary"],
                    "avg_ppg": 0,
                    "roster_position": "P",
                    "match_id": match_id,
                    "ss_pool_own": side_row["sim_own"],
                }
            )
    if sp_rows:
        db.table("slate_players").insert(sp_rows).execute()

    return {
        "matches_inserted": len(match_rows),
        "slate_players_inserted": len(sp_rows),
    }


def upsert_pp_fs_lines(
    *, slate_id: str, rows: list[dict], resolved: dict[str, str]
) -> int:
    """Upsert prizepicks_lines rows for any row with a non-null pp_fs_line.

    Stat type is always 'Fantasy Score'. The unique index on
    (slate_id, raw_player_name, stat_type) ensures re-uploads update in
    place rather than duplicating.

    Re-upload behavior: if the new CSV omits a player who had an FS line
    previously, that line is deactivated (is_active=false) — keeps the
    history but stops it from showing in the active list.
    """
    db = get_client()

    # Soft-deactivate any existing FS lines for this slate
    db.table("prizepicks_lines").update({"is_active": False}).eq(
        "slate_id", slate_id
    ).eq("stat_type", "Fantasy Score").execute()

    # Insert/upsert active lines from this upload
    pp_rows = []
    for r in rows:
        if r.get("pp_fs_line") is None:
            continue
        cid = resolved.get(r["name"])
        if not cid:
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
    if pp_rows:
        # on_conflict matches the unique index (slate_id, raw_player_name, stat_type)
        db.table("prizepicks_lines").upsert(
            pp_rows, on_conflict="slate_id,raw_player_name,stat_type"
        ).execute()
    return len(pp_rows)


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
    """End-to-end manual slate ingestion.

    With dry_run=True returns parse + validation results without writing
    anything — used to power the Admin tab's preview screen.
    """
    result: dict = {
        "ok": False,
        "warnings": [],
        "errors": [],
        "unmatched_names": [],
        "summary": {},
    }

    # ── 1. Parse + validate CSV ──────────────────────────────────────
    rows, parse_errs = parse_csv(csv_text)
    if parse_errs:
        result["errors"].extend(parse_errs)
        return result
    if not rows:
        result["errors"].append("CSV had no data rows")
        return result

    # ── 2. Resolve names ─────────────────────────────────────────────
    resolved, unresolved = resolve_names(rows)
    if unresolved:
        result["unmatched_names"] = unresolved
        result["warnings"].append(
            f"{len(unresolved)} unmatched name(s) — players must exist in the players table"
        )

    # ── 3. Pair matches ──────────────────────────────────────────────
    matches, pair_warnings = pair_matches(rows, resolved)
    result["warnings"].extend(pair_warnings)

    # ── 4. Sanity counts ─────────────────────────────────────────────
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

    # If unmatched names exist OR no matches paired, surface as preview-only
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
        result["ok"] = True
        return result

    # ── 5. Write to DB ───────────────────────────────────────────────
    slate_id = upsert_slate(
        sport=sport,
        slate_date=slate_date,
        tournament=tournament,
        surface=surface,
        lock_time=lock_time,
    )
    write_summary = replace_slate_contents(
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
