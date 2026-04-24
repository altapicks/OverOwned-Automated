"""Contest ownership service.

Live Leverage Tracker backs its field-ownership data with backend storage
so that one operator's CSV upload is visible to every user of the app.
Replaces the previous localStorage-per-user flow in the Tracker tab.

Schema (see migrations/004_id_overrides_and_contest_own.sql):
  contest_ownership         — current state, one row per (slate, player)
  contest_ownership_history — append-only snapshot log per upload

The upload endpoint accepts DK's standard contest export CSV:
  Rank,EntryId,EntryName,TimeRemaining,Points,Lineup,,Player,Roster Position,%Drafted,FPTS
For Showdown the same player appears twice (CPT + UTIL); we sum their
%Drafted into a single ownership value, capped at 200%.
"""
from __future__ import annotations

import io
import logging
import re
from typing import Iterable, Optional

from app.db import get_client

logger = logging.getLogger(__name__)


# ── CSV parser ───────────────────────────────────────────────────────


def parse_dk_contest_csv(raw_bytes: bytes) -> tuple[dict[str, float], Optional[int], Optional[str]]:
    """Parse DK's contest entrants export.

    Returns:
        (ownership_by_player, total_entries, contest_name)
        - ownership_by_player: {"Jannik Sinner": 38.2, ...}
        - total_entries: inferred from max Rank value when present
        - contest_name: extracted from first-cell header if present (e.g.
          "Contest: $100K Classic Tennis"), else None

    DK contest CSVs have a 7-ish-column lineup dump on the left and a
    player breakdown on the right. Columns of interest on each data row:
      col[8]  = Player name
      col[9]  = Roster Position (CPT / UTIL / P)
      col[10] = %Drafted (e.g. "38.2%")

    Showdown players appear on two rows (CPT + UTIL); ownership is summed
    to get combined field exposure. Classic players appear on one row.
    """
    raw = raw_bytes.decode("utf-8-sig", errors="replace")
    lines = raw.splitlines()

    own: dict[str, float] = {}
    total_entries: Optional[int] = None
    contest_name: Optional[str] = None
    header_seen = False

    def _parse_csv_line(line: str) -> list[str]:
        # Minimal RFC4180-ish parser matching the frontend's existing one
        cols: list[str] = []
        cur = ""
        in_quotes = False
        i = 0
        while i < len(line):
            c = line[i]
            if c == '"':
                if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                    cur += '"'
                    i += 1
                else:
                    in_quotes = not in_quotes
            elif c == "," and not in_quotes:
                cols.append(cur)
                cur = ""
            else:
                cur += c
            i += 1
        cols.append(cur)
        return cols

    for raw_line in lines:
        if not raw_line.strip():
            continue
        cols = _parse_csv_line(raw_line)

        # Header detection (DK exports: first data header row starts with "Rank,")
        if cols[0].strip() == "Rank":
            header_seen = True
            continue

        # Contest-name line: first column sometimes has "Contest: <name>"
        if cols[0].startswith("Contest:") and contest_name is None:
            contest_name = cols[0].split(":", 1)[1].strip()
            continue

        if not header_seen:
            # skip pre-header chatter
            continue

        # Track max rank for total_entries
        try:
            rank_val = int(cols[0])
            if total_entries is None or rank_val > total_entries:
                total_entries = rank_val
        except (ValueError, IndexError):
            pass

        # Ownership row: cols[8]=player, cols[10]=%Drafted
        if len(cols) < 11:
            continue
        player = (cols[8] or "").strip()
        pct_raw = (cols[10] or "").strip().rstrip("%")
        if not player or not pct_raw:
            continue
        try:
            pct = float(pct_raw)
        except ValueError:
            continue
        # Sum (Showdown CPT + UTIL for the same player). Cap at 200.
        own[player] = min(200.0, own.get(player, 0.0) + pct)

    return own, total_entries, contest_name


# ── Persistence ──────────────────────────────────────────────────────


def ingest_ownership(
    slate_id: str,
    ownership: dict[str, float],
    uploaded_by: Optional[str] = None,
    contest_name: Optional[str] = None,
    total_entries: Optional[int] = None,
) -> dict:
    """Upsert current ownership + append snapshot. Returns summary.

    Does two writes:
      1. UPSERT into contest_ownership     — replaces current state
      2. INSERT into contest_ownership_history — append-only audit

    Idempotent on (slate_id, player_name). Re-uploading the same CSV
    replaces the current state cleanly and appends another history row.
    """
    db = get_client()
    if not ownership:
        return {"upserted": 0, "history_inserted": 0, "slate_id": slate_id}

    now_rows = [
        {
            "slate_id": slate_id,
            "player_name": name,
            "actual_own_pct": round(pct, 2),
            "uploaded_by": uploaded_by,
            "contest_name": contest_name,
            "total_entries": total_entries,
        }
        for name, pct in ownership.items()
    ]

    # 1. Replace current
    db.table("contest_ownership").upsert(
        now_rows, on_conflict="slate_id,player_name"
    ).execute()

    # 2. Append to history
    db.table("contest_ownership_history").insert(now_rows).execute()

    return {
        "upserted": len(now_rows),
        "history_inserted": len(now_rows),
        "slate_id": slate_id,
        "contest_name": contest_name,
        "total_entries": total_entries,
    }


def get_ownership(slate_id: str) -> dict:
    """Return current ownership for a slate, plus upload metadata.

    Shape:
      {
        "slate_id": ...,
        "uploaded_at": ...,
        "contest_name": ...,
        "total_entries": ...,
        "ownership": {"Player Name": 38.2, ...}
      }
    """
    db = get_client()
    rows = (
        db.table("contest_ownership")
        .select("*")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )
    if not rows:
        return {
            "slate_id": slate_id,
            "uploaded_at": None,
            "contest_name": None,
            "total_entries": None,
            "ownership": {},
        }

    # All rows share the same upload metadata (they're upserted together).
    first = rows[0]
    return {
        "slate_id": slate_id,
        "uploaded_at": first.get("uploaded_at"),
        "contest_name": first.get("contest_name"),
        "total_entries": first.get("total_entries"),
        "ownership": {r["player_name"]: float(r["actual_own_pct"]) for r in rows},
    }


def clear_ownership(slate_id: str) -> int:
    """Delete current ownership for a slate. Leaves history intact.
    Returns number of rows deleted."""
    db = get_client()
    result = (
        db.table("contest_ownership")
        .delete()
        .eq("slate_id", slate_id)
        .execute()
    )
    return len(result.data or [])
