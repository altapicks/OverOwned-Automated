"""Read-side service: builds the frontend-facing FrontendSlate from DB rows.

This is what the API returns to the React app. The shape matches the
existing slate.json schema exactly so no UI changes are needed.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from app.db import get_client
from app.models import (
    FrontendMatch,
    FrontendMatchOdds,
    FrontendPlayer,
    FrontendPPLine,
    FrontendSlate,
)

logger = logging.getLogger(__name__)


def get_frontend_slate(slate_id: str) -> Optional[FrontendSlate]:
    """Hydrate a complete slate into the shape the frontend expects."""
    db = get_client()
    slate = db.table("slates").select("*").eq("id", slate_id).single().execute().data
    if not slate:
        return None

    # Match rows
    matches = (
        db.table("matches")
        .select("*, player_a:player_a_id(display_name), player_b:player_b_id(display_name)")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    # Slate player rows with display names
    players = (
        db.table("slate_players")
        .select("*, player:player_id(display_name)")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    # ── Build matches ─────────────────────────────────────────────────
    frontend_matches: list[FrontendMatch] = []
    for m in matches:
        pa_name = (m.get("player_a") or {}).get("display_name") or m["player_a_id"]
        pb_name = (m.get("player_b") or {}).get("display_name") or m["player_b_id"]
        frontend_matches.append(
            FrontendMatch(
                player_a=pa_name,
                player_b=pb_name,
                start_time=m.get("start_time"),
                tournament=m.get("tournament") or "",
                surface=m.get("surface"),
                odds=FrontendMatchOdds(),  # populated by odds service later
                adj_a=0,
                adj_b=0,
            )
        )

    # ── Collapse slate_players into per-canonical entries ─────────────
    # For showdown, multiple roster_positions per player get merged with
    # per-position salaries/ids on one row. For classic, just one row.
    by_canonical: dict[str, dict] = {}
    for sp in players:
        cid = sp["player_id"]
        display = (sp.get("player") or {}).get("display_name") or sp.get(
            "dk_display_name"
        )
        entry = by_canonical.setdefault(
            cid,
            {"name": display, "id": 0, "salary": 0, "avg_ppg": 0},
        )
        rp = sp.get("roster_position") or "P"
        if rp in ("P", "FLEX"):
            # Classic: P. Showdown flex: FLEX.
            entry["id"] = sp["dk_player_id"]
            entry["salary"] = sp["salary"]
            entry["avg_ppg"] = float(sp["avg_ppg"] or 0)
            entry["flex_id"] = sp["dk_player_id"]
            entry["flex_salary"] = sp["salary"]
        elif rp == "CPT":
            entry["cpt_id"] = sp["dk_player_id"]
            entry["cpt_salary"] = sp["salary"]
        elif rp == "ACPT":
            entry["acpt_id"] = sp["dk_player_id"]
            entry["acpt_salary"] = sp["salary"]

    frontend_players = [FrontendPlayer(**e) for e in by_canonical.values()]

    meta = {
        "id": slate["id"],
        "dk_draft_group_id": slate["dk_draft_group_id"],
        "first_seen_at": slate["first_seen_at"],
        "last_synced_at": slate["last_synced_at"],
        "contest_type": slate.get("contest_type") or "classic",
        "is_fallback": bool(slate.get("is_fallback")),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    return FrontendSlate(
        date=slate["slate_date"],
        sport=slate["sport"],
        slate_label=slate.get("slate_label"),
        lock_time=slate.get("lock_time"),
        matches=frontend_matches,
        dk_players=frontend_players,
        pp_lines=[],  # populated by PP service later
        meta=meta,
    )


def get_today_slate(sport: str) -> Optional[FrontendSlate]:
    """Return the most relevant active slate for a sport.

    Preference order:
      1. Classic slate for today (if any)
      2. Classic slate for any date (most recent)
      3. Showdown fallback for today (if is_fallback=true or no Classic at all)

    This mirrors the watcher's classify/fallback logic — if a Classic got
    ingested today, users see it; if only a Showdown fallback exists, they
    see that instead. Used by /api/slates/today.
    """
    db = get_client()

    # Most recent Classic, active status
    classic = (
        db.table("slates")
        .select("id")
        .eq("sport", sport)
        .eq("status", "active")
        .eq("contest_type", "classic")
        .order("slate_date", desc=True)
        .order("first_seen_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if classic:
        return get_frontend_slate(classic[0]["id"])

    # Fall through to Showdown (fallback or otherwise)
    showdown = (
        db.table("slates")
        .select("id")
        .eq("sport", sport)
        .eq("status", "active")
        .eq("contest_type", "showdown")
        .order("slate_date", desc=True)
        .order("first_seen_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if showdown:
        return get_frontend_slate(showdown[0]["id"])

    return None


def list_slates(sport: str, limit: int = 30) -> list[dict]:
    """Archive manifest — what dates do we have slates for?"""
    db = get_client()
    rows = (
        db.table("slates")
        .select("id, slate_date, slate_label, status")
        .eq("sport", sport)
        .order("slate_date", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    return [
        {
            "id": r["id"],
            "date": r["slate_date"],
            "label": r.get("slate_label"),
            "status": r.get("status"),
        }
        for r in rows
    ]
