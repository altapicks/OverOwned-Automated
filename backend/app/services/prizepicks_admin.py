"""PrizePicks manual-entry service.

This service exists for admin entry of PrizePicks projections via the
/api/prizepicks/lines endpoints. No scraping, no polling — you paste the
current lines into the admin UI and the service stores them.

Schema:
  prizepicks_lines    — current state of every active line
  line_movements      — append-only diff log (written by DB trigger
                        on prizepicks_lines changes, no app code needed)

Admin gate: admin_users table. User's auth UUID must exist there to write.
All reads are public (anon via RLS).

v6.5 — list_lines_for_slate default changed from "Fantasy Score only" to
ALL stat types. The PP tab UI now has stat-category tabs, defaulting to
Fantasy Score on the client side. This lets the same endpoint serve the
PP tab's tab filter without an extra request per stat. Existing callers
that explicitly pass stat_type are unaffected.
"""
from __future__ import annotations

import logging
from typing import Optional

from app.db import get_client
from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)


def resolve_player(sport: str, raw_name: str) -> Optional[str]:
    """Best-effort resolve a raw PP player name to a canonical_id.
    Returns None if no confident match — the line still gets stored with
    raw_player_name intact; player_id just stays null."""
    if not raw_name:
        return None
    normalizer = PlayerNormalizer(sport=sport)
    # Don't create_if_missing — PP may have players not on any DK slate
    result = normalizer.resolve(raw_name, source="prizepicks", create_if_missing=False)
    return result.canonical_id if result.auto_resolved else None


def is_admin_user(user_id: Optional[str]) -> bool:
    """Check admin_users table. Service-role bypasses RLS — this is the
    app-level guard. RLS provides a second layer at the DB level."""
    if not user_id:
        return False
    db = get_client()
    row = (
        db.table("admin_users")
        .select("user_id")
        .eq("user_id", user_id)
        .limit(1)
        .execute()
        .data
    )
    return bool(row)


def list_lines_for_slate(slate_id: str, stat_type: Optional[str] = None) -> list[dict]:
    """List active lines for a slate.

    stat_type behavior (v6.5 — default flipped):
      None or "all"      → return every stat type (default)
      "<specific name>"  → filter to that stat type exactly

    The PrizePicks tab UI now filters client-side via tabs. Returning all
    stat types in one shot keeps the tab switch instant — no refetch
    needed when the user clicks Aces, Double Faults, etc.
    """
    db = get_client()
    q = (
        db.table("prizepicks_lines")
        .select("*")
        .eq("slate_id", slate_id)
        .eq("is_active", True)
    )
    if stat_type is not None and stat_type.lower() != "all":
        q = q.eq("stat_type", stat_type)
    rows = q.order("last_updated_at", desc=True).execute().data or []
    return rows


def upsert_line(
    *,
    slate_id: str,
    raw_player_name: str,
    stat_type: str,
    current_line: float,
    match_id: Optional[str] = None,
    notes: Optional[str] = None,
    entered_by: Optional[str] = None,
    sport: str = "tennis",
) -> dict:
    """Insert a new line, or reactivate/update an existing one.
    Uniqueness is on (slate_id, raw_player_name, stat_type) where is_active=true.
    If a matching active row exists: update current_line (trigger writes movement).
    Else: insert (trigger writes movement with direction='new').
    """
    db = get_client()
    player_id = resolve_player(sport, raw_player_name)

    existing = (
        db.table("prizepicks_lines")
        .select("*")
        .eq("slate_id", slate_id)
        .eq("raw_player_name", raw_player_name)
        .eq("stat_type", stat_type)
        .eq("is_active", True)
        .limit(1)
        .execute()
        .data
    )

    if existing:
        row = existing[0]
        patch = {"current_line": current_line, "entered_by": entered_by}
        if notes is not None:
            patch["notes"] = notes
        if match_id is not None:
            patch["match_id"] = match_id
        if player_id and not row.get("player_id"):
            patch["player_id"] = player_id
        result = (
            db.table("prizepicks_lines")
            .update(patch)
            .eq("id", row["id"])
            .execute()
        )
        return result.data[0] if result.data else row

    insert = {
        "slate_id": slate_id,
        "raw_player_name": raw_player_name,
        "stat_type": stat_type,
        "current_line": current_line,
        "player_id": player_id,
        "match_id": match_id,
        "notes": notes,
        "entered_by": entered_by,
        "is_active": True,
    }
    result = db.table("prizepicks_lines").insert(insert).execute()
    return result.data[0] if result.data else insert


def update_line(
    *,
    line_id: str,
    current_line: Optional[float] = None,
    notes: Optional[str] = None,
    entered_by: Optional[str] = None,
) -> Optional[dict]:
    db = get_client()
    patch = {"entered_by": entered_by} if entered_by else {}
    if current_line is not None:
        patch["current_line"] = current_line
    if notes is not None:
        patch["notes"] = notes
    if not patch:
        return None
    result = db.table("prizepicks_lines").update(patch).eq("id", line_id).execute()
    return result.data[0] if result.data else None


def soft_delete_line(line_id: str, entered_by: Optional[str] = None) -> Optional[dict]:
    """Set is_active=false. Trigger writes a movement row with direction='removed'."""
    db = get_client()
    patch = {"is_active": False}
    if entered_by:
        patch["entered_by"] = entered_by
    result = db.table("prizepicks_lines").update(patch).eq("id", line_id).execute()
    return result.data[0] if result.data else None


def bulk_upsert(
    *,
    slate_id: str,
    rows: list[dict],
    entered_by: Optional[str] = None,
    sport: str = "tennis",
) -> dict:
    """Bulk upsert from CSV-paste or similar. Each row: {raw_player_name, stat_type, current_line, notes?}.
    Returns summary: {inserted: N, updated: M, skipped: K}.
    """
    summary = {"inserted": 0, "updated": 0, "skipped": 0}
    db = get_client()
    for row in rows:
        try:
            name = (row.get("raw_player_name") or row.get("player") or "").strip()
            stat = (row.get("stat_type") or row.get("stat") or "").strip()
            line = row.get("current_line") or row.get("line")
            if not name or not stat or line is None:
                summary["skipped"] += 1
                continue
            existed = bool(
                db.table("prizepicks_lines")
                .select("id")
                .eq("slate_id", slate_id)
                .eq("raw_player_name", name)
                .eq("stat_type", stat)
                .eq("is_active", True)
                .limit(1)
                .execute()
                .data
            )
            upsert_line(
                slate_id=slate_id,
                raw_player_name=name,
                stat_type=stat,
                current_line=float(line),
                notes=row.get("notes"),
                entered_by=entered_by,
                sport=sport,
            )
            if existed:
                summary["updated"] += 1
            else:
                summary["inserted"] += 1
        except Exception as e:
            logger.warning("Bulk upsert row failed: %s — row=%s", e, row)
            summary["skipped"] += 1
    return summary
