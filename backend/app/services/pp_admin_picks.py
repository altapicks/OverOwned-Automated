"""PrizePicks admin daily picks service (v6.13).

One Hidden Gem and one Biggest Trap per slate, set by admin manually
through the PrizePicks tab UI. Replaces algorithmic "Top PP Fade"
detection — the model can be wrong; admin judgment is the source of truth.

Schema:
  pp_admin_picks  — single row per slate_id, see migration SQL.

Admin gate: same as prizepicks_admin.py — the user's auth UUID must
exist in admin_users. RLS provides DB-level enforcement; this module
provides the app-level check that runs before the upsert.

Read path is public (anon RLS allows SELECT) so the slate reader can
pull picks into the slate response without authentication.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from app.db import get_client
from app.services.prizepicks_admin import is_admin_user

logger = logging.getLogger(__name__)


def get_picks(slate_id: str) -> Optional[dict]:
    """Return the admin picks row for a slate, or None if none set yet.

    Public — no admin gate required. Used by slate_reader to attach picks
    to the slate response.

    Returns the full row including timestamps and set_by, so the frontend
    can show "Set 3 hours ago" if useful.
    """
    if not slate_id:
        return None
    db = get_client()
    rows = (
        db.table("pp_admin_picks")
        .select("*")
        .eq("slate_id", slate_id)
        .limit(1)
        .execute()
        .data
        or []
    )
    return rows[0] if rows else None


def _validate_player_in_slate(slate_id: str, raw_player_name: str) -> bool:
    """Soft-check: confirm the player name appears in this slate's
    prizepicks_lines. Prevents typos like 'Snner' from getting saved.

    Returns True if the player has at least one active line in this
    slate. False on any error (errs on the side of letting it through —
    we don't want to block a legitimate save because of a transient DB
    hiccup; the gem/trap is just a label).
    """
    if not raw_player_name:
        return False
    try:
        db = get_client()
        rows = (
            db.table("prizepicks_lines")
            .select("id")
            .eq("slate_id", slate_id)
            .eq("raw_player_name", raw_player_name)
            .eq("is_active", True)
            .limit(1)
            .execute()
            .data
            or []
        )
        return bool(rows)
    except Exception as e:
        logger.warning(
            "pp_admin_picks: player validation failed (allowing through): %s", e
        )
        return True


def set_pick(
    *,
    slate_id: str,
    kind: str,                       # 'gem' | 'trap'
    raw_player_name: Optional[str],  # None or '' to clear
    user_id: Optional[str] = None,   # auth UUID for admin gate
    set_by_label: Optional[str] = None,  # display name / email for audit
) -> dict:
    """Set or clear the gem/trap for a slate.

    Admin-gated: raises PermissionError if user_id is not in admin_users.
    The app-level gate matches the existing PP line entry pattern.

    Pass raw_player_name=None or '' to clear that pick (e.g. admin
    changed their mind, want to re-mark as nothing).

    Returns the updated row.
    """
    if kind not in ("gem", "trap"):
        raise ValueError(f"kind must be 'gem' or 'trap', got {kind!r}")
    if not slate_id:
        raise ValueError("slate_id required")
    if not is_admin_user(user_id):
        raise PermissionError("admin only")

    cleaned_name = (raw_player_name or "").strip() or None

    if cleaned_name and not _validate_player_in_slate(slate_id, cleaned_name):
        # Hard reject — admin probably typo'd. Better to fail loudly than
        # save a bad name that won't render.
        raise ValueError(
            f"player {cleaned_name!r} has no active PrizePicks lines in this slate. "
            "Check spelling — name must match exactly as it appears in the PP table."
        )

    db = get_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    # Read existing row (or None) so we can preserve the OTHER pick.
    existing = get_picks(slate_id) or {}

    # Build the upsert payload. Only update the field for this kind;
    # preserve the other kind's current value.
    payload = {
        "slate_id": slate_id,
        "gem_player_name":  existing.get("gem_player_name"),
        "trap_player_name": existing.get("trap_player_name"),
        "gem_set_at":       existing.get("gem_set_at"),
        "trap_set_at":      existing.get("trap_set_at"),
        "gem_set_by":       existing.get("gem_set_by"),
        "trap_set_by":      existing.get("trap_set_by"),
    }
    if kind == "gem":
        payload["gem_player_name"] = cleaned_name
        payload["gem_set_at"] = now_iso if cleaned_name else None
        payload["gem_set_by"] = set_by_label or user_id if cleaned_name else None
    else:  # trap
        payload["trap_player_name"] = cleaned_name
        payload["trap_set_at"] = now_iso if cleaned_name else None
        payload["trap_set_by"] = set_by_label or user_id if cleaned_name else None

    # Supabase upsert on the primary key. ON CONFLICT (slate_id) DO UPDATE.
    result = (
        db.table("pp_admin_picks")
        .upsert(payload, on_conflict="slate_id")
        .execute()
    )
    return result.data[0] if result.data else payload


def clear_picks(slate_id: str, user_id: Optional[str] = None) -> bool:
    """Delete the entire pp_admin_picks row for a slate. Admin-gated.

    Used when admin wants to start fresh — both gem and trap cleared in
    one call. Returns True if a row was deleted, False if none existed.
    """
    if not is_admin_user(user_id):
        raise PermissionError("admin only")
    db = get_client()
    result = (
        db.table("pp_admin_picks")
        .delete()
        .eq("slate_id", slate_id)
        .execute()
    )
    return bool(result.data)
