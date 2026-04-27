"""PrizePicks admin daily picks routes (v6.13).

One Hidden Gem and one Biggest Trap per slate, set by admin manually.
Replaces algorithmic "Top PP Fade" with admin curation.

Auth pattern matches app.routes.prizepicks exactly:
  - Reads are public (no auth header required)
  - Writes require Authorization: Bearer <supabase-jwt>
  - JWT is verified via db.auth.get_user(token); resolved user_id is
    checked against admin_users table

Mounted under /api/prizepicks alongside the existing PP routes.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from app.db import get_client
from app.services import prizepicks_admin as svc
from app.services.pp_admin_picks import (
    clear_picks,
    get_picks,
    set_pick,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/prizepicks", tags=["prizepicks"])


# ── Auth helper — copied from app.routes.prizepicks for behavioral parity.
# If you'd rather DRY this up, move require_admin into a shared module
# and import it from both routers. Kept inline here to avoid touching
# the existing PP routes file at all.
async def require_admin(authorization: Optional[str] = Header(default=None)) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        db = get_client()
        resp = db.auth.get_user(token)
        user = getattr(resp, "user", None)
        if not user:
            raise HTTPException(401, "Invalid token")
        user_id = user.id
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(401, f"Token verification failed: {e}")

    if not svc.is_admin_user(user_id):
        raise HTTPException(403, "Admin access required")
    return user_id


# ── Public read ────────────────────────────────────────────────────

@router.get("/admin-picks")
async def get_admin_picks(slate_id: str = Query(..., description="UUID of the slate")):
    """Return the gem/trap picks for a slate, or a stable null shape if none.

    Public read — anyone viewing the PP tab can see what the admin marked.
    Always returns the same key shape so the frontend doesn't handle 404s.
    """
    row = get_picks(slate_id)
    if row:
        return row
    return {
        "slate_id": slate_id,
        "gem_player_name": None,
        "trap_player_name": None,
        "gem_set_at": None,
        "trap_set_at": None,
        "gem_set_by": None,
        "trap_set_by": None,
    }


# ── Admin write ────────────────────────────────────────────────────

class SetPickRequest(BaseModel):
    slate_id: str = Field(..., min_length=1)
    kind: str = Field(..., pattern="^(gem|trap)$")
    raw_player_name: Optional[str] = None  # null/'' to clear
    set_by_label: Optional[str] = None     # email/display name for audit


@router.post("/admin-picks")
async def post_admin_picks(
    body: SetPickRequest,
    admin_user_id: str = Depends(require_admin),
):
    """Admin only. Set or clear a single gem/trap.

    Body:
      slate_id        — required
      kind            — 'gem' or 'trap'
      raw_player_name — string to set, or null/empty to clear
      set_by_label    — optional display string (typically email) for audit

    Returns the updated picks row.
    """
    try:
        row = set_pick(
            slate_id=body.slate_id,
            kind=body.kind,
            raw_player_name=body.raw_player_name,
            user_id=admin_user_id,
            set_by_label=body.set_by_label,
        )
        return row
    except PermissionError:
        # Shouldn't reach here — require_admin already gated. Defensive.
        raise HTTPException(403, "admin only")
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.exception("pp admin picks set failed")
        raise HTTPException(500, f"unexpected error: {e}")


@router.delete("/admin-picks")
async def delete_admin_picks(
    slate_id: str = Query(...),
    admin_user_id: str = Depends(require_admin),
):
    """Admin only. Wipe both gem and trap for a slate."""
    try:
        deleted = clear_picks(slate_id, user_id=admin_user_id)
        return {"ok": True, "deleted": deleted}
    except PermissionError:
        raise HTTPException(403, "admin only")
    except Exception as e:
        logger.exception("pp admin picks clear failed")
        raise HTTPException(500, f"unexpected error: {e}")
