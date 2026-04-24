"""Contest ownership routes — backs the Live Leverage Tracker tab.

Public:
  GET  /api/tracker/{slate_id}/ownership    — read current ownership

Admin-only (Bearer token required):
  POST /api/tracker/{slate_id}/ownership    — upload DK contest CSV, ingest
  DELETE /api/tracker/{slate_id}/ownership  — clear the slate's ownership

Auth reuses the same pattern as the PrizePicks admin endpoints:
Supabase JWT → users.id → admin_users membership check.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, File, Header, HTTPException, UploadFile

from app.db import get_client
from app.services import contest_ownership as svc
from app.services import prizepicks_admin as pp_admin  # reuse is_admin_user

router = APIRouter(prefix="/api/tracker", tags=["tracker"])


# ── Auth helper (same shape as prizepicks.require_admin) ─────────────


async def require_admin(
    authorization: Optional[str] = Header(default=None),
) -> str:
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

    if not pp_admin.is_admin_user(user_id):
        raise HTTPException(403, "Admin access required")
    return user_id


# ── Public read ─────────────────────────────────────────────────────


@router.get("/{slate_id}/ownership")
async def get_ownership(slate_id: str):
    """Return current contest ownership for a slate.

    Shape:
      {
        "slate_id": "...",
        "uploaded_at": "...",
        "contest_name": "...",
        "total_entries": 12345,
        "ownership": {"Jannik Sinner": 38.2, ...}
      }
    If no upload exists, ownership is {} and uploaded_at is null.
    """
    return svc.get_ownership(slate_id)


# ── Admin write ─────────────────────────────────────────────────────


@router.post("/{slate_id}/ownership")
async def upload_ownership(
    slate_id: str,
    file: UploadFile = File(..., description="DK contest entrants export CSV"),
    admin_user_id: str = Depends(require_admin),
):
    """Ingest a DK contest CSV. Replaces any previous ownership for this
    slate and appends a snapshot to contest_ownership_history."""
    raw = await file.read()
    if not raw:
        raise HTTPException(400, "Empty file")
    if len(raw) > 10 * 1024 * 1024:  # 10MB cap
        raise HTTPException(413, "File too large (max 10MB)")

    ownership, total_entries, contest_name = svc.parse_dk_contest_csv(raw)
    if not ownership:
        raise HTTPException(
            400,
            "No ownership data found. Expected DK contest export with "
            "Player / Roster Position / %Drafted columns.",
        )

    summary = svc.ingest_ownership(
        slate_id=slate_id,
        ownership=ownership,
        uploaded_by=admin_user_id,
        contest_name=contest_name,
        total_entries=total_entries,
    )
    return {"summary": summary}


@router.delete("/{slate_id}/ownership")
async def clear_ownership(
    slate_id: str,
    admin_user_id: str = Depends(require_admin),
):
    """Clear current ownership for a slate. History is preserved."""
    deleted = svc.clear_ownership(slate_id)
    return {"deleted": deleted, "slate_id": slate_id}
