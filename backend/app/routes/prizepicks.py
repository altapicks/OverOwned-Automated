"""PrizePicks API routes.

Reads are public (RLS on prizepicks_lines enforces is_active + active slate).
Writes require admin_users membership. The admin check reads the user's
Supabase JWT from the Authorization header.

Design note: this service uses the Supabase service_role key server-side,
which bypasses RLS. Our app-level is_admin_user() check is the real gate.
Keeping RLS enforced on the table means the frontend (anon key) can't
write directly even if someone finds the endpoint.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from app.db import get_client
from app.services import prizepicks_admin as svc

router = APIRouter(prefix="/api/prizepicks", tags=["prizepicks"])


# ── Auth helper ────────────────────────────────────────────────────

async def require_admin(authorization: Optional[str] = Header(default=None)) -> str:
    """Extract the Supabase user from a Bearer token and verify admin.

    Supabase puts the user_id in the JWT `sub` claim. Rather than verify
    the JWT signature here (which would require the JWT secret), we ask
    Supabase's auth endpoint to resolve it. This is one extra API call
    but avoids secret management.
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    try:
        # supabase-py's auth.get_user verifies the token and returns the user
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

@router.get("/lines")
async def list_lines(slate_id: str = Query(..., description="UUID of the slate")):
    """Return active PrizePicks lines for a slate. Public."""
    return {"lines": svc.list_lines_for_slate(slate_id)}


# ── Admin write ────────────────────────────────────────────────────

class CreateLineRequest(BaseModel):
    slate_id: str
    raw_player_name: str = Field(..., min_length=1)
    stat_type: str = Field(..., min_length=1)
    current_line: float
    match_id: Optional[str] = None
    notes: Optional[str] = None


@router.post("/lines")
async def create_line(body: CreateLineRequest, admin_user_id: str = Depends(require_admin)):
    row = svc.upsert_line(
        slate_id=body.slate_id,
        raw_player_name=body.raw_player_name,
        stat_type=body.stat_type,
        current_line=body.current_line,
        match_id=body.match_id,
        notes=body.notes,
        entered_by=admin_user_id,
    )
    return {"line": row}


class UpdateLineRequest(BaseModel):
    current_line: Optional[float] = None
    notes: Optional[str] = None


@router.patch("/lines/{line_id}")
async def patch_line(
    line_id: str, body: UpdateLineRequest, admin_user_id: str = Depends(require_admin)
):
    row = svc.update_line(
        line_id=line_id,
        current_line=body.current_line,
        notes=body.notes,
        entered_by=admin_user_id,
    )
    if not row:
        raise HTTPException(404, "Line not found or no change")
    return {"line": row}


@router.delete("/lines/{line_id}")
async def delete_line(line_id: str, admin_user_id: str = Depends(require_admin)):
    row = svc.soft_delete_line(line_id=line_id, entered_by=admin_user_id)
    if not row:
        raise HTTPException(404, "Line not found")
    return {"line": row}


class BulkLineRow(BaseModel):
    raw_player_name: str
    stat_type: str
    current_line: float
    notes: Optional[str] = None


class BulkRequest(BaseModel):
    slate_id: str
    rows: list[BulkLineRow]


@router.post("/lines/bulk")
async def bulk_upsert(body: BulkRequest, admin_user_id: str = Depends(require_admin)):
    row_dicts = [r.model_dump() for r in body.rows]
    summary = svc.bulk_upsert(
        slate_id=body.slate_id, rows=row_dicts, entered_by=admin_user_id
    )
    return {"summary": summary}


# ── Movements read ─────────────────────────────────────────────────

@router.get("/movements")
async def list_movements(
    slate_id: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
):
    """Last N line movements for a slate. Used by the Live Lines panel."""
    db = get_client()
    rows = (
        db.table("line_movements")
        .select("*")
        .eq("slate_id", slate_id)
        .order("detected_at", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    return {"movements": rows}
