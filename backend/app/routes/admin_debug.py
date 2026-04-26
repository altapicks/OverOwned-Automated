"""Extra admin debug routes — write operations gated by a simple token.

These exist so we can manually trigger expensive ingest jobs from the
browser when the cron schedule won't fire before slate lock. The token
is read from the ADMIN_DEBUG_TOKEN env var, with a safe default so the
route works out of the box on Railway without env changes.

Rotate the token by setting ADMIN_DEBUG_TOKEN in Railway env vars.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/debug", tags=["admin-debug"])


_DEFAULT_TOKEN = "oo_dbg_8f2k9d4q"


def _check_token(provided: Optional[str]) -> None:
    expected = os.getenv("ADMIN_DEBUG_TOKEN", _DEFAULT_TOKEN)
    if not provided or provided.strip() != expected:
        raise HTTPException(status_code=401, detail="invalid admin debug token")


def _extract_token(
    authorization: Optional[str],
    x_admin_token: Optional[str],
    x_debug_token: Optional[str],
) -> Optional[str]:
    if x_admin_token:
        return x_admin_token
    if x_debug_token:
        return x_debug_token
    if authorization:
        # Accept "Bearer <tok>" or raw "<tok>"
        a = authorization.strip()
        if a.lower().startswith("bearer "):
            return a[7:].strip()
        return a
    return None


@router.post("/dk/force-ingest")
async def dk_force_ingest(
    sport: str = "TEN",
    authorization: Optional[str] = Header(default=None),
    x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token"),
    x_debug_token: Optional[str] = Header(default=None, alias="X-Debug-Token"),
) -> Dict[str, Any]:
    """Manually run the DK featured-slate ingest for a sport.

    Equivalent to a single tick of the daily cron job, but on-demand.
    Useful when slate_players is empty and we cannot wait for 11:00 UTC.

    Auth: pass the debug token via Authorization: Bearer <tok>,
    X-Admin-Token, or X-Debug-Token header.
    """
    tok = _extract_token(authorization, x_admin_token, x_debug_token)
    _check_token(tok)

    try:
        from app.services.dk_auto_ingest import fetch_featured_slate
    except Exception as e:
        log.exception("failed to import dk_auto_ingest: %s", e)
        raise HTTPException(status_code=500, detail=f"import failed: {e}")

    try:
        result = await fetch_featured_slate(sport)
        return {"ok": True, "sport": sport, "result": result}
    except Exception as e:
        log.exception("force-ingest failed: %s", e)
        return {"ok": False, "sport": sport, "error": str(e)}


@router.get("/slate/players-count")
def slate_players_count(slate_id: str) -> Dict[str, Any]:
    """Quick read of how many slate_players rows exist for a slate id.

    No auth required (read-only). Lets us verify ingest worked.
    """
    from app.db import get_client

    db = get_client()
    resp = (
        db.table("slate_players")
        .select("dk_player_id, salary, player_id", count="exact")
        .eq("slate_id", slate_id)
        .order("salary", desc=True)
        .limit(50)
        .execute()
    )
    rows = resp.data or []
    count = getattr(resp, "count", None)
    if count is None:
        count = len(rows)
    return {
        "slate_id": slate_id,
        "count": count,
        "sample": rows[:50],
    }
