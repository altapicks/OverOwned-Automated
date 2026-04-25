"""Admin route for DK auto-ingest — v6.2.

POST /api/admin/dk/fetch-featured (admin-gated, JSON body optional)

Triggers an immediate DK auto-ingest cycle for the configured sport(s).
Useful for:
  - First-run verification before flipping DK_AUTO_INGEST_ENABLED=true
  - Manually re-pulling the slate after DK posts a corrected version
  - Testing after the daily 11:00 UTC cron runs

Body (optional):
  {
    "sport": "TEN"     // default: "TEN"
  }

Response: the same summary dict run_dk_auto_ingest_tick / fetch_featured_slate
emits — includes status, draft_group_id, slate_label, and ingest counts.

Notes:
  - Runs regardless of DK_AUTO_INGEST_ENABLED. The flag only gates the
    automatic daily cron; manual admin triggers always work.
  - Idempotent: ingest_draft_group keys on dk_draft_group_id, so repeat
    calls just refresh the existing slate row.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.db import get_client
from app.services import dk_auto_ingest
from app.services import prizepicks_admin as pp_admin  # reuse is_admin_user

router = APIRouter(prefix="/api/admin/dk", tags=["admin"])
logger = logging.getLogger(__name__)


# ── Auth helper (same shape as admin_slate.require_admin) ─────────────
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
    if not pp_admin.is_admin_user(user_id):
        raise HTTPException(403, "Admin access required")
    return user_id


class FetchFeaturedRequest(BaseModel):
    sport: str = "TEN"  # DK sport code (TEN, NBA, MMA, NFL)


@router.post("/fetch-featured")
async def fetch_featured(
    body: Optional[FetchFeaturedRequest] = None,
    admin_user_id: str = Depends(require_admin),
) -> dict:
    """Trigger an immediate DK Featured-Classic auto-ingest.

    Bypasses the dk_auto_ingest_enabled flag — manual admin action is
    always permitted, the flag only controls the daily cron.
    """
    sport_code = (body.sport if body else "TEN").upper().strip()
    logger.info(
        "Admin DK fetch-featured: sport=%s by user=%s",
        sport_code,
        admin_user_id,
    )
    try:
        result = await dk_auto_ingest.fetch_featured_slate(sport_code)
    except Exception as e:
        logger.exception("Admin DK fetch-featured failed: %s", e)
        raise HTTPException(500, f"DK auto-ingest failed: {e}")
    return result
