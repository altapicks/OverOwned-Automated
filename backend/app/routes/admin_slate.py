"""Admin route for manual slate uploads — v6.0.

POST /api/admin/slates/upload  (multipart form, admin-gated)

Form fields:
  - csv:          file (required) — the slate CSV
  - sport:        string (required) — 'tennis' / 'mma' / etc.
  - slate_date:   string (required) — YYYY-MM-DD
  - tournament:   string (required) — display label (e.g. "Madrid")
  - surface:      string (optional) — 'clay' / 'hard' / 'grass'
  - lock_time:    string (optional) — ISO 8601; fallback for matches missing per-row start_time
  - dry_run:      bool   (optional, default false) — preview without writing

Response: result dict from manual_slate_ingest.ingest_manual_slate().
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile

from app.db import get_client
from app.services import manual_slate_ingest as ingest
from app.services import prizepicks_admin as pp_admin  # reuse is_admin_user

router = APIRouter(prefix="/api/admin/slates", tags=["admin"])

logger = logging.getLogger(__name__)


# ── Auth helper (same shape as tracker.require_admin) ────────────────
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


@router.post("/upload")
async def upload_manual_slate(
    csv: UploadFile = File(...),
    sport: str = Form(...),
    slate_date: str = Form(...),
    tournament: str = Form(...),
    surface: Optional[str] = Form(default=None),
    lock_time: Optional[str] = Form(default=None),
    dry_run: bool = Form(default=False),
    admin_user_id: str = Depends(require_admin),
) -> dict:
    """Ingest a manual slate from a CSV upload.

    On dry_run=true, validates and returns a preview without writing
    anything to the DB — used by the Admin tab's preview screen before
    "Publish Slate" is clicked.
    """
    raw = await csv.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        # Fallback for Excel "Save As CSV" with windows-1252
        try:
            text = raw.decode("windows-1252")
        except UnicodeDecodeError:
            raise HTTPException(400, "CSV file must be UTF-8 or Windows-1252 encoded")

    logger.info(
        "Manual slate upload: sport=%s date=%s tournament=%r dry_run=%s by user=%s",
        sport, slate_date, tournament, dry_run, admin_user_id,
    )

    result = ingest.ingest_manual_slate(
        csv_text=text,
        sport=sport,
        slate_date=slate_date,
        tournament=tournament,
        surface=surface,
        lock_time=lock_time,
        dry_run=dry_run,
    )
    return result
