# ─── START — app/routes/weather.py — replace entire file with this ───
"""Admin routes for weather refresh.

Exposed:
    POST /api/admin/weather/refresh?slate_id=X&force=false
        Refreshes weather for every match on the slate. Idempotent within
        the MIN_REFRESH_MINUTES window unless force=true.

    GET  /api/admin/weather/status?slate_id=X
        Per-match weather state for a slate. Lightweight diagnostic — useful
        when troubleshooting why a match isn't showing weather.

Both endpoints are admin-gated via a self-contained header check
(X-Admin-Token must match the ADMIN_TOKEN env var). Self-contained because
the v6.5 codebase doesn't standardize an admin_required dependency yet.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from app.db import get_client
from app.services.weather import refresh_weather_for_slate
from app.services.tennis_venues import lookup_venue

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/weather", tags=["admin", "weather"])


def _check_admin(x_admin_token: Optional[str]) -> None:
    """Self-contained admin gate. Compares X-Admin-Token header to the
    ADMIN_TOKEN env var. Raises 401 on mismatch and 500 if env var unset.
    """
    expected = os.getenv("ADMIN_TOKEN", "").strip()
    if not expected:
        raise HTTPException(
            500,
            "ADMIN_TOKEN env var not set on the server; admin routes "
            "are unavailable until configured.",
        )
    if not x_admin_token or x_admin_token.strip() != expected:
        raise HTTPException(401, "missing or invalid X-Admin-Token header")


@router.post("/refresh")
def post_refresh(
    slate_id: str = Query(...),
    force: bool = Query(False),
    x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
) -> Dict[str, Any]:
    """Trigger a weather refresh for the given slate.

    `force=true` bypasses the per-match freshness check and re-fetches every
    match's forecast, regardless of how recently it was last updated. Use
    sparingly — eats AccuWeather API budget.
    """
    _check_admin(x_admin_token)
    if not slate_id:
        raise HTTPException(400, "slate_id required")
    return refresh_weather_for_slate(slate_id, force=force)


@router.get("/status")
def get_status(
    slate_id: str = Query(...),
    x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
) -> Dict[str, Any]:
    """Return per-match weather diagnostic data for a slate."""
    _check_admin(x_admin_token)
    if not slate_id:
        raise HTTPException(400, "slate_id required")

    db = get_client()
    rows: List[Dict[str, Any]] = (
        db.table("matches")
        .select("id, tournament, start_time, weather, "
                "player_a:player_a_id(display_name), "
                "player_b:player_b_id(display_name)")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    out_matches: List[Dict[str, Any]] = []
    for m in rows:
        venue = lookup_venue(m.get("tournament"))
        weather = m.get("weather") if isinstance(m.get("weather"), dict) else None
        out_matches.append({
            "match_id": m.get("id"),
            "tournament": m.get("tournament"),
            "start_time": m.get("start_time"),
            "player_a": (m.get("player_a") or {}).get("display_name"),
            "player_b": (m.get("player_b") or {}).get("display_name"),
            "venue_resolved": venue is not None,
            "venue_name": (venue or {}).get("name"),
            "weather_present": weather is not None,
            "weather_fetched_at": (weather or {}).get("fetched_at"),
            "is_indoor": (weather or {}).get("is_indoor"),
            "condition": ((weather or {}).get("forecast") or {}).get("condition"),
            "temperature_f": ((weather or {}).get("forecast") or {}).get("temperature_f"),
        })

    return {
        "slate_id": slate_id,
        "match_count": len(out_matches),
        "matches": out_matches,
    }
# ─── END — app/routes/weather.py ───
