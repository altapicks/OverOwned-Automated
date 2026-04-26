"""Admin routes for weather refresh.

Exposed:
    POST /api/admin/weather/refresh?slate_id=X&force=false
        Refreshes weather for every match on the slate. Idempotent within
        the MIN_REFRESH_MINUTES window unless force=true.

    GET  /api/admin/weather/status?slate_id=X
        Per-match weather state for a slate. Lightweight diagnostic — useful
        when troubleshooting why a match isn't showing weather.

Both endpoints are admin-gated via the existing admin_required dependency,
matching the pattern used by /api/admin/contest-ownership.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.db import get_client
from app.deps import admin_required
from app.services.weather import refresh_weather_for_slate
from app.services.tennis_venues import lookup_venue

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/weather", tags=["admin", "weather"])


@router.post("/refresh")
def post_refresh(
    slate_id: str = Query(...),
    force: bool = Query(False),
    _: Any = Depends(admin_required),
) -> Dict[str, Any]:
    """Trigger a weather refresh for the given slate.

    `force=true` bypasses the per-match freshness check and re-fetches every
    match's forecast, regardless of how recently it was last updated. Use
    sparingly — eats API budget.
    """
    if not slate_id:
        raise HTTPException(400, "slate_id required")
    return refresh_weather_for_slate(slate_id, force=force)


@router.get("/status")
def get_status(
    slate_id: str = Query(...),
    _: Any = Depends(admin_required),
) -> Dict[str, Any]:
    """Return per-match weather diagnostic data for a slate."""
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
