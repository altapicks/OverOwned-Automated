"""Public API for slates. Consumed by the React frontend."""
from fastapi import APIRouter, HTTPException

from app.services.slate_reader import get_frontend_slate, get_today_slate, list_slates
from app.workers.slate_watcher import run_slate_watcher_once

router = APIRouter(prefix="/api/slates", tags=["slates"])


@router.get("/today")
async def today(sport: str = "tennis"):
    """Return the most recent active slate for a sport. Matches the existing
    slate.json schema exactly — the React app can swap its fetch URL here
    without any other changes."""
    slate = get_today_slate(sport)
    if not slate:
        raise HTTPException(status_code=404, detail=f"No active slate for sport={sport}")
    return slate.model_dump()


@router.get("/{slate_id}")
async def by_id(slate_id: str):
    slate = get_frontend_slate(slate_id)
    if not slate:
        raise HTTPException(status_code=404, detail="Slate not found")
    return slate.model_dump()


@router.get("/manifest/{sport}")
async def manifest(sport: str, limit: int = 30):
    """Archive manifest. Replaces the static /slates/{sport}/manifest.json
    pattern the frontend was using."""
    return {"slates": list_slates(sport, limit=limit)}


@router.post("/refresh")
async def refresh():
    """Manually trigger a slate watcher cycle. Useful for dev and for the
    admin panel. In production the scheduled worker handles this automatically."""
    result = await run_slate_watcher_once()
    return {"status": "ok", "result": result}
