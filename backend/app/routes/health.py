"""Health + status endpoints. Railway uses these for liveness probes."""
from datetime import datetime, timezone

from fastapi import APIRouter

from app import __version__
from app.db import get_client

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    """Liveness probe. Checks DB connectivity as well."""
    db_ok = False
    last_ingest: str | None = None
    unmatched = 0
    try:
        db = get_client()
        # Light DB ping — this should be fast
        r = (
            db.table("ingestion_log")
            .select("started_at")
            .eq("status", "ok")
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        db_ok = True
        if r.data:
            last_ingest = r.data[0]["started_at"]
        um = (
            db.table("unmatched_names")
            .select("id", count="exact")
            .eq("resolved", False)
            .execute()
        )
        unmatched = um.count or 0
    except Exception:
        db_ok = False

    return {
        "status": "ok" if db_ok else "degraded",
        "version": __version__,
        "db_ok": db_ok,
        "last_successful_ingest": last_ingest,
        "unmatched_count": unmatched,
        "now": datetime.now(timezone.utc).isoformat(),
    }
