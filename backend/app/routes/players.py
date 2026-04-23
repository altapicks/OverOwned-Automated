"""Admin API for the players master table and unmatched queue.

Used by:
  - Dev: curl during name-matching fixup
  - Eventually: an admin UI in the React app for resolving unmatched names
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import get_client

router = APIRouter(prefix="/api/players", tags=["players"])


@router.get("/master")
async def list_master(sport: str = "tennis", limit: int = 500):
    db = get_client()
    rows = (
        db.table("players")
        .select("canonical_id, display_name, aliases, country, hand")
        .eq("sport", sport)
        .order("display_name")
        .limit(limit)
        .execute()
        .data
        or []
    )
    return {"players": rows}


@router.get("/unmatched")
async def list_unmatched(sport: str = "tennis", resolved: bool = False):
    db = get_client()
    rows = (
        db.table("unmatched_names")
        .select("*")
        .eq("sport", sport)
        .eq("resolved", resolved)
        .order("first_seen_at", desc=True)
        .limit(200)
        .execute()
        .data
        or []
    )
    return {"unmatched": rows}


class ResolveRequest(BaseModel):
    resolve_to: str  # canonical_id to merge into


@router.post("/unmatched/{id_}/resolve")
async def resolve_unmatched(id_: int, body: ResolveRequest):
    """Manually resolve an unmatched name: mark it resolved and add to the
    target player's aliases."""
    db = get_client()
    row = (
        db.table("unmatched_names").select("*").eq("id", id_).single().execute().data
    )
    if not row:
        raise HTTPException(404, "Unmatched row not found")
    if row["resolved"]:
        return {"status": "already_resolved"}

    # Add alias
    target = (
        db.table("players")
        .select("aliases")
        .eq("canonical_id", body.resolve_to)
        .single()
        .execute()
        .data
    )
    if not target:
        raise HTTPException(404, f"Target player {body.resolve_to} not found")
    aliases = target.get("aliases") or {}
    source = row["source"]
    existing = aliases.get(source)
    if isinstance(existing, str) and existing != row["raw_name"]:
        aliases[source] = [existing, row["raw_name"]]
    elif isinstance(existing, list):
        if row["raw_name"] not in existing:
            existing.append(row["raw_name"])
            aliases[source] = existing
    else:
        aliases[source] = row["raw_name"]
    db.table("players").update({"aliases": aliases}).eq(
        "canonical_id", body.resolve_to
    ).execute()

    db.table("unmatched_names").update(
        {"resolved": True, "resolved_to": body.resolve_to}
    ).eq("id", id_).execute()
    return {"status": "ok", "resolved_to": body.resolve_to}
