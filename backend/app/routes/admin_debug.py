"""Admin debug endpoints — read-only diagnostics.

These never write to the DB. Used to verify provider integrations
(PrizePicks, SGO, Kalshi) when the silent "fetched=0" or
"matched=0" pattern surfaces in production.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from app.config import get_settings
from app.db import get_client
from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/debug", tags=["admin-debug"])


PP_BASE = "https://api.prizepicks.com"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_PP_HEADERS = {
    "User-Agent": _UA,
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://app.prizepicks.com",
    "Referer": "https://app.prizepicks.com/",
}


@router.get("/pp/diagnose")
async def pp_diagnose() -> dict[str, Any]:
    """Read-only PrizePicks diagnostic.

    Returns:
      - leagues_status: HTTP status + count of tennis leagues found
      - tennis_leagues: list of {id, name, sport} dicts that matched
      - per_league: for each tennis league_id, the raw PP response
                    summary (HTTP status, count of projections, sample
                    of (player_name, stat_type, line))
      - name_resolution: against the players table — how many sample
                         names normalize to a canonical_id, sample of
                         unmatched names
    """
    out: dict[str, Any] = {
        "leagues_status": None,
        "tennis_leagues": [],
        "per_league": [],
        "name_resolution": {},
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        # 1. /leagues
        try:
            r = await client.get(f"{PP_BASE}/leagues", headers=_PP_HEADERS)
            out["leagues_status"] = {
                "http": r.status_code,
                "ct": r.headers.get("content-type"),
                "body_size": len(r.content),
            }
            payload = r.json() if r.status_code == 200 else {}
        except Exception as e:
            out["leagues_status"] = {"error": repr(e)}
            return out

        all_leagues = payload.get("data") or []
        tennis_leagues = []
        for item in all_leagues:
            attrs = item.get("attributes") or {}
            name = (attrs.get("name") or "")
            sport = (attrs.get("sport") or "")
            if "tennis" in name.lower() or "tennis" in sport.lower():
                tennis_leagues.append(
                    {
                        "id": item.get("id"),
                        "name": name,
                        "sport": sport,
                    }
                )
        out["tennis_leagues"] = tennis_leagues
        out["leagues_status"]["total_leagues_returned"] = len(all_leagues)

        if not tennis_leagues:
            # Show a sample so we know what PP is returning
            out["sample_first_5_leagues"] = [
                {
                    "id": item.get("id"),
                    "name": (item.get("attributes") or {}).get("name"),
                    "sport": (item.get("attributes") or {}).get("sport"),
                }
                for item in all_leagues[:5]
            ]
            return out

        # 2. /projections per tennis league
        sample_names: list[str] = []
        for lg in tennis_leagues:
            lid = lg["id"]
            try:
                r = await client.get(
                    f"{PP_BASE}/projections",
                    params={
                        "league_id": lid,
                        "per_page": 500,
                        "single_stat": "true",
                    },
                    headers=_PP_HEADERS,
                )
                proj_payload = r.json() if r.status_code == 200 else {}
            except Exception as e:
                out["per_league"].append(
                    {"league_id": lid, "error": repr(e)}
                )
                continue

            data = proj_payload.get("data") or []
            included = proj_payload.get("included") or []
            player_lookup: dict[str, str] = {}
            for inc in included:
                if (inc.get("type") or "").lower() != "new_player":
                    continue
                attrs = inc.get("attributes") or {}
                nm = attrs.get("name") or attrs.get("display_name")
                if nm and inc.get("id"):
                    player_lookup[str(inc["id"])] = nm

            stat_histogram: dict[str, int] = {}
            samples: list[dict] = []
            for item in data:
                attrs = item.get("attributes") or {}
                stat = attrs.get("stat_type") or ""
                stat_histogram[stat] = stat_histogram.get(stat, 0) + 1
                if len(samples) < 5:
                    rel = (item.get("relationships") or {}).get("new_player") or {}
                    pdata = rel.get("data") or {}
                    pid = pdata.get("id")
                    nm = player_lookup.get(str(pid)) if pid else None
                    if nm:
                        samples.append(
                            {
                                "player": nm,
                                "stat_type": stat,
                                "line": attrs.get("line_score"),
                            }
                        )
                    if nm and nm not in sample_names:
                        sample_names.append(nm)

            out["per_league"].append(
                {
                    "league_id": lid,
                    "league_name": lg["name"],
                    "http": r.status_code,
                    "projections_count": len(data),
                    "included_players_count": len(player_lookup),
                    "stat_histogram": stat_histogram,
                    "sample_projections": samples,
                }
            )

        # 3. Name resolution check (don't write — just resolve)
        if sample_names:
            try:
                normalizer = PlayerNormalizer(sport="tennis")
                resolved = []
                unresolved = []
                for nm in sample_names[:25]:
                    res = normalizer.resolve(
                        nm, source="prizepicks", create_if_missing=False
                    )
                    if res.canonical_id:
                        resolved.append({"name": nm, "cid": res.canonical_id})
                    else:
                        unresolved.append(nm)
                out["name_resolution"] = {
                    "tested": len(sample_names[:25]),
                    "resolved_count": len(resolved),
                    "unresolved_count": len(unresolved),
                    "sample_resolved": resolved[:5],
                    "sample_unresolved": unresolved[:10],
                }
            except Exception as e:
                out["name_resolution"] = {"error": repr(e)}

    return out


@router.get("/watcher/status")
async def watcher_status() -> dict[str, Any]:
    """Confirms the in-process scheduler is alive and lists its jobs."""
    try:
        from app.workers.slate_watcher import _scheduler
    except Exception as e:
        return {"alive": False, "import_error": repr(e)}

    if _scheduler is None:
        return {"alive": False, "reason": "scheduler is None (lifespan not started)"}

    jobs = []
    for j in _scheduler.get_jobs():
        jobs.append(
            {
                "id": j.id,
                "next_run": str(j.next_run_time) if j.next_run_time else None,
                "trigger": str(j.trigger),
            }
        )
    return {"alive": True, "running": _scheduler.running, "jobs": jobs}
