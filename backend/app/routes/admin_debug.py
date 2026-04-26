"""Admin debug endpoints — read-only diagnostics.

PrizePicks calls go through Oxylabs Web Scraper API to bypass Cloudflare.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter

from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/debug", tags=["admin-debug"])

PP_BASE = "https://api.prizepicks.com"
OXY_ENDPOINT = "https://realtime.oxylabs.io/v1/queries"


def _oxy_creds() -> tuple[str, str] | None:
    user = os.getenv("OXYLABS_USERNAME")
    pw = os.getenv("OXYLABS_PASSWORD")
    if not user or not pw:
        return None
    return user, pw


async def _http_get_json(url: str, params: dict | None = None) -> tuple[int, dict, int]:
    """Returns (target_status, target_json, body_size_bytes_of_envelope)."""
    creds = _oxy_creds()
    if not creds:
        return 0, {"_error": "no oxylabs creds"}, 0

    user, pw = creds

    if params:
        from urllib.parse import urlencode
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{urlencode(params)}"

    body = {
        "source": "universal",
        "url": url,
        "geo_location": "United States",
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            OXY_ENDPOINT,
            json=body,
            auth=(user, pw),
            headers={"Content-Type": "application/json"},
        )

    if r.status_code != 200:
        return r.status_code, {"_error": "oxylabs non-200", "body": r.text[:500]}, len(r.content)

    try:
        env = r.json()
    except Exception:
        return 0, {"_error": "oxylabs envelope non-json"}, len(r.content)

    results = env.get("results") or []
    if not results:
        return 0, {"_error": "oxylabs no results"}, len(r.content)

    first = results[0]
    target_status = first.get("status_code") or 0
    raw_content = first.get("content") or ""

    if isinstance(raw_content, dict):
        return target_status, raw_content, len(r.content)
    try:
        return target_status, json.loads(raw_content), len(r.content)
    except (TypeError, json.JSONDecodeError):
        return target_status, {
            "_error": "target content not json",
            "first_200": str(raw_content)[:200],
        }, len(r.content)


@router.get("/pp/diagnose")
async def pp_diagnose() -> dict[str, Any]:
    out: dict[str, Any] = {
        "oxylabs_configured": _oxy_creds() is not None,
        "leagues_status": None,
        "tennis_leagues": [],
        "per_league": [],
        "name_resolution": {},
    }

    if not _oxy_creds():
        out["leagues_status"] = {"error": "OXYLABS_USERNAME/PASSWORD env vars not set"}
        return out

    # 1. /leagues
    try:
        status, payload, size = await _http_get_json(f"{PP_BASE}/leagues")
        out["leagues_status"] = {"http": status, "envelope_size": size}
    except Exception as e:
        out["leagues_status"] = {"error": repr(e)}
        return out

    if status != 200 or "_error" in payload:
        out["leagues_status"]["error"] = payload.get("_error", "non-200")
        if "first_200" in payload:
            out["leagues_status"]["first_200"] = payload["first_200"]
        if "body" in payload:
            out["leagues_status"]["body"] = payload["body"]
        return out

    all_leagues = payload.get("data") or []
    out["leagues_status"]["total_leagues_returned"] = len(all_leagues)

    tennis_leagues = []
    for item in all_leagues:
        attrs = item.get("attributes") or {}
        name = (attrs.get("name") or "")
        sport = (attrs.get("sport") or "")
        if "tennis" in name.lower() or "tennis" in sport.lower():
            tennis_leagues.append(
                {"id": item.get("id"), "name": name, "sport": sport}
            )
    out["tennis_leagues"] = tennis_leagues

    if not tennis_leagues:
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
            pstatus, proj_payload, _ = await _http_get_json(
                f"{PP_BASE}/projections",
                params={"league_id": lid, "per_page": 500, "single_stat": "true"},
            )
        except Exception as e:
            out["per_league"].append({"league_id": lid, "error": repr(e)})
            continue

        if pstatus != 200 or "_error" in proj_payload:
            out["per_league"].append(
                {"league_id": lid, "league_name": lg["name"],
                 "http": pstatus, "error": proj_payload.get("_error")}
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
                        {"player": nm, "stat_type": stat,
                         "line": attrs.get("line_score")}
                    )
                if nm and nm not in sample_names:
                    sample_names.append(nm)

        out["per_league"].append(
            {
                "league_id": lid,
                "league_name": lg["name"],
                "http": pstatus,
                "projections_count": len(data),
                "included_players_count": len(player_lookup),
                "stat_histogram": stat_histogram,
                "sample_projections": samples,
            }
        )

    # 3. Name resolution
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
