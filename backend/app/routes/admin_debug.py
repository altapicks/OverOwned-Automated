"""Admin debug endpoints — read-only diagnostics.

Uses curl_cffi (Chrome TLS impersonation) for PrizePicks calls
because PP's Cloudflare CDN 403s plain httpx requests.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter

from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/debug", tags=["admin-debug"])

PP_BASE = "https://api.prizepicks.com"


def _have_curl_cffi() -> bool:
    try:
        import curl_cffi.requests  # noqa: F401
        return True
    except Exception:
        return False


async def _http_get_json(url: str, params: dict | None = None) -> tuple[int, dict, int]:
    """Returns (status, parsed_json, body_size_bytes)."""
    if _have_curl_cffi():
        from curl_cffi.requests import AsyncSession
        async with AsyncSession(impersonate="chrome120") as session:
            r = await session.get(url, params=params, timeout=20)
            try:
                payload = r.json()
            except Exception:
                payload = {}
            return r.status_code, payload, len(r.content)

    import httpx
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, params=params,
                             headers={"User-Agent": "Mozilla/5.0",
                                      "Accept": "application/json"})
        try:
            payload = r.json()
        except Exception:
            payload = {}
        return r.status_code, payload, len(r.content)


@router.get("/pp/diagnose")
async def pp_diagnose() -> dict[str, Any]:
    out: dict[str, Any] = {
        "curl_cffi_installed": _have_curl_cffi(),
        "leagues_status": None,
        "tennis_leagues": [],
        "per_league": [],
        "name_resolution": {},
    }

    # 1. /leagues
    try:
        status, payload, size = await _http_get_json(f"{PP_BASE}/leagues")
        out["leagues_status"] = {"http": status, "body_size": size}
    except Exception as e:
        out["leagues_status"] = {"error": repr(e)}
        return out

    if status != 200:
        out["leagues_status"]["error"] = "non-200; check curl_cffi install"
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
