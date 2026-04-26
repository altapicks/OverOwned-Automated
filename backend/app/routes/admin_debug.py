"""
Admin debug routes for diagnosing PP ingest, watcher, and slate state.

Read-only endpoints (/pp/diagnose, /pp/lines, /watcher/status) are open
because they don't write or cost anything. Write/expensive endpoints
(/pp/run) require ADMIN_TOKEN.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from app.db import get_client
from app.services import prizepicks_direct as pp_direct

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/debug", tags=["admin-debug"])


def _check_admin(request: Request, token: Optional[str]) -> None:
    expected = os.getenv("ADMIN_TOKEN", "")
    if not expected:
        raise HTTPException(500, "ADMIN_TOKEN not configured")
    provided = (
        request.headers.get("X-Admin-Token")
        or request.headers.get("x-admin-token")
        or token
        or ""
    )
    if provided.strip() != expected.strip():
        raise HTTPException(401, "bad admin token")


# ---------------------------------------------------------------------------
# Read-only — no auth
# ---------------------------------------------------------------------------


@router.get("/watcher/status")
def watcher_status() -> Dict[str, Any]:
    try:
        from app.workers import slate_watcher as sw
    except Exception as e:
        return {"error": f"watcher import failed: {e}"}

    sched = getattr(sw, "_SCHEDULER", None) or getattr(sw, "scheduler", None)
    if sched is None:
        return {"alive": False, "reason": "no scheduler instance"}

    jobs = []
    try:
        for j in sched.get_jobs():
            jobs.append({
                "id": j.id,
                "name": j.name,
                "next_run": str(getattr(j, "next_run_time", None)),
                "trigger": str(j.trigger),
            })
    except Exception as e:
        jobs = [{"error": str(e)}]

    return {
        "alive": True,
        "running": getattr(sched, "running", None),
        "job_count": len(jobs),
        "jobs": jobs,
    }


@router.get("/pp/diagnose")
def pp_diagnose() -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "oxylabs_configured": bool(
            os.getenv("OXYLABS_USERNAME") and os.getenv("OXYLABS_PASSWORD")
        ),
        "leagues_status": {},
        "tennis_leagues": [],
        "per_league": [],
        "raw_first_projection_attributes": None,
        "name_resolution": {},
    }

    status, leagues = pp_direct.fetch_leagues()
    out["leagues_status"] = {
        "http": status,
        "total_leagues_returned": len(leagues),
    }
    if status != 200:
        return out

    tennis_lgs: List[Dict[str, Any]] = []
    for lg in leagues:
        attr = lg.get("attributes", {}) or {}
        name = (attr.get("name") or "").strip()
        if name.upper().startswith("TENNIS"):
            tennis_lgs.append({
                "id": str(lg.get("id") or ""),
                "name": name,
                "sport": attr.get("sport") or "",
            })
    out["tennis_leagues"] = tennis_lgs

    raw_first_dump_set = False
    sample_unresolved: List[str] = []
    sample_resolved: List[str] = []
    tested_n = 0

    for lg in tennis_lgs:
        if lg["name"].upper() == "TENNIS LIVE":
            out["per_league"].append({
                "league_id": lg["id"],
                "league_name": lg["name"],
                "skipped": "live_league",
            })
            continue

        lid = lg["id"]
        status, body = pp_direct.fetch_projections(lid)
        data = body.get("data", []) or []
        included = body.get("included", []) or []
        player_idx = pp_direct._build_player_index(included)

        odds_hist: Counter = Counter()
        stat_hist: Counter = Counter()
        sample_projs: List[Dict[str, Any]] = []

        for i, proj in enumerate(data):
            attr = proj.get("attributes", {}) or {}
            stat_type = (attr.get("stat_type") or "").strip()
            odds_type = pp_direct._normalize_odds_type(attr.get("odds_type"))
            stat_hist[stat_type] += 1
            odds_hist[odds_type] += 1

            if not raw_first_dump_set and i == 0:
                out["raw_first_projection_attributes"] = attr
                raw_first_dump_set = True

            if len(sample_projs) < 8:
                rel = (proj.get("relationships") or {}).get("new_player") or {}
                pp_pid = str((rel.get("data") or {}).get("id") or "")
                pinfo = player_idx.get(pp_pid, {})
                sample_projs.append({
                    "player": (pinfo.get("name") or "").strip(),
                    "stat_type": stat_type,
                    "odds_type": odds_type,
                    "line": attr.get("line_score"),
                })

            if tested_n < 5:
                rel = (proj.get("relationships") or {}).get("new_player") or {}
                pp_pid = str((rel.get("data") or {}).get("id") or "")
                pinfo = player_idx.get(pp_pid, {})
                raw_name = (pinfo.get("name") or "").strip()
                if raw_name and raw_name not in sample_resolved + sample_unresolved:
                    tested_n += 1
                    res = pp_direct._resolve_player(raw_name)
                    if res:
                        sample_resolved.append(raw_name)
                    else:
                        sample_unresolved.append(raw_name)

        out["per_league"].append({
            "league_id": lid,
            "league_name": lg["name"],
            "http": status,
            "projections_count": len(data),
            "included_players_count": len(player_idx),
            "odds_type_histogram": dict(odds_hist),
            "stat_histogram": dict(stat_hist),
            "sample_projections": sample_projs,
        })

    out["name_resolution"] = {
        "tested": tested_n,
        "resolved_count": len(sample_resolved),
        "unresolved_count": len(sample_unresolved),
        "sample_resolved": sample_resolved[:10],
        "sample_unresolved": sample_unresolved[:10],
    }

    return out


@router.get("/pp/lines")
def pp_lines(slate_id: str = Query(...)) -> Dict[str, Any]:
    db = get_client()
    resp = (
        db.table("prizepicks_lines")
        .select("player_id,raw_player_name,stat_type,odds_type,current_line,is_active")
        .eq("slate_id", slate_id)
        .eq("is_active", True)
        .order("raw_player_name")
        .order("stat_type")
        .order("odds_type")
        .execute()
    )
    rows = resp.data or []
    by_stat: Counter = Counter()
    by_odds: Counter = Counter()
    by_stat_odds: Counter = Counter()
    for r in rows:
        by_stat[r["stat_type"]] += 1
        by_odds[r["odds_type"]] += 1
        by_stat_odds[(r["stat_type"], r["odds_type"])] += 1
    return {
        "slate_id": slate_id,
        "row_count": len(rows),
        "by_stat_type": dict(by_stat),
        "by_odds_type": dict(by_odds),
        "by_stat_and_odds": {f"{k[0]}|{k[1]}": v for k, v in by_stat_odds.items()},
        "sample": rows[:30],
    }


# ---------------------------------------------------------------------------
# Write — keeps token gate (costs Oxylabs credits + writes to DB)
# ---------------------------------------------------------------------------


@router.post("/pp/run")
def pp_run(
    request: Request,
    slate_id: str = Query(...),
    token: Optional[str] = Query(None),
) -> Dict[str, Any]:
    _check_admin(request, token)
    return pp_direct.run_prizepicks_direct(slate_id)


@router.get("/pp/run")
def pp_run_get(
    request: Request,
    slate_id: str = Query(...),
    token: Optional[str] = Query(None),
) -> Dict[str, Any]:
    _check_admin(request, token)
    return pp_direct.run_prizepicks_direct(slate_id)
