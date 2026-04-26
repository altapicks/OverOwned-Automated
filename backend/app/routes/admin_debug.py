"""Admin debug routes — auth-free for now since they're read-only or idempotent."""
from __future__ import annotations

import logging
import os
from collections import Counter
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Query

from app.config import get_settings
from app.db import get_client
from app.services import prizepicks_direct as pp_direct

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/debug", tags=["admin-debug"])


@router.get("/watcher/status")
def watcher_status() -> Dict[str, Any]:
    try:
        from app.workers import slate_watcher as sw
    except Exception as e:
        return {"error": f"watcher import failed: {e}"}

    sched = (
        getattr(sw, "_scheduler", None)
        or getattr(sw, "_SCHEDULER", None)
        or getattr(sw, "scheduler", None)
    )
    if sched is None:
        sw_attrs = sorted(a for a in dir(sw) if "sched" in a.lower())
        return {
            "alive": False,
            "reason": "no scheduler instance",
            "looked_for": ["_scheduler", "_SCHEDULER", "scheduler"],
            "module_scheduler_attrs": sw_attrs,
        }

    jobs = []
    try:
        for j in sched.get_jobs():
            jobs.append(
                {
                    "id": j.id,
                    "name": j.name,
                    "next_run": str(getattr(j, "next_run_time", None)),
                    "trigger": str(j.trigger),
                }
            )
    except Exception as e:
        jobs = [{"error": str(e)}]

    return {
        "alive": True,
        "running": getattr(sched, "running", None),
        "job_count": len(jobs),
        "jobs": jobs,
    }


@router.get("/sgo/probe")
def sgo_probe(
    league: str = Query("ATP", description="ATP or WTA"),
    limit: int = Query(2, ge=1, le=10),
) -> Dict[str, Any]:
    """Server-side probe of SportsGameOdds /v2/events for tennis.

    Returns the raw first event(s) so we can see the actual payload shape
    (teams vs players vs eventName, which odds keys are present, what the
    Pinnacle byBookmaker entries look like, etc.). Read-only.
    """
    s = get_settings()
    if not s.sgo_api_key:
        return {"error": "SGO_API_KEY not configured"}

    url = "https://api.sportsgameodds.com/v2/events"
    params = {
        "leagueID": league,
        "type": "match",
        "oddsAvailable": "true",
        "limit": limit,
    }
    headers = {"x-api-key": s.sgo_api_key}

    try:
        with httpx.Client(timeout=20.0) as client:
            r = client.get(url, params=params, headers=headers)
            status = r.status_code
            try:
                body = r.json()
            except Exception:
                return {"http": status, "non_json_body": r.text[:2000]}
    except Exception as e:
        return {"error": f"sgo_request_failed: {e}"}

    if status != 200:
        return {"http": status, "body": body}

    events = body.get("data") or []
    summary = {
        "http": status,
        "league": league,
        "event_count": len(events),
        "top_level_keys": sorted(list(body.keys())),
    }
    if not events:
        summary["raw"] = body
        return summary

    first = events[0]
    summary["first_event_top_keys"] = sorted(list(first.keys()))
    summary["first_event_teams"] = first.get("teams")
    summary["first_event_players"] = first.get("players")
    summary["first_event_eventName"] = first.get("eventName") or first.get("name")
    summary["first_event_status"] = first.get("status")
    summary["first_event_startsAt"] = first.get("startsAt") or (first.get("status") or {}).get("startsAt")
    odds = first.get("odds") or {}
    summary["first_event_odds_key_count"] = len(odds)
    summary["first_event_odds_keys_sample"] = sorted(list(odds.keys()))[:60]

    # Show full body for one odds entry so we can see byBookmaker shape
    if odds:
        sample_key = next(iter(odds.keys()))
        summary["first_event_odds_sample_entry"] = {sample_key: odds[sample_key]}

    # Full first event (capped) so I can see anything I missed
    summary["first_event_full"] = first

    if len(events) > 1:
        second = events[1]
        summary["second_event_summary"] = {
            "top_keys": sorted(list(second.keys())),
            "teams": second.get("teams"),
            "players": second.get("players"),
            "eventName": second.get("eventName") or second.get("name"),
            "odds_key_count": len(second.get("odds") or {}),
        }

    return summary


@router.get("/pp/probe")
def pp_probe(
    url: str = Query("https://api.prizepicks.com/projections?league_id=5&per_page=500&single_stat=true"),
    render: bool = Query(False),
) -> Dict[str, Any]:
    return pp_direct.oxy_probe(url, render=render)


@router.get("/pp/diagnose")
def pp_diagnose() -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "oxylabs_configured": bool(os.getenv("OXYLABS_USERNAME") and os.getenv("OXYLABS_PASSWORD")),
        "leagues_status": {},
        "tennis_leagues": [],
        "per_league": [],
        "raw_first_projection_attributes": None,
        "name_resolution": {},
    }

    status, leagues = pp_direct.fetch_leagues()
    out["leagues_status"] = {"http": status, "total_leagues_returned": len(leagues)}
    if status != 200:
        return out

    tennis_lgs: List[Dict[str, Any]] = []
    for lg in leagues:
        attr = lg.get("attributes", {}) or {}
        name = (attr.get("name") or "").strip()
        if name.upper().startswith("TENNIS"):
            tennis_lgs.append({"id": str(lg.get("id") or ""), "name": name, "sport": attr.get("sport") or ""})
    out["tennis_leagues"] = tennis_lgs

    raw_first_dump_set = False
    sample_unresolved: List[str] = []
    sample_resolved: List[str] = []
    tested_n = 0

    for lg in tennis_lgs:
        if lg["name"].upper() == "TENNIS LIVE":
            out["per_league"].append({"league_id": lg["id"], "league_name": lg["name"], "skipped": "live_league"})
            continue
        lid = lg["id"]
        status, body, err = pp_direct.fetch_projections(lid)
        if status != 200:
            out["per_league"].append(
                {
                    "league_id": lid,
                    "league_name": lg["name"],
                    "http": status,
                    "error": err,
                    "projections_count": 0,
                    "included_players_count": 0,
                    "odds_type_histogram": {},
                    "stat_histogram": {},
                    "sample_projections": [],
                }
            )
            continue

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
                sample_projs.append(
                    {
                        "player": (pinfo.get("name") or "").strip(),
                        "stat_type": stat_type,
                        "odds_type": odds_type,
                        "line": attr.get("line_score"),
                    }
                )

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

        out["per_league"].append(
            {
                "league_id": lid,
                "league_name": lg["name"],
                "http": status,
                "projections_count": len(data),
                "included_players_count": len(player_idx),
                "odds_type_histogram": dict(odds_hist),
                "stat_histogram": dict(stat_hist),
                "sample_projections": sample_projs,
            }
        )

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


@router.get("/pp/run")
def pp_run_get(slate_id: str = Query(...)) -> Dict[str, Any]:
    return pp_direct.run_prizepicks_direct(slate_id)


@router.post("/pp/run")
def pp_run(slate_id: str = Query(...)) -> Dict[str, Any]:
    return pp_direct.run_prizepicks_direct(slate_id)
