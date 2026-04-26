"""
PrizePicks direct ingest via Oxylabs Web Scraper API.

Captures all 3 PP variants per (player, stat_type): standard / demon / goblin.
Median across the three feeds the DK engine via slate_reader's posted_lines
projection — see slate_reader._project_posted_lines_for_match.
"""
from __future__ import annotations

import json as _json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.db import get_client
from app.services.normalizer import PlayerNormalizer

log = logging.getLogger(__name__)

OXY_USER = os.getenv("OXYLABS_USERNAME", "")
OXY_PASS = os.getenv("OXYLABS_PASSWORD", "")
OXY_ENDPOINT = "https://realtime.oxylabs.io/v1/queries"

PP_BASE = "https://api.prizepicks.com"
TENNIS_LEAGUE_NAME_ALLOW = {"TENNIS"}

ALLOWED_STAT_TYPES = {
    "Aces",
    "Break Points Won",
    "Double Faults",
    "Fantasy Score",
    "Total Games",
    "Total Games Won",
    "Total Sets",
    "Total Tie Breaks",
}

PP_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://app.prizepicks.com/",
    "Origin": "https://app.prizepicks.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


# ---------------------------------------------------------------------------
# Player resolver
# ---------------------------------------------------------------------------
def _resolve_player(raw_name: str, sport: str = "tennis") -> Optional[str]:
    """
    Resolves PP raw player names to canonical_id. Auto-creates new players
    if not already in the DB so PP-only entrants (qualifiers, ITF crossover,
    etc.) still land.
    """
    if not raw_name:
        return None
    try:
        normalizer = PlayerNormalizer(sport=sport)
        result = normalizer.resolve(
            raw_name, source="prizepicks", create_if_missing=True
        )
        if result.auto_resolved or result.was_new:
            return result.canonical_id
        log.warning(
            "_resolve_player low_confidence raw=%r best=%r score=%s",
            raw_name,
            result.canonical_id,
            result.score,
        )
        return None
    except Exception as e:
        log.warning("resolve_player failed for %s: %s", raw_name, e)
        return None


# ---------------------------------------------------------------------------
# Oxylabs fetch
# ---------------------------------------------------------------------------
def _oxy_payload(url: str, render: bool = False) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "source": "universal",
        "url": url,
        "geo_location": "United States",
        "user_agent_type": "desktop",
        "headers": PP_HEADERS,
    }
    if render:
        p["render"] = "html"
    return p


def oxy_probe(url: str, render: bool = False, timeout: float = 60.0) -> Dict[str, Any]:
    if not (OXY_USER and OXY_PASS):
        return {"error": "oxylabs_creds_missing"}
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(
                OXY_ENDPOINT,
                auth=(OXY_USER, OXY_PASS),
                json=_oxy_payload(url, render=render),
            )
            out: Dict[str, Any] = {
                "envelope_status": r.status_code,
                "envelope_size": len(r.content),
            }
            try:
                env = r.json()
            except Exception:
                out["envelope_raw_first_1000"] = r.text[:1000]
                return out
            results = env.get("results", []) or []
            out["job"] = env.get("job", {})
            out["result_count"] = len(results)
            if results:
                first = results[0]
                content = first.get("content")
                content_preview: Any = None
                if isinstance(content, str):
                    content_preview = content[:600]
                elif isinstance(content, dict):
                    content_preview = {
                        k: (str(v)[:200] if not isinstance(v, (dict, list)) else type(v).__name__)
                        for k, v in list(content.items())[:10]
                    }
                out["first_result"] = {
                    "status_code": first.get("status_code"),
                    "url": first.get("url"),
                    "task_id": first.get("task_id"),
                    "created_at": first.get("created_at"),
                    "updated_at": first.get("updated_at"),
                    "content_type": type(content).__name__,
                    "content_preview": content_preview,
                }
            return out
    except Exception as e:
        return {"error": f"probe_exception: {e}"}


def _oxy_fetch_json(
    url: str, timeout: float = 60.0, render: bool = False
) -> Tuple[int, Dict[str, Any], str]:
    if not (OXY_USER and OXY_PASS):
        return 0, {}, "oxylabs_creds_missing"
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(
                OXY_ENDPOINT,
                auth=(OXY_USER, OXY_PASS),
                json=_oxy_payload(url, render=render),
            )
            if r.status_code != 200:
                return r.status_code, {}, f"oxy_envelope_http_{r.status_code}: {r.text[:300]}"
            env = r.json()
            results = env.get("results", []) or []
            if not results:
                return 0, {}, f"oxy_zero_results: job={env.get('job', {})}"
            first = results[0]
            inner_status = first.get("status_code", 0)
            content = first.get("content")
            if inner_status != 200:
                preview = ""
                if isinstance(content, str):
                    preview = content[:300]
                elif isinstance(content, dict):
                    preview = _json.dumps(content)[:300]
                return inner_status, {}, f"pp_inner_http_{inner_status}: {preview}"
            if isinstance(content, str):
                try:
                    content = _json.loads(content)
                except Exception as e:
                    return 0, {}, f"json_decode_failed: {e} :: {content[:300]}"
            if not isinstance(content, dict):
                return 0, {}, f"unexpected_content_type: {type(content).__name__}"
            return 200, content, ""
    except Exception as e:
        return 0, {}, f"oxy_exception: {e}"


# ---------------------------------------------------------------------------
# PP API surface
# ---------------------------------------------------------------------------
def fetch_leagues() -> Tuple[int, List[Dict[str, Any]]]:
    status, body, err = _oxy_fetch_json(f"{PP_BASE}/leagues")
    if status != 200:
        log.error("fetch_leagues failed: %s", err)
        return status, []
    return 200, body.get("data", []) or []


def fetch_projections(league_id: str) -> Tuple[int, Dict[str, Any], str]:
    url = f"{PP_BASE}/projections?league_id={league_id}&per_page=500&single_stat=true"
    status, body, err = _oxy_fetch_json(url, render=False)
    if status == 200:
        return status, body, ""
    log.warning(
        "fetch_projections plain failed (%s): %s — retrying with render", status, err
    )
    status2, body2, err2 = _oxy_fetch_json(url, render=True)
    if status2 == 200:
        return status2, body2, ""
    return status2, {}, f"plain={err} | render={err2}"


def get_tennis_league_ids() -> List[str]:
    status, leagues = fetch_leagues()
    if status != 200:
        return []
    out: List[str] = []
    for lg in leagues:
        attr = lg.get("attributes", {}) or {}
        name = (attr.get("name") or "").strip().upper()
        if name in TENNIS_LEAGUE_NAME_ALLOW:
            lid = str(lg.get("id") or "")
            if lid:
                out.append(lid)
    return out


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------
def _normalize_odds_type(raw: Optional[str]) -> str:
    if not raw:
        return "standard"
    s = str(raw).strip().lower()
    if s in {"demon", "goblin", "standard"}:
        return s
    return "standard"


def _build_player_index(included: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for inc in included:
        if (inc.get("type") or "").lower() != "new_player":
            continue
        pid = str(inc.get("id") or "")
        if not pid:
            continue
        idx[pid] = inc.get("attributes", {}) or {}
    return idx


# ---------------------------------------------------------------------------
# Main ingest
# ---------------------------------------------------------------------------
def run_prizepicks_direct(slate_id: str) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "slate_id": slate_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "leagues": [],
        "wrote": 0,
        "deactivated": 0,
        "row_failures": 0,
        "odds_type_histogram": {},
        "stat_histogram": {},
        "errors": [],
    }
    if not (OXY_USER and OXY_PASS):
        summary["errors"].append("oxylabs_creds_missing")
        return summary

    league_ids = get_tennis_league_ids()
    if not league_ids:
        summary["errors"].append("no_tennis_leagues_resolved")
        return summary

    db = get_client()
    overall_odds_hist: Counter = Counter()
    overall_stat_hist: Counter = Counter()
    total_wrote = 0
    total_deactivated = 0
    total_failures = 0

    for lid in league_ids:
        status, body, err = fetch_projections(lid)
        if status != 200:
            summary["leagues"].append(
                {"id": lid, "http": status, "wrote": 0, "error": err}
            )
            summary["errors"].append(f"projections_failed[{lid}]: {err}")
            continue

        data = body.get("data", []) or []
        included = body.get("included", []) or []
        player_idx = _build_player_index(included)

        grouped: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = defaultdict(dict)

        for proj in data:
            attr = proj.get("attributes", {}) or {}
            stat_type = (attr.get("stat_type") or "").strip()
            if stat_type not in ALLOWED_STAT_TYPES:
                continue
            odds_type = _normalize_odds_type(attr.get("odds_type"))
            line_score = attr.get("line_score")
            if line_score is None:
                continue

            rel = (proj.get("relationships") or {}).get("new_player") or {}
            pdata = rel.get("data") or {}
            pp_pid = str(pdata.get("id") or "")
            if not pp_pid:
                continue
            pinfo = player_idx.get(pp_pid, {})
            raw_name = (pinfo.get("name") or "").strip()
            if not raw_name:
                continue

            resolved = _resolve_player(raw_name)
            if not resolved:
                continue

            grouped[(stat_type, odds_type)][resolved] = {
                "slate_id": slate_id,
                "player_id": resolved,
                "raw_player_name": raw_name,
                "stat_type": stat_type,
                "current_line": float(line_score),
                "odds_type": odds_type,
                "league": "TENNIS",
                "is_active": True,
                "notes": f"pp_proj_id={proj.get('id')}",
            }
            overall_stat_hist[stat_type] += 1
            overall_odds_hist[odds_type] += 1

        league_wrote = 0
        league_deact = 0
        league_failures = 0

        for (stat_type, odds_type), per_player in grouped.items():
            rows = list(per_player.values())
            try:
                resp = (
                    db.table("prizepicks_lines")
                    .update({"is_active": False})
                    .eq("slate_id", slate_id)
                    .eq("stat_type", stat_type)
                    .eq("odds_type", odds_type)
                    .eq("is_active", True)
                    .execute()
                )
                league_deact += len(resp.data or [])
            except Exception as e:
                summary["errors"].append(
                    f"deactivate_failed[{stat_type}/{odds_type}]: {e}"
                )
                continue

            for row in rows:
                try:
                    resp = db.table("prizepicks_lines").insert(row).execute()
                    if resp.data:
                        league_wrote += len(resp.data)
                except Exception as e:
                    league_failures += 1
                    msg = str(e)[:200]
                    if len(summary["errors"]) < 30:
                        summary["errors"].append(
                            f"insert_failed[{stat_type}/{odds_type}/{row['player_id']}]: {msg}"
                        )

        summary["leagues"].append(
            {
                "id": lid,
                "http": 200,
                "wrote": league_wrote,
                "deactivated": league_deact,
                "row_failures": league_failures,
            }
        )
        total_wrote += league_wrote
        total_deactivated += league_deact
        total_failures += league_failures

    summary["wrote"] = total_wrote
    summary["deactivated"] = total_deactivated
    summary["row_failures"] = total_failures
    summary["odds_type_histogram"] = dict(overall_odds_hist)
    summary["stat_histogram"] = dict(overall_stat_hist)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()

    log.info(
        "PP ingest done slate=%s wrote=%d deact=%d fail=%d odds=%s stat=%s",
        slate_id,
        total_wrote,
        total_deactivated,
        total_failures,
        summary["odds_type_histogram"],
        summary["stat_histogram"],
    )
    return summary


# ---------------------------------------------------------------------------
# Scheduler shim
#
# slate_watcher and /api/slates/refresh both call:
#     await pp_direct_svc.fetch_tick("TEN")
#
# This shim resolves the active classic tennis slate, then runs the
# (sync) ingest in a thread so it doesn't block the event loop.
# ---------------------------------------------------------------------------
async def fetch_tick(sport_code: str = "TEN") -> Dict[str, Any]:
    import asyncio

    if sport_code != "TEN":
        return {"skipped": "not_tennis"}

    db = get_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    rows = (
        db.table("slates")
        .select("id, lock_time, slate_date, first_seen_at")
        .eq("sport", "tennis")
        .eq("status", "active")
        .eq("contest_type", "classic")
        .eq("is_fallback", False)
        .order("slate_date", desc=True)
        .order("first_seen_at", desc=True)
        .execute()
        .data
        or []
    )
    if not rows:
        log.info("pp_direct fetch_tick: no active classic tennis slate")
        return {"skipped": "no_active_slate"}

    upcoming = [r for r in rows if r.get("lock_time") and r["lock_time"] > now_iso]
    upcoming.sort(key=lambda c: c["lock_time"])
    slate_id = upcoming[0]["id"] if upcoming else rows[0]["id"]

    return await asyncio.to_thread(run_prizepicks_direct, slate_id)
