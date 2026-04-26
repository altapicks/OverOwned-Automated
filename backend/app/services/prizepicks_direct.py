"""
PrizePicks direct ingest via Oxylabs Web Scraper API.

Captures all 3 PP variants per (player, stat_type):
  - standard
  - demon (harder over, higher payout)
  - goblin (easier over, lower payout)

Distinguished by `attributes.odds_type` on each PP projection.

Stored in prizepicks_lines with new `odds_type` column.
Unique constraint scope: (slate_id, player_id, stat_type, odds_type) WHERE is_active=true.
"""

from __future__ import annotations

import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx

from app.db.supabase_client import get_service_client
from app.services.player_resolver import resolve_player_name

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OXY_USER = os.getenv("OXYLABS_USERNAME", "")
OXY_PASS = os.getenv("OXYLABS_PASSWORD", "")
OXY_ENDPOINT = "https://realtime.oxylabs.io/v1/queries"

PP_BASE = "https://api.prizepicks.com"
TENNIS_LEAGUE_NAME_ALLOW = {"TENNIS"}  # exclude TENNIS LIVE
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


# ---------------------------------------------------------------------------
# Oxylabs fetch helpers
# ---------------------------------------------------------------------------


def _oxy_payload(url: str) -> Dict[str, Any]:
    return {
        "source": "universal",
        "url": url,
        "geo_location": "United States",
        "render": "html",
        "user_agent_type": "desktop",
    }


def _oxy_fetch_json(url: str, timeout: float = 60.0) -> Tuple[int, Dict[str, Any]]:
    if not (OXY_USER and OXY_PASS):
        log.error("Oxylabs creds missing")
        return 0, {}
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(
                OXY_ENDPOINT,
                auth=(OXY_USER, OXY_PASS),
                json=_oxy_payload(url),
            )
        if r.status_code != 200:
            log.error("Oxylabs envelope HTTP %s for %s", r.status_code, url)
            return r.status_code, {}
        env = r.json()
        results = env.get("results", []) or []
        if not results:
            log.error("Oxylabs returned 0 results for %s", url)
            return 0, {}
        first = results[0]
        inner_status = first.get("status_code", 0)
        content = first.get("content")
        if inner_status != 200:
            log.error("PP returned HTTP %s via oxy for %s", inner_status, url)
            return inner_status, {}
        if isinstance(content, str):
            import json as _json
            try:
                content = _json.loads(content)
            except Exception as e:
                log.error("Failed to parse PP body for %s: %s", url, e)
                return 0, {}
        if not isinstance(content, dict):
            log.error("Unexpected PP body type for %s: %s", url, type(content))
            return 0, {}
        return 200, content
    except Exception as e:
        log.exception("Oxylabs fetch failed for %s: %s", url, e)
        return 0, {}


# ---------------------------------------------------------------------------
# PP API surface
# ---------------------------------------------------------------------------


def fetch_leagues() -> Tuple[int, List[Dict[str, Any]]]:
    status, body = _oxy_fetch_json(f"{PP_BASE}/leagues")
    if status != 200:
        return status, []
    leagues = body.get("data", []) or []
    return 200, leagues


def fetch_projections(league_id: str) -> Tuple[int, Dict[str, Any]]:
    url = f"{PP_BASE}/projections?league_id={league_id}&per_page=500&single_stat=true"
    return _oxy_fetch_json(url)


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
    """
    PP `attributes.odds_type` values seen in the wild:
      - "standard"
      - "demon"
      - "goblin"
    Sometimes None / empty -> treat as standard.
    """
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
    """
    Pull all tennis projections (excluding TENNIS LIVE), capture all
    odds_type variants, write to prizepicks_lines.
    """
    summary: Dict[str, Any] = {
        "slate_id": slate_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "leagues": [],
        "wrote": 0,
        "deactivated": 0,
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

    sb = get_service_client()
    overall_odds_hist: Counter = Counter()
    overall_stat_hist: Counter = Counter()
    total_wrote = 0
    total_deactivated = 0

    for lid in league_ids:
        status, body = fetch_projections(lid)
        if status != 200:
            summary["leagues"].append({"id": lid, "http": status, "wrote": 0})
            continue

        data = body.get("data", []) or []
        included = body.get("included", []) or []
        player_idx = _build_player_index(included)

        # Group projections by (stat_type, odds_type) per league for scoped deactivate+insert
        grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

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

            resolved = resolve_player_name(raw_name, slate_id=slate_id)
            if not resolved:
                continue

            grouped[(stat_type, odds_type)].append({
                "slate_id": slate_id,
                "player_id": resolved,
                "raw_player_name": raw_name,
                "stat_type": stat_type,
                "current_line": float(line_score),
                "odds_type": odds_type,
                "league": "TENNIS",
                "is_active": True,
                "notes": f"pp_proj_id={proj.get('id')}",
            })
            overall_stat_hist[stat_type] += 1
            overall_odds_hist[odds_type] += 1

        league_wrote = 0
        league_deact = 0

        for (stat_type, odds_type), rows in grouped.items():
            # Defensive per-scope dedupe (keep last)
            seen: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                seen[r["player_id"]] = r
            deduped = list(seen.values())

            # Deactivate existing active rows for this scope
            try:
                resp = (
                    sb.table("prizepicks_lines")
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

            # Insert fresh rows
            try:
                resp = sb.table("prizepicks_lines").insert(deduped).execute()
                league_wrote += len(resp.data or [])
            except Exception as e:
                summary["errors"].append(
                    f"insert_failed[{stat_type}/{odds_type}]: {e}"
                )

        summary["leagues"].append({
            "id": lid,
            "http": 200,
            "wrote": league_wrote,
            "deactivated": league_deact,
        })
        total_wrote += league_wrote
        total_deactivated += league_deact

    summary["wrote"] = total_wrote
    summary["deactivated"] = total_deactivated
    summary["odds_type_histogram"] = dict(overall_odds_hist)
    summary["stat_histogram"] = dict(overall_stat_hist)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    log.info(
        "PP ingest done slate=%s wrote=%d deact=%d odds=%s stat=%s",
        slate_id,
        total_wrote,
        total_deactivated,
        summary["odds_type_histogram"],
        summary["stat_histogram"],
    )
    return summary
