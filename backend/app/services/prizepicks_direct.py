"""Direct PrizePicks projections ingest.

Replaces sgo_prizepicks.py (SGO doesn't carry tennis player props in v2).
Hits api.prizepicks.com directly — no auth required for the public projections
endpoint. PrizePicks is the source of truth for what's actually live on the
PrizePicks app, so this gives us the exact lines users are picking against.

Schema: writes to prizepicks_lines using the same deactivate-then-insert
pattern as manual_slate_ingest.upsert_pp_fs_lines, scoped per
(slate_id, stat_type). This preserves the manual CSV override path:
- Manual CSV with only Fantasy Score → wipes & re-inserts Fantasy Score,
  leaves Aces/Breaks/DFs intact.
- This service running every 15min → wipes & re-inserts ALL stat_types it
  collects, leaves any stat_types it didn't collect intact.

Kalshi win-% is NEVER touched by this service. Kalshi remains the sole
source of win probability per the spec.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from app.config import get_settings
from app.db import get_client
from app.services import notifier
from app.services.http import CircuitBreaker, request_with_retry
from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)

PP_BASE = "https://api.prizepicks.com"
# Realistic desktop UA — PrizePicks' CDN sometimes 403s on bare httpx UAs.
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://app.prizepicks.com",
    "Referer": "https://app.prizepicks.com/",
}

# PrizePicks tennis stat_type strings → what we write to prizepicks_lines.stat_type.
# Frontend Pivots tab filters on these exact strings; keep parity with the manual
# CSV ("Fantasy Score") and add the player-prop categories the engine wants.
STAT_TYPE_WHITELIST = {
    "Fantasy Score": "Fantasy Score",
    "Aces": "Aces",
    "Double Faults": "Double Faults",
    "Breaks": "Breaks",
    "Total Games Won": "Total Games Won",
    "Sets Won": "Sets Won",
    # Some seasons PP uses "Games Won" instead of "Total Games Won"
    "Games Won": "Total Games Won",
}

_breaker_pp = CircuitBreaker(failure_threshold=5, cooldown_seconds=600)
_breaker_pp._name = "prizepicks_direct"


async def _pp_breaker_opened(name: str):
    await notifier.notify_error(
        "prizepicks_direct_circuit_breaker",
        f"PrizePicks direct circuit breaker {name} opened after 5 consecutive failures.",
    )


_breaker_pp._on_open = _pp_breaker_opened


# ─────────────────────────────────────────────────────────────────────
# Schedule gate
# ─────────────────────────────────────────────────────────────────────

async def _has_upcoming_matches() -> bool:
    db = get_client()
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    rows = (
        db.table("matches")
        .select(
            "id, slate_id, slates!inner(sport, status, contest_type, is_fallback)"
        )
        .gte("start_time", datetime.now(timezone.utc).isoformat())
        .lte("start_time", cutoff)
        .eq("slates.sport", "tennis")
        .eq("slates.status", "active")
        .eq("slates.contest_type", "classic")
        .eq("slates.is_fallback", False)
        .limit(1)
        .execute()
        .data
        or []
    )
    return len(rows) > 0


# ─────────────────────────────────────────────────────────────────────
# League discovery — PrizePicks shifts league_ids for tennis between seasons
# (sometimes one combined "TENNIS" id, sometimes split ATP/WTA). Discover
# at runtime so we never go stale.
# ─────────────────────────────────────────────────────────────────────

async def _discover_tennis_league_ids() -> list[int]:
    url = f"{PP_BASE}/leagues"
    try:
        r = await request_with_retry(
            "GET",
            url,
            headers=_HEADERS,
            breaker=_breaker_pp,
            max_retries=3,
        )
        payload = r.json()
    except Exception as e:
        logger.exception("PP /leagues fetch failed: %s", e)
        return []

    ids: list[int] = []
    for item in payload.get("data") or []:
        attrs = item.get("attributes") or {}
        # Match any league whose name OR sport contains "tennis" (case-insensitive)
        name = (attrs.get("name") or "").lower()
        sport = (attrs.get("sport") or "").lower()
        if "tennis" not in name and "tennis" not in sport:
            continue
        try:
            ids.append(int(item.get("id")))
        except (TypeError, ValueError):
            continue
    if not ids:
        logger.warning("PP /leagues returned no tennis leagues; payload had %d items",
                       len(payload.get("data") or []))
    else:
        logger.info("PP tennis league_ids discovered: %s", ids)
    return ids


# ─────────────────────────────────────────────────────────────────────
# Projections fetch + parse
# ─────────────────────────────────────────────────────────────────────

async def _fetch_projections(league_id: int) -> dict:
    url = f"{PP_BASE}/projections"
    params = {
        "league_id": league_id,
        "per_page": 500,
        "single_stat": "true",
    }
    r = await request_with_retry(
        "GET",
        url,
        params=params,
        headers=_HEADERS,
        breaker=_breaker_pp,
        max_retries=3,
    )
    return r.json()


def _parse_projections(payload: dict) -> list[dict]:
    """Convert PP JSONAPI envelope into [{raw_player_name, stat_type, line}, ...].

    PP shape:
      data[].attributes.line_score (float)
      data[].attributes.stat_type (string)
      data[].relationships.new_player.data.id (numeric string)
      included[] contains type='new_player' rows with attributes.name
    """
    rows: list[dict] = []
    included = payload.get("included") or []
    player_lookup: dict[str, str] = {}
    for inc in included:
        if (inc.get("type") or "").lower() != "new_player":
            continue
        attrs = inc.get("attributes") or {}
        nm = attrs.get("name") or attrs.get("display_name")
        if nm and inc.get("id"):
            player_lookup[str(inc["id"])] = nm

    for item in payload.get("data") or []:
        attrs = item.get("attributes") or {}
        stat_raw = attrs.get("stat_type") or ""
        stat_type = STAT_TYPE_WHITELIST.get(stat_raw)
        if not stat_type:
            continue
        try:
            line = float(attrs.get("line_score"))
        except (TypeError, ValueError):
            continue

        # Player name from the relationship → included lookup
        rel = (item.get("relationships") or {}).get("new_player") or {}
        pdata = rel.get("data") or {}
        pid = pdata.get("id")
        if pid is None:
            continue
        name = player_lookup.get(str(pid))
        if not name:
            continue

        rows.append(
            {
                "raw_player_name": name,
                "stat_type": stat_type,
                "line": line,
            }
        )
    return rows


# ─────────────────────────────────────────────────────────────────────
# Main tick
# ─────────────────────────────────────────────────────────────────────

async def fetch_tick(sport_code: str = "TEN") -> dict:
    if sport_code != "TEN":
        return {"skipped": "not_tennis"}
    if not await _has_upcoming_matches():
        return {"skipped": "no_upcoming_matches"}

    league_ids = await _discover_tennis_league_ids()
    if not league_ids:
        return {"skipped": "no_tennis_leagues_on_pp"}

    db = get_client()
    normalizer = PlayerNormalizer(sport="tennis")

    slates = (
        db.table("slates")
        .select("id, sport, status, contest_type, is_fallback")
        .eq("sport", "tennis")
        .eq("status", "active")
        .eq("contest_type", "classic")
        .eq("is_fallback", False)
        .execute()
        .data
        or []
    )
    if not slates:
        return {"skipped": "no_active_slates"}

    matches = (
        db.table("matches")
        .select("id, slate_id, player_a_id, player_b_id, start_time")
        .in_("slate_id", [s["id"] for s in slates])
        .execute()
        .data
        or []
    )
    if not matches:
        return {"skipped": "no_matches"}

    # Map every roster player to their slate_id (used to attach PP props)
    cid_to_slate: dict[str, str] = {}
    for m in matches:
        cid_to_slate[m["player_a_id"]] = m["slate_id"]
        cid_to_slate[m["player_b_id"]] = m["slate_id"]

    # Collect projections from each tennis league
    collected: list[dict] = []
    for lid in league_ids:
        try:
            payload = await _fetch_projections(lid)
            _breaker_pp.record_success()
            parsed = _parse_projections(payload)
            collected.extend(parsed)
            logger.info("PP league_id=%d projections=%d", lid, len(parsed))
        except httpx.HTTPStatusError as e:
            logger.error("PP HTTP error league_id=%d: %s", lid, e)
        except Exception as e:
            logger.exception("PP fetch failed league_id=%d: %s", lid, e)

    if not collected:
        return {"fetched": 0, "matched": 0, "written": 0}

    # Group by (slate_id, stat_type) so deactivate-then-insert is scoped correctly
    by_slate_stat: dict[tuple[str, str], list[dict]] = {}
    matched_count = 0
    for c in collected:
        res = normalizer.resolve(
            c["raw_player_name"], source="prizepicks", create_if_missing=False
        )
        if not res.canonical_id or res.canonical_id not in cid_to_slate:
            continue
        slate_id = cid_to_slate[res.canonical_id]
        key = (slate_id, c["stat_type"])
        by_slate_stat.setdefault(key, []).append(
            {
                "slate_id": slate_id,
                "player_id": res.canonical_id,
                "raw_player_name": c["raw_player_name"],
                "stat_type": c["stat_type"],
                "current_line": c["line"],
                "league": "tennis",
                "is_active": True,
            }
        )
        matched_count += 1

    # Deactivate-then-insert per (slate_id, stat_type). Last writer wins on
    # the partial-active uniqueness index. CSV uploads with Fantasy Score
    # only will deactivate FS but leave Aces/Breaks/DFs intact (and vice
    # versa) — preserves the manual override path per the spec.
    written = 0
    for (slate_id, stat_type), rows in by_slate_stat.items():
        db.table("prizepicks_lines").update({"is_active": False}).eq(
            "slate_id", slate_id
        ).eq("stat_type", stat_type).execute()
        try:
            resp = db.table("prizepicks_lines").insert(rows).execute()
            written += len(getattr(resp, "data", None) or [])
        except Exception as exc:
            logger.exception(
                "PP direct insert failed slate=%s stat=%s rows=%d err=%r",
                slate_id,
                stat_type,
                len(rows),
                exc,
            )

    logger.info(
        "PP direct tick: collected=%d matched=%d written=%d slates=%d stat_types=%d",
        len(collected),
        matched_count,
        written,
        len({k[0] for k in by_slate_stat}),
        len({k[1] for k in by_slate_stat}),
    )
    return {
        "fetched": len(collected),
        "matched": matched_count,
        "written": written,
    }
