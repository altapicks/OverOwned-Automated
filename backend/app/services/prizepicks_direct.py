"""Direct PrizePicks projections ingest using curl_cffi (Chrome TLS impersonation).

PrizePicks' Cloudflare CDN 403s any non-browser TLS fingerprint, including
plain httpx. curl_cffi exposes libcurl-impersonate which produces a real
Chrome JA3/HTTP-2 signature — Cloudflare lets it through.

Schema: writes to prizepicks_lines using deactivate-then-insert per
(slate_id, stat_type), preserving the manual CSV override path.

Kalshi win-% is NEVER touched here.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.db import get_client
from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)

PP_BASE = "https://api.prizepicks.com"

# Stat-type whitelist. Frontend Pivots tab filters on these exact strings.
STAT_TYPE_WHITELIST = {
    "Fantasy Score": "Fantasy Score",
    "Aces": "Aces",
    "Double Faults": "Double Faults",
    "Breaks": "Breaks",
    "Total Games Won": "Total Games Won",
    "Sets Won": "Sets Won",
    "Games Won": "Total Games Won",
}


# ─────────────────────────────────────────────────────────────────────
# HTTP — curl_cffi with httpx fallback
# ─────────────────────────────────────────────────────────────────────

def _have_curl_cffi() -> bool:
    try:
        import curl_cffi.requests  # noqa: F401
        return True
    except Exception:
        return False


async def _http_get_json(url: str, params: dict | None = None) -> tuple[int, dict]:
    """GET a JSON endpoint with Chrome TLS impersonation.

    Returns (http_status, parsed_json). Falls back to httpx if curl_cffi
    is missing — but PrizePicks will 403 the fallback. The fallback is
    only there to keep the rest of the app importable if curl_cffi
    fails to install on Railway.
    """
    if _have_curl_cffi():
        from curl_cffi.requests import AsyncSession
        async with AsyncSession(impersonate="chrome120") as session:
            r = await session.get(url, params=params, timeout=20)
            try:
                payload = r.json()
            except Exception:
                payload = {}
            return r.status_code, payload

    import httpx
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            url,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )
        try:
            payload = r.json()
        except Exception:
            payload = {}
        return r.status_code, payload


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
# League discovery
# ─────────────────────────────────────────────────────────────────────

async def _discover_tennis_league_ids() -> list[int]:
    try:
        status, payload = await _http_get_json(f"{PP_BASE}/leagues")
    except Exception as e:
        logger.exception("PP /leagues fetch failed: %s", e)
        return []

    if status != 200:
        logger.error(
            "PP /leagues HTTP %d (curl_cffi installed=%s)",
            status,
            _have_curl_cffi(),
        )
        return []

    ids: list[int] = []
    for item in payload.get("data") or []:
        attrs = item.get("attributes") or {}
        name = (attrs.get("name") or "").lower()
        sport = (attrs.get("sport") or "").lower()
        if "tennis" not in name and "tennis" not in sport:
            continue
        try:
            ids.append(int(item.get("id")))
        except (TypeError, ValueError):
            continue

    if not ids:
        logger.warning(
            "PP /leagues returned %d leagues, none tennis",
            len(payload.get("data") or []),
        )
    else:
        logger.info("PP tennis league_ids discovered: %s", ids)
    return ids


# ─────────────────────────────────────────────────────────────────────
# Projections fetch + parse
# ─────────────────────────────────────────────────────────────────────

async def _fetch_projections(league_id: int) -> dict:
    status, payload = await _http_get_json(
        f"{PP_BASE}/projections",
        params={
            "league_id": league_id,
            "per_page": 500,
            "single_stat": "true",
        },
    )
    if status != 200:
        logger.error("PP /projections HTTP %d for league %d", status, league_id)
        return {}
    return payload


def _parse_projections(payload: dict) -> list[dict]:
    """JSONAPI envelope → list of {raw_player_name, stat_type, line}."""
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

    cid_to_slate: dict[str, str] = {}
    for m in matches:
        cid_to_slate[m["player_a_id"]] = m["slate_id"]
        cid_to_slate[m["player_b_id"]] = m["slate_id"]

    collected: list[dict] = []
    for lid in league_ids:
        try:
            payload = await _fetch_projections(lid)
            parsed = _parse_projections(payload)
            collected.extend(parsed)
            logger.info("PP league_id=%d projections=%d", lid, len(parsed))
        except Exception as e:
            logger.exception("PP fetch failed league_id=%d: %s", lid, e)

    if not collected:
        return {"fetched": 0, "matched": 0, "written": 0}

    by_slate_stat: dict[tuple[str, str], list[dict]] = {}
    matched_count = 0
    unresolved_sample: list[str] = []
    for c in collected:
        res = normalizer.resolve(
            c["raw_player_name"], source="prizepicks", create_if_missing=False
        )
        if not res.canonical_id:
            if len(unresolved_sample) < 10:
                unresolved_sample.append(c["raw_player_name"])
            continue
        if res.canonical_id not in cid_to_slate:
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

    if unresolved_sample:
        logger.info("PP unresolved player names sample: %s", unresolved_sample)

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
