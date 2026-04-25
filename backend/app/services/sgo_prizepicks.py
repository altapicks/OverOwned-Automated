"""SGO PrizePicks-style props ingest.

Pulls Fantasy Score, Aces, and Break Points per player from SportsGameOdds
and writes them to prizepicks_lines using the same deactivate-then-insert
pattern as manual_slate_ingest.upsert_pp_fs_lines.

Manual CSV uploads remain the override path for last-minute slate edits —
this service runs on the same 15-min tick as the rest of the watcher and
refreshes only the SGO-sourced rows. A subsequent CSV re-upload will
deactivate everything and re-insert from the CSV, restoring manual
authority.
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

API_BASE = "https://api.sportsgameodds.com/v2"
LEAGUE_ATP = "ATP"
LEAGUE_WTA = "WTA"
PREFERRED_BOOK = "pinnacle"

# SGO statID → prizepicks_lines.stat_type (must match what the frontend
# already filters on for the PrizePicks tab).
PROP_MAP = {
    "fantasyScore": "Fantasy Score",
    "aces": "Aces",
    "breakPoints": "Break Points",
}

_breaker_pp = CircuitBreaker(failure_threshold=5, cooldown_seconds=600)
_breaker_pp._name = "sgo_pp"


async def _pp_breaker_opened(name: str):
    await notifier.notify_error(
        "sgo_pp_circuit_breaker",
        f"SGO PrizePicks circuit breaker {name} opened after 5 consecutive failures.",
    )


_breaker_pp._on_open = _pp_breaker_opened


def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _line_for(odd: Optional[dict]) -> Optional[float]:
    """Pinnacle-preferred line. Falls back to consensus bookLine."""
    if not odd:
        return None
    pin = (odd.get("byBookmaker") or {}).get(PREFERRED_BOOK)
    if pin and pin.get("available"):
        ln = _f(pin.get("overUnder") or pin.get("line"))
        if ln is not None:
            return ln
    return _f(odd.get("bookLine") or odd.get("line") or odd.get("overUnder"))


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


async def fetch_tick(sport_code: str = "TEN") -> dict:
    s = get_settings()
    if not s.sgo_api_key:
        return {"skipped": "no_api_key"}
    if sport_code != "TEN":
        return {"skipped": "not_tennis"}
    if not await _has_upcoming_matches():
        return {"skipped": "no_upcoming_matches"}

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

    # Map every roster player to their slate (used to attach SGO props).
    cid_to_slate: dict[str, str] = {}
    for m in matches:
        cid_to_slate[m["player_a_id"]] = m["slate_id"]
        cid_to_slate[m["player_b_id"]] = m["slate_id"]

    collected: list[dict] = []
    for league in (LEAGUE_ATP, LEAGUE_WTA):
        try:
            url = f"{API_BASE}/events"
            params = {
                "leagueID": league,
                "type": "match",
                "oddsAvailable": "true",
                "limit": 50,
            }
            r = await request_with_retry(
                "GET",
                url,
                params=params,
                headers={"x-api-key": s.sgo_api_key},
                breaker=_breaker_pp,
                max_retries=3,
            )
            events = r.json().get("data") or []
            _breaker_pp.record_success()

            for ev in events:
                teams = ev.get("teams") or {}
                home = (teams.get("home") or {}).get("names", {}).get("long")
                away = (teams.get("away") or {}).get("names", {}).get("long")
                if not home or not away:
                    continue
                odds = ev.get("odds") or {}
                for side, name in (("home", home), ("away", away)):
                    for stat_id, stat_label in PROP_MAP.items():
                        odd = odds.get(f"{stat_id}-{side}-game-ou-over")
                        line = _line_for(odd)
                        if line is None:
                            continue
                        collected.append(
                            {
                                "raw_player_name": name,
                                "stat_type": stat_label,
                                "line": line,
                                "league": league,
                            }
                        )
        except httpx.HTTPStatusError as e:
            logger.error("SGO PP HTTP error for %s: %s", league, e)
        except Exception as e:
            logger.exception("SGO PP fetch failed for %s: %s", league, e)

    if not collected:
        return {"fetched": 0, "matched": 0, "written": 0}

    # Group by (slate_id, stat_type) so the deactivate-then-insert
    # operates on the same partial-unique scope as upsert_pp_fs_lines.
    by_slate_stat: dict[tuple[str, str], list[dict]] = {}
    matched_count = 0
    for c in collected:
        res = normalizer.resolve(
            c["raw_player_name"], source="sgo_pp", create_if_missing=False
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

    written = 0
    for (slate_id, stat_type), rows in by_slate_stat.items():
        # Deactivate all prior active rows for this (slate, stat_type) — covers
        # both prior SGO inserts and prior CSV inserts. Last writer wins on
        # the partial-active uniqueness index. A subsequent CSV upload will
        # deactivate these rows and re-insert from the CSV, restoring the
        # manual override path.
        db.table("prizepicks_lines").update({"is_active": False}).eq(
            "slate_id", slate_id
        ).eq("stat_type", stat_type).execute()
        try:
            resp = db.table("prizepicks_lines").insert(rows).execute()
            written += len(getattr(resp, "data", None) or [])
        except Exception as exc:
            logger.exception(
                "SGO PP insert failed slate=%s stat=%s rows=%d err=%r",
                slate_id,
                stat_type,
                len(rows),
                exc,
            )

    logger.info(
        "SGO PP tick: collected=%d matched=%d written=%d slates=%d",
        len(collected),
        matched_count,
        written,
        len({k[0] for k in by_slate_stat}),
    )
    return {"fetched": len(collected), "matched": matched_count, "written": written}
