"""The Odds API v4 integration.

Fetches ATP and WTA tennis odds, transforms to the engine.js-compatible
field shape (ml_a, ml_b, gw_a_line, gw_a_over, etc.), and writes to
matches.odds.the_odds_api + appends to odds_history.

The Odds API for tennis provides:
  * h2h (moneyline)    → ml_a, ml_b  (converted decimal → American)
  * totals (total games in match) → best-effort split into gw_a_line/over, gw_b_line/over

It does NOT provide set-winner, ace, DF, or break markets for tennis.
Those fields stay unset in matches.odds.the_odds_api — engine.js reads
them as undefined and falls back to neutral contributions.

Conditional polling: only fetches when there's at least one active
non-fallback Classic slate with a match starting within 24h. When
nothing upcoming, skips entirely to conserve credits.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from app.config import get_settings
from app.db import get_client
from app.services import notifier
from app.services.http import CircuitBreaker, request_with_retry
from app.services.normalizer import PlayerNormalizer, normalize_for_match

logger = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
SPORT_TENNIS_ATP = "tennis_atp"
SPORT_TENNIS_WTA = "tennis_wta"

_breaker_oa = CircuitBreaker(failure_threshold=5, cooldown_seconds=600)
_breaker_oa._name = "the_odds_api"


async def _oa_breaker_opened(name: str):
    await notifier.notify_error(
        "odds_api_circuit_breaker",
        f"Circuit breaker {name} opened after 5 consecutive failures. Pausing for 10 min.",
    )


_breaker_oa._on_open = _oa_breaker_opened


@dataclass
class TennisOddsRow:
    """Normalized output per match — what gets merged into matches.odds.the_odds_api."""
    player_a_name: str
    player_b_name: str
    commence_time: datetime
    ml_a: Optional[int] = None  # American odds
    ml_b: Optional[int] = None
    gw_a_line: Optional[float] = None
    gw_a_over: Optional[int] = None
    gw_b_line: Optional[float] = None
    gw_b_over: Optional[int] = None
    raw: dict = None  # full response row for audit

    def to_engine_shape(self) -> dict:
        """Flat dict of engine.js-compatible fields + raw payload + fetched_at."""
        d = {"fetched_at": datetime.now(timezone.utc).isoformat()}
        for k in ("ml_a", "ml_b", "gw_a_line", "gw_a_over", "gw_b_line", "gw_b_over"):
            v = getattr(self, k)
            if v is not None:
                d[k] = v
        if self.raw:
            d["raw"] = self.raw
        return d


# ── Utilities ───────────────────────────────────────────────────────

def decimal_to_american(decimal: float) -> int:
    """Convert decimal odds to American integer (rounded)."""
    if decimal <= 1.0:
        return 0
    if decimal >= 2.0:
        return round((decimal - 1) * 100)
    return round(-100 / (decimal - 1))


def _pick_book(books: list[dict], preferred: list[str]) -> Optional[dict]:
    """Return the first bookmaker in `books` matching any key in `preferred`,
    else the first book. Preferred list is case-insensitive key match."""
    if not books:
        return None
    pref_lower = [p.lower() for p in preferred]
    for book in books:
        if book.get("key", "").lower() in pref_lower:
            return book
    return books[0]


def _parse_event(event: dict) -> Optional[TennisOddsRow]:
    """Transform one /odds response item into a TennisOddsRow.

    Preferred book order: Pinnacle (sharpest), then DraftKings, then FanDuel,
    then the first available. This reduces vig noise across the week.
    """
    try:
        home = event.get("home_team") or ""
        away = event.get("away_team") or ""
        commence = event.get("commence_time")
        if not home or not away or not commence:
            return None
        commence_dt = datetime.fromisoformat(commence.replace("Z", "+00:00"))

        books = event.get("bookmakers") or []
        book = _pick_book(books, ["pinnacle", "draftkings", "fanduel"])
        if not book:
            return None

        row = TennisOddsRow(
            player_a_name=home,
            player_b_name=away,
            commence_time=commence_dt,
            raw={"book": book.get("key"), "event_id": event.get("id")},
        )

        for market in book.get("markets") or []:
            mkey = market.get("key")
            outcomes = market.get("outcomes") or []
            if mkey == "h2h":
                for o in outcomes:
                    name = o.get("name")
                    price = o.get("price")
                    if not name or price is None:
                        continue
                    am = decimal_to_american(float(price))
                    if name == home:
                        row.ml_a = am
                    elif name == away:
                        row.ml_b = am
            elif mkey == "totals":
                # Tennis totals = total games in match. Best-effort per-player line:
                # assume both players account for ~half, adjusted by their win prob.
                # For now, write total into both gw_a_line and gw_b_line as a simple
                # starting signal — engine.js adjustLine uses the over odds to nudge,
                # so directional information is preserved.
                line = None
                over_price = None
                for o in outcomes:
                    name = (o.get("name") or "").lower()
                    if name == "over":
                        line = o.get("point")
                        over_price = o.get("price")
                # Split total games in half for a naive per-player starting line.
                # Engine.js uses gw_a_line/over and gw_b_line/over separately. Odds API
                # only gives us the combined total, so we approximate by halving.
                if line is not None and over_price is not None:
                    half = float(line) / 2.0
                    am = decimal_to_american(float(over_price))
                    row.gw_a_line = half
                    row.gw_a_over = am
                    row.gw_b_line = half
                    row.gw_b_over = am
        return row
    except (KeyError, ValueError, TypeError) as e:
        logger.debug("Skipping malformed Odds API event: %s", e)
        return None


# ── Fetch + ingest ──────────────────────────────────────────────────

async def _has_upcoming_matches(sport: str) -> bool:
    """Credit-conservation gate: only fetch if there's an active Classic
    slate with a match starting within 24h."""
    db = get_client()
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    rows = (
        db.table("matches")
        .select("id, slate_id, slates!inner(sport, status, contest_type, is_fallback)")
        .gte("start_time", datetime.now(timezone.utc).isoformat())
        .lte("start_time", cutoff)
        .eq("slates.sport", sport)
        .eq("slates.status", "active")
        .eq("slates.contest_type", "classic")
        .eq("slates.is_fallback", False)
        .limit(1)
        .execute()
        .data
        or []
    )
    return len(rows) > 0


async def fetch_tick(sport_code: str) -> dict:
    """One ingestion cycle for a sport (TEN = both ATP and WTA).

    Returns summary dict: {fetched: N, matched: M, credits_remaining: X}.
    """
    s = get_settings()
    if not s.odds_api_key:
        logger.info("ODDS_API_KEY not set — skipping Odds API fetch")
        return {"skipped": "no_api_key"}

    sport_name = "tennis" if sport_code == "TEN" else sport_code.lower()
    if sport_name != "tennis":
        # Only tennis is live right now; extend when other sports are enabled.
        return {"skipped": "not_tennis"}

    if not await _has_upcoming_matches("tennis"):
        logger.info("Odds API tick skipped: no matches within 24h")
        return {"skipped": "no_upcoming_matches"}

    fetched_total = 0
    matched_total = 0
    credits_remaining: Optional[int] = None

    for tour in (SPORT_TENNIS_ATP, SPORT_TENNIS_WTA):
        try:
            url = f"{API_BASE}/sports/{tour}/odds"
            params = {
                "apiKey": s.odds_api_key,
                "regions": "us",
                "markets": "h2h,totals",
                "oddsFormat": "decimal",
            }
            r = await request_with_retry(
                "GET", url, params=params, breaker=_breaker_oa, max_retries=3
            )
            events = r.json()
            remaining_header = r.headers.get("x-requests-remaining")
            if remaining_header:
                try:
                    credits_remaining = int(remaining_header)
                except ValueError:
                    pass
            _breaker_oa.record_success()

            rows = [row for row in (_parse_event(e) for e in events) if row]
            fetched_total += len(rows)
            matched = await _ingest_rows(rows, tour)
            matched_total += matched

        except httpx.HTTPStatusError as e:
            logger.error("Odds API HTTP error for %s: %s", tour, e)
        except Exception as e:
            logger.exception("Odds API fetch failed for %s: %s", tour, e)

    if credits_remaining is not None:
        logger.info("The Odds API credits remaining: %d", credits_remaining)
        if credits_remaining < 500:
            await notifier.notify_error(
                "odds_api_credits_low",
                f"Only {credits_remaining} Odds API credits left this month.",
                {"remaining": credits_remaining},
            )

    return {
        "fetched": fetched_total,
        "matched": matched_total,
        "credits_remaining": credits_remaining,
    }


async def _ingest_rows(rows: list[TennisOddsRow], tour: str) -> int:
    """Match Odds API rows to our matches and write odds. Returns count matched."""
    if not rows:
        return 0
    db = get_client()
    normalizer = PlayerNormalizer(sport="tennis")

    # Pull candidate matches: active Classic slates, not past, not fallback
    candidate_matches = (
        db.table("matches")
        .select("id, slate_id, player_a_id, player_b_id, start_time, slates!inner(sport, status, contest_type, is_fallback)")
        .gte("start_time", (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())
        .lte("start_time", (datetime.now(timezone.utc) + timedelta(hours=72)).isoformat())
        .eq("slates.sport", "tennis")
        .eq("slates.status", "active")
        .eq("slates.contest_type", "classic")
        .eq("slates.is_fallback", False)
        .execute()
        .data
        or []
    )

    matched = 0
    for row in rows:
        a = normalizer.resolve(row.player_a_name, source="odds_api", create_if_missing=False)
        b = normalizer.resolve(row.player_b_name, source="odds_api", create_if_missing=False)
        if not a.canonical_id or not b.canonical_id:
            continue

        # Find match in our slates with matching players (either order) + start_time ±3h
        for m in candidate_matches:
            m_start = m.get("start_time")
            if m_start:
                try:
                    m_dt = datetime.fromisoformat(m_start.replace("Z", "+00:00"))
                    if abs((m_dt - row.commence_time).total_seconds()) > 10800:
                        continue
                except (ValueError, TypeError):
                    continue

            pa = m["player_a_id"]
            pb = m["player_b_id"]
            swap = False
            if {pa, pb} == {a.canonical_id, b.canonical_id}:
                # Check if our A matches their home (no swap) or their away (swap)
                if pa == b.canonical_id:
                    swap = True
            else:
                continue

            # Build engine-shape fields (swap if our a is their away)
            eng_fields = row.to_engine_shape()
            if swap:
                eng_fields = _swap_ab_fields(eng_fields)

            await _write_match_odds(
                match_id=m["id"],
                slate_id=m["slate_id"],
                source="the_odds_api",
                engine_fields=eng_fields,
                raw_market_payload={"tour": tour, "row": row.raw},
            )
            matched += 1
            break

    return matched


def _swap_ab_fields(d: dict) -> dict:
    """Swap all _a <-> _b fields, preserving non-swappable keys."""
    out = {}
    for k, v in d.items():
        if k.endswith("_a") and k != "raw":
            out[k[:-2] + "_b"] = v
        elif k.endswith("_b") and k != "raw":
            out[k[:-2] + "_a"] = v
        elif "_a_" in k:
            out[k.replace("_a_", "_b_")] = v
        elif "_b_" in k:
            out[k.replace("_b_", "_a_")] = v
        else:
            out[k] = v
    return out


async def _write_match_odds(
    match_id: str, slate_id: str, source: str,
    engine_fields: dict, raw_market_payload: dict,
):
    """Merge engine-shape fields into matches.odds.<source>, append to odds_history.

    Also seeds matches.opening_odds on first ingest for this source — that
    column is frozen forever after, giving archived slates a canonical
    closing-line-movement baseline independent of any user's localStorage.
    """
    db = get_client()
    row = (
        db.table("matches").select("odds, opening_odds").eq("id", match_id).single().execute().data
    )
    if not row:
        return
    current = row.get("odds") or {}
    if not isinstance(current, dict):
        current = {}
    current[source] = engine_fields

    # Also promote flat ml_a/ml_b/gw_*/etc. to the top level of odds so
    # engine.js (which reads match.odds.ml_a directly) sees them without
    # knowing about source sub-keys. Source-prefixed copy stays for audit.
    for k in ("ml_a", "ml_b", "gw_a_line", "gw_a_over", "gw_b_line", "gw_b_over"):
        if k in engine_fields:
            current[k] = engine_fields[k]

    db.table("matches").update({"odds": current}).eq("id", match_id).execute()

    # Opening odds preservation — write only if this source has never been
    # recorded. Never overwrite. Frontend reads this for archived-slate delta.
    opening = row.get("opening_odds") or {}
    if not isinstance(opening, dict):
        opening = {}
    if source not in opening:
        opening[source] = engine_fields
        db.table("matches").update({"opening_odds": opening}).eq("id", match_id).execute()

    db.table("odds_history").insert(
        {
            "match_id": match_id,
            "slate_id": slate_id,
            "source": source,
            "market": "h2h+totals",
            "payload": {"engine_fields": engine_fields, "raw": raw_market_payload},
        }
    ).execute()
