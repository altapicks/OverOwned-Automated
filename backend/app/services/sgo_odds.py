"""SportsGameOdds (SGO) v2 integration for tennis odds.

Pulls Pinnacle odds for the markets the engine actually consumes:
    games-{home,away}-game-ou        → gw_*_line / gw_*_over / gw_*_under
    games-all-game-ou                → games_total_line / *_over / *_under
    points-all-game-ou               → sets_total_line + p3set derivation
    points-{home,away}-game-sp       → set_spread_*_line / *_odds
    games-{home,away}-game-sp        → game_spread_*_line / *_odds

Moneyline (points-*-game-ml-*) is intentionally NOT pulled — Kalshi is the
sole source of win probability (kalshi_prob_a / kalshi_prob_b). Reading SGO
moneyline would give us a second, conflicting win-% signal.

set_a_20 / set_a_21 / set_b_20 / set_b_21 are derived from the existing
matches.odds.kalshi_prob_a + p3set during ingest, so the engine has the
four set-score American odds hasRichOdds() expects without needing SGO ML.

Top-level promoted keys (engine.js reads these without a source prefix):
    gw_a_line, gw_a_over, gw_b_line, gw_b_over,
    set_a_20, set_a_21, set_b_20, set_b_21,
    p3set
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
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

_breaker_sgo = CircuitBreaker(failure_threshold=5, cooldown_seconds=600)
_breaker_sgo._name = "sgo_odds"


async def _sgo_breaker_opened(name: str):
    await notifier.notify_error(
        "sgo_circuit_breaker",
        f"SGO odds circuit breaker {name} opened after 5 consecutive failures.",
    )


_breaker_sgo._on_open = _sgo_breaker_opened


# ── Data shape ────────────────────────────────────────────────────────
@dataclass
class TennisOddsRow:
    player_a_name: str
    player_b_name: str
    commence_time: datetime
    fields: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)

    def to_engine_shape(self) -> dict:
        out = {"fetched_at": datetime.now(timezone.utc).isoformat()}
        out.update(self.fields)
        if self.raw:
            out["raw"] = self.raw
        return out


# ── Utilities ─────────────────────────────────────────────────────────
def _american_to_int(am) -> Optional[int]:
    if am is None:
        return None
    s = str(am).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _float_or_none(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _american_to_prob(am: Optional[int]) -> Optional[float]:
    """Convert American odds to implied probability [0..1]."""
    if am is None:
        return None
    try:
        a = float(am)
    except (TypeError, ValueError):
        return None
    if a == 0:
        return None
    if a > 0:
        return 100.0 / (a + 100.0)
    return (-a) / ((-a) + 100.0)


def _prob_to_american(p: Optional[float]) -> Optional[int]:
    """Convert implied probability [0..1] to American odds (rounded int)."""
    if p is None:
        return None
    try:
        x = float(p)
    except (TypeError, ValueError):
        return None
    if x <= 0.0 or x >= 1.0:
        return None
    if x >= 0.5:
        return int(round(-100.0 * x / (1.0 - x)))
    return int(round(100.0 * (1.0 - x) / x))


def _odds_for(odd: Optional[dict]) -> tuple[Optional[int], Optional[float]]:
    """Return (american_odds, line) preferring Pinnacle, else consensus."""
    if not odd:
        return None, None
    pin = (odd.get("byBookmaker") or {}).get(PREFERRED_BOOK)
    if pin and pin.get("available"):
        am = _american_to_int(pin.get("odds"))
        line = _float_or_none(
            pin.get("overUnder") or pin.get("spread") or pin.get("line")
        )
        if am is not None:
            return am, line
    am = _american_to_int(odd.get("bookOdds") or odd.get("fairOdds"))
    line = _float_or_none(
        odd.get("bookOverUnder")
        or odd.get("fairOverUnder")
        or odd.get("bookSpread")
        or odd.get("fairSpread")
        or odd.get("overUnder")
        or odd.get("spread")
        or odd.get("line")
    )
    return am, line


def _parse_event(event: dict) -> Optional[TennisOddsRow]:
    """Parse one SGO event into a TennisOddsRow.

    Set-score derivation (set_a_20 / set_a_21 / set_b_20 / set_b_21) is
    done later in _ingest_rows once we've matched the SGO event to one
    of our slate matches and can read kalshi_prob_a from matches.odds.
    Doing it here would require SGO moneyline, which we no longer pull.
    """
    teams = event.get("teams") or {}
    home = (teams.get("home") or {}).get("names", {}).get("long")
    away = (teams.get("away") or {}).get("names", {}).get("long")
    starts_at = (event.get("status") or {}).get("startsAt") or event.get("startsAt")
    if not home or not away or not starts_at:
        return None
    try:
        commence = datetime.fromisoformat(str(starts_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None

    odds = event.get("odds") or {}
    fields: dict = {}

    # Total Sets (best-of-3 → line typically 2.5; engine derives p3set)
    over_odds, sets_line = _odds_for(odds.get("points-all-game-ou-over"))
    under_odds, sets_line2 = _odds_for(odds.get("points-all-game-ou-under"))
    if sets_line is None:
        sets_line = sets_line2
    if sets_line is not None:
        fields["sets_total_line"] = sets_line
    if over_odds is not None:
        fields["sets_total_over"] = over_odds
    if under_odds is not None:
        fields["sets_total_under"] = under_odds

    # Engine direct: p3set = P(match goes to 3 sets) on a best-of-3.
    if sets_line is not None and abs(float(sets_line) - 2.5) < 0.01:
        p_over = _american_to_prob(over_odds)
        p_under = _american_to_prob(under_odds)
        if p_over is not None and p_under is not None and (p_over + p_under) > 0:
            fields["p3set"] = round(p_over / (p_over + p_under), 4)
        elif p_over is not None:
            fields["p3set"] = round(p_over, 4)

    # Set Spread (+/-1.5 sets) — SGO native
    a_odds, a_line = _odds_for(odds.get("points-home-game-sp-home"))
    b_odds, b_line = _odds_for(odds.get("points-away-game-sp-away"))
    if a_line is not None:
        fields["set_spread_a_line"] = a_line
    if a_odds is not None:
        fields["set_spread_a_odds"] = a_odds
    if b_line is not None:
        fields["set_spread_b_line"] = b_line
    if b_odds is not None:
        fields["set_spread_b_odds"] = b_odds

    # Total games (match-level)
    g_over, g_line = _odds_for(odds.get("games-all-game-ou-over"))
    g_under, g_line2 = _odds_for(odds.get("games-all-game-ou-under"))
    if g_line is None:
        g_line = g_line2
    if g_line is not None:
        fields["games_total_line"] = g_line
    if g_over is not None:
        fields["games_total_over"] = g_over
    if g_under is not None:
        fields["games_total_under"] = g_under

    # Per-player Total Games O/U → engine's gw_*_line / gw_*_over / gw_*_under
    for side, prefix in (("home", "a"), ("away", "b")):
        over_o, over_line = _odds_for(odds.get(f"games-{side}-game-ou-over"))
        under_o, under_line = _odds_for(odds.get(f"games-{side}-game-ou-under"))
        line = over_line if over_line is not None else under_line
        if line is not None:
            fields[f"gw_{prefix}_line"] = line
        if over_o is not None:
            fields[f"gw_{prefix}_over"] = over_o
        if under_o is not None:
            fields[f"gw_{prefix}_under"] = under_o

    # Per-player Game Spread
    for side, prefix in (("home", "a"), ("away", "b")):
        sp_o, sp_line = _odds_for(odds.get(f"games-{side}-game-sp-{side}"))
        if sp_line is not None:
            fields[f"game_spread_{prefix}_line"] = sp_line
        if sp_o is not None:
            fields[f"game_spread_{prefix}_odds"] = sp_o

    if not fields:
        return None

    return TennisOddsRow(
        player_a_name=home,
        player_b_name=away,
        commence_time=commence,
        fields=fields,
        raw={
            "event_id": event.get("eventID"),
            "league": event.get("leagueID"),
            "book": PREFERRED_BOOK,
        },
    )


def _augment_set_scores_from_kalshi(eng_fields: dict, kalshi_wp_a: Optional[float]) -> None:
    """Mutate eng_fields in place to add set_a_20 / set_a_21 / set_b_20 /
    set_b_21 derived from Kalshi win probability and our p3set.

        P(a wins 2-0) = wp_a * (1 - p3set)
        P(a wins 2-1) = wp_a * p3set
        P(b wins 2-0) = (1 - wp_a) * (1 - p3set)
        P(b wins 2-1) = (1 - wp_a) * p3set

    Each is then converted back to American odds for the engine.
    No-op if wp_a or p3set is missing.
    """
    p3set = eng_fields.get("p3set")
    if kalshi_wp_a is None or p3set is None:
        return
    try:
        wp_a = float(kalshi_wp_a)
        p3 = float(p3set)
    except (TypeError, ValueError):
        return
    if not (0.0 < wp_a < 1.0) or not (0.0 <= p3 <= 1.0):
        return
    wp_b = 1.0 - wp_a
    pairs = {
        "set_a_20": wp_a * (1.0 - p3),
        "set_a_21": wp_a * p3,
        "set_b_20": wp_b * (1.0 - p3),
        "set_b_21": wp_b * p3,
    }
    for k, prob in pairs.items():
        am = _prob_to_american(prob)
        if am is not None:
            eng_fields[k] = am


# ── Schedule gate ─────────────────────────────────────────────────────
async def _has_upcoming_matches(sport: str) -> bool:
    db = get_client()
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    rows = (
        db.table("matches")
        .select(
            "id, slate_id, slates!inner(sport, status, contest_type, is_fallback)"
        )
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


# ── Main fetch ────────────────────────────────────────────────────────
async def fetch_tick(sport_code: str = "TEN") -> dict:
    s = get_settings()
    if not s.sgo_api_key:
        logger.info("SGO_API_KEY not set — skipping SGO odds fetch")
        return {"skipped": "no_api_key"}
    if sport_code != "TEN":
        return {"skipped": "not_tennis"}
    if not await _has_upcoming_matches("tennis"):
        logger.info("SGO odds tick skipped: no matches within 24h")
        return {"skipped": "no_upcoming_matches"}

    fetched_total = 0
    matched_total = 0
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
                breaker=_breaker_sgo,
                max_retries=3,
            )
            payload = r.json()
            events = payload.get("data") or []
            _breaker_sgo.record_success()
            rows = [row for row in (_parse_event(e) for e in events) if row]
            fetched_total += len(rows)
            matched_total += await _ingest_rows(rows, league)
        except httpx.HTTPStatusError as e:
            logger.error("SGO HTTP error for %s: %s", league, e)
        except Exception as e:
            logger.exception("SGO fetch failed for %s: %s", league, e)

    return {"fetched": fetched_total, "matched": matched_total}


# ── Ingest ────────────────────────────────────────────────────────────
async def _ingest_rows(rows: list[TennisOddsRow], league: str) -> int:
    if not rows:
        return 0
    db = get_client()
    normalizer = PlayerNormalizer(sport="tennis")

    candidate_matches = (
        db.table("matches")
        .select(
            "id, slate_id, player_a_id, player_b_id, start_time, odds,"
            " slates!inner(sport, status, contest_type, is_fallback)"
        )
        .gte(
            "start_time",
            (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        )
        .lte(
            "start_time",
            (datetime.now(timezone.utc) + timedelta(hours=72)).isoformat(),
        )
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
        a = normalizer.resolve(row.player_a_name, source="sgo", create_if_missing=False)
        b = normalizer.resolve(row.player_b_name, source="sgo", create_if_missing=False)
        if not a.canonical_id or not b.canonical_id:
            continue
        # SGO is also strict — refuse to attach odds based on a low-confidence
        # fuzzy guess. (Same defensive guard as PP.)
        if not (a.auto_resolved or a.was_new):
            continue
        if not (b.auto_resolved or b.was_new):
            continue
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
            if {pa, pb} != {a.canonical_id, b.canonical_id}:
                continue
            swap = pa == b.canonical_id  # our A == their away → swap
            eng_fields = row.to_engine_shape()
            if swap:
                eng_fields = _swap_ab_fields(eng_fields)

            # Now that we know which side of the match we're on, use the
            # match's existing kalshi_prob_a (if present) to derive the
            # four set-score American odds the engine reads directly.
            existing_odds = m.get("odds") or {}
            wp_a = existing_odds.get("kalshi_prob_a") if isinstance(existing_odds, dict) else None
            _augment_set_scores_from_kalshi(eng_fields, wp_a)

            await _write_match_odds(
                match_id=m["id"],
                slate_id=m["slate_id"],
                source="sgo",
                engine_fields=eng_fields,
                raw_market_payload={"league": league, "row": row.raw},
            )
            matched += 1
            break
    return matched


def _swap_ab_fields(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if k == "raw":
            out[k] = v
        elif k.endswith("_a"):
            out[k[:-2] + "_b"] = v
        elif k.endswith("_b"):
            out[k[:-2] + "_a"] = v
        elif "_a_" in k:
            out[k.replace("_a_", "_b_")] = v
        elif "_b_" in k:
            out[k.replace("_b_", "_a_")] = v
        else:
            out[k] = v
    return out


async def _write_match_odds(
    *,
    match_id: str,
    slate_id: str,
    source: str,
    engine_fields: dict,
    raw_market_payload: dict,
):
    """Same shape as odds_api._write_match_odds — keeps opening_odds + history.

    Touches only matches.odds[source] and the engine-flat keys promoted at
    the top level. NEVER modifies matches.odds.kalshi or kalshi_prob_a/_b.
    Moneyline keys (ml_a / ml_b) are intentionally NOT promoted — Kalshi
    is the sole source of win probability.
    """
    db = get_client()
    row = (
        db.table("matches")
        .select("odds, opening_odds")
        .eq("id", match_id)
        .single()
        .execute()
        .data
    )
    if not row:
        return

    current = row.get("odds") or {}
    if not isinstance(current, dict):
        current = {}
    current[source] = engine_fields

    # Engine-flat top-level promotion. Engine.js reads these without a
    # source prefix; hasRichOdds() in particular gates on set_a_20 +
    # gw_a_line being non-null. ml_a / ml_b are deliberately excluded.
    promoted_keys = (
        "gw_a_line",
        "gw_a_over",
        "gw_a_under",
        "gw_b_line",
        "gw_b_over",
        "gw_b_under",
        "set_a_20",
        "set_a_21",
        "set_b_20",
        "set_b_21",
        "p3set",
    )
    for k in promoted_keys:
        if k in engine_fields and engine_fields[k] is not None:
            current[k] = engine_fields[k]

    # Defensive: if a previous deploy promoted ml_a / ml_b at top level,
    # strip them now so they can't conflict with kalshi_prob_a/_b.
    for stale_key in ("ml_a", "ml_b"):
        if stale_key in current:
            current.pop(stale_key, None)

    db.table("matches").update({"odds": current}).eq("id", match_id).execute()

    opening = row.get("opening_odds") or {}
    if not isinstance(opening, dict):
        opening = {}
    if source not in opening:
        opening[source] = engine_fields
        db.table("matches").update({"opening_odds": opening}).eq(
            "id", match_id
        ).execute()

    db.table("odds_history").insert(
        {
            "match_id": match_id,
            "slate_id": slate_id,
            "source": source,
            "market": "totals+spreads+per_player_games+set_score",
            "payload": {"engine_fields": engine_fields, "raw": raw_market_payload},
        }
    ).execute()
