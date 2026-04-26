"""SportsGameOdds (SGO) v2 integration for tennis odds.

Replaces the legacy odds_api.py role for ML and games-won lines, and adds
markets the Odds API never carried: total sets O/U (for p3set derivation),
per-player set spread (+/-1.5 sets), and per-player game spread.

Pinnacle is the priority book (sharpest tennis lines on the market). When
Pinnacle has not posted a given market for an event yet, the consensus
bookOdds field is used as a fallback so the slate is never empty.

Output shape on matches.odds.sgo (engine.js compatible flat keys):
    ml_a, ml_b
    gw_a_line, gw_a_over, gw_a_under
    gw_b_line, gw_b_over, gw_b_under
    games_total_line, games_total_over, games_total_under
    sets_total_line, sets_total_over, sets_total_under
    set_spread_a_line, set_spread_a_odds
    set_spread_b_line, set_spread_b_odds
    game_spread_a_line, game_spread_a_odds
    game_spread_b_line, game_spread_b_odds
    set_a_20, set_a_21, set_b_20, set_b_21   ← engine direct keys
    p3set                                     ← engine direct key
    raw

Top-level promoted keys (engine.js reads these without a source prefix):
    ml_a, ml_b,
    gw_a_line, gw_a_over, gw_b_line, gw_b_over,
    set_a_20, set_a_21, set_b_20, set_b_21,
    p3set

Kalshi win-% fields (kalshi_prob_a / kalshi_prob_b) are NEVER touched by
this service. Kalshi remains the sole source of win probability.
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

    # Moneyline
    ml_a, _ = _odds_for(odds.get("points-home-game-ml-home"))
    ml_b, _ = _odds_for(odds.get("points-away-game-ml-away"))
    if ml_a is not None:
        fields["ml_a"] = ml_a
    if ml_b is not None:
        fields["ml_b"] = ml_b

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
    # When the total-sets O/U line is 2.5, "Over 2.5" implies the 3rd-set
    # path. Use the de-vigged probability of Over.
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

    # Engine direct: set_{a,b}_{20,21}
    #
    # On a best-of-3 the four set-score American odds are required by
    # engine.js's hasRichOdds() gate. Derive from set-spread + p3set:
    #
    #   a wins 2-0  ↔ a covers -1.5 sets AND match goes 2 sets
    #               ≈ P(a wins) * P(NOT 3 sets) * (a-share of straight wins)
    #
    # Simpler de-vig approach using only what SGO gives us:
    #   P(a wins 2-0) = P(a wins outright) * (1 - p3set)
    #   P(a wins 2-1) = P(a wins outright) * p3set
    # and convert each back to American odds.
    p_a = _american_to_prob(ml_a)
    p_b = _american_to_prob(ml_b)
    p3 = fields.get("p3set")
    if p_a is not None and p_b is not None and p3 is not None:
        s = p_a + p_b
        if s > 0:
            wp_a = p_a / s
            wp_b = p_b / s
            for
