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
    raw

Top-level promoted keys (engine.js reads these without a source prefix):
    ml_a, ml_b, gw_a_line, gw_a_over, gw_b_line, gw_b_over

Kalshi win-% fields (kalshi_prob_a / kalshi_prob_b) are NEVER touched by this
service. Kalshi remains the sole source of win probability.
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


def _american_to_int(am: Optional[str]) -> Optional[int]:
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


def _odds_for(odd: dict) -> tuple[Optional[int], Optional[float]]:
    """Return (american
