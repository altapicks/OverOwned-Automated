"""Pydantic schemas for API contract and internal typing."""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


# ──────────────────────────────────────────────────────────────────────
# Raw DK shapes — what we pull from DK's endpoints
# ──────────────────────────────────────────────────────────────────────
class DKDraftGroup(BaseModel):
    draft_group_id: int
    sport: str
    contest_type: str
    slate_label: Optional[str] = None
    lock_time: Optional[datetime] = None
    salary_cap: int = 50000


class DKDraftable(BaseModel):
    """One entry in DK's draftables response. For tennis showdown a single
    player shows up three times (CPT/ACPT/FLEX with different salaries)."""
    dk_player_id: int
    display_name: str
    salary: int
    roster_position: str  # 'P' for classic, 'CPT'/'FLEX' for showdown
    avg_ppg: Optional[float] = None
    competition_id: Optional[int] = None
    competition_name: Optional[str] = None  # "Player A vs Player B"
    start_time: Optional[datetime] = None


# ──────────────────────────────────────────────────────────────────────
# Frontend-facing shape — matches the existing slate.json schema so the
# React app can consume /api/slates/today without structural changes.
# ──────────────────────────────────────────────────────────────────────
class FrontendMatchOdds(BaseModel):
    """Odds block — populated by downstream services (Odds API, Kalshi, bet365).
    Shape matches existing slate.json to avoid UI changes, plus Kalshi fields
    added in piece #2 for the gold [K] badge in the Odds column."""
    ml_a: Optional[int] = None
    ml_b: Optional[int] = None
    set_a_20: Optional[int] = None
    set_a_21: Optional[int] = None
    set_b_20: Optional[int] = None
    set_b_21: Optional[int] = None
    gw_a_line: Optional[float] = None
    gw_a_over: Optional[int] = None
    gw_b_line: Optional[float] = None
    gw_b_over: Optional[int] = None
    brk_a_line: Optional[float] = None
    brk_a_over: Optional[int] = None
    brk_b_line: Optional[float] = None
    brk_b_over: Optional[int] = None
    ace_a_5plus: Optional[int] = None
    ace_a_10plus: Optional[int] = None
    ace_b_5plus: Optional[int] = None
    ace_b_10plus: Optional[int] = None
    df_a_2plus: Optional[int] = None
    df_a_3plus: Optional[int] = None
    df_b_2plus: Optional[int] = None
    df_b_3plus: Optional[int] = None
    # Kalshi implied probabilities (0.0-1.0). Promoted from matches.odds.kalshi_prob_a/b
    # by the ingestion layer. Frontend reads these to render the gold [K] Odds badge.
    kalshi_prob_a: Optional[float] = None
    kalshi_prob_b: Optional[float] = None
    # P(match goes 3 sets). Used by engine.js sharp mode to derive 4-way set
    # betting probabilities when only Sets Played odds are available (typical
    # of Underdog-style prop feeds that don't post a full 4-way set market).
    # Populated by the Underdog transformer.
    p3set: Optional[float] = None
    # Raw posted lines from PrizePicks/Underdog — used for fade signal comparison
    # (projection vs posted line) and Track Record grading. NOT projection inputs.
    # Schema: {"a": {"aces": 4.5, "games_won": 12.5, ...}, "b": {...}}
    posted_lines: Optional[dict] = None


class FrontendMatch(BaseModel):
    player_a: str
    player_b: str
    start_time: Optional[str] = None
    tournament: str = ""
    surface: Optional[str] = None
    odds: FrontendMatchOdds = Field(default_factory=FrontendMatchOdds)
    # Opening odds — frozen on first ingest, never updated. Used by the
    # archive view so the "moved from X to Y" delta is canonical across all
    # users (live view still uses per-user localStorage baselines).
    opening_odds: FrontendMatchOdds = Field(default_factory=FrontendMatchOdds)
    adj_a: float = 0
    adj_b: float = 0


class FrontendPlayer(BaseModel):
    name: str
    id: int
    salary: int
    avg_ppg: float = 0
    # Showdown fields — None for classic slates. Preserved for forward compat
    # with the existing engine.js showdown handling.
    cpt_id: Optional[int] = None
    cpt_salary: Optional[int] = None
    acpt_id: Optional[int] = None
    acpt_salary: Optional[int] = None
    flex_id: Optional[int] = None
    flex_salary: Optional[int] = None
    # Manually-provided pool ownership pct (0-100). When set, the frontend
    # uses it directly and skips Monte Carlo for this player. Null for
    # slates where Alta hasn't supplied values — those fall back to
    # Monte Carlo. Populated from slate_players.ss_pool_own.
    ss_pool_own: Optional[float] = None


class FrontendPPLine(BaseModel):
    player: str
    stat: str
    line: float
    mult: Optional[str] = ""
    # Source platform: 'prizepicks' for Fantasy Score lines from admin UI,
    # 'underdog' for stat-prop lines promoted from matches.odds.posted_lines.
    source: Optional[str] = "prizepicks"


class FrontendSlate(BaseModel):
    """The exact shape the React app expects to consume."""
    date: str
    sport: str = "tennis"
    slate_label: Optional[str] = None
    lock_time: Optional[str] = None
    matches: list[FrontendMatch]
    dk_players: list[FrontendPlayer]
    pp_lines: list[FrontendPPLine] = Field(default_factory=list)
    meta: dict = Field(default_factory=dict)  # ingestion timestamps, source versions, etc.


# ──────────────────────────────────────────────────────────────────────
# Admin / ops shapes
# ──────────────────────────────────────────────────────────────────────
class UnmatchedName(BaseModel):
    id: int
    source: str
    sport: str
    raw_name: str
    best_guess_id: Optional[str] = None
    best_guess_score: Optional[float] = None
    first_seen_at: datetime


class HealthStatus(BaseModel):
    status: Literal["ok", "degraded", "down"]
    version: str
    db_ok: bool
    last_successful_ingest: Optional[datetime] = None
    unmatched_count: int = 0
