"""SportsGameOdds (SGO) v2 integration for tennis odds.

Pulls Pinnacle odds for the markets the engine actually consumes:
    games-{home,away}-game-ou      → gw_*_line / gw_*_over / gw_*_under
    games-all-game-ou              → games_total_line / *_over / *_under
    points-all-game-ou             → sets_total_line + p3set derivation
    points-{home,away}-game-sp     → set_spread_*_line / *_odds
    games-{home,away}-game-sp      → game_spread_*_line / *_odds

Moneyline (points-*-game-ml-*) is intentionally NOT pulled — Kalshi is
the sole source of win probability (kalshi_prob_a / kalshi_prob_b).

v6.5 — set 4-way (set_a_20 / _21 / b_20 / b_21) is now derived from the
DEVIGGED set-spread 2-way market when both sides are present (e.g.,
A -1.5 @ -350 vs B +1.5 @ +250). The Kalshi-prob × p3set independence
math is kept as a fallback for rows that don't carry both sides of the
set spread. The set-spread path is sharper because the spread market
prices straight-set sweep probability directly, instead of assuming
that set count is independent of who wins.

NAME RESOLUTION
───────────────
SGO returns players by their team-feed full names (e.g. "Catherine McNally")
which sometimes differ from DK / Kalshi names (e.g. "Caty McNally"). To
handle that durably without false positives, this module layers a last-name
+ opposing-sides fallback ON TOP of the strict normalizer:

  1. Try strict full-name resolution first.
  2. If strict fails, ask: do these two SGO surnames uniquely identify
     exactly one slate match, with the two players on opposite sides?
  3. If yes, accept that orientation. False positives are impossible —
     there's only one match it can map to and the orientation is locked.
  4. On success (strict OR fallback), write the SGO-side full name into
     players.aliases.sgo so the next encounter is a direct cache hit.
"""
from __future__ import annotations

import logging
import unicodedata
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


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c)
    )


def _last_name_key(name: str) -> str:
    """Normalized last-name token. Handles particles + accents.

    'Catherine McNally' → 'mcnally'
    'Vít Kopřiva' → 'kopriva'
    'Alex de Minaur' → 'minaur'
    """
    if not name:
        return ""
    cleaned = _strip_accents(name).strip().lower()
    parts = [
        p
        for p in cleaned.split()
        if p
        not in {
            "de", "van", "der", "den", "da", "di", "du",
            "le", "la", "el", "al", "del",
        }
    ]
    if not parts:
        return cleaned
    return parts[-1]


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

    if sets_line is not None and abs(float(sets_line) - 2.5) < 0.01:
        p_over = _american_to_prob(over_odds)
        p_under = _american_to_prob(under_odds)
        if p_over is not None and p_under is not None and (p_over + p_under) > 0:
            fields["p3set"] = round(p_over / (p_over + p_under), 4)
        elif p_over is not None:
            fields["p3set"] = round(p_over, 4)

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


# ── Set 4-way derivation ──────────────────────────────────────────────


def _derive_set_4way_from_spread(
    eng_fields: dict, kalshi_wp_a: Optional[float]
) -> bool:
    """v6.5 — sharp path. Compute set_a_20 / _21 / b_20 / b_21 from the
    devigged Pinnacle set-spread 2-way market.

    Inputs required (all from eng_fields except wp_a):
        set_spread_a_line, set_spread_a_odds (American)
        set_spread_b_line, set_spread_b_odds (American)
        p3set
        kalshi_wp_a

    The set spread in tennis bo3 is always ±1.5 — there are only two
    possible margins (2-0 or 2-1), so -1.5 covers iff the favorite sweeps.
    Devig math:
        p_a_devig = (1/A_implied) / ((1/A_implied) + (1/B_implied))
                    where A_implied = americanToProb(set_spread_a_odds)
                    (note we already use raw probs, not 1/decimal)
        Actually: p_a_devig = p_a_raw / (p_a_raw + p_b_raw).

    Then with the favorite's straight-sweep probability anchored:
        wp_a, wp_b from Kalshi
        p_a20 = devigged P(A -1.5 covers) = devigged P(A wins 2-0)
        p_a21 = wp_a - p_a20
        p_b21 = p3set - p_a21         # because total 3-set prob = p_a21 + p_b21
        p_b20 = wp_b - p_b21

    All four sum to 1.0 by construction. Returns True on success, False
    on any precondition miss → caller falls through to the kalshi-only
    derivation.
    """
    a_line = eng_fields.get("set_spread_a_line")
    b_line = eng_fields.get("set_spread_b_line")
    a_odds = eng_fields.get("set_spread_a_odds")
    b_odds = eng_fields.get("set_spread_b_odds")
    p3set = eng_fields.get("p3set")

    if None in (a_line, b_line, a_odds, b_odds, p3set, kalshi_wp_a):
        return False

    try:
        a_line_f = float(a_line)
        b_line_f = float(b_line)
        wp_a = float(kalshi_wp_a)
        p3 = float(p3set)
    except (TypeError, ValueError):
        return False

    # Spreads must be a matched ±1.5 pair (or any opposite-sign pair really,
    # but tennis bo3 only has 1.5).
    if abs(a_line_f + b_line_f) > 0.01:
        return False
    if abs(abs(a_line_f) - 1.5) > 0.01:
        return False
    if not (0.0 < wp_a < 1.0) or not (0.0 <= p3 <= 1.0):
        return False

    p_a_raw = _american_to_prob(a_odds)
    p_b_raw = _american_to_prob(b_odds)
    if p_a_raw is None or p_b_raw is None:
        return False
    total = p_a_raw + p_b_raw
    # Sanity: vig should land in a reasonable band. Tennis Pinnacle vig on
    # set spread is ~1-3%. Anything wildly outside means the market isn't a
    # true 2-way pair (e.g., one side suspended). Bail to fallback.
    if total <= 0.95 or total > 1.20:
        return False

    p_a_devig = p_a_raw / total

    # Translate "side A's spread devig" → "P(A wins 2-0)".
    # If A's line is -1.5, A covers iff A sweeps → p_a_devig = P(A 2-0).
    # If A's line is +1.5, A covers iff A wins ≥1 set → 1-p_a_devig = P(B 2-0).
    if a_line_f < 0:
        p_a20 = p_a_devig
    else:
        # A is the underdog at +1.5. Derive P(B 2-0) directly, then back into
        # P(A 2-0) via wp + p3set bookkeeping.
        p_b20_direct = 1.0 - p_a_devig
        # p_a20 = wp_a - p_a21, where p_a21 = p3set - p_b21 = p3set - (wp_b - p_b20_direct)
        # → p_a20 = wp_a - p3set + (1 - wp_a) - p_b20_direct
        #         = 1 - p3set - p_b20_direct
        p_a20 = 1.0 - p3 - p_b20_direct

    if not (0.0 <= p_a20 <= 1.0):
        return False

    wp_b = 1.0 - wp_a
    p_a21 = wp_a - p_a20
    p_b21 = p3 - p_a21
    p_b20 = wp_b - p_b21

    # Tolerance for accumulated float noise; clamp tiny negatives to 0.
    eps = 0.005
    for nm, val in (("p_a20", p_a20), ("p_a21", p_a21), ("p_b20", p_b20), ("p_b21", p_b21)):
        if val < -eps:
            return False

    p_a20 = max(0.0, min(1.0, p_a20))
    p_a21 = max(0.0, min(1.0, p_a21))
    p_b20 = max(0.0, min(1.0, p_b20))
    p_b21 = max(0.0, min(1.0, p_b21))

    pairs = {
        "set_a_20": p_a20,
        "set_a_21": p_a21,
        "set_b_20": p_b20,
        "set_b_21": p_b21,
    }
    for k, prob in pairs.items():
        am = _prob_to_american(prob)
        if am is not None:
            eng_fields[k] = am

    eng_fields["set_4way_source"] = "spread_devig"
    return True


def _augment_set_scores_from_kalshi(
    eng_fields: dict, kalshi_wp_a: Optional[float]
) -> None:
    """Fallback derivation. Assumes outcome ⊥ set count (independence).
    Used when set-spread devig isn't available (e.g., Pinnacle hasn't
    posted both sides of the spread, or the line isn't ±1.5).
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
    eng_fields["set_4way_source"] = "kalshi_independence"


def _augment_set_scores(eng_fields: dict, kalshi_wp_a: Optional[float]) -> None:
    """Wrapper: try set-spread devig first; on failure use kalshi independence."""
    if _derive_set_4way_from_spread(eng_fields, kalshi_wp_a):
        return
    _augment_set_scores_from_kalshi(eng_fields, kalshi_wp_a)


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
    fallback_total = 0
    aliases_seeded_total = 0

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
            stats = await _ingest_rows(rows, league)
            matched_total += stats["matched"]
            fallback_total += stats["matched_via_fallback"]
            aliases_seeded_total += stats["aliases_seeded"]
        except httpx.HTTPStatusError as e:
            logger.error("SGO HTTP error for %s: %s", league, e)
        except Exception as e:
            logger.exception("SGO fetch failed for %s: %s", league, e)

    return {
        "fetched": fetched_total,
        "matched": matched_total,
        "matched_via_fallback": fallback_total,
        "aliases_seeded": aliases_seeded_total,
    }


# ── Alias seeding ─────────────────────────────────────────────────────


def _seed_alias(canonical_id: str, source: str, raw_name: str) -> bool:
    """Persist a source-specific alias for an existing canonical_id.

    Returns True if a new alias was actually written, False if already
    present (idempotent — safe to call on every successful match).
    """
    if not canonical_id or not raw_name:
        return False
    db = get_client()
    try:
        row = (
            db.table("players")
            .select("aliases")
            .eq("canonical_id", canonical_id)
            .single()
            .execute()
            .data
            or {}
        )
    except Exception as e:
        logger.warning("alias seed: failed to read player %s: %s", canonical_id, e)
        return False

    aliases = row.get("aliases") or {}
    existing = aliases.get(source)
    if isinstance(existing, str):
        if existing == raw_name:
            return False
        aliases[source] = [existing, raw_name]
    elif isinstance(existing, list):
        if raw_name in existing:
            return False
        existing.append(raw_name)
        aliases[source] = existing
    else:
        aliases[source] = raw_name

    try:
        db.table("players").update({"aliases": aliases}).eq(
            "canonical_id", canonical_id
        ).execute()
        return True
    except Exception as e:
        logger.warning("alias seed: failed to write player %s: %s", canonical_id, e)
        return False


# ── Surname-pair fallback ─────────────────────────────────────────────


def _surname_fallback_match(
    sgo_a_name: str,
    sgo_b_name: str,
    candidate_matches: list[dict],
    surname_index: dict[str, list[tuple[str, str]]],
) -> Optional[dict]:
    """Locate a slate match where the two SGO surnames map uniquely to its
    two players on opposing sides. Returns dict with the matched canonical
    ids and the chosen match row, or None if no unambiguous mapping exists.
    """
    a_key = _last_name_key(sgo_a_name)
    b_key = _last_name_key(sgo_b_name)
    if not a_key or not b_key or a_key == b_key:
        return None

    a_candidates = surname_index.get(a_key) or []
    b_candidates = surname_index.get(b_key) or []
    if not a_candidates or not b_candidates:
        return None

    matches_to_match: dict[str, dict] = {m["id"]: m for m in candidate_matches}

    a_by_match: dict[str, set[tuple[str, str]]] = {}
    for cid, side in a_candidates:
        for m in candidate_matches:
            if m["player_a_id"] == cid or m["player_b_id"] == cid:
                a_by_match.setdefault(m["id"], set()).add((cid, side))

    b_by_match: dict[str, set[tuple[str, str]]] = {}
    for cid, side in b_candidates:
        for m in candidate_matches:
            if m["player_a_id"] == cid or m["player_b_id"] == cid:
                b_by_match.setdefault(m["id"], set()).add((cid, side))

    common_match_ids = set(a_by_match.keys()) & set(b_by_match.keys())
    if len(common_match_ids) != 1:
        return None

    match_id = next(iter(common_match_ids))
    a_in = a_by_match[match_id]
    b_in = b_by_match[match_id]
    if len(a_in) != 1 or len(b_in) != 1:
        return None

    a_cid, a_side = next(iter(a_in))
    b_cid, b_side = next(iter(b_in))
    if a_cid == b_cid or a_side == b_side:
        return None

    return {
        "match": matches_to_match[match_id],
        "sgo_a_canonical_id": a_cid,
        "sgo_b_canonical_id": b_cid,
    }


# ── Ingest loop ───────────────────────────────────────────────────────


async def _ingest_rows(rows: list[TennisOddsRow], league: str) -> dict:
    if not rows:
        return {"matched": 0, "matched_via_fallback": 0, "aliases_seeded": 0}

    db = get_client()
    normalizer = PlayerNormalizer(sport="tennis")

    cutoff_lo = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    cutoff_hi = (datetime.now(timezone.utc) + timedelta(hours=72)).isoformat()
    candidate_matches = (
        db.table("matches")
        .select("id, slate_id, player_a_id, player_b_id, start_time, odds")
        .gte("start_time", cutoff_lo)
        .lte("start_time", cutoff_hi)
        .execute()
        .data
        or []
    )
    if not candidate_matches:
        return {"matched": 0, "matched_via_fallback": 0, "aliases_seeded": 0}

    needed_cids: set[str] = set()
    for m in candidate_matches:
        if m.get("player_a_id"):
            needed_cids.add(m["player_a_id"])
        if m.get("player_b_id"):
            needed_cids.add(m["player_b_id"])
    cid_to_display: dict[str, str] = {}
    if needed_cids:
        try:
            prows = (
                db.table("players")
                .select("canonical_id, display_name")
                .in_("canonical_id", list(needed_cids))
                .execute()
                .data
                or []
            )
            cid_to_display = {
                p["canonical_id"]: p.get("display_name") or "" for p in prows
            }
        except Exception as e:
            logger.warning("SGO: failed to load player display names: %s", e)

    surname_index: dict[str, list[tuple[str, str]]] = {}
    for m in candidate_matches:
        for side, cid_field in (("a", "player_a_id"), ("b", "player_b_id")):
            cid = m.get(cid_field)
            if not cid:
                continue
            display = cid_to_display.get(cid) or ""
            key_source = display or cid.replace("_", " ")
            key = _last_name_key(key_source)
            if not key:
                continue
            surname_index.setdefault(key, []).append((cid, side))

    matched = 0
    matched_via_fallback = 0
    aliases_seeded = 0

    for row in rows:
        # Strict full-name resolution first.
        a = normalizer.resolve(
            row.player_a_name, source="sgo", create_if_missing=False
        )
        b = normalizer.resolve(
            row.player_b_name, source="sgo", create_if_missing=False
        )

        strict_ok = (
            a.canonical_id
            and b.canonical_id
            and (a.auto_resolved or a.was_new)
            and (b.auto_resolved or b.was_new)
        )

        target_match: Optional[dict] = None
        sgo_a_cid: Optional[str] = None
        sgo_b_cid: Optional[str] = None
        used_fallback = False

        if strict_ok:
            sgo_a_cid = a.canonical_id
            sgo_b_cid = b.canonical_id
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
                if {pa, pb} == {sgo_a_cid, sgo_b_cid}:
                    target_match = m
                    break

        # Surname + opposing-sides fallback. Only fires when strict path
        # didn't lock on a slate match. Refuses any ambiguity.
        if target_match is None:
            fb = _surname_fallback_match(
                row.player_a_name,
                row.player_b_name,
                candidate_matches,
                surname_index,
            )
            if fb is not None:
                m = fb["match"]
                m_start = m.get("start_time")
                ok_time = True
                if m_start:
                    try:
                        m_dt = datetime.fromisoformat(m_start.replace("Z", "+00:00"))
                        if abs((m_dt - row.commence_time).total_seconds()) > 10800:
                            ok_time = False
                    except (ValueError, TypeError):
                        ok_time = False
                if ok_time:
                    target_match = m
                    sgo_a_cid = fb["sgo_a_canonical_id"]
                    sgo_b_cid = fb["sgo_b_canonical_id"]
                    used_fallback = True
                    logger.info(
                        "SGO surname-fallback matched: %r vs %r → %s vs %s",
                        row.player_a_name,
                        row.player_b_name,
                        sgo_a_cid,
                        sgo_b_cid,
                    )

        if target_match is None or sgo_a_cid is None or sgo_b_cid is None:
            continue

        # Auto-seed aliases so the next encounter is a strict-path direct hit.
        if _seed_alias(sgo_a_cid, "sgo", row.player_a_name):
            aliases_seeded += 1
        if _seed_alias(sgo_b_cid, "sgo", row.player_b_name):
            aliases_seeded += 1

        # Orientation: our match.player_a_id is "side a". If SGO's home
        # is actually our side b, swap engine-fields a/b suffixes.
        pa = target_match["player_a_id"]
        swap = pa == sgo_b_cid
        eng_fields = row.to_engine_shape()
        if swap:
            eng_fields = _swap_ab_fields(eng_fields)

        existing_odds = target_match.get("odds") or {}
        wp_a = (
            existing_odds.get("kalshi_prob_a")
            if isinstance(existing_odds, dict)
            else None
        )
        # v6.5: prefer set-spread devig; fall back to Kalshi independence.
        _augment_set_scores(eng_fields, wp_a)

        await _write_match_odds(
            match_id=target_match["id"],
            slate_id=target_match["slate_id"],
            source="sgo",
            engine_fields=eng_fields,
            raw_market_payload={"league": league, "row": row.raw},
        )
        matched += 1
        if used_fallback:
            matched_via_fallback += 1

    return {
        "matched": matched,
        "matched_via_fallback": matched_via_fallback,
        "aliases_seeded": aliases_seeded,
    }


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
    """Merge SGO odds into matches.odds[source] + promote engine-flat keys.

    Touches only matches.odds[source] and the engine-flat top-level keys.
    NEVER modifies matches.odds.kalshi or kalshi_prob_a/_b. Moneyline keys
    (ml_a / ml_b) are intentionally NOT promoted — Kalshi is the sole
    source of win probability.
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
    # source prefix; hasRichOdds() (v6.1+) gates on a wp source + a set
    # market + a games market. ml_a / ml_b are deliberately excluded.
    promoted_keys = (
        "gw_a_line", "gw_a_over", "gw_a_under",
        "gw_b_line", "gw_b_over", "gw_b_under",
        "set_a_20", "set_a_21", "set_b_20", "set_b_21",
        "p3set",
        # v6.5: also expose set + game spread fields top-level so the UI
        # can show "A -1.5 -350" etc. and so the engine has them around
        # if a future projection tweak wants to consume them.
        "set_spread_a_line", "set_spread_a_odds",
        "set_spread_b_line", "set_spread_b_odds",
        "game_spread_a_line", "game_spread_a_odds",
        "game_spread_b_line", "game_spread_b_odds",
        "set_4way_source",
    )
    for k in promoted_keys:
        if k in engine_fields and engine_fields[k] is not None:
            current[k] = engine_fields[k]

    # Defensive: strip any stale ml_a / ml_b promoted by earlier deploys.
    for stale_key in ("ml_a", "ml_b"):
        if stale_key in current:
            current.pop(stale_key, None)

    db.table("matches").update({"odds": current}).eq("id", match_id).execute()

    # Opening odds preservation: write per-source first-seen snapshot once.
    opening = row.get("opening_odds") or {}
    if not isinstance(opening, dict):
        opening = {}
    if source not in opening:
        opening[source] = engine_fields
    db.table("matches").update({"opening_odds": opening}).eq(
        "id", match_id
    ).execute()

    # Append-only history row for line-movement audits.
    db.table("odds_history").insert(
        {
            "match_id": match_id,
            "slate_id": slate_id,
            "source": source,
            "market": "totals+spreads+per_player_games+set_score",
            "payload": {"engine_fields": engine_fields, "raw": raw_market_payload},
        }
    ).execute()
