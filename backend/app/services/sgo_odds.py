"""SportsGameOdds (SGO) v2 integration for tennis odds.

Pulls Pinnacle odds for the markets the engine actually consumes:
    games-{home,away}-game-ou      → gw_*_line / gw_*_over / gw_*_under
    games-all-game-ou              → games_total_line / *_over / *_under
    points-all-game-ou             → sets_total_line + p3set derivation
    points-{home,away}-game-sp     → set_spread_*_line / *_odds
    games-{home,away}-game-sp      → game_spread_*_line / *_odds

Moneyline (points-*-game-ml-*) is intentionally NOT pulled — Kalshi is
the sole source of win probability (kalshi_prob_a / kalshi_prob_b).
Reading SGO moneyline would give us a second, conflicting win-% signal.

set_a_20 / set_a_21 / set_b_20 / set_b_21 are derived from the existing
matches.odds.kalshi_prob_a + p3set during ingest, so the engine has the
four set-score American odds hasRichOdds() expects without needing SGO ML.

Top-level promoted keys (engine.js reads these without a source prefix):
    gw_a_line, gw_a_over, gw_b_line, gw_b_over,
    set_a_20, set_a_21, set_b_20, set_b_21, p3set


NAME RESOLUTION
───────────────
SGO returns players by their team-feed full names (e.g. "Catherine McNally")
which sometimes differ from the names DK / Kalshi use (e.g. "Caty McNally").
Strict full-name resolution via PlayerNormalizer fails on those divergences,
so this module layers a last-name + opposing-sides fallback ON TOP of the
normalizer when create_if_missing=False:

  1. Try strict full-name resolution first.
  2. If strict fails, scope to the candidate slate matches already loaded
     in memory and ask: do these two SGO surnames uniquely identify exactly
     one slate match, with the two players on opposite sides?
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
    """Normalized last-name token. 'Catherine McNally' → 'mcnally',
    'Vít Kopřiva' → 'kopriva'. Empty string if name is empty.

    Drops common particles ('de', 'van', 'der', etc.) so 'Alex de Minaur'
    → 'minaur' rather than 'de'.
    """
    if not name:
        return ""
    cleaned = _strip_accents(name).strip().lower()
    parts = [
        p
        for p in cleaned.split()
        if p
        not in {
            "de",
            "van",
            "der",
            "den",
            "da",
            "di",
            "du",
            "le",
            "la",
            "el",
            "al",
            "del",
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


def _augment_set_scores_from_kalshi(
    eng_fields: dict, kalshi_wp_a: Optional[float]
) -> None:
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


# ── Ingest ────────────────────────────────────────────────────────────


def _seed_alias(canonical_id: str, source: str, raw_name: str) -> bool:
    """Persist a source-specific alias for an existing canonical_id.

    Returns True if a new alias row was actually written, False if the
    alias was already present (idempotent).
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
    if existing == raw_name:
        return False
    if isinstance(existing, list) and raw_name in existing:
        return False

    if existing is None:
        aliases[source] = raw_name
    elif isinstance(existing, str):
        aliases[source] = [existing, raw_name]
    elif isinstance(existing, list):
        existing.append(raw_name)
        aliases[source] = existing
    else:
        aliases[source] = raw_name

    try:
        db.table("players").update({"aliases": aliases}).eq(
            "canonical_id", canonical_id
        ).execute()
    except Exception as e:
        logger.warning(
            "alias seed: failed to persist %s.%s = %r (%s)",
            canonical_id,
            source,
            raw_name,
            e,
        )
        return False

    logger.info(
        "SGO alias seeded: %s.aliases.%s = %r", canonical_id, source, raw_name
    )
    return True


def _surname_fallback_match(
    sgo_a_name: str,
    sgo_b_name: str,
    candidate_matches: list[dict],
    surname_index: dict[str, list[tuple[str, str]]],
) -> Optional[dict]:
    """Last-name + opposing-sides fallback resolver.

    Returns the slate match dict if exactly one candidate match has both
    SGO surnames present on opposite sides. Returns None if no unique
    match found OR if the surnames map to multiple matches (ambiguous —
    refuse rather than guess).

    surname_index: {surname_key: [(canonical_id, side), ...]} pre-built
    from candidate_matches so we don't re-scan on every event.
    """
    key_a = _last_name_key(sgo_a_name)
    key_b = _last_name_key(sgo_b_name)
    if not key_a or not key_b or key_a == key_b:
        return None

    a_hits = surname_index.get(key_a) or []
    b_hits = surname_index.get(key_b) or []
    if not a_hits or not b_hits:
        return None

    # Find slate match(es) where one player has surname key_a and the
    # other has surname key_b, on opposite sides.
    candidates_by_match: dict[str, dict] = {m["id"]: m for m in candidate_matches}
    matches_found: list[tuple[str, str]] = []  # (match_id, sgo_a_side)

    for cid_a, side_a in a_hits:
        for cid_b, side_b in b_hits:
            if cid_a == cid_b:
                continue  # same player — degenerate
            if side_a == side_b:
                continue  # both on same side — invalid
            # Find the match that has cid_a on side_a AND cid_b on side_b
            for m in candidate_matches:
                pa = m.get("player_a_id")
                pb = m.get("player_b_id")
                if side_a == "a" and pa == cid_a and pb == cid_b:
                    matches_found.append((m["id"], "a"))
                elif side_a == "b" and pb == cid_a and pa == cid_b:
                    matches_found.append((m["id"], "b"))

    # Deduplicate (same match might be found via multiple cid pairings)
    unique_match_ids = {mf[0] for mf in matches_found}
    if len(unique_match_ids) != 1:
        return None  # zero or ambiguous — refuse

    match_id = next(iter(unique_match_ids))
    sgo_a_side = next(mf[1] for mf in matches_found if mf[0] == match_id)
    m = candidates_by_match[match_id]
    # Resolve which canonical_ids correspond to SGO's a and b
    if sgo_a_side == "a":
        sgo_a_cid = m["player_a_id"]
        sgo_b_cid = m["player_b_id"]
    else:
        sgo_a_cid = m["player_b_id"]
        sgo_b_cid = m["player_a_id"]

    return {
        "match": m,
        "sgo_a_canonical_id": sgo_a_cid,
        "sgo_b_canonical_id": sgo_b_cid,
        "sgo_a_side": sgo_a_side,
    }


async def _ingest_rows(rows: list[TennisOddsRow], league: str) -> dict:
    if not rows:
        return {"matched": 0, "matched_via_fallback": 0, "aliases_seeded": 0}

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

    # Build a surname → [(canonical_id, side), ...] index for fallback.
    # Pull display names for the candidate-match players in a single round-trip.
    needed_cids = set()
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
            cid_to_display = {p["canonical_id"]: p.get("display_name") or "" for p in prows}
        except Exception as e:
            logger.warning("SGO: failed to load player display names: %s", e)

    surname_index: dict[str, list[tuple[str, str]]] = {}
    for m in candidate_matches:
        for side, cid_field in (("a", "player_a_id"), ("b", "player_b_id")):
            cid = m.get(cid_field)
            if not cid:
                continue
            display = cid_to_display.get(cid) or ""
            # Use the canonical_id last token as a fallback if display is empty
            key_source = display or cid.replace("_", " ")
            key = _last_name_key(key_source)
            if not key:
                continue
            surname_index.setdefault(key, []).append((cid, side))

    matched = 0
    matched_via_fallback = 0
    aliases_seeded = 0

    for row in rows:
        # Strict path first
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

        if target_match is None:
            # Surname + opposing-sides fallback. Only fires when strict
            # resolution didn't land on a slate match. Refuses ambiguity.
            fb = _surname_fallback_match(
                row.player_a_name,
                row.player_b_name,
                candidate_matches,
                surname_index,
            )
            if fb is not None:
                # Time sanity (3h window like strict path)
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

        # Auto-seed aliases so next encounter is a strict-path direct hit.
        if _seed_alias(sgo_a_cid, "sgo", row.player_a_name):
            aliases_seeded += 1
        if _seed_alias(sgo_b_cid, "sgo", row.player_b_name):
            aliases_seeded += 1

        # Orientation: our match.player_a_id is "side a". If SGO's home is
        # actually our side b, swap the engine-fields a/b suffixes.
        pa = target_match["player_a_id"]
        swap = pa == sgo_b_cid  # our A == their away → swap
        eng_fields = row.to_engine_shape()
        if swap:
            eng_fields = _swap_ab_fields(eng_fields)

        existing_odds = target_match.get("odds") or {}
        wp_a = (
            existing_odds.get("kalshi_prob_a")
            if isinstance(existing_odds, dict)
            else None
        )
        _augment_set_scores_from_kalshi(eng_fields, wp_a)

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
        else
