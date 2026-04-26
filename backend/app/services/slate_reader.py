"""Read-side service: builds the frontend-facing FrontendSlate from DB rows.

This is what the API returns to the React app. The shape matches the existing
slate.json schema exactly so no UI changes are needed.

PP integration model (the part that runs the show):

  prizepicks_lines (raw, all 3 variants per stat, multiplier per variant) ──┐
                                                                            │
                                          ┌─────────────────────────────────┘
                                          ▼
                ┌─────────────────────────────────────────┐
                ▼                                         ▼
        slate.pp_lines                       match.odds.posted_lines
        (PP tab UI) — every variant          — engine.js posted_lines
        emitted as a separate row            override path → DK/PP proj.
        (player, stat, line, mult,           Uses MULTIPLIER-AWARE Poisson
        odds_type). Fantasy Score is         fit (v6.5) to convert (line,mult)
        what the PP tab displays per the     pairs into a projection center,
        user's spec; other stats are         instead of taking the line
        exposed for edge calcs.              value verbatim.

v6.4 — pp_lines now emits one row per (player, stat, odds_type) instead of
collapsing to one row per (player, stat). Multiplier is included so the PP
tab UI can render goblin/demon variants alongside standard with their
correct payouts.

v6.5 — engine-side aggregation now uses a multiplier-aware Poisson fit for
Aces / Double Faults / Break Points Won / Total Games Won. Each variant
(line, mult) implies a Poisson λ via P(X ≥ ⌈line⌉) ≈ 1/mult; we fit each
and average. This is sharper than the previous "use standard if present
else median of lines" because (a) most stat-prop rows have no standard
variant, (b) the median of {goblin 4.5, demon 6.5} is just 5.5 regardless
of the multipliers, which throws away the price information.

v6.5 — also: posted_lines.{a,b}.games_won / games_lost are now SUPPRESSED
when matches.odds.gw_a_line / gw_b_line are present. Per spec, GW/GL must
come from Pinnacle (SGO) not PP. The engine reads the Pinnacle gw_*_line
fields directly in sharp mode, so dropping them from posted_lines lets the
engine consume them. PP games_won is still kept as a fallback when SGO
hasn't posted the games-O/U markets yet.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from statistics import median
from typing import Optional

from app.db import get_client
from app.models import (
    FrontendMatch,
    FrontendMatchOdds,
    FrontendPlayer,
    FrontendPPLine,
    FrontendSlate,
)

# v6.9: lookup_venue used to derive match.tournament/surface from the slate's
# slate_label when the upstream ingester left match.tournament null. Same
# venue lookup the weather pipeline uses (with token-fallback so labels like
# "Featured (Madrid)" still resolve to the "madrid open" key). Wrapped in a
# try/except so this file remains importable even if tennis_venues.py is
# missing in some deployment.
try:
    from app.services.tennis_venues import lookup_venue
except ImportError:
    def lookup_venue(_name):  # type: ignore
        return None

logger = logging.getLogger(__name__)

# v6.6: weather support is gated on FrontendMatch having a `weather` field.
# Detected at module import; pass weather through if the model declares it,
# silently skip if not. This lets slate_reader.py and models.py be deployed
# independently — neither breaks the other.
_FRONTEND_MATCH_HAS_WEATHER = (
    (hasattr(FrontendMatch, "model_fields")
        and "weather" in getattr(FrontendMatch, "model_fields", {}))
    or
    (hasattr(FrontendMatch, "__fields__")
        and "weather" in getattr(FrontendMatch, "__fields__", {}))
)
if not _FRONTEND_MATCH_HAS_WEATHER:
    logger.warning(
        "slate_reader: FrontendMatch model has no `weather` field. "
        "Weather data will be ingested but not exposed to the frontend until "
        "models.py is updated to include `weather: Optional[dict]`."
    )

# PP stat_type → canonical name used by frontend pp_lines display.
PP_STAT_TO_ENGINE = {
    "Aces": "Aces",
    "Double Faults": "Double Faults",
    "Break Points Won": "Breakpoints Won",
    "Fantasy Score": "Fantasy Score",
    "Total Games": "Total Games",
    "Total Games Won": "Games Won",
    "Total Sets": "Total Sets",
    "Total Tie Breaks": "Total Tie Breaks",
}

# PP stat_type → posted_lines key used by engine.js applyPostedLineOverrides().
#
# IMPORTANT: only stat types that PP genuinely publishes per-player are
# mapped here. PP's "Total Games" and "Total Sets" are MATCH-LEVEL totals
# (e.g. 24.5 games / 2.5 sets played) — they are NOT a per-player line and
# must not be projected onto posted_lines as games_lost / sets_won.
#
# games_lost is derived cross-side below (a.games_lost = b.games_won).
# sets_won / sets_lost are intentionally left to engine baseline math
# from the Kalshi win probability — the engine handles that correctly.
PP_STAT_TO_POSTED_LINE_KEY = {
    "Aces": "aces",
    "Double Faults": "dfs",
    "Break Points Won": "breaks",
    "Total Games Won": "games_won",
}

# v6.5: stat types we treat as Poisson-distributed (count of discrete events).
# Multiplier-aware λ-fit is applied to these. Fantasy Score is excluded —
# it's a continuous-ish weighted score, Poisson doesn't fit.
POISSON_PP_STATS = frozenset({
    "Aces",
    "Double Faults",
    "Break Points Won",
    "Total Games Won",
})


# ── Multiplier-aware Poisson fit (v6.5) ──────────────────────────────


def _fit_poisson_lambda_from_pp(line: float, multiplier: float) -> Optional[float]:
    """Solve for the Poisson λ implied by a single PP (line, multiplier) pair.

    PP "more" pays `multiplier`, so the implied probability of the over is
    approximately 1/multiplier (PP juice is small and built into the mult).

    We interpret "over line" as P(X >= ceil(line)) for half-integer lines
    (the typical case — e.g. 5.5 means X >= 6) and as P(X >= line+1) for
    whole-integer lines.

    Returns None if inputs are unusable (mult <= 1, line <= 0, or implied
    prob outside (0.05, 0.95) where Newton's method becomes unreliable).
    """
    try:
        line_f = float(line)
        mult_f = float(multiplier)
    except (TypeError, ValueError):
        return None

    if mult_f <= 1.0 or line_f <= 0.0:
        return None

    p_over = 1.0 / mult_f
    if not (0.05 < p_over < 0.95):
        return None

    # Determine k such that "over line" means X >= k.
    if abs(line_f - round(line_f)) < 1e-9:
        # Whole integer line (rare on PP for these stats, but possible)
        k = int(round(line_f)) + 1
    else:
        k = math.ceil(line_f)
    if k <= 0:
        return None

    # Newton iteration on f(λ) = P(X >= k, λ) - p_over = 0.
    # df/dλ = P(X = k-1, λ) (telescoping derivative of Poisson tail).
    lam = max(line_f, 0.5)
    for _ in range(60):
        # P(X >= k, λ) = 1 - sum_{i=0..k-1} e^-λ λ^i / i!
        s = 0.0
        # Compute the sum in log-stable order; for the small λ ranges
        # we hit (typically 1-15 for tennis), direct compute is fine.
        for i in range(k):
            try:
                s += math.exp(-lam) * (lam ** i) / math.factorial(i)
            except OverflowError:
                return None
        p_cur = 1.0 - s
        err = p_cur - p_over
        if abs(err) < 5e-4:
            return max(0.05, lam)
        try:
            deriv = math.exp(-lam) * (lam ** (k - 1)) / math.factorial(k - 1)
        except OverflowError:
            return None
        if deriv < 1e-9:
            break
        # f(λ) decreases as λ rises (more mass above k, no — wait, P(X>=k)
        # INCREASES as λ rises). df/dλ = +P(X=k-1, λ). So Newton step:
        #   λ_new = λ - err / deriv
        lam -= err / deriv
        lam = max(0.05, min(lam, 50.0))

    # Didn't converge to tolerance; return last iterate if it's in range.
    if 0.05 <= lam <= 50.0:
        return lam
    return None


def _compute_next_tournament(
    cpi_rows: list[dict],
    frontend_matches: list[FrontendMatch],
    slate_label: Optional[str],
) -> Optional[dict]:
    """v6.10: server-side next-tournament resolution.

    Reads the calendar columns (start_date, end_date, tour) on
    tournament_cpi_base. Identifies the slate's "current" tournament
    (largest by match count, with substring fallback against slate_label),
    finds its row, then picks the soonest-starting row whose start_date is
    on or after current.end_date.

    Returns a serializable dict the frontend can render directly:
        {
          "tournament_key": "rome",
          "display_name":   "Italian Open",
          "start_date":     "2026-05-05",
          "end_date":       "2026-05-17",
          "surface":        "clay",
          "tour":           "both",
          "days_until":     9,
          "ms_until":       777600000,   # ms from now to start_date 00:00 UTC
        }
    Or None when:
      - cpi_rows is empty (migration not run)
      - no current tournament can be identified
      - current tournament has no end_date set
      - no future tournament exists in the calendar
    """
    if not cpi_rows:
        return None

    # Find current tournament from frontend matches. Largest by count.
    counts: dict[str, int] = {}
    for fm in frontend_matches:
        t = (getattr(fm, "tournament", None) or "").strip()
        if t:
            counts[t] = counts.get(t, 0) + 1
    if not counts:
        # Last resort: try slate_label itself
        current_name = (slate_label or "").strip()
    else:
        current_name = max(counts.items(), key=lambda kv: kv[1])[0]
    if not current_name:
        return None

    lc = current_name.lower()
    current_row: Optional[dict] = None
    for r in cpi_rows:
        key = (r.get("tournament_key") or "").lower()
        dn = (r.get("display_name") or "").lower()
        if key and key in lc:
            current_row = r
            break
        if dn and (dn in lc or lc in dn):
            current_row = r
            break
    if not current_row:
        return None

    end_date_str = current_row.get("end_date")
    if not end_date_str:
        return None
    try:
        current_end = datetime.fromisoformat(
            f"{end_date_str}T00:00:00+00:00"
        )
    except (TypeError, ValueError):
        return None

    # Pick next: smallest start_date that is on-or-after current_end
    # AND not the current row itself. Same-day handoff supported with `>=`.
    candidates: list[tuple[datetime, dict]] = []
    for r in cpi_rows:
        if r.get("tournament_key") == current_row.get("tournament_key"):
            continue
        sd = r.get("start_date")
        if not sd:
            continue
        try:
            start_dt = datetime.fromisoformat(f"{sd}T00:00:00+00:00")
        except (TypeError, ValueError):
            continue
        if start_dt >= current_end:
            candidates.append((start_dt, r))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    next_dt, next_row = candidates[0]

    now = datetime.now(timezone.utc)
    ms_until = int((next_dt - now).total_seconds() * 1000)
    days_until = max(0, int((next_dt - now).total_seconds() / 86400))

    return {
        "tournament_key": next_row.get("tournament_key"),
        "display_name":   next_row.get("display_name"),
        "start_date":     next_row.get("start_date"),
        "end_date":       next_row.get("end_date"),
        "surface":        next_row.get("surface"),
        "tour":           next_row.get("tour"),
        "days_until":     days_until,
        "ms_until":       ms_until,
    }


def _build_opening_odds_model(raw: Optional[dict]) -> Optional[FrontendMatchOdds]:
    """Flatten stored opening_odds (source-keyed) into FrontendMatchOdds."""
    if not raw or not isinstance(raw, dict):
        return None
    flat: dict = {}
    kalshi = raw.get("kalshi") or {}
    if isinstance(kalshi, dict):
        if kalshi.get("implied_prob_a") is not None:
            flat["kalshi_prob_a"] = kalshi["implied_prob_a"]
        if kalshi.get("implied_prob_b") is not None:
            flat["kalshi_prob_b"] = kalshi["implied_prob_b"]
    odds_api = raw.get("the_odds_api") or {}
    if isinstance(odds_api, dict):
        for k in ("ml_a", "ml_b", "gw_a_line", "gw_a_over", "gw_b_line", "gw_b_over"):
            if odds_api.get(k) is not None:
                flat[k] = odds_api[k]
    if not flat:
        return None
    return FrontendMatchOdds(**flat)


def _aggregate_pp_lines_for_engine(rows: list[dict]) -> dict[tuple[str, str], dict]:
    """Aggregate PP variants into a single projection center per (player, stat).

    v6.5 — for Poisson stats (Aces / Double Faults / Break Points Won /
    Total Games Won), fit a Poisson λ from each variant's (line, multiplier)
    and average. This is sharper than median(lines) because it uses the
    multiplier signal, which encodes how aggressively PP shifted that
    variant's line off the fair value.

    For Fantasy Score (and any other non-Poisson stat that ever lands here),
    fall back to the previous logic: standard line if present, else median
    across variants.

    The return value is what writes into match.odds.posted_lines, so the
    "line" field below is treated by engine.js as the projection center
    (lambda for Poisson stats).

    Key: (player_id, stat_type) → {"line": float, "raw_player_name": str}
    """
    bucket: dict[tuple[str, str], list[tuple[str, float, Optional[float], str]]] = {}
    for r in rows:
        pid = r.get("player_id")
        stat = r.get("stat_type")
        line = r.get("current_line")
        odds_type = r.get("odds_type") or "standard"
        raw_name = r.get("raw_player_name") or ""
        mult_raw = r.get("multiplier")
        if not pid or not stat or line is None:
            continue
        try:
            ln = float(line)
        except (TypeError, ValueError):
            continue
        try:
            mult_f = float(mult_raw) if mult_raw is not None else None
        except (TypeError, ValueError):
            mult_f = None
        bucket.setdefault((pid, stat), []).append((odds_type, ln, mult_f, raw_name))

    out: dict[tuple[str, str], dict] = {}
    for (pid, stat), variants in bucket.items():
        chosen_name = variants[0][3]
        chosen_line: Optional[float] = None
        method = ""

        # ── Poisson-fit path (v6.5) ────────────────────────────────────
        if stat in POISSON_PP_STATS:
            lambdas: list[float] = []
            for odds_type, ln, mult_f, _ in variants:
                if mult_f is None:
                    # No multiplier → can't fit. Skip, fall through to
                    # line-based fallback at the bottom.
                    continue
                lam = _fit_poisson_lambda_from_pp(ln, mult_f)
                if lam is not None:
                    lambdas.append(lam)
            if lambdas:
                chosen_line = sum(lambdas) / len(lambdas)
                method = "poisson_fit"

        # ── Standard-or-median fallback ────────────────────────────────
        if chosen_line is None:
            std = [v for v in variants if v[0] == "standard"]
            if std:
                chosen_line = std[0][1]
                chosen_name = std[0][3]
                method = "standard_line"
            else:
                lines = [v[1] for v in variants]
                chosen_line = float(median(lines))
                method = "line_median"

        out[(pid, stat)] = {
            "line": chosen_line,
            "raw_player_name": chosen_name,
            "method": method,
            "n_variants": len(variants),
        }
    return out


def _emit_pp_lines_all_variants(rows: list[dict]) -> list[dict]:
    """Emit one row per (player, stat_type, odds_type, line) for the PP tab UI.

    Each row carries: player, stat (canonical name), line, mult, odds_type,
    source. The PP tab can now display every variant per stat with its
    distinct line and multiplier — e.g. Sinner Aces goblin 2.5 AND
    goblin 3.5 will both surface as separate rows when PP publishes both.

    Lines are emitted in stable (player, stat, odds_type, line) order so the
    UI can group them deterministically. Dedup is at the
    (player, stat, odds_type, line) tuple — same line in the same variant for
    the same player only emits once even if PP returned duplicates due to
    flash-sale toggling mid-scrape.
    """
    emitted: list[dict] = []
    seen: set[tuple[str, str, str, float]] = set()

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            r.get("raw_player_name") or "",
            r.get("stat_type") or "",
            r.get("odds_type") or "standard",
            float(r.get("current_line") or 0),
        ),
    )

    for r in sorted_rows:
        raw_name = (r.get("raw_player_name") or "").strip()
        stat_type = (r.get("stat_type") or "").strip()
        line = r.get("current_line")
        if not raw_name or not stat_type or line is None:
            continue
        try:
            ln = float(line)
        except (TypeError, ValueError):
            continue

        odds_type = (r.get("odds_type") or "standard").strip().lower()
        if odds_type not in {"standard", "goblin", "demon"}:
            odds_type = "standard"

        engine_stat = PP_STAT_TO_ENGINE.get(stat_type, stat_type)

        key = (raw_name, engine_stat, odds_type, ln)
        if key in seen:
            continue
        seen.add(key)

        # Multiplier may be missing on rows scraped before v6.4 — emit as
        # empty string in that case so the UI doesn't render "None".
        mult_raw = r.get("multiplier")
        if mult_raw is None:
            mult_str = ""
        else:
            try:
                mult_str = f"{float(mult_raw):.2f}"
            except (TypeError, ValueError):
                mult_str = ""

        emitted.append(
            {
                "player": raw_name,
                "stat": engine_stat,
                "line": ln,
                "mult": mult_str,
                "odds_type": odds_type,
                "source": "prizepicks",
            }
        )
    return emitted


def _project_posted_lines_for_match(
    pa_id: str,
    pb_id: str,
    pp_agg: dict[tuple[str, str], dict],
    wp_a: Optional[float],
    existing_posted: Optional[dict],
    sgo_has_gw_lines: bool = False,
) -> Optional[dict]:
    """Build match.odds.posted_lines.{a,b} from per-player PP aggregations.

    engine.js applyPostedLineOverrides reads:
        aces, dfs, breaks, games_won, games_lost, sets_won, sets_lost

    Source-of-truth policy (per spec, v6.5):
        aces / dfs / breaks  → PP (this is the only source for these in
                                tennis; Pinnacle does not post serve props)
        games_won/_lost      → Pinnacle (SGO gw_a_line / gw_b_line) when
                                SGO has them; PP as fallback otherwise
        sets_won/_lost       → engine baseline math from Kalshi wp + p3set
                                (NOT projected here at all)

    When sgo_has_gw_lines is True, we suppress games_won/games_lost from
    posted_lines so the engine consumes the Pinnacle line in sharp mode
    instead of having it overridden post-hoc by PP.

    Cross-side derivation: when only one side has a games_won line (and
    SGO hasn't posted gw lines), set the other side's games_lost from it
    — a's games_lost = b's games_won by definition.

    If existing_posted (from manual CSV upload) has any keys, those win on
    collision — manual override is sacred.
    """
    sides: dict[str, dict] = {}

    for side_key, pid in [("a", pa_id), ("b", pb_id)]:
        side_dict: dict = {}
        for pp_stat, posted_key in PP_STAT_TO_POSTED_LINE_KEY.items():
            # v6.5: when SGO has Pinnacle gw lines, do NOT propagate PP's
            # games_won line into posted_lines. Engine reads gw_a_line /
            # gw_b_line directly in sharp mode and that's the path we want.
            if posted_key == "games_won" and sgo_has_gw_lines:
                continue
            agg = pp_agg.get((pid, pp_stat))
            if agg is not None:
                side_dict[posted_key] = float(agg["line"])
        if side_dict:
            sides[side_key] = side_dict

    a_side = sides.get("a", {})
    b_side = sides.get("b", {})
    # Cross-side games_lost derivation only kicks in when games_won was
    # actually written above (i.e., SGO didn't suppress it).
    if "games_lost" not in a_side and "games_won" in b_side:
        a_side["games_lost"] = b_side["games_won"]
    if "games_lost" not in b_side and "games_won" in a_side:
        b_side["games_lost"] = a_side["games_won"]
    if a_side:
        sides["a"] = a_side
    if b_side:
        sides["b"] = b_side

    if existing_posted and isinstance(existing_posted, dict):
        for sk in ("a", "b"):
            manual_side = existing_posted.get(sk)
            if isinstance(manual_side, dict):
                merged = sides.get(sk, {})
                merged.update(
                    {k: v for k, v in manual_side.items() if v is not None}
                )
                sides[sk] = merged

    return sides or None


def get_frontend_slate(slate_id: str) -> Optional[FrontendSlate]:
    """Hydrate a complete slate into the shape the frontend expects."""
    db = get_client()
    slate = (
        db.table("slates").select("*").eq("id", slate_id).single().execute().data
    )
    if not slate:
        return None

    matches = (
        db.table("matches")
        .select(
            "*, player_a:player_a_id(display_name), player_b:player_b_id(display_name)"
        )
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    players = (
        db.table("slate_players")
        .select("*, player:player_id(display_name)")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    # Pull every active PP row for this slate, all 3 variants. Multiplier is
    # included so the PP tab can render goblin/demon variants with their
    # correct payouts.
    pp_rows = (
        db.table("prizepicks_lines")
        .select(
            "player_id, raw_player_name, stat_type, current_line, odds_type, multiplier"
        )
        .eq("slate_id", slate_id)
        .eq("is_active", True)
        .execute()
        .data
        or []
    )

    # Engine-side projection center: multiplier-aware Poisson fit per
    # (player, stat). v6.5.
    pp_agg = _aggregate_pp_lines_for_engine(pp_rows)

    # v6.9: Resolve the slate's slate_label against the venue dictionary
    # ONCE up front. When the upstream ingester writes matches with
    # tournament=null (which it does on Featured slates — slate_label
    # carries the tournament identity instead), every match gets these
    # values as a fallback. Same lookup_venue() the weather pipeline uses,
    # so what worked there works here too.
    slate_label = slate.get("slate_label")
    fallback_venue = lookup_venue(slate_label) if slate_label else None
    fallback_tournament_name: Optional[str] = None
    fallback_surface: Optional[str] = None
    if fallback_venue:
        # venue["name"] is the stadium ("Caja Mágica") — for tournament we
        # want a clean tournament-style label. The matched_key (e.g.
        # "madrid open") titlecased is closest. If you'd rather show the
        # stadium name, swap to fallback_venue.get("name").
        matched_key = fallback_venue.get("_matched_key") or ""
        if matched_key:
            # Title-case with overrides for acronyms — naive .title() turns
            # "us open" into "Us Open". Apply a small override pass.
            _ACRONYM_FIX = {
                "Us Open": "US Open",
                "Atp Finals": "ATP Finals",
                "Wta Finals": "WTA Finals",
                "Bmw Open": "BMW Open",
                "Atp": "ATP",
                "Wta": "WTA",
            }
            titled = matched_key.title()
            fallback_tournament_name = _ACRONYM_FIX.get(titled, titled)
        fallback_surface = fallback_venue.get("surface")
        logger.info(
            "slate_reader: using slate_label fallback for matches: "
            "slate_label=%r → tournament=%r surface=%r",
            slate_label, fallback_tournament_name, fallback_surface,
        )

    frontend_matches: list[FrontendMatch] = []
    for m in matches:
        pa_id = m["player_a_id"]
        pb_id = m["player_b_id"]
        pa_name = (m.get("player_a") or {}).get("display_name") or pa_id
        pb_name = (m.get("player_b") or {}).get("display_name") or pb_id

        odds = m.get("odds") or {}
        wp_a = odds.get("kalshi_prob_a")
        existing_posted = odds.get("posted_lines")
        # v6.5: if SGO has posted Pinnacle games-won lines, the engine
        # should consume those directly. Suppress PP games_won from
        # posted_lines so applyPostedLineOverrides doesn't clobber the
        # Pinnacle-derived gw_a / gw_b in buildPlayerStats output.
        sgo_has_gw_lines = (
            odds.get("gw_a_line") is not None
            and odds.get("gw_b_line") is not None
        )

        projected = _project_posted_lines_for_match(
            pa_id, pb_id, pp_agg, wp_a, existing_posted,
            sgo_has_gw_lines=sgo_has_gw_lines,
        )
        if projected:
            odds = {**odds, "posted_lines": projected}

        match_kwargs = {
            "player_a": pa_name,
            "player_b": pb_name,
            "start_time": m.get("start_time"),
            # v6.9: tournament+surface fallback — when the DB row has a
            # populated tournament, use it. Otherwise fall back to the
            # slate-label-derived tournament name (precomputed above).
            # Frontend tiles check `match.tournament` first; this ensures
            # the homepage tile resolves on Featured slates that ship
            # with null per-match tournament strings.
            "tournament": (m.get("tournament") or fallback_tournament_name or ""),
            "surface": (m.get("surface") or fallback_surface),
            "odds": FrontendMatchOdds(**odds),
            "opening_odds": _build_opening_odds_model(m.get("opening_odds")),
            "closing_odds": _build_opening_odds_model(m.get("closing_odds")),
            "adj_a": 0,
            "adj_b": 0,
        }
        # v6.6: pass weather through if the model accepts it. Detected
        # once at module import (see _FRONTEND_MATCH_HAS_WEATHER above).
        if _FRONTEND_MATCH_HAS_WEATHER:
            match_kwargs["weather"] = m.get("weather")

        frontend_matches.append(FrontendMatch(**match_kwargs))

    by_canonical: dict[str, dict] = {}
    for sp in players:
        cid = sp["player_id"]
        display = (sp.get("player") or {}).get("display_name") or sp.get(
            "dk_display_name"
        )
        entry = by_canonical.setdefault(
            cid,
            {"name": display, "id": 0, "salary": 0, "avg_ppg": 0},
        )
        rp = sp.get("roster_position") or "P"
        effective_dk_id = sp.get("dk_player_id_override") or sp["dk_player_id"]
        if rp in ("P", "FLEX"):
            entry["id"] = effective_dk_id
            entry["salary"] = sp["salary"]
            entry["avg_ppg"] = float(sp["avg_ppg"] or 0)
            entry["flex_id"] = effective_dk_id
            entry["flex_salary"] = sp["salary"]
        elif rp == "CPT":
            entry["cpt_id"] = effective_dk_id
            entry["cpt_salary"] = sp["salary"]
        elif rp == "ACPT":
            entry["acpt_id"] = effective_dk_id
            entry["acpt_salary"] = sp["salary"]
        val = sp.get("ss_pool_own")
        if val is not None and entry.get("ss_pool_own") is None:
            try:
                entry["ss_pool_own"] = float(val)
            except (TypeError, ValueError):
                pass

    frontend_players = [FrontendPlayer(**e) for e in by_canonical.values()]

    meta = {
        "id": slate["id"],
        "dk_draft_group_id": slate["dk_draft_group_id"],
        "first_seen_at": slate["first_seen_at"],
        "last_synced_at": slate["last_synced_at"],
        "contest_type": slate.get("contest_type") or "classic",
        "is_fallback": bool(slate.get("is_fallback")),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        # v6.9: slate_label also exposed at the top level of FrontendSlate,
        # but consumers (e.g. homepage tiles) sometimes look in meta.
        # Cheap to mirror, no breakage risk.
        "slate_label": slate.get("slate_label"),
    }

    # PP tab UI feed: every variant per (player, stat) emitted as a separate
    # row with its line and multiplier. The PP tab now (v6.5 frontend) shows
    # ALL stat types via tabs, defaulting to Fantasy Score.
    pp_lines_out = _emit_pp_lines_all_variants(pp_rows)

    # v6.7: Tournament CPI base data. Manually-maintained reference values from
    # `tournament_cpi_base` table — small dim table (~20 rows). Pulled once per
    # slate read and attached to meta so the homepage can render the CPI tile
    # without a separate API call. Engine does NOT consume CPI yet — display
    # only. If the table doesn't exist (migration not run), we just skip and
    # the frontend shows "—" for CPI.
    #
    # v6.10: Widened SELECT to include start_date / end_date / tour so the
    # frontend can compute "Next Tournament" countdown. ALSO computes
    # meta.next_tournament server-side as a convenience block so the
    # homepage tile doesn't have to redo the date-comparison logic. The
    # frontend keeps its client-side detector as a fallback for when this
    # field is absent (e.g. running against an older API).
    cpi_rows: list[dict] = []
    try:
        cpi_rows = (
            db.table("tournament_cpi_base")
            .select(
                "tournament_key, display_name, base_cpi, surface, source, "
                "notes, start_date, end_date, tour"
            )
            .execute()
            .data
            or []
        )
    except Exception as e:
        logger.info(
            "tournament_cpi_base unavailable (likely migration not run): %s", e
        )

    # v6.10: server-side next_tournament resolution. We try to identify the
    # current tournament from the slate's matches (which now have
    # tournament populated thanks to the v6.9 slate_label fallback), find
    # its row in cpi_rows, then locate the soonest-starting row whose
    # start_date is on or after current.end_date.
    next_tournament_block = _compute_next_tournament(
        cpi_rows, frontend_matches, slate.get("slate_label")
    )

    meta = dict(meta) if meta else {}
    meta["tournament_cpi_base"] = cpi_rows
    if next_tournament_block:
        meta["next_tournament"] = next_tournament_block

    return FrontendSlate(
        date=slate["slate_date"],
        sport=slate["sport"],
        slate_label=slate.get("slate_label"),
        lock_time=slate.get("lock_time"),
        matches=frontend_matches,
        dk_players=frontend_players,
        pp_lines=pp_lines_out,
        meta=meta,
    )


def get_today_slate(sport: str) -> Optional[FrontendSlate]:
    """Return the most relevant active slate for a sport."""
    db = get_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    def _has_players(slate_id: str) -> bool:
        result = (
            db.table("slate_players")
            .select("slate_id", count="exact")
            .eq("slate_id", slate_id)
            .limit(1)
            .execute()
        )
        return (result.count or 0) > 0

    def _pick(contest_type: str) -> Optional[str]:
        candidates = (
            db.table("slates")
            .select("id, lock_time, slate_date, first_seen_at")
            .eq("sport", sport)
            .eq("status", "active")
            .eq("contest_type", contest_type)
            .order("slate_date", desc=True)
            .order("first_seen_at", desc=True)
            .execute()
            .data
            or []
        )
        if not candidates:
            return None
        upcoming = [
            c for c in candidates if c.get("lock_time") and c["lock_time"] > now_iso
        ]
        upcoming.sort(key=lambda c: c["lock_time"])
        for c in upcoming:
            if _has_players(c["id"]):
                return c["id"]
        for c in candidates:
            if _has_players(c["id"]):
                return c["id"]
        return candidates[0]["id"]

    classic_id = _pick("classic")
    if classic_id:
        return get_frontend_slate(classic_id)

    showdown_id = _pick("showdown")
    if showdown_id:
        return get_frontend_slate(showdown_id)

    return None


def list_slates(sport: str, limit: int = 30) -> list[dict]:
    """Archive manifest — what dates do we have slates for?"""
    db = get_client()
    rows = (
        db.table("slates")
        .select("id, slate_date, slate_label, status")
        .eq("sport", sport)
        .order("slate_date", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    return [
        {
            "id": r["id"],
            "date": r["slate_date"],
            "label": r.get("slate_label"),
            "status": r.get("status"),
        }
        for r in rows
    ]
