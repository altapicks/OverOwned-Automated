"""Read-side service: builds the frontend-facing FrontendSlate from DB rows.

This is what the API returns to the React app. The shape matches the
existing slate.json schema exactly so no UI changes are needed.

PP integration model (the part that runs the show):

  prizepicks_lines (raw, all 3 variants per stat) ──┐
                                                    │
                                                    ▼
                         per-(canonical_id, stat) MEDIAN line
                                                    │
                                  ┌─────────────────┴─────────────────┐
                                  ▼                                   ▼
                      slate.pp_lines (PP tab UI)          match.odds.posted_lines
                      — Fantasy Score only, used          — projected per-side, used
                        for over/under edge display         by engine.js posted_lines
                                                            override path → DK proj
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

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
# These are the keys the DK projection function actually reads. Keep aligned
# with the reader in frontend/src/engine.js (search applyPostedLineOverrides).
PP_STAT_TO_POSTED_LINE_KEY = {
    "Aces": "aces",
    "Double Faults": "dfs",
    "Break Points Won": "breaks",
    "Total Games Won": "games_won",
    # The engine separately consumes games_lost and sets_won/sets_lost
    # — those are derived below from Total Games and Total Sets.
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


def _aggregate_pp_lines(rows: list[dict]) -> dict[tuple[str, str], dict]:
    """Median across standard/demon/goblin per (canonical_id, stat_type).

    PP runs each prop in three flavors:
      standard — closest to the "true" line, but only ~15% of props publish it
      demon    — line shifted UP (harder over, boosted multiplier)
      goblin   — line shifted DOWN (easier over, reduced multiplier)
    Median across all three approximates the unobserved true line and keeps
    Aces/DF/Sets/Games Won props (which are almost exclusively demon/goblin)
    usable as projection inputs.

    Falls back to the standard line alone if it exists, otherwise the
    median, ensuring single-variant rows still pass through.

    Key: (player_id, stat_type) → {"line": float, "raw_player_name": str}
    """
    bucket: dict[tuple[str, str], list[tuple[str, float, str]]] = {}
    for r in rows:
        pid = r.get("player_id")
        stat = r.get("stat_type")
        line = r.get("current_line")
        odds_type = r.get("odds_type") or "standard"
        raw_name = r.get("raw_player_name") or ""
        if not pid or not stat or line is None:
            continue
        try:
            ln = float(line)
        except (TypeError, ValueError):
            continue
        bucket.setdefault((pid, stat), []).append((odds_type, ln, raw_name))

    out: dict[tuple[str, str], dict] = {}
    for key, variants in bucket.items():
        std = [v for v in variants if v[0] == "standard"]
        if std:
            chosen_line = std[0][1]
            chosen_name = std[0][2]
        else:
            lines = [v[1] for v in variants]
            chosen_line = float(median(lines))
            chosen_name = variants[0][2]
        out[key] = {"line": chosen_line, "raw_player_name": chosen_name}
    return out


def _project_posted_lines_for_match(
    pa_id: str,
    pb_id: str,
    pp_agg: dict[tuple[str, str], dict],
    wp_a: Optional[float],
    existing_posted: Optional[dict],
) -> Optional[dict]:
    """Build match.odds.posted_lines.{a,b} from per-player PP medians.

    engine.js applyPostedLineOverrides reads:
      aces, dfs, breaks, games_won, games_lost, sets_won, sets_lost

    PP gives us aces, dfs, breaks (= Break Points Won), games_won
    (= Total Games Won) directly. We derive games_lost from Total Games
    when present, and sets_won/sets_lost from Total Sets + Kalshi wp
    when present.

    If existing_posted (from manual CSV upload, preserved as override)
    has any keys, those win — manual override is sacred.
    """
    sides: dict[str, dict] = {}
    for side_key, pid in [("a", pa_id), ("b", pb_id)]:
        side_dict: dict = {}
        for pp_stat, posted_key in PP_STAT_TO_POSTED_LINE_KEY.items():
            agg = pp_agg.get((pid, pp_stat))
            if agg is not None:
                side_dict[posted_key] = float(agg["line"])

        # Derive games_lost from Total Games when both halves are known.
        # Total Games is a match-level prop posted per player on PP, but
        # PP's Total Games is the sum of *both* players' games — divide
        # via wp-weighted share. When wp not available, split 50/50.
        tg = pp_agg.get((pid, "Total Games"))
        gw = side_dict.get("games_won")
        if tg is not None and gw is not None:
            # PP Total Games is per-player on tennis (their player line).
            # games_lost = total_games - games_won when both are about
            # the same player.
            side_dict["games_lost"] = max(0.0, float(tg["line"]) - gw)

        # Derive sets_won from Total Sets when present.
        # Total Sets on PP is the player's sets won (not match total).
        ts = pp_agg.get((pid, "Total Sets"))
        if ts is not None:
            side_dict["sets_won"] = float(ts["line"])
            # Best-of-3 expected sets-played by side ≈ 2 + p3set.
            # Without p3set we approximate: loser averages ~2.3, winner ~2.5.
            if wp_a is not None:
                this_side_wp = wp_a if side_key == "a" else 1 - wp_a
                exp_sets_played = 2.0 + 0.3 + 0.4 * (1 - abs(this_side_wp - 0.5) * 2)
            else:
                exp_sets_played = 2.4
            side_dict["sets_lost"] = max(
                0.0, exp_sets_played - side_dict["sets_won"]
            )

        if side_dict:
            sides[side_key] = side_dict

    # Merge with existing manual override (manual wins on collision).
    if existing_posted and isinstance(existing_posted, dict):
        for sk in ("a", "b"):
            manual_side = existing_posted.get(sk)
            if isinstance(manual_side, dict):
                merged = sides.get(sk, {})
                merged.update({k: v for k, v in manual_side.items() if v is not None})
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

    # Pull every active PP row for this slate, all 3 variants. Aggregate
    # to one median line per (canonical_id, stat_type) — see _aggregate.
    pp_rows = (
        db.table("prizepicks_lines")
        .select("player_id, raw_player_name, stat_type, current_line, odds_type")
        .eq("slate_id", slate_id)
        .eq("is_active", True)
        .execute()
        .data
        or []
    )
    pp_agg = _aggregate_pp_lines(pp_rows)

    frontend_matches: list[FrontendMatch] = []
    for m in matches:
        pa_id = m["player_a_id"]
        pb_id = m["player_b_id"]
        pa_name = (m.get("player_a") or {}).get("display_name") or pa_id
        pb_name = (m.get("player_b") or {}).get("display_name") or pb_id

        odds = m.get("odds") or {}
        wp_a = odds.get("kalshi_prob_a")
        existing_posted = odds.get("posted_lines")

        # Project PP medians onto posted_lines so engine.js consumes
        # them via its existing applyPostedLineOverrides() path. This
        # is THE link that takes the DK tab off the fallback model.
        projected = _project_posted_lines_for_match(
            pa_id, pb_id, pp_agg, wp_a, existing_posted
        )
        if projected:
            odds = {**odds, "posted_lines": projected}

        frontend_matches.append(
            FrontendMatch(
                player_a=pa_name,
                player_b=pb_name,
                start_time=m.get("start_time"),
                tournament=m.get("tournament") or "",
                surface=m.get("surface"),
                odds=FrontendMatchOdds(**odds),
                opening_odds=_build_opening_odds_model(m.get("opening_odds")),
                closing_odds=_build_opening_odds_model(m.get("closing_odds")),
                adj_a=0,
                adj_b=0,
            )
        )

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
    }

    # PP tab UI feed: one line per (player, stat) using the median,
    # source='prizepicks'. PP TAB display filters to Fantasy Score only
    # via the frontend Supabase query — these other stats here exist so
    # any PP-tab edge calc that wants them can read them, no extra fetch.
    pp_lines_out: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    for (pid, pp_stat), agg in pp_agg.items():
        engine_stat = PP_STAT_TO_ENGINE.get(pp_stat, pp_stat)
        key = (agg["raw_player_name"], engine_stat)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        pp_lines_out.append(
            {
                "player": agg["raw_player_name"],
                "stat": engine_stat,
                "line": float(agg["line"]),
                "mult": "",
                "source": "prizepicks",
            }
        )
