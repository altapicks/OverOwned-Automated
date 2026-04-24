"""Read-side service: builds the frontend-facing FrontendSlate from DB rows.

This is what the API returns to the React app. The shape matches the
existing slate.json schema exactly so no UI changes are needed.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
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


def _build_opening_odds_model(raw: Optional[dict]) -> FrontendMatchOdds:
    """Flatten stored opening_odds (source-keyed) into FrontendMatchOdds.

    Stored shape:
      {"kalshi": {"implied_prob_a": 0.62, "implied_prob_b": 0.38, ...},
       "the_odds_api": {"ml_a": -175, "ml_b": 137, ...}}

    Flat keys used by engine.js/frontend:
      - kalshi_prob_a / kalshi_prob_b   from kalshi.implied_prob_*
      - ml_a / ml_b                     from the_odds_api.ml_*
    """
    if not raw or not isinstance(raw, dict):
        return FrontendMatchOdds()
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
    return FrontendMatchOdds(**flat)


def get_frontend_slate(slate_id: str) -> Optional[FrontendSlate]:
    """Hydrate a complete slate into the shape the frontend expects."""
    db = get_client()
    slate = db.table("slates").select("*").eq("id", slate_id).single().execute().data
    if not slate:
        return None

    # Match rows
    matches = (
        db.table("matches")
        .select("*, player_a:player_a_id(display_name), player_b:player_b_id(display_name)")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    # Slate player rows with display names
    players = (
        db.table("slate_players")
        .select("*, player:player_id(display_name)")
        .eq("slate_id", slate_id)
        .execute()
        .data
        or []
    )

    # ── Build matches ─────────────────────────────────────────────────
    frontend_matches: list[FrontendMatch] = []
    for m in matches:
        pa_name = (m.get("player_a") or {}).get("display_name") or m["player_a_id"]
        pb_name = (m.get("player_b") or {}).get("display_name") or m["player_b_id"]
        frontend_matches.append(
            FrontendMatch(
                player_a=pa_name,
                player_b=pb_name,
                start_time=m.get("start_time"),
                tournament=m.get("tournament") or "",
                surface=m.get("surface"),
                odds=FrontendMatchOdds(**(m.get("odds") or {})),
                # opening_odds may include kalshi_prob_a/kalshi_prob_b sub-keys
                # nested under 'kalshi'. Flatten the flat keys to the top level
                # so the frontend reads match.opening_odds.kalshi_prob_a without
                # drilling. Mirrors how live odds are laid out.
                opening_odds=_build_opening_odds_model(m.get("opening_odds")),
                # closing_odds uses the same nested shape as opening_odds,
                # captured at slate lock time. Frontend uses it everywhere
                # except the Live Leverage Tracker.
                closing_odds=_build_opening_odds_model(m.get("closing_odds")),
                adj_a=0,
                adj_b=0,
            )
        )

    # ── Collapse slate_players into per-canonical entries ─────────────
    # For showdown, multiple roster_positions per player get merged with
    # per-position salaries/ids on one row. For classic, just one row.
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
        # v5.10: prefer dk_player_id_override when set. The slate_watcher
        # worker overwrites dk_player_id on every 15-min poll, so any manual
        # SQL correction to dk_player_id gets clobbered. Override column is
        # never touched by the worker — operators set it and it persists.
        effective_dk_id = sp.get("dk_player_id_override") or sp["dk_player_id"]
        if rp in ("P", "FLEX"):
            # Classic: P. Showdown flex: FLEX.
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
        # ss_pool_own — set on any row for this player (usually the P/FLEX
        # row). None-safe: float(None) would throw, so only cast if present.
        # Overrides subsequent overwrites only if currently unset, so a CPT
        # row with NULL doesn't wipe a P row's value.
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

    # PP lines: pull active rows from prizepicks_lines table for this slate
    # and include posted_lines (Underdog stat props) from matches.odds for
    # multi-stat edge detection. Frontend buildProjections uses this to
    # compute ppEdge per player, which feeds Hidden Gem + PP Fade signals
    # in the DK tab and OverOwned Build ruleset.
    pp_rows = (
        db.table("prizepicks_lines")
        .select("raw_player_name, stat_type, current_line")
        .eq("slate_id", slate_id)
        .eq("is_active", True)
        .execute()
        .data
        or []
    )
    pp_lines_out = [
        {
            "player": r["raw_player_name"],
            "stat": r["stat_type"],
            "line": float(r["current_line"]),
            "mult": "",  # multiplier not tracked in current admin UI
            "source": "prizepicks",
        }
        for r in pp_rows
    ]

    # Also add Underdog stat-prop lines from matches.odds.posted_lines as
    # a second source. Each match row has {"a": {games_won, aces, dfs, ...},
    # "b": {...}} written by the Underdog ingestion SQL. Convert to the
    # same flat {player, stat, line} shape as pp_lines so the frontend's
    # ppRows builder can process both uniformly.
    UD_STAT_MAP = {
        "games_won": "Games Won",
        "aces": "Aces",
        "dfs": "Double Faults",
        "breaks": "Breakpoints Won",
        "games_played": "Games Played",
        "first_set_games_won": "1st Set Games Won",
        "first_set_games_played": "1st Set Games Played",
        "sets_played": "Sets Played",
        "sets_won": "Sets Won",
        "tiebreakers_played": "Tiebreakers Played",
    }
    for m in matches:
        odds = m.get("odds") or {}
        posted = odds.get("posted_lines") or {}
        for side_key, player_field in [("a", "player_a"), ("b", "player_b")]:
            p = m.get(player_field) or {}
            pname = p.get("display_name")
            side_lines = posted.get(side_key) or {}
            if not pname or not side_lines:
                continue
            for ud_key, display_stat in UD_STAT_MAP.items():
                val = side_lines.get(ud_key)
                if val is None:
                    continue
                pp_lines_out.append({
                    "player": pname,
                    "stat": display_stat,
                    "line": float(val),
                    "mult": "",
                    "source": "underdog",
                })

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
    """Return the most relevant active slate for a sport.

    Preference order:
      1. Classic slate for today (if any)
      2. Classic slate for any date (most recent)
      3. Showdown fallback for today (if is_fallback=true or no Classic at all)

    This mirrors the watcher's classify/fallback logic — if a Classic got
    ingested today, users see it; if only a Showdown fallback exists, they
    see that instead. Used by /api/slates/today.
    """
    db = get_client()

    # Most recent Classic, active status
    classic = (
        db.table("slates")
        .select("id")
        .eq("sport", sport)
        .eq("status", "active")
        .eq("contest_type", "classic")
        .order("slate_date", desc=True)
        .order("first_seen_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if classic:
        return get_frontend_slate(classic[0]["id"])

    # Fall through to Showdown (fallback or otherwise)
    showdown = (
        db.table("slates")
        .select("id")
        .eq("sport", sport)
        .eq("status", "active")
        .eq("contest_type", "showdown")
        .order("slate_date", desc=True)
        .order("first_seen_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if showdown:
        return get_frontend_slate(showdown[0]["id"])

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
