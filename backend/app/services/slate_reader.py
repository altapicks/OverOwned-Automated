"""Read-side service: builds the frontend-facing FrontendSlate from DB rows.
This is what the API returns to the React app. The shape matches the existing
slate.json schema exactly so no UI changes are needed.
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


# PP stat_type → canonical name used by frontend engine. PP's wire labels
# already match what we want for most, but we normalize a couple to keep
# the engine happy and consistent with the Underdog UD_STAT_MAP shape.
PP_STAT_TO_ENGINE = {
    "Aces": "Aces",
    "Double Faults": "Double Faults",
    "Break Points Won": "Breakpoints Won",  # match UD label
    "Fantasy Score": "Fantasy Score",
    "Total Games": "Total Games",
    "Total Games Won": "Games Won",          # match UD label
    "Total Sets": "Total Sets",
    "Total Tie Breaks": "Total Tie Breaks",
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


def get_frontend_slate(slate_id: str) -> Optional[FrontendSlate]:
    """Hydrate a complete slate into the shape the frontend expects."""
    db = get_client()
    slate = db.table("slates").select("*").eq("id", slate_id).single().execute().data
    if not slate:
        return None

    matches = (
        db.table("matches")
        .select("*, player_a:player_a_id(display_name), player_b:player_b_id(display_name)")
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
                opening_odds=_build_opening_odds_model(m.get("opening_odds")),
                closing_odds=_build_opening_odds_model(m.get("closing_odds")),
                adj_a=0,
                adj_b=0,
            )
        )

    by_canonical: dict[str, dict] = {}
    for sp in players:
        cid = sp["player_id"]
        display = (sp.get("player") or {}).get("display_name") or sp.get("dk_display_name")
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

    # PP lines: only the STANDARD variant feeds the engine. Demon/goblin
    # lines are shifted away from true and would corrupt projections if
    # the engine averaged across all three. The PP tab UI shows each
    # variant separately for edge analysis; the DK engine just needs the
    # truthful base line per stat.
    pp_rows = (
        db.table("prizepicks_lines")
        .select("raw_player_name, stat_type, current_line, odds_type")
        .eq("slate_id", slate_id)
        .eq("is_active", True)
        .eq("odds_type", "standard")
        .execute()
        .data
        or []
    )
    pp_lines_out = []
    for r in pp_rows:
        engine_stat = PP_STAT_TO_ENGINE.get(r["stat_type"], r["stat_type"])
        pp_lines_out.append(
            {
                "player": r["raw_player_name"],
                "stat": engine_stat,
                "line": float(r["current_line"]),
                "mult": "",
                "source": "prizepicks",
            }
        )

    # Underdog stat-prop lines from matches.odds.posted_lines (kept as a
    # secondary source — when both PP and UD have a line for the same
    # player+stat, engine.js currently uses whichever is added last.
    # PP rows are added first here so UD wins ties, which is the existing
    # behavior. Don't reorder without checking engine.js.)
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
            c for c in candidates
            if c.get("lock_time") and c["lock_time"] > now_iso
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
