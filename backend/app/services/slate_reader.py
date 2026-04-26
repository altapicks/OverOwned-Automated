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
        (player, stat, line, mult,           Uses median across variants
        odds_type). Fantasy Score is         when standard is absent so
        what the PP TAB actually displays    the engine still gets a single
        per the user's spec; other stats     projection center per stat.
        are exposed for edge calcs.

v6.4 — pp_lines now emits one row per (player, stat, odds_type) instead of
collapsing to one row per (player, stat). Multiplier is included so the PP
tab UI can render goblin/demon variants alongside standard with their
correct payouts. The engine's posted_lines projection logic is unchanged.
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
    """Median across standard/demon/goblin per (canonical_id, stat_type).

    Used ONLY to drive match.odds.posted_lines for the engine. The engine
    needs ONE projection center per stat — the median (or standard if it
    exists) is the right choice. The PP tab UI uses
    _emit_pp_lines_all_variants() instead which keeps every row separate.

    PP runs each prop in three flavors:
      standard  — closest to the "true" line, but only ~15% of props publish it
      demon     — line shifted UP (harder over, boosted multiplier)
      goblin    — line shifted DOWN (easier over, reduced multiplier)
    Median across all three approximates the unobserved true line and keeps
    Aces/DF/Sets/Games Won props (which are almost exclusively demon/goblin)
    usable as projection inputs.

    Falls back to the standard line alone if it exists, otherwise the median,
    ensuring single-variant rows still pass through.

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
) -> Optional[dict]:
    """Build match.odds.posted_lines.{a,b} from per-player PP medians.

    engine.js applyPostedLineOverrides reads:
        aces, dfs, breaks, games_won, games_lost, sets_won, sets_lost

    PP gives us aces, dfs, breaks (= Break Points Won), and games_won
    (= Total Games Won) directly per player. We derive games_lost cross-side
    (a.games_lost = b.games_won) since a's games_lost are the games b
    actually wins. sets_won / sets_lost are intentionally NOT derived —
    engine baseline math handles them from Kalshi wp, which is far more
    reliable than trying to back them out of the match-level Total Sets line
    PP publishes.

    If existing_posted (from manual CSV upload) has any keys, those win on
    collision — manual override is sacred.
    """
    sides: dict[str, dict] = {}

    for side_key, pid in [("a", pa_id), ("b", pb_id)]:
        side_dict: dict = {}
        for pp_stat, posted_key in PP_STAT_TO_POSTED_LINE_KEY.items():
            agg = pp_agg.get((pid, pp_stat))
            if agg is not None:
                side_dict[posted_key] = float(agg["line"])
        if side_dict:
            sides[side_key] = side_dict

    a_side = sides.get("a", {})
    b_side = sides.get("b", {})
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

    # Engine-side projection center: one median line per (player, stat).
    pp_agg = _aggregate_pp_lines_for_engine(pp_rows)

    frontend_matches: list[FrontendMatch] = []
    for m in matches:
        pa_id = m["player_a_id"]
        pb_id = m["player_b_id"]
        pa_name = (m.get("player_a") or {}).get("display_name") or pa_id
        pb_name = (m.get("player_b") or {}).get("display_name") or pb_id

        odds = m.get("odds") or {}
        wp_a = odds.get("kalshi_prob_a")
        existing_posted = odds.get("posted_lines")

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

    # PP tab UI feed: every variant per (player, stat) emitted as a separate
    # row with its line and multiplier. The PP TAB display filters to Fantasy
    # Score by default per the user's spec — the other stats are exposed here
    # so any PP-tab edge calc that wants them can read them without an extra
    # fetch, and so the user can see goblin/demon line-up / line-down splits.
    pp_lines_out = _emit_pp_lines_all_variants(pp_rows)

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
