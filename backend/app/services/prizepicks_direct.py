"""
PrizePicks direct ingest via Oxylabs Web Scraper API.

Captures all 3 PP variants per (player, stat_type): standard / demon / goblin.
Each variant is persisted as a separate row in prizepicks_lines with its own
odds_type and multiplier, so the PP tab UI can display the full variant set.

The engine still consumes ONE projection center per (player, stat) via
slate_reader._aggregate_pp_lines() → match.odds.posted_lines (multiplier-
aware Poisson λ-fit per variant, averaged — see slate_reader v6.5).

v6.4 — adds payout_multiplier capture. PP's projections endpoint exposes
multiplier under attributes.payout_multiplier on goblin/demon variants and
implicitly = 2.0 on standard. We store the explicit value when present and
fall back to the typical-by-odds_type inference when absent (older API
response shapes don't carry the field).

v6.5.3 — two important fixes:

  1. DOUBLES FILTER. PP publishes Fantasy Score lines for doubles teams
     (e.g. "Hsieh S-W / Kenin S", "Heliovaara H / Patten H"). DK tennis on
     DraftKings is singles-only, so these lines can't match any DK player
     and only clutter the PP tab + line-movements feed. We detect them by
     the " / " separator in the raw_player_name and skip ingestion.

  2. DIFF-BASED PERSISTENCE. The previous ingester deactivated ALL active
     rows for each (slate, stat_type, odds_type) bucket and re-inserted
     every fresh row, on every tick. The line_movements trigger interpreted
     this as "removed" + "new" event pairs even when the actual line value
     was unchanged — so every line appeared to "move" on every tick (e.g.
     Polina Kudermetova BP × 5 → 5). The new logic diffs fresh vs. existing
     keyed by (player, stat, odds_type) and only writes when something
     actually changed:
        - exact match (same line + same multiplier): NO-OP — trigger silent
        - line value changed:                         UPDATE current_line  → trigger fires "up"/"down"
        - multiplier changed only:                    UPDATE multiplier    → no movement event written
        - new line never seen:                        INSERT               → trigger fires "new"
        - existing key vanished from fresh feed:      UPDATE is_active=F   → trigger fires "removed"

v6.5.5 — two correctness fixes for production data loss bugs:

  1. PAGINATION. The previous fetch_projections() only fetched page=1 with
     per_page=500. Tennis routinely has 600-700+ projections per league
     (~32 players × 4-8 stat types × up to 3 variants). Anything past row
     500 was silently dropped, and *which* 500 rows came back varied by
     scrape — so each tick saw a slightly different subset of PP's true
     state. Combined with the v6.5.3 diff logic, this caused mass false-
     deactivations: rows that actually existed on PP got marked is_active
     = false because they happened to land on page 2 of that scrape's
     response. The user's symptom: 12+ Break Points Won lines deactivated
     in one tick (Sonmez, Pegula, Noskova, Rybakina, etc.) while those
     same lines were clearly still live on PP's UI.

     Fix: walk pages until we get an empty page or len(data) < per_page.
     Dedupe `included` (player records) by id across pages. Hard cap at
     10 pages as a safety guard against an infinite loop on a malformed
     response. If page 1 fails, return the error. If page >1 fails, we
     have a partial fetch; flag it via the _partial key so the caller
     knows to skip deactivations.

  2. CIRCUIT BREAKER. Even with pagination, transient API failures or
     PP server-side issues could return abnormally few rows. We now
     compare fresh_total to existing_total before applying the diff:
       - if pagination returned _partial=True → skip deactivations
       - if fresh_total < 50% of existing_total (and existing was
         meaningful, i.e. >= 50 rows) → skip deactivations
     In both cases we still apply inserts and updates (better data is
     always good), but withhold the destructive deactivation pass to
     prevent a single bad fetch from nuking valid data. Logged loudly
     so it's visible if it triggers.
"""

from __future__ import annotations

import json as _json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.db import get_client
from app.services.normalizer import PlayerNormalizer

log = logging.getLogger(__name__)

OXY_USER = os.getenv("OXYLABS_USERNAME", "")
OXY_PASS = os.getenv("OXYLABS_PASSWORD", "")
OXY_ENDPOINT = "https://realtime.oxylabs.io/v1/queries"

PP_BASE = "https://api.prizepicks.com"

TENNIS_LEAGUE_NAME_ALLOW = {"TENNIS"}

ALLOWED_STAT_TYPES = {
    "Aces",
    "Break Points Won",
    "Double Faults",
    "Fantasy Score",
    "Total Games",
    "Total Games Won",
    "Total Sets",
    "Total Tie Breaks",
}

# Typical PP multiplier defaults when attributes.payout_multiplier is absent.
DEFAULT_MULT_BY_ODDS_TYPE = {
    "standard": 2.0,
    "goblin": 1.5,
    "demon": 3.0,
}

PP_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://app.prizepicks.com/",
    "Origin": "https://app.prizepicks.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# v6.5.3 tolerance for treating two float values as "the same line". PP
# always posts at 0.5 increments so anything tighter than this means
# they really are equal.
_LINE_EQ_EPS = 0.001
_MULT_EQ_EPS = 0.005


# ---------------------------------------------------------------------------
# Player resolver
# ---------------------------------------------------------------------------

def _resolve_player(raw_name: str, sport: str = "tennis") -> Optional[str]:
    if not raw_name:
        return None
    try:
        normalizer = PlayerNormalizer(sport=sport)
        result = normalizer.resolve(
            raw_name, source="prizepicks", create_if_missing=True
        )
        if result.auto_resolved or result.was_new:
            return result.canonical_id
        log.warning(
            "_resolve_player low_confidence raw=%r best=%r score=%s",
            raw_name, result.canonical_id, result.score,
        )
        return None
    except Exception as e:
        log.warning("resolve_player failed for %s: %s", raw_name, e)
        return None


def _is_doubles_name(raw_name: str) -> bool:
    """v6.5.3: heuristic for detecting PP doubles entries.

    PP formats doubles teams as "<player_a> / <player_b>" with a single
    forward slash flanked by spaces, e.g. "Hsieh S-W / Kenin S" or
    "Heliovaara H / Patten H". Singles names in tennis never contain that
    pattern (hyphenated surnames use "-", not "/"). One-shot match catches
    every doubles row in the feed without needing a tournament-level flag.
    """
    return " / " in (raw_name or "")


# ---------------------------------------------------------------------------
# Oxylabs fetch
# ---------------------------------------------------------------------------

def _oxy_payload(url: str, render: bool = False) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "source": "universal",
        "url": url,
        "geo_location": "United States",
        "user_agent_type": "desktop",
        "headers": PP_HEADERS,
    }
    if render:
        p["render"] = "html"
    return p


def oxy_probe(url: str, render: bool = False, timeout: float = 60.0) -> Dict[str, Any]:
    if not (OXY_USER and OXY_PASS):
        return {"error": "oxylabs_creds_missing"}
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(
                OXY_ENDPOINT,
                auth=(OXY_USER, OXY_PASS),
                json=_oxy_payload(url, render=render),
            )
            out: Dict[str, Any] = {
                "envelope_status": r.status_code,
                "envelope_size": len(r.content),
            }
            try:
                env = r.json()
            except Exception:
                out["envelope_raw_first_1000"] = r.text[:1000]
                return out
            results = env.get("results", []) or []
            out["job"] = env.get("job", {})
            out["result_count"] = len(results)
            if results:
                first = results[0]
                content = first.get("content")
                content_preview: Any = None
                if isinstance(content, str):
                    content_preview = content[:600]
                elif isinstance(content, dict):
                    content_preview = {
                        k: (str(v)[:200] if not isinstance(v, (dict, list)) else type(v).__name__)
                        for k, v in list(content.items())[:10]
                    }
                out["first_result"] = {
                    "status_code": first.get("status_code"),
                    "url": first.get("url"),
                    "task_id": first.get("task_id"),
                    "created_at": first.get("created_at"),
                    "updated_at": first.get("updated_at"),
                    "content_type": type(content).__name__,
                    "content_preview": content_preview,
                }
            return out
    except Exception as e:
        return {"error": f"probe_exception: {e}"}


def _oxy_fetch_json(
    url: str, timeout: float = 60.0, render: bool = False
) -> Tuple[int, Dict[str, Any], str]:
    if not (OXY_USER and OXY_PASS):
        return 0, {}, "oxylabs_creds_missing"
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(
                OXY_ENDPOINT,
                auth=(OXY_USER, OXY_PASS),
                json=_oxy_payload(url, render=render),
            )
            if r.status_code != 200:
                return r.status_code, {}, f"oxy_envelope_http_{r.status_code}: {r.text[:300]}"
            env = r.json()
            results = env.get("results", []) or []
            if not results:
                return 0, {}, f"oxy_zero_results: job={env.get('job', {})}"
            first = results[0]
            inner_status = first.get("status_code", 0)
            content = first.get("content")
            if inner_status != 200:
                preview = ""
                if isinstance(content, str):
                    preview = content[:300]
                elif isinstance(content, dict):
                    preview = _json.dumps(content)[:300]
                return inner_status, {}, f"pp_inner_http_{inner_status}: {preview}"
            if isinstance(content, str):
                try:
                    content = _json.loads(content)
                except Exception as e:
                    return 0, {}, f"json_decode_failed: {e} :: {content[:300]}"
            if not isinstance(content, dict):
                return 0, {}, f"unexpected_content_type: {type(content).__name__}"
            return 200, content, ""
    except Exception as e:
        return 0, {}, f"oxy_exception: {e}"


# ---------------------------------------------------------------------------
# PP API surface
# ---------------------------------------------------------------------------

def fetch_leagues() -> Tuple[int, List[Dict[str, Any]]]:
    status, body, err = _oxy_fetch_json(f"{PP_BASE}/leagues")
    if status != 200:
        log.error("fetch_leagues failed: %s", err)
        return status, []
    return 200, body.get("data", []) or []


def fetch_projections(league_id: str) -> Tuple[int, Dict[str, Any], str]:
    """v6.5.5: walk all PP projection pages for a league.

    PP uses standard JSON:API ?page=N pagination. We walk the pages and
    aggregate `data` + `included` (deduped by id) until either:
      - an empty `data` page is returned     → no more results
      - len(data) < per_page                 → last page reached
      - 10 pages fetched                     → safety guard

    Page 1 failure is fatal (returns the error). Page >1 failure leaves
    us with a partial fetch — we set ``_partial: True`` in the response
    so the caller can skip destructive deactivations.

    Tennis routinely has 600-700+ projections per league. Without this
    pagination, ~200 props per scrape were silently dropped depending on
    PP's response ordering, and the v6.5.3 diff logic was deactivating
    them as missing. That's the bug this fixes.
    """
    per_page = 500
    max_pages = 10
    all_data: List[Dict[str, Any]] = []
    seen_included: Dict[str, Dict[str, Any]] = {}
    partial = False

    for page in range(1, max_pages + 1):
        url = (
            f"{PP_BASE}/projections"
            f"?league_id={league_id}"
            f"&per_page={per_page}"
            f"&single_stat=true"
            f"&page={page}"
        )
        status, body, err = _oxy_fetch_json(url, render=False)
        if status != 200:
            log.warning(
                "fetch_projections league=%s page=%d plain failed (%s): %s "
                "— retrying with render",
                league_id, page, status, err,
            )
            status, body, err = _oxy_fetch_json(url, render=True)

        if status != 200:
            if page == 1:
                # Page 1 is fatal — no data at all, surface the error.
                return status, {}, err
            # Mid-walk failure: return what we have, flag as partial.
            partial = True
            log.warning(
                "fetch_projections league=%s pagination broke at page %d: %s "
                "— continuing with partial result",
                league_id, page, err,
            )
            break

        page_data = body.get("data", []) or []
        page_included = body.get("included", []) or []

        # Dedupe `included` across pages — the same player record appears
        # on every page that has props for that player, but we only need
        # one copy in the final aggregated index.
        for inc in page_included:
            inc_id = str(inc.get("id") or "")
            if inc_id and inc_id not in seen_included:
                seen_included[inc_id] = inc

        if not page_data:
            break  # explicit "no more results"

        all_data.extend(page_data)

        if len(page_data) < per_page:
            break  # short page = last page

    else:
        # Loop fell off after max_pages without breaking — capped, treat
        # as partial since we may not have walked everything.
        partial = True
        log.warning(
            "fetch_projections league=%s hit max_pages=%d cap, treating as partial",
            league_id, max_pages,
        )

    return 200, {
        "data": all_data,
        "included": list(seen_included.values()),
        "_partial": partial,
    }, ""


def get_tennis_league_ids() -> List[str]:
    status, leagues = fetch_leagues()
    if status != 200:
        return []
    out: List[str] = []
    for l in leagues:
        attr = l.get("attributes", {}) or {}
        name = (attr.get("name") or "").upper().strip()
        if name in TENNIS_LEAGUE_NAME_ALLOW:
            lid = str(l.get("id") or "")
            if lid:
                out.append(lid)
    return out


def _normalize_odds_type(raw: Optional[str]) -> str:
    if not raw:
        return "standard"
    r = str(raw).strip().lower()
    if r in {"goblin", "demon", "standard"}:
        return r
    return "standard"


def _extract_multiplier(attr: Dict[str, Any], odds_type: str) -> float:
    """Pull the multiplier off a PP projection attributes payload.

    PP's response shape has historically exposed it under a few different
    versions: payout_multiplier (current), odds_type_multiplier (legacy),
    and flat multiplier (occasional A/B). We check all three. When none
    exist or all are zero, fall back to the conservative defaults in
    DEFAULT_MULT_BY_ODDS_TYPE so the row still has a usable number.
    """
    for key in ("payout_multiplier", "odds_type_multiplier", "multiplier"):
        v = attr.get(key)
        if v is None:
            continue
        try:
            f = float(v)
            if f > 0:
                return f
        except (TypeError, ValueError):
            continue
    return DEFAULT_MULT_BY_ODDS_TYPE.get(odds_type, 2.0)


def _build_player_index(included: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for inc in included:
        if (inc.get("type") or "").lower() != "new_player":
            continue
        pid = str(inc.get("id") or "")
        if not pid:
            continue
        idx[pid] = inc.get("attributes", {}) or {}
    return idx


# ---------------------------------------------------------------------------
# Diff helpers (v6.5.3)
# ---------------------------------------------------------------------------

def _line_eq(a: Optional[float], b: Optional[float]) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        return abs(float(a) - float(b)) <= _LINE_EQ_EPS
    except (TypeError, ValueError):
        return False


def _mult_eq(a: Optional[float], b: Optional[float]) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= _MULT_EQ_EPS
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Main ingest
# ---------------------------------------------------------------------------

def run_prizepicks_direct(slate_id: str) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "slate_id": slate_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "leagues": [],
        "inserted": 0,
        "updated_line": 0,
        "updated_mult_only": 0,
        "unchanged": 0,
        "deactivated": 0,
        "skipped_doubles": 0,
        "row_failures": 0,
        "odds_type_histogram": {},
        "stat_histogram": {},
        "errors": [],
    }
    if not (OXY_USER and OXY_PASS):
        summary["errors"].append("oxylabs_creds_missing")
        return summary

    league_ids = get_tennis_league_ids()
    if not league_ids:
        summary["errors"].append("no_tennis_leagues_resolved")
        return summary

    db = get_client()
    overall_odds_hist: Counter = Counter()
    overall_stat_hist: Counter = Counter()

    # ── 1. Fetch existing active rows for the slate ONCE ────────────────
    # Index by (raw_player_name, stat_type, odds_type). Multiple rows per
    # key are allowed — PP occasionally A/B-tests two different lines at
    # the same odds_type (e.g. two demon variants). We carry them as a
    # list and match on line value during the diff.
    try:
        existing_rows = (
            db.table("prizepicks_lines")
            .select("id, raw_player_name, stat_type, odds_type, current_line, multiplier, player_id, notes")
            .eq("slate_id", slate_id)
            .eq("is_active", True)
            .execute()
            .data
            or []
        )
    except Exception as e:
        log.exception("Failed to load existing PP rows for slate=%s: %s", slate_id, e)
        existing_rows = []
        summary["errors"].append(f"load_existing_failed: {e}")

    existing_idx: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in existing_rows:
        key = (r["raw_player_name"], r["stat_type"], r["odds_type"])
        existing_idx[key].append(r)

    # ── 2. Pull fresh state from PP across every tennis league ──────────
    fresh_idx: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    skipped_doubles = 0
    any_partial_fetch = False  # v6.5.5: any league had a partial fetch?

    for lid in league_ids:
        status, body, err = fetch_projections(lid)
        if status != 200:
            summary["leagues"].append(
                {"id": lid, "http": status, "error": err}
            )
            summary["errors"].append(f"projections_failed[{lid}]: {err}")
            continue

        if body.get("_partial"):
            any_partial_fetch = True

        data = body.get("data", []) or []
        included = body.get("included", []) or []
        player_idx = _build_player_index(included)
        league_fresh_count = 0

        for proj in data:
            attr = proj.get("attributes", {}) or {}
            stat_type = (attr.get("stat_type") or "").strip()
            if stat_type not in ALLOWED_STAT_TYPES:
                continue

            odds_type = _normalize_odds_type(attr.get("odds_type"))
            line_score = attr.get("line_score")
            if line_score is None:
                continue

            try:
                line_val = float(line_score)
            except (TypeError, ValueError):
                continue

            multiplier = _extract_multiplier(attr, odds_type)

            rel = (proj.get("relationships") or {}).get("new_player") or {}
            pdata = rel.get("data") or {}
            pp_pid = str(pdata.get("id") or "")
            if not pp_pid:
                continue
            pinfo = player_idx.get(pp_pid, {})
            raw_name = (pinfo.get("name") or "").strip()
            if not raw_name:
                continue

            # v6.5.3: drop doubles squares before any normalizer/DB work.
            # DK tennis is singles-only — these rows can't match a DK
            # player and just clutter the PP tab + movement feed.
            if _is_doubles_name(raw_name):
                skipped_doubles += 1
                continue

            resolved = _resolve_player(raw_name)
            if not resolved:
                continue

            fresh_idx[(raw_name, stat_type, odds_type)].append({
                "slate_id": slate_id,
                "player_id": resolved,
                "raw_player_name": raw_name,
                "stat_type": stat_type,
                "current_line": line_val,
                "odds_type": odds_type,
                "multiplier": multiplier,
                "league": "TENNIS",
                "is_active": True,
                "notes": f"pp_proj_id={proj.get('id')}",
            })
            league_fresh_count += 1
            overall_stat_hist[stat_type] += 1
            overall_odds_hist[odds_type] += 1

        summary["leagues"].append(
            {
                "id": lid,
                "http": 200,
                "fresh_rows": league_fresh_count,
                "partial": bool(body.get("_partial")),
                "raw_data_count": len(data),
            }
        )

    summary["skipped_doubles"] = skipped_doubles

    # ── 2.5 Circuit breaker (v6.5.5) ────────────────────────────────────
    # The diff loop below has a destructive deactivation pass. If our
    # fresh data is suspiciously incomplete (partial pagination, transient
    # API failure, PP server hiccup), running that pass nukes valid rows
    # that just happen to be missing from this particular response. We
    # detect two suspicious-fetch signatures and disarm deactivation when
    # either fires; inserts and updates still apply (better data is good).
    fresh_total = sum(len(rows) for rows in fresh_idx.values())
    existing_total = sum(len(rows) for rows in existing_idx.values())
    summary["fresh_total"] = fresh_total
    summary["existing_total"] = existing_total

    deactivate_safe = True
    breaker_reason: Optional[str] = None

    if any_partial_fetch:
        deactivate_safe = False
        breaker_reason = "partial_fetch (one or more leagues paginated incompletely)"
    elif existing_total >= 20 and fresh_total < 0.5 * existing_total:
        deactivate_safe = False
        breaker_reason = (
            f"fresh_total={fresh_total} < 50% of existing_total={existing_total}; "
            f"likely transient PP API issue"
        )

    if not deactivate_safe:
        summary["circuit_breaker"] = breaker_reason
        log.warning(
            "PP ingest CIRCUIT BREAKER engaged for slate=%s: %s. "
            "Inserts/updates will apply; deactivations SKIPPED to prevent data loss.",
            slate_id, breaker_reason,
        )

    # ── 3. Diff fresh vs existing ───────────────────────────────────────
    inserted = 0
    updated_line = 0
    updated_mult_only = 0
    unchanged = 0
    deactivated = 0
    row_failures = 0

    for key, fresh_list in fresh_idx.items():
        existing_list = list(existing_idx.get(key, []))  # defensive copy
        consumed_existing_ids: set[str] = set()

        # Pass 1 — exact matches: same line + same multiplier → no-op.
        for fresh in fresh_list:
            match: Optional[Dict[str, Any]] = None
            for ex in existing_list:
                if ex["id"] in consumed_existing_ids:
                    continue
                if _line_eq(ex.get("current_line"), fresh["current_line"]) and \
                   _mult_eq(ex.get("multiplier"), fresh["multiplier"]):
                    match = ex
                    break
            if match is not None:
                consumed_existing_ids.add(match["id"])
                fresh["_settled"] = True
                unchanged += 1

        # Pass 2 — line-equal multiplier-different: write multiplier only.
        # This is rare (PP usually moves the line and the multiplier
        # together when a price changes) but we handle it cleanly. Multi
        # update does NOT fire the line_movements trigger because
        # current_line is unchanged — exactly what we want.
        for fresh in fresh_list:
            if fresh.get("_settled"):
                continue
            match = None
            for ex in existing_list:
                if ex["id"] in consumed_existing_ids:
                    continue
                if _line_eq(ex.get("current_line"), fresh["current_line"]):
                    match = ex
                    break
            if match is not None:
                consumed_existing_ids.add(match["id"])
                try:
                    db.table("prizepicks_lines").update({
                        "multiplier": fresh["multiplier"],
                        "notes": fresh["notes"],
                    }).eq("id", match["id"]).execute()
                    updated_mult_only += 1
                    fresh["_settled"] = True
                except Exception as e:
                    row_failures += 1
                    if len(summary["errors"]) < 30:
                        summary["errors"].append(
                            f"mult_update_failed[{key}/{match['id']}]: {str(e)[:200]}"
                        )

        # Pass 3 — line moved: pick any leftover existing row at this key
        # and UPDATE it with the new line. Trigger fires "up"/"down".
        for fresh in fresh_list:
            if fresh.get("_settled"):
                continue
            target: Optional[Dict[str, Any]] = None
            for ex in existing_list:
                if ex["id"] in consumed_existing_ids:
                    continue
                target = ex
                break
            if target is not None:
                consumed_existing_ids.add(target["id"])
                try:
                    db.table("prizepicks_lines").update({
                        "current_line": fresh["current_line"],
                        "multiplier": fresh["multiplier"],
                        "notes": fresh["notes"],
                    }).eq("id", target["id"]).execute()
                    updated_line += 1
                    fresh["_settled"] = True
                except Exception as e:
                    row_failures += 1
                    if len(summary["errors"]) < 30:
                        summary["errors"].append(
                            f"line_update_failed[{key}/{target['id']}]: {str(e)[:200]}"
                        )

        # Pass 4 — brand new line: INSERT. Trigger fires "new".
        for fresh in fresh_list:
            if fresh.get("_settled"):
                continue
            try:
                payload = {k: v for k, v in fresh.items() if not k.startswith("_")}
                db.table("prizepicks_lines").insert(payload).execute()
                inserted += 1
            except Exception as e:
                row_failures += 1
                if len(summary["errors"]) < 30:
                    summary["errors"].append(
                        f"insert_failed[{key}]: {str(e)[:200]}"
                    )

        # Pass 5 — leftover existing rows for this key not consumed by
        # any fresh row → PP no longer publishes them, deactivate. Trigger
        # fires "removed". v6.5.5: skipped when circuit breaker engaged.
        if deactivate_safe:
            for ex in existing_list:
                if ex["id"] in consumed_existing_ids:
                    continue
                try:
                    db.table("prizepicks_lines").update({
                        "is_active": False,
                    }).eq("id", ex["id"]).execute()
                    deactivated += 1
                except Exception as e:
                    row_failures += 1
                    if len(summary["errors"]) < 30:
                        summary["errors"].append(
                            f"deactivate_failed[{key}/{ex['id']}]: {str(e)[:200]}"
                        )

    # ── 4. Keys present in existing but absent from fresh entirely ─────
    # PP dropped all variants for this (player, stat, odds_type) — could
    # mean the prop got pulled, the player is no longer on the slate, etc.
    # v6.5.5: gated on circuit breaker so a partial / failed fetch can't
    # mass-deactivate valid data.
    if deactivate_safe:
        for key, existing_list in existing_idx.items():
            if key in fresh_idx:
                continue  # handled above
            for ex in existing_list:
                try:
                    db.table("prizepicks_lines").update({
                        "is_active": False,
                    }).eq("id", ex["id"]).execute()
                    deactivated += 1
                except Exception as e:
                    row_failures += 1
                    if len(summary["errors"]) < 30:
                        summary["errors"].append(
                            f"deactivate_missing_failed[{key}/{ex['id']}]: {str(e)[:200]}"
                        )

    summary["inserted"] = inserted
    summary["updated_line"] = updated_line
    summary["updated_mult_only"] = updated_mult_only
    summary["unchanged"] = unchanged
    summary["deactivated"] = deactivated
    summary["row_failures"] = row_failures
    summary["odds_type_histogram"] = dict(overall_odds_hist)
    summary["stat_histogram"] = dict(overall_stat_hist)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()

    log.info(
        "PP ingest done slate=%s ins=%d line_upd=%d mult_upd=%d unchanged=%d deact=%d "
        "doubles_skipped=%d fail=%d fresh=%d existing=%d breaker=%s odds=%s stat=%s",
        slate_id, inserted, updated_line, updated_mult_only, unchanged,
        deactivated, skipped_doubles, row_failures, fresh_total, existing_total,
        breaker_reason or "none",
        summary["odds_type_histogram"], summary["stat_histogram"],
    )
    return summary


# ---------------------------------------------------------------------------
# Scheduler shim
# ---------------------------------------------------------------------------

async def fetch_tick(sport_code: str = "TEN") -> Dict[str, Any]:
    import asyncio
    if sport_code != "TEN":
        return {"skipped": "not_tennis"}

    db = get_client()
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = (
        db.table("slates")
        .select("id, lock_time, slate_date, first_seen_at")
        .eq("sport", "tennis")
        .eq("status", "active")
        .eq("contest_type", "classic")
        .eq("is_fallback", False)
        .order("slate_date", desc=True)
        .order("first_seen_at", desc=True)
        .execute()
        .data
        or []
    )
    if not rows:
        log.info("pp_direct fetch_tick: no active classic tennis slate")
        return {"skipped": "no_active_slate"}

    upcoming = [r for r in rows if r.get("lock_time") and r["lock_time"] > now_iso]
    upcoming.sort(key=lambda c: c["lock_time"])
    slate_id = upcoming[0]["id"] if upcoming else rows[0]["id"]

    return await asyncio.to_thread(run_prizepicks_direct, slate_id)
