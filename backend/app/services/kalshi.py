"""Kalshi Trading API v2 client.

CORRECT AUTH SCHEME (confirmed from Kalshi's official docs):
  - Headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-SIGNATURE, KALSHI-ACCESS-TIMESTAMP
  - Signature: RSA-PSS with SHA256 over the string:
        <timestamp_ms> + HTTP_METHOD + <path_without_query>
  - Per-request signing (no token caching — each request signs itself)

This is NOT JWT. Earlier draft briefs got this wrong.

Base URL (verified):
  * General markets (tennis/sports/other): https://trading-api.kalshi.com/trade-api/v2
  * Elections only:                         https://api.elections.kalshi.com/trade-api/v2

We read from env KALSHI_API_BASE. Update that Railway variable to the
general-markets URL before first successful run.
"""
from __future__ import annotations

import base64
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from app.config import get_settings
from app.db import get_client
from app.services import notifier
from app.services.http import CircuitBreaker, request_with_retry
from app.services.normalizer import PlayerNormalizer

logger = logging.getLogger(__name__)

_breaker_kalshi = CircuitBreaker(failure_threshold=5, cooldown_seconds=600)
_breaker_kalshi._name = "kalshi"


async def _kalshi_breaker_opened(name: str):
    await notifier.notify_error(
        "kalshi_circuit_breaker",
        f"Circuit breaker {name} opened after 5 consecutive failures. Pausing for 10 min.",
    )


_breaker_kalshi._on_open = _kalshi_breaker_opened

# Tennis match-winner series on Kalshi. These are the binary "who wins"
# markets we want. Other tennis series (set winners, totals, tournament
# futures) aren't used here.
TENNIS_MATCH_SERIES = ("KXATPMATCH", "KXWTAMATCH")


def _strip_accents(s: str) -> str:
    """Normalize unicode for matching: 'Müller' → 'Muller'."""
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFKD", s or "")
        if not unicodedata.combining(c)
    )


def _last_name_key(name: str) -> str:
    """Extract a normalized last-name matching key from a full or partial name.

    Examples:
      "Jannik Sinner"       → "sinner"
      "Sinner"              → "sinner"
      "Müller-Schär"        → "muller-schar"   (keep hyphen, strip accents)
      "Van de Zandschulp"   → "zandschulp"     (drop lowercase particles)
      "De Minaur"           → "minaur"         (drop 'de')
    """
    if not name:
        return ""
    cleaned = _strip_accents(name).strip().lower()
    # Drop common surname particles before taking the last token
    parts = [
        p for p in cleaned.split()
        if p not in {"de", "van", "der", "den", "da", "di", "du", "le", "la", "el", "al", "del"}
    ]
    if not parts:
        return cleaned
    # The last token is the last name
    return parts[-1]

# Module-level cache so we don't re-parse the PEM on every request.
_private_key: Optional[RSAPrivateKey] = None


def _normalize_pem(raw: str) -> str:
    """Recover a valid PEM from common env-var mangling.

    Railway (and many other hosts) store multi-line env values inconsistently.
    Common mangling patterns we handle:
      * Literal "\\n" inserted instead of real newlines
      * Everything on one line, no newlines at all
      * Wrapped in single/double quotes
      * Extra leading/trailing whitespace
      * CRLF instead of LF

    Strategy:
      1. Strip surrounding whitespace + quotes
      2. Replace literal "\\n" with real newlines
      3. If BEGIN/END markers exist but no newlines between them, re-wrap
         the base64 body at 64 chars per line
    """
    if not raw:
        return ""
    s = raw.strip()
    # Strip wrapping quotes if present
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    # Replace literal \n sequences (two chars: backslash + n) with real newlines
    s = s.replace("\\n", "\n").replace("\\r", "")
    # Normalize CRLF
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # If there are no newlines at all but the markers are present,
    # reconstruct by wrapping the body.
    if "\n" not in s and "-----BEGIN" in s and "-----END" in s:
        import re
        m = re.match(r"^(-----BEGIN [A-Z ]+-----)(.*?)(-----END [A-Z ]+-----)\s*$", s)
        if m:
            begin, body, end = m.group(1), m.group(2).strip(), m.group(3)
            # Chunk body into 64-char lines
            chunks = [body[i : i + 64] for i in range(0, len(body), 64)]
            s = begin + "\n" + "\n".join(chunks) + "\n" + end + "\n"

    if not s.endswith("\n"):
        s += "\n"
    return s


def _load_private_key() -> Optional[RSAPrivateKey]:
    """Parse KALSHI_PRIVATE_KEY env var as PEM, with self-healing for
    common env-var mangling."""
    global _private_key
    if _private_key is not None:
        return _private_key
    s = get_settings()
    raw = s.kalshi_private_key or ""
    if not raw.strip():
        logger.warning("KALSHI_PRIVATE_KEY env var is empty")
        return None
    pem = _normalize_pem(raw)
    try:
        key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
        if not isinstance(key, RSAPrivateKey):
            logger.error("Kalshi private key loaded but is not RSA")
            return None
        _private_key = key
        logger.info(
            "Kalshi private key loaded successfully (size=%d bits, key_id_prefix=%s...)",
            key.key_size,
            (s.kalshi_key_id or "")[:8],
        )
        return key
    except Exception as e:
        # Give operator actionable hints about what went wrong
        headline = type(e).__name__
        preview_lines = pem.splitlines()
        first = preview_lines[0] if preview_lines else ""
        last = preview_lines[-1] if preview_lines else ""
        logger.error(
            "Failed to parse KALSHI_PRIVATE_KEY (%s: %s). "
            "PEM had %d lines after normalization. First line: %r Last line: %r. "
            "Common causes: (1) pasted with literal \\n instead of real newlines "
            "(usually fine — self-heal handles it), (2) missing BEGIN/END markers, "
            "(3) wrong key format (need RSA PKCS1 or PKCS8 PEM, not OpenSSH). "
            "Regenerate at kalshi.com/account/profile and re-paste — Railway "
            "editors usually preserve newlines when pasting multi-line text directly.",
            headline, str(e)[:200], len(preview_lines), first, last,
        )
        return None


def _sign_request(method: str, path: str, timestamp_ms: str) -> Optional[str]:
    """Produce the KALSHI-ACCESS-SIGNATURE header value.

    Signs the concatenation: timestamp_ms + HTTP_METHOD + path (no query).
    Returns base64-encoded signature.
    """
    key = _load_private_key()
    if key is None:
        return None
    # Strip query params — Kalshi signs path only
    path_only = path.split("?")[0]
    message = f"{timestamp_ms}{method.upper()}{path_only}".encode("utf-8")
    try:
        signature = key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")
    except Exception as e:
        logger.error("Kalshi signature creation failed: %s", e)
        return None


def _auth_headers(method: str, full_url: str) -> Optional[dict]:
    """Build the three Kalshi auth headers for a request."""
    s = get_settings()
    if not s.kalshi_key_id:
        return None
    path = urlparse(full_url).path
    ts = str(int(time.time() * 1000))
    sig = _sign_request(method, path, ts)
    if not sig:
        return None
    return {
        "KALSHI-ACCESS-KEY": s.kalshi_key_id,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Accept": "application/json",
    }


async def _get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    """Authenticated GET. Returns None on failure (logs)."""
    s = get_settings()
    if not s.kalshi_api_base:
        return None
    url = s.kalshi_api_base.rstrip("/") + path
    headers = _auth_headers("GET", url)
    if not headers:
        return None
    try:
        r = await request_with_retry(
            "GET", url, headers=headers, params=params, breaker=_breaker_kalshi, max_retries=3
        )
        _breaker_kalshi.record_success()
        return r.json()
    except httpx.HTTPStatusError as e:
        # 401 typically means wrong base URL or invalid signature — critical
        status = e.response.status_code if e.response else None
        if status == 401:
            logger.error(
                "Kalshi 401 Unauthorized — check KALSHI_API_BASE (is it the right one "
                "for general markets vs elections?), KALSHI_KEY_ID, and KALSHI_PRIVATE_KEY."
            )
        else:
            logger.error("Kalshi HTTP error %s: %s", status, e)
        return None
    except Exception as e:
        logger.error("Kalshi request failed: %s", e)
        return None


# ── Market discovery + ingestion ────────────────────────────────────

_markets_cache: dict = {"data": None, "fetched_at": 0.0}
_MARKETS_TTL = 3600  # 1 hour


async def list_tennis_markets() -> list[dict]:
    """Fetch all open tennis match-winner markets.

    Kalshi organizes tennis match-winner markets under these series tickers:
      * KXATPMATCH — ATP (men's) single-match winner
      * KXWTAMATCH — WTA (women's) single-match winner

    We hit each directly rather than paging through /series looking for
    keyword matches, which was returning hundreds of unrelated markets
    (tournament futures, set winners, over/under games, etc.).

    Each market has a sub_title like "Vacherot vs Korda" with last names
    only — that's what we parse.
    """
    now = time.time()
    if _markets_cache["data"] is not None and (now - _markets_cache["fetched_at"]) < _MARKETS_TTL:
        return _markets_cache["data"]

    all_markets = []
    per_series_counts = {}
    for series_ticker in TENNIS_MATCH_SERIES:
        # Page through /markets filtered by series + status=open. Kalshi paginates
        # via cursor but 200 per page is enough for current tennis volume.
        m_resp = await _get(
            "/markets",
            params={
                "series_ticker": series_ticker,
                "status": "open",
                "limit": 200,
            },
        )
        markets = (m_resp or {}).get("markets") or []
        for mkt in markets:
            mkt["_series_ticker"] = series_ticker
        all_markets.extend(markets)
        per_series_counts[series_ticker] = len(markets)

    _markets_cache.update(data=all_markets, fetched_at=now)
    logger.info(
        "Kalshi tennis markets fetched: %s (total=%d)",
        per_series_counts,
        len(all_markets),
    )

    # Diagnostic: if nothing matched, log a few sample titles so we can
    # see what pattern Kalshi is actually using (for future parser tuning).
    if all_markets:
        sample = all_markets[:5]
        logger.debug(
            "Kalshi market samples: %s",
            [
                {
                    "ticker": m.get("ticker"),
                    "title": m.get("title"),
                    "sub_title": m.get("sub_title"),
                    "yes_sub_title": m.get("yes_sub_title"),
                }
                for m in sample
            ],
        )

    return all_markets


def _parse_player_names_from_title(title: str) -> Optional[tuple[str, str]]:
    """Extract two player names from a Kalshi match-winner market subtitle.

    Kalshi's tennis match-winner markets use subtitles like:
      "Vacherot vs Korda"         ← most common, LAST NAMES only
      "Vacherot vs. Korda"
      "Sinner vs Alcaraz"

    Preserves case for downstream normalization — we don't assume title-case.
    """
    if not title:
        return None
    t = title.strip()
    # Normalize all "vs" variants to a single " vs "
    for sep_variant in (" vs. ", " VS ", " VS. ", " Vs ", " Vs. "):
        t = t.replace(sep_variant, " vs ")
    if " vs " not in t:
        return None
    parts = t.split(" vs ", 1)
    if len(parts) != 2:
        return None
    left = parts[0].strip(" ?.,:()-—")
    right = parts[1].split("?")[0].strip(" ,.:()-—")
    if not left or not right:
        return None
    return left, right


async def fetch_tick() -> dict:
    """One ingestion cycle for Kalshi tennis.

    Matching strategy:
      1. Pull open markets from KXATPMATCH + KXWTAMATCH series
      2. Parse "LastA vs LastB" from each market's sub_title
      3. Build a last-name → player_id map from DK slate roster
      4. For each Kalshi market, look up both last names in the map
      5. If both resolve and they're on the same match in our slate, write odds
    """
    s = get_settings()
    if not s.kalshi_key_id or not s.kalshi_private_key:
        return {"skipped": "no_kalshi_creds"}
    if not s.kalshi_api_base:
        return {"skipped": "no_kalshi_base"}

    markets = await list_tennis_markets()
    if not markets:
        return {"skipped": "no_markets", "markets_found": 0}

    db = get_client()

    # Pull all candidate matches + their player display names for last-name matching.
    # We ask Supabase to expand player_a and player_b via the FK relationships so
    # we don't need a separate round trip per player.
    candidate_matches = (
        db.table("matches")
        .select(
            "id, slate_id, player_a_id, player_b_id, start_time,"
            " player_a:players!matches_player_a_id_fkey(canonical_id, display_name),"
            " player_b:players!matches_player_b_id_fkey(canonical_id, display_name),"
            " slates!inner(sport, status, contest_type, is_fallback)"
        )
        .eq("slates.sport", "tennis")
        .eq("slates.status", "active")
        .eq("slates.contest_type", "classic")
        .eq("slates.is_fallback", False)
        .execute()
        .data
        or []
    )

    # Build last-name → match context lookup. Value is list because multiple
    # players could share a last name across different matches.
    last_name_index: dict[str, list[dict]] = {}
    for m in candidate_matches:
        for side_key, player_field in [("a", "player_a"), ("b", "player_b")]:
            p = m.get(player_field) or {}
            pname = p.get("display_name") or ""
            key = _last_name_key(pname)
            if not key:
                continue
            last_name_index.setdefault(key, []).append(
                {
                    "match_id": m["id"],
                    "slate_id": m["slate_id"],
                    "side": side_key,
                    "canonical_id": p.get("canonical_id"),
                    "opponent_key": _last_name_key(
                        (m.get("player_a" if side_key == "b" else "player_b") or {}).get("display_name") or ""
                    ),
                }
            )

    matched = 0
    skipped_no_parse = 0
    skipped_no_match = 0
    unmatched_samples: list[str] = []  # for diagnostics

    for mkt in markets:
        # Kalshi puts the match description in sub_title. yes_sub_title sometimes
        # has just one name ("Sinner") — fall back to title or event ticker as
        # last resort.
        subtitle = (
            mkt.get("sub_title")
            or mkt.get("subtitle")  # defensive: older API might use this
            or mkt.get("title")
            or ""
        )
        pair = _parse_player_names_from_title(subtitle)
        if not pair:
            skipped_no_parse += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(f"no_parse: {subtitle!r}")
            continue

        a_name, b_name = pair
        a_key = _last_name_key(a_name)
        b_key = _last_name_key(b_name)
        if not a_key or not b_key:
            skipped_no_parse += 1
            continue

        # Find matches where BOTH last-names exist and are opponents of each other
        candidates_a = last_name_index.get(a_key, [])
        candidates_b = last_name_index.get(b_key, [])

        best_match = None
        for ca in candidates_a:
            for cb in candidates_b:
                if ca["match_id"] == cb["match_id"] and ca["side"] != cb["side"]:
                    best_match = (ca, cb)
                    break
            if best_match:
                break

        if not best_match:
            skipped_no_match += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(f"no_dk_match: {subtitle!r} ({a_key}/{b_key})")
            continue

        ca, cb = best_match
        # Our canonical A player — was that Kalshi's "a"?
        our_a_id = candidate_matches[0]["player_a_id"] if False else None  # unused
        # Determine orientation: if ca is our side "a", Kalshi's A == our A (no swap)
        swap = ca["side"] == "b"

        yes_price = (
            mkt.get("yes_bid")
            or mkt.get("yes_ask")
            or mkt.get("last_price")
        )
        no_price = mkt.get("no_bid") or mkt.get("no_ask")
        implied_a = None
        if yes_price is not None:
            try:
                # Kalshi prices in cents (0-100). Yes price = prob of first-named outcome.
                implied = float(yes_price) / 100.0
                # If Kalshi's "Yes" refers to player A but our canonical a is player B
                # (swap=True), invert.
                implied_a = 1 - implied if swap else implied
            except (TypeError, ValueError):
                pass

        odds_block = {
            "implied_prob_a": implied_a,
            "implied_prob_b": (1 - implied_a) if implied_a is not None else None,
            "market_ticker": mkt.get("ticker"),
            "series_ticker": mkt.get("_series_ticker"),
            "last_price": yes_price,
            "no_price": no_price,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "raw": {
                "subtitle": subtitle,
                "status": mkt.get("status"),
                "yes_bid": mkt.get("yes_bid"),
                "no_bid": mkt.get("no_bid"),
            },
        }

        await _write_kalshi_odds(ca["match_id"], ca["slate_id"], odds_block)
        matched += 1

    result = {
        "markets_found": len(markets),
        "matched": matched,
        "skipped_no_parse": skipped_no_parse,
        "skipped_no_match": skipped_no_match,
    }
    if unmatched_samples and matched == 0:
        logger.info("Kalshi unmatched samples: %s", unmatched_samples)
    return result


async def _write_kalshi_odds(match_id: str, slate_id: str, odds_block: dict):
    """Merge kalshi odds into matches.odds.kalshi, append to odds_history.

    Also promote kalshi_prob_a/kalshi_prob_b to the top level of matches.odds
    so the frontend can read `match.odds.kalshi_prob_a` directly without
    drilling into source-prefixed sub-objects. Source sub-object stays for
    audit + the fallback display logic that shows "via Kalshi" vs "via Market".
    """
    db = get_client()
    row = db.table("matches").select("odds").eq("id", match_id).single().execute().data
    if not row:
        return
    current = row.get("odds") or {}
    if not isinstance(current, dict):
        current = {}
    current["kalshi"] = odds_block
    # Promote flat keys (engine.js + frontend read these without source drilling)
    if odds_block.get("implied_prob_a") is not None:
        current["kalshi_prob_a"] = odds_block["implied_prob_a"]
    if odds_block.get("implied_prob_b") is not None:
        current["kalshi_prob_b"] = odds_block["implied_prob_b"]

    db.table("matches").update({"odds": current}).eq("id", match_id).execute()

    db.table("odds_history").insert(
        {
            "match_id": match_id,
            "slate_id": slate_id,
            "source": "kalshi",
            "market": "match_winner",
            "payload": odds_block,
        }
    ).execute()
