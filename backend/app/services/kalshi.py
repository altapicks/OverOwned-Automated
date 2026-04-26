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


TENNIS_MATCH_SERIES = ("KXATPMATCH", "KXWTAMATCH")


def _strip_accents(s: str) -> str:
    import unicodedata

    return "".join(
        c
        for c in unicodedata.normalize("NFKD", s or "")
        if not unicodedata.combining(c)
    )


def _last_name_key(name: str) -> str:
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


_private_key: Optional[RSAPrivateKey] = None


def _normalize_pem(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        s = s[1:-1].strip()
    s = s.replace("\\n", "\n").replace("\\r", "")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    if "\n" not in s and "-----BEGIN" in s and "-----END" in s:
        import re

        m = re.match(r"^(-----BEGIN [A-Z ]+-----)(.*?)(-----END [A-Z ]+-----)\s*$", s)
        if m:
            begin, body, end = m.group(1), m.group(2).strip(), m.group(3)
            chunks = [body[i : i + 64] for i in range(0, len(body), 64)]
            s = begin + "\n" + "\n".join(chunks) + "\n" + end + "\n"
    if not s.endswith("\n"):
        s += "\n"
    return s


def _load_private_key() -> Optional[RSAPrivateKey]:
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
            headline,
            str(e)[:200],
            len(preview_lines),
            first,
            last,
        )
        return None


def _sign_request(method: str, path: str, timestamp_ms: str) -> Optional[str]:
    key = _load_private_key()
    if key is None:
        return None
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
    s = get_settings()
    if not s.kalshi_api_base:
        return None
    url = s.kalshi_api_base.rstrip("/") + path
    headers = _auth_headers("GET", url)
    if not headers:
        return None
    try:
        r = await request_with_retry(
            "GET",
            url,
            headers=headers,
            params=params,
            breaker=_breaker_kalshi,
            max_retries=3,
        )
        _breaker_kalshi.record_success()
        return r.json()
    except httpx.HTTPStatusError as e:
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


_markets_cache: dict = {"data": None, "fetched_at": 0.0}
_MARKETS_TTL = 3600


async def list_tennis_markets() -> list[dict]:
    now = time.time()
    if (
        _markets_cache["data"] is not None
        and (now - _markets_cache["fetched_at"]) < _MARKETS_TTL
    ):
        return _markets_cache["data"]

    all_markets = []
    per_series_counts = {}
    for series_ticker in TENNIS_MATCH_SERIES:
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
    if not title:
        return None
    t = title.strip()
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
    """One ingestion cycle for Kalshi tennis."""
    s = get_settings()
    if not s.kalshi_key_id or not s.kalshi_private_key:
        return {"skipped": "no_kalshi_creds"}
    if not s.kalshi_api_base:
        return {"skipped": "no_kalshi_base"}

    markets = await list_tennis_markets()
    if not markets:
        return {"skipped": "no_markets", "markets_found": 0}

    events: dict[str, list[dict]] = {}
    for mkt in markets:
        et = mkt.get("event_ticker") or ""
        if not et:
            continue
        events.setdefault(et, []).append(mkt)

    db = get_client()
    normalizer = PlayerNormalizer(sport="tennis")

    candidate_matches = (
        db.table("matches")
        .select(
            "id, slate_id, player_a_id, player_b_id,"
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

    player_to_match: dict[str, dict] = {}
    for m in candidate_matches:
        for side_key, player_field in [("a", "player_a"), ("b", "player_b")]:
            p = m.get(player_field) or {}
            cid = p.get("canonical_id")
            if cid:
                player_to_match[cid] = {
                    "match_id": m["id"],
                    "slate_id": m["slate_id"],
                    "side": side_key,
                }

    matched = 0
    skipped_bad_event = 0
    skipped_no_match = 0
    skipped_low_confidence = 0
    unmatched_samples: list[str] = []

    for event_ticker, mkts in events.items():
        if len(mkts) != 2:
            skipped_bad_event += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(
                    f"event_has_{len(mkts)}_markets: {event_ticker}"
                )
            continue

        side0, side1 = mkts[0], mkts[1]
        name0 = (side0.get("yes_sub_title") or "").strip()
        name1 = (side1.get("yes_sub_title") or "").strip()
        if not name0 or not name1:
            skipped_bad_event += 1
            continue

        r0 = normalizer.resolve(name0, source="kalshi", create_if_missing=False)
        r1 = normalizer.resolve(name1, source="kalshi", create_if_missing=False)

        # ── DURABLE FIX FOR RINDERKNECH→FILS CORRUPTION ──────────────
        # Refuse to write Kalshi odds onto a match unless BOTH names
        # auto-resolved with high confidence. Kalshi sometimes uses
        # surname-only or last-name-first market subtitles that the
        # fuzzy matcher would otherwise collapse onto a wrong canonical
        # (e.g. "Rinderknech" → arthur_fils because they share a first
        # name in our players table).
        if not r0.auto_resolved or not r1.auto_resolved:
            skipped_low_confidence += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(
                    f"low_confidence: {name0!r}→{r0.canonical_id}/{r0.score} "
                    f"vs {name1!r}→{r1.canonical_id}/{r1.score}"
                )
            continue

        cm0 = player_to_match.get(r0.canonical_id) if r0.canonical_id else None
        cm1 = player_to_match.get(r1.canonical_id) if r1.canonical_id else None

        if (
            not cm0
            or not cm1
            or cm0["match_id"] != cm1["match_id"]
            or cm0["side"] == cm1["side"]
        ):
            skipped_no_match += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(
                    f"no_dk_match: {name0!r} vs {name1!r} "
                    f"(resolved: {r0.canonical_id}/{r1.canonical_id})"
                )
            continue

        prob0 = _dollar_to_prob(
            side0.get("yes_bid_dollars"), side0.get("yes_ask_dollars")
        )
        prob1 = _dollar_to_prob(
            side1.get("yes_bid_dollars"), side1.get("yes_ask_dollars")
        )

        if cm0["side"] == "a":
            implied_a, implied_b = prob0, prob1
        else:
            implied_a, implied_b = prob1, prob0

        odds_block = {
            "implied_prob_a": implied_a,
            "implied_prob_b": implied_b,
            "event_ticker": event_ticker,
            "ticker_a": side0["ticker"] if cm0["side"] == "a" else side1["ticker"],
            "ticker_b": side1["ticker"] if cm0["side"] == "a" else side0["ticker"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "raw": {
                "a": {
                    "yes_sub_title": name0 if cm0["side"] == "a" else name1,
                    "yes_bid_dollars": side0.get("yes_bid_dollars")
                    if cm0["side"] == "a"
                    else side1.get("yes_bid_dollars"),
                    "yes_ask_dollars": side0.get("yes_ask_dollars")
                    if cm0["side"] == "a"
                    else side1.get("yes_ask_dollars"),
                },
                "b": {
                    "yes_sub_title": name1 if cm0["side"] == "a" else name0,
                    "yes_bid_dollars": side1.get("yes_bid_dollars")
                    if cm0["side"] == "a"
                    else side0.get("yes_bid_dollars"),
                    "yes_ask_dollars": side1.get("yes_ask_dollars")
                    if cm0["side"] == "a"
                    else side0.get("yes_ask_dollars"),
                },
            },
        }
        await _write_kalshi_odds(cm0["match_id"], cm0["slate_id"], odds_block)
        matched += 1

    result = {
        "markets_found": len(markets),
        "events_found": len(events),
        "matched": matched,
        "skipped_bad_event": skipped_bad_event,
        "skipped_no_match": skipped_no_match,
        "skipped_low_confidence": skipped_low_confidence,
    }
    if unmatched_samples and matched == 0:
        logger.info("Kalshi unmatched samples: %s", unmatched_samples)
    return result


def _dollar_to_prob(bid: Optional[str], ask: Optional[str]) -> Optional[float]:
    def _parse(v):
        if v is None:
            return None
        try:
            n = float(v)
            if 0.0 <= n <= 1.0:
                return n
        except (TypeError, ValueError):
            pass
        return None

    b = _parse(bid)
    a = _parse(ask)
    if b is not None and a is not None:
        return (b + a) / 2.0
    return b if b is not None else a


async def _write_kalshi_odds(match_id: str, slate_id: str, odds_block: dict):
    """Merge kalshi odds into matches.odds.kalshi, append to odds_history."""
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
    current["kalshi"] = odds_block
    if odds_block.get("implied_prob_a") is not None:
        current["kalshi_prob_a"] = odds_block["implied_prob_a"]
    if odds_block.get("implied_prob_b") is not None:
        current["kalshi_prob_b"] = odds_block["implied_prob_b"]

    db.table("matches").update({"odds": current}).eq("id", match_id).execute()

    opening = row.get("opening_odds") or {}
    if not isinstance(opening, dict):
        opening = {}
    if "kalshi" not in opening:
        opening["kalshi"] = odds_block
        db.table("matches").update({"opening_odds": opening}).eq(
            "id", match_id
        ).execute()

    db.table("odds_history").insert(
        {
            "match_id": match_id,
            "slate_id": slate_id,
            "source": "kalshi",
            "market": "match_winner",
            "payload": odds_block,
        }
    ).execute()
