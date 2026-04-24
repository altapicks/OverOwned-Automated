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

# Module-level cache so we don't re-parse the PEM on every request.
_private_key: Optional[RSAPrivateKey] = None


def _load_private_key() -> Optional[RSAPrivateKey]:
    """Parse KALSHI_PRIVATE_KEY env var as PEM."""
    global _private_key
    if _private_key is not None:
        return _private_key
    s = get_settings()
    pem = s.kalshi_private_key.strip() if s.kalshi_private_key else ""
    if not pem:
        return None
    try:
        key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
        if not isinstance(key, RSAPrivateKey):
            logger.error("Kalshi private key is not RSA")
            return None
        _private_key = key
        return key
    except Exception as e:
        logger.error("Failed to parse Kalshi private key: %s", e)
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
    """Find all tennis-related markets. Discover series ticker patterns
    at runtime since Kalshi's exact tennis taxonomy changes.

    Strategy: hit /series, filter for tennis-keyword matches, then for
    each matching series fetch /markets?series_ticker=<ticker>.
    """
    now = time.time()
    if _markets_cache["data"] is not None and (now - _markets_cache["fetched_at"]) < _MARKETS_TTL:
        return _markets_cache["data"]

    # Step 1: discover series
    series_resp = await _get("/series", params={"limit": 100})
    if not series_resp:
        return []
    series = series_resp.get("series") or []
    tennis_series = [
        s for s in series
        if any(
            kw in (s.get("title") or "").lower() or kw in (s.get("ticker") or "").lower()
            for kw in ("tennis", "atp", "wta", "roland garros", "wimbledon", "us open", "australian open")
        )
    ]
    if not tennis_series:
        logger.info("Kalshi: no tennis series found in /series response")
        _markets_cache.update(data=[], fetched_at=now)
        return []

    logger.info(
        "Kalshi tennis series discovered: %s",
        [s.get("ticker") for s in tennis_series],
    )

    # Step 2: fetch markets for each tennis series
    all_markets = []
    for ts in tennis_series:
        ticker = ts.get("ticker")
        if not ticker:
            continue
        m_resp = await _get("/markets", params={"series_ticker": ticker, "status": "open", "limit": 200})
        if not m_resp:
            continue
        all_markets.extend(m_resp.get("markets") or [])

    _markets_cache.update(data=all_markets, fetched_at=now)
    logger.info("Kalshi: fetched %d open tennis markets across %d series",
                len(all_markets), len(tennis_series))
    return all_markets


def _parse_player_names_from_title(title: str) -> Optional[tuple[str, str]]:
    """Extract 'Player A' and 'Player B' from a market title.

    Kalshi titles vary. Common patterns:
      "Who will win Sinner vs Alcaraz?"
      "Sinner vs Alcaraz — winner"
      "Winner of Sinner-Alcaraz"
    """
    if not title:
        return None
    t = title.lower().replace(" vs. ", " vs ").replace(" - ", " vs ").replace("—", "vs")
    if " vs " not in t:
        return None
    # Find the segment containing " vs "
    for sep in (" vs ",):
        if sep in t:
            parts = t.split(sep)
            if len(parts) == 2:
                # Strip leading preamble words like "who will win", "winner of", etc.
                left = parts[0].replace("who will win", "").replace("winner of", "").replace("winner", "").strip(" ?.:")
                right = parts[1].split("?")[0].split(".")[0].strip(" ,:")
                if left and right:
                    return left.title(), right.title()
    return None


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

    db = get_client()
    normalizer = PlayerNormalizer(sport="tennis")

    candidate_matches = (
        db.table("matches")
        .select("id, slate_id, player_a_id, player_b_id, start_time, players!matches_player_a_id_fkey(display_name), slates!inner(sport, status, contest_type, is_fallback)")
        .eq("slates.sport", "tennis")
        .eq("slates.status", "active")
        .eq("slates.contest_type", "classic")
        .eq("slates.is_fallback", False)
        .execute()
        .data
        or []
    )

    matched = 0
    for mkt in markets:
        title = mkt.get("title") or mkt.get("subtitle") or ""
        pair = _parse_player_names_from_title(title)
        if not pair:
            continue
        a_name, b_name = pair
        a = normalizer.resolve(a_name, source="kalshi", create_if_missing=False)
        b = normalizer.resolve(b_name, source="kalshi", create_if_missing=False)
        if not a.canonical_id or not b.canonical_id:
            continue

        for m in candidate_matches:
            if {m["player_a_id"], m["player_b_id"]} == {a.canonical_id, b.canonical_id}:
                swap = m["player_a_id"] == b.canonical_id
                yes_price = mkt.get("yes_bid") or mkt.get("last_price")
                no_price = mkt.get("no_bid")
                implied_a = None
                if yes_price is not None:
                    try:
                        # Kalshi prices are cents (0-100). Convert to probability.
                        implied = float(yes_price) / 100.0
                        implied_a = 1 - implied if swap else implied
                    except (TypeError, ValueError):
                        pass

                odds_block = {
                    "implied_prob_a": implied_a,
                    "market_ticker": mkt.get("ticker"),
                    "last_price": yes_price,
                    "no_price": no_price,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "raw": {"title": title, "status": mkt.get("status")},
                }

                await _write_kalshi_odds(m["id"], m["slate_id"], odds_block)
                matched += 1
                break

    return {"markets_found": len(markets), "matched": matched}


async def _write_kalshi_odds(match_id: str, slate_id: str, odds_block: dict):
    """Merge kalshi odds into matches.odds.kalshi, append to odds_history."""
    db = get_client()
    row = db.table("matches").select("odds").eq("id", match_id).single().execute().data
    if not row:
        return
    current = row.get("odds") or {}
    if not isinstance(current, dict):
        current = {}
    current["kalshi"] = odds_block
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
