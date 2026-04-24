"""Shared async HTTP client + simple circuit breaker.

Used by odds_api.py and kalshi.py. Prevents every service from creating
its own connection pool and duplicating retry logic.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


@dataclass
class CircuitBreaker:
    """Minimal circuit breaker. Opens after N consecutive failures,
    stays open for cooldown_seconds, then half-opens on next call."""
    failure_threshold: int = 5
    cooldown_seconds: int = 600  # 10 min
    _failures: int = 0
    _opened_at: Optional[float] = None
    _name: str = "default"
    _on_open: Optional[Any] = field(default=None, repr=False)  # callable(name) -> awaitable

    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.time() - self._opened_at > self.cooldown_seconds:
            logger.info("Circuit %s half-opening after cooldown", self._name)
            self._opened_at = None
            self._failures = 0
            return False
        return True

    def record_success(self):
        self._failures = 0

    async def record_failure(self):
        self._failures += 1
        if self._failures >= self.failure_threshold and self._opened_at is None:
            self._opened_at = time.time()
            logger.error(
                "Circuit %s OPEN after %d consecutive failures",
                self._name,
                self._failures,
            )
            if self._on_open:
                try:
                    await self._on_open(self._name)
                except Exception as e:
                    logger.warning("Circuit breaker on_open callback failed: %s", e)


_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    """Singleton async HTTP client. Reused across services to share
    connection pool, reducing handshake overhead."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            headers={"User-Agent": "OverOwned/0.1 (+https://overowned.io)"},
        )
    return _client


async def close_http_client():
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None


async def request_with_retry(
    method: str,
    url: str,
    *,
    breaker: Optional[CircuitBreaker] = None,
    max_retries: int = 3,
    retry_on_5xx: bool = True,
    **kwargs,
) -> httpx.Response:
    """Make an HTTP request with exponential backoff retry and optional
    circuit-breaker integration.

    Raises on 4xx (not retried — likely auth/input issue).
    Retries on 5xx and transport errors. Opens breaker after threshold.
    """
    if breaker and breaker.is_open():
        raise RuntimeError(f"Circuit breaker '{breaker._name}' is OPEN")

    client = get_http_client()
    retryable = (httpx.TransportError, httpx.TimeoutException)
    if retry_on_5xx:
        retryable = retryable + (httpx.HTTPStatusError,)

    last_response: Optional[httpx.Response] = None
    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(retryable),
            reraise=True,
        ):
            with attempt:
                r = await client.request(method, url, **kwargs)
                last_response = r
                if r.status_code >= 500 and retry_on_5xx:
                    raise httpx.HTTPStatusError(
                        f"Server error {r.status_code}", request=r.request, response=r
                    )
                # 4xx not retried — raise so caller handles
                if r.status_code >= 400:
                    r.raise_for_status()
                return r
    except Exception as e:
        if breaker:
            await breaker.record_failure()
        raise
    # Unreachable
    return last_response  # type: ignore
