"""DraftKings API client.

DK exposes two relevant JSON endpoints:
  1. https://www.draftkings.com/lobby/getcontests?sport=TEN
       → { DraftGroups: [...], Contests: [...] }
       Lists active draft groups for a sport. One DraftGroup per slate.
  2. https://api.draftkings.com/draftgroups/v1/draftgroups/{id}/draftables
       → { draftables: [...], competitions: [...] }
       Full player pool for a draft group.

Neither requires auth. Both are hit by community scrapers at low frequency
without issue. We respect a sensible poll interval (15 min default) and add
a real UA string to avoid looking like a bot to DK's edge.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import httpx
from dateutil import parser as dateparser
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.models import DKDraftable, DKDraftGroup

logger = logging.getLogger(__name__)

LOBBY_URL = "https://www.draftkings.com/lobby/getcontests"
DRAFTABLES_URL = "https://api.draftkings.com/draftgroups/v1/draftgroups/{dgid}/draftables"

# DK's edge occasionally returns 403 for generic UAs. A real Chrome UA string
# gets through reliably.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Sport codes DK uses internally
SPORT_CODE_MAP = {
    "TEN": "tennis",
    "NBA": "nba",
    "MMA": "mma",
    "NFL": "nfl",
}


class DKError(Exception):
    """Raised when DK returns something we can't parse or the edge refuses us."""


def _parse_dk_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return dateparser.isoparse(s)
    except (ValueError, TypeError):
        return None


async def _get_json(client: httpx.AsyncClient, url: str, params: Optional[dict] = None) -> Any:
    """GET with retry on transient failures. 4xx are not retried."""
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        reraise=True,
    ):
        with attempt:
            r = await client.get(url, params=params, timeout=20.0)
            if r.status_code == 403:
                raise DKError(f"DK refused request (403): {url}")
            if r.status_code >= 500:
                # Retryable — raise a transient error type
                raise httpx.TransportError(f"DK {r.status_code} at {url}")
            r.raise_for_status()
            return r.json()


class DraftKingsClient:
    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self._own_client = http_client is None
        self._client = http_client or httpx.AsyncClient(headers=DEFAULT_HEADERS)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def close(self):
        if self._own_client:
            await self._client.aclose()

    async def list_draft_groups(self, sport_code: str) -> list[DKDraftGroup]:
        """Return active draft groups for a sport (TEN, NBA, MMA, ...)."""
        data = await _get_json(self._client, LOBBY_URL, params={"sport": sport_code})
        draft_groups = data.get("DraftGroups") or []

        results: list[DKDraftGroup] = []
        seen_ids: set[int] = set()
        for dg in draft_groups:
            try:
                dgid = int(dg["DraftGroupId"])
            except (KeyError, ValueError, TypeError):
                continue
            if dgid in seen_ids:
                continue
            seen_ids.add(dgid)

            contest_type = dg.get("ContestType") or dg.get("GameTypeName") or ""
            is_showdown = "showdown" in contest_type.lower() or "captain" in contest_type.lower()

            results.append(
                DKDraftGroup(
                    draft_group_id=dgid,
                    sport=SPORT_CODE_MAP.get(sport_code, sport_code.lower()),
                    contest_type="Showdown" if is_showdown else "Classic",
                    slate_label=dg.get("DraftGroupTag") or dg.get("ContestStartTimeSuffix"),
                    lock_time=_parse_dk_datetime(dg.get("StartDate") or dg.get("StartDateEst")),
                    salary_cap=int(dg.get("SalaryCap") or 50000),
                )
            )

        logger.info("DK lobby: sport=%s draft_groups=%d", sport_code, len(results))
        return results

    async def get_draftables(self, draft_group_id: int) -> tuple[list[DKDraftable], list[dict]]:
        """Return (draftables, competitions) for a draft group.

        Competitions are the match/game container — tennis uses them as the
        "Player A vs Player B" wrapper we use to pair players into matches.
        """
        url = DRAFTABLES_URL.format(dgid=draft_group_id)
        data = await _get_json(self._client, url)

        raw_draftables = data.get("draftables") or []
        competitions = data.get("competitions") or []

        results: list[DKDraftable] = []
        for d in raw_draftables:
            try:
                comp = d.get("competition") or {}
                results.append(
                    DKDraftable(
                        dk_player_id=int(d["playerId"]),
                        display_name=d.get("displayName") or "",
                        salary=int(d.get("salary") or 0),
                        roster_position=d.get("rosterSlotId")
                        and str(d.get("position") or "P")
                        or str(d.get("position") or "P"),
                        avg_ppg=float(d["draftStatAttributes"][0]["value"])
                        if d.get("draftStatAttributes")
                        and d["draftStatAttributes"][0].get("value") not in (None, "-")
                        else None,
                        competition_id=int(comp["competitionId"]) if comp.get("competitionId") else None,
                        competition_name=comp.get("name"),
                        start_time=_parse_dk_datetime(comp.get("startTime")),
                    )
                )
            except (KeyError, ValueError, TypeError) as e:
                logger.debug("Skipping malformed draftable: %s", e)
                continue

        logger.info(
            "DK draftables: dgid=%d draftables=%d competitions=%d",
            draft_group_id,
            len(results),
            len(competitions),
        )
        return results, competitions
