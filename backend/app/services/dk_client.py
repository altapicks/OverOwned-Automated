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

v6.2 NOTE — DK's 2026 lobby payload renamed several fields. We now key off:
  - ContestTypeId (int; 106 = Classic, 372 = Showdown/alt-format)
  - DraftGroupTag (e.g. "Featured", or absent for Short Slate)
  - GameTypeId (int; mirrors ContestTypeId for tennis)
The legacy ContestType / GameTypeName string fields no longer exist.
SalaryCap is also no longer in the lobby — we default to 50000.

v6.3 FIX — DK's draftables payload returns BOTH `playerId` (global 6-digit
profile id, e.g. 694327) AND `draftableId` (per-slate 8-digit id like
42760136). DK's contest-entry CSV upload only accepts the per-slate
`draftableId`. We now persist `draftableId` as the canonical dk_player_id
(matches the salary CSV the user downloads from DK's contest page) so
exported lineups upload cleanly. The 6-digit `playerId` is preserved on the
draftable for cross-slate identity if ever needed (debug only).
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

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SPORT_CODE_MAP = {
    "TEN": "tennis",
    "NBA": "nba",
    "MMA": "mma",
    "NFL": "nfl",
}

CONTEST_TYPE_ID_MAP = {
    106: "Classic",
    372: "Showdown",
    201: "Classic",
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
                raise httpx.TransportError(f"DK {r.status_code} at {url}")
            r.raise_for_status()
            return r.json()


def _resolve_contest_type(dg: dict) -> str:
    ctid = dg.get("ContestTypeId")
    if isinstance(ctid, int) and ctid in CONTEST_TYPE_ID_MAP:
        return CONTEST_TYPE_ID_MAP[ctid]
    legacy = (dg.get("ContestType") or dg.get("GameTypeName") or "").strip()
    if legacy:
        return legacy
    return "Classic"


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
            contest_type = _resolve_contest_type(dg)
            tag = (dg.get("DraftGroupTag") or "").strip()
            suffix = (dg.get("ContestStartTimeSuffix") or "").strip()
            slate_label = " ".join(s for s in (tag, suffix) if s) or None
            results.append(
                DKDraftGroup(
                    draft_group_id=dgid,
                    sport=SPORT_CODE_MAP.get(sport_code, sport_code.lower()),
                    contest_type=contest_type,
                    slate_label=slate_label,
                    lock_time=_parse_dk_datetime(dg.get("StartDate") or dg.get("StartDateEst")),
                    salary_cap=int(dg.get("SalaryCap") or 50000),
                )
            )
        logger.info(
            "DK lobby: sport=%s draft_groups=%d (raw=%d)",
            sport_code,
            len(results),
            len(draft_groups),
        )
        return results

    async def list_draft_groups_raw(self, sport_code: str) -> list[dict]:
        data = await _get_json(self._client, LOBBY_URL, params={"sport": sport_code})
        return list(data.get("DraftGroups") or [])

    async def get_draftables(self, draft_group_id: int) -> tuple[list[DKDraftable], list[dict]]:
        """Return (draftables, competitions) for a draft group.

        v6.3 FIX: persist `draftableId` (per-slate 8-digit id, e.g. 42760136)
        as `dk_player_id`. This is the id format DK's contest-entry CSV upload
        accepts. The salary export the user downloads from DK uses the same
        8-digit id under the "ID" column. Falls back to `playerId` only when
        `draftableId` is somehow missing (defensive — should never happen).
        """
        url = DRAFTABLES_URL.format(dgid=draft_group_id)
        data = await _get_json(self._client, url)
        raw_draftables = data.get("draftables") or []
        competitions = data.get("competitions") or []
        results: list[DKDraftable] = []
        for d in raw_draftables:
            try:
                # Prefer draftableId (8-digit, slate-scoped, CSV-upload-compatible).
                # Fall back to playerId only if draftableId is missing.
                raw_id = d.get("draftableId")
                if raw_id is None:
                    raw_id = d.get("playerId")
                if raw_id is None:
                    continue
                dk_player_id = int(raw_id)

                comp = d.get("competition") or {}
                results.append(
                    DKDraftable(
                        dk_player_id=dk_player_id,
                        display_name=d.get("displayName") or "",
                        salary=int(d.get("salary") or 0),
                        roster_position=d.get("rosterSlotId") and str(d.get("position") or "P") or str(d.get("position") or "P"),
                        avg_ppg=float(d["draftStatAttributes"][0]["value"])
                            if d.get("draftStatAttributes") and d["draftStatAttributes"][0].get("value") not in (None, "-")
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
