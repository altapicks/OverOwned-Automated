"""Discord webhook notifier.

Fire-and-forget. Never raises — we don't want a failed notification to crash
an ingestion run. Missing webhook URLs are logged once and then silently skipped.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


async def _post(webhook_url: str, payload: dict) -> None:
    if not webhook_url:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(webhook_url, json=payload)
            if r.status_code >= 400:
                logger.warning(
                    "Discord webhook returned %d: %s", r.status_code, r.text[:200]
                )
    except Exception as e:
        logger.warning("Discord webhook failed: %s", e)


async def notify_new_slate(
    sport: str,
    slate_date: str,
    slate_label: Optional[str],
    draft_group_id: int,
    player_count: int,
    match_count: int,
    lock_time: Optional[str],
    slate_type: str = "classic",
    is_fallback: bool = False,
) -> None:
    """Posted when the watcher detects a newly-published draft group."""
    s = get_settings()
    if not s.discord_webhook_slates:
        return

    if is_fallback:
        title = f"⚠️  {sport.upper()} Showdown (fallback — no Classic today)"
        color = 0xF59E0B  # amber
    elif slate_type == "showdown":
        title = f"🎯 New {sport.upper()} Showdown slate"
        color = 0xA55EEA  # purple
    else:
        title = f"🎾 New {sport.upper()} Classic slate"
        color = 0xF5C518  # OverOwned gold

    embed = {
        "title": title,
        "color": color,
        "fields": [
            {"name": "Date", "value": slate_date, "inline": True},
            {"name": "Label", "value": slate_label or "—", "inline": True},
            {"name": "DK Draft Group", "value": str(draft_group_id), "inline": True},
            {"name": "Players", "value": str(player_count), "inline": True},
            {"name": "Matches", "value": str(match_count), "inline": True},
            {"name": "Lock", "value": lock_time or "—", "inline": True},
        ],
        "footer": {"text": "OverOwned slate watcher"},
    }
    await _post(s.discord_webhook_slates, {"embeds": [embed]})


async def notify_unmatched(sport: str, source: str, raw_name: str, best_guess: str, score: float):
    """Posted when a name needs manual review."""
    s = get_settings()
    if not s.discord_webhook_slates:
        return
    embed = {
        "title": "⚠️  Unmatched player name",
        "color": 0xF59E0B,
        "fields": [
            {"name": "Sport", "value": sport, "inline": True},
            {"name": "Source", "value": source, "inline": True},
            {"name": "Raw name", "value": f"`{raw_name}`", "inline": False},
            {
                "name": "Best guess",
                "value": f"`{best_guess}` ({score:.0f}%)" if best_guess else "—",
                "inline": False,
            },
        ],
    }
    await _post(s.discord_webhook_slates, {"embeds": [embed]})


async def notify_error(job: str, error: str, context: Optional[dict] = None) -> None:
    """Posted when an ingestion job fails."""
    s = get_settings()
    webhook = s.discord_webhook_errors or s.discord_webhook_slates
    if not webhook:
        return
    ctx_str = ""
    if context:
        ctx_str = "\n".join(f"**{k}:** `{v}`" for k, v in context.items())
    embed = {
        "title": f"🚨 Ingestion error: {job}",
        "description": f"```{error[:500]}```\n{ctx_str}",
        "color": 0xEF4444,
    }
    await _post(webhook, {"embeds": [embed]})
