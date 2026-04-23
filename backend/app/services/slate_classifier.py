"""Slate type classifier.

Tennis DFS on DraftKings comes in a few flavors. We want Classic by default
(full-day ATP/WTA main draw), fall back to Showdown when no Classic exists
(Grand Slam finals days, isolated single-match events), and reject everything
else (Tiers, Pick'Em, various promo formats).

Detection strategy, in order of confidence:
  1. Keyword match on DK's contest_type / slate_label
  2. Structural check: roster_position set (CPT/FLEX present = Showdown)
  3. Default: Classic, unless contradicted by the above

We explicitly do NOT use game-count heuristics (e.g. "NBA Classic has 4+
games") because rain-delayed or schedule-oddity days produce legitimate
Classics with fewer games, and we'd misclassify them.
"""
from __future__ import annotations

import logging
from enum import Enum
from typing import Iterable

from app.models import DKDraftable, DKDraftGroup

logger = logging.getLogger(__name__)


class SlateType(str, Enum):
    CLASSIC = "classic"
    SHOWDOWN = "showdown"
    OTHER = "other"


# Keywords that indicate a NON-Classic slate. Match case-insensitively
# against the combined contest_type + slate_label string.
_SHOWDOWN_KEYWORDS = frozenset([
    "showdown",
    "single game",
    "single-game",
    "singlegame",
    "captain mode",
    "captain",
    "cpt mode",
])

_OTHER_KEYWORDS = frozenset([
    "tiers",
    "pick",       # pick'em, pick6, pickem
    "pick6",
    "pick'em",
    "satellite",
    "qualifier",
    "express",
    "turbo",
    "afternoon",
    "primetime",
    "madness",    # promotional formats
    "best ball",
    # Reduced-game variants. DK tennis uses contest_type='Classic' for both
    # the main slate AND the reduced-game variants — the differentiator is
    # slate_label (e.g. "(TEN Short Slate)"). These are filtered via label.
    "short slate",
    "short",
])


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    t = text.lower()
    return any(kw in t for kw in keywords)


def classify_slate(
    draft_group: DKDraftGroup,
    draftables: list[DKDraftable],
) -> SlateType:
    """Classify a DK draft group.

    Returns SlateType.CLASSIC for standard full-field main slates.
    Returns SlateType.SHOWDOWN for single-game / captain-mode slates.
    Returns SlateType.OTHER for Tiers / Pick'Em / Express / promotional formats.
    """
    contest_type = (draft_group.contest_type or "").strip()
    slate_label = (draft_group.slate_label or "").strip()
    combined = f"{contest_type} {slate_label}".strip()

    # ── Layer 1: explicit "other" formats ─────────────────────────
    # These are rejected outright — user never wants them in the DB.
    if _contains_any(combined, _OTHER_KEYWORDS):
        logger.info(
            "Classified as OTHER (keyword match): dgid=%d contest_type=%r label=%r",
            draft_group.draft_group_id,
            contest_type,
            slate_label,
        )
        return SlateType.OTHER

    # ── Layer 2: explicit "showdown" keywords ──────────────────────
    if _contains_any(combined, _SHOWDOWN_KEYWORDS):
        return SlateType.SHOWDOWN

    # ── Layer 3: structural — roster positions ────────────────────
    # Showdown slates have CPT (captain) and FLEX positions. Classic has
    # a single position per sport (tennis: "P"; NBA: "G"/"F"/"C"/etc.).
    # Tennis Classic typically uses "P" for all players.
    positions = {d.roster_position for d in draftables if d.roster_position}
    if {"CPT", "FLEX"}.issubset(positions) or "CPT" in positions:
        return SlateType.SHOWDOWN

    # ── Layer 4: default to Classic ──────────────────────────────
    # If nothing above triggered, it's a standard full-field slate.
    return SlateType.CLASSIC


def pick_slates_to_ingest(
    classified: list[tuple[DKDraftGroup, list[DKDraftable], SlateType]],
    allowed_types: set[SlateType],
    fallback_to_showdown: bool = True,
) -> list[tuple[DKDraftGroup, list[DKDraftable], SlateType, bool]]:
    """Given all draft groups for a sport and their classifications, decide
    which ones to actually ingest.

    Returns a list of (draft_group, draftables, slate_type, is_fallback) tuples.

    Rules:
      - Ingest all slates matching allowed_types
      - If no Classic exists AND fallback_to_showdown AND Showdown exists:
          ingest the Showdown(s) with is_fallback=True
      - OTHER is never ingested
    """
    results: list[tuple] = []

    # Classic-first path: if any Classic exists and CLASSIC is allowed, ingest them
    classics = [(dg, d, t) for dg, d, t in classified if t == SlateType.CLASSIC]
    if classics and SlateType.CLASSIC in allowed_types:
        for dg, d, t in classics:
            results.append((dg, d, t, False))

    # Showdown path:
    #   - If SHOWDOWN is explicitly allowed, ingest all showdowns
    #   - OR if no Classic was ingested and fallback_to_showdown is on, ingest as fallback
    showdowns = [(dg, d, t) for dg, d, t in classified if t == SlateType.SHOWDOWN]
    if showdowns:
        if SlateType.SHOWDOWN in allowed_types:
            for dg, d, t in showdowns:
                results.append((dg, d, t, False))
        elif not classics and fallback_to_showdown:
            for dg, d, t in showdowns:
                results.append((dg, d, t, True))
                logger.warning(
                    "Ingesting Showdown as fallback (no Classic today): dgid=%d",
                    dg.draft_group_id,
                )

    # OTHER slates never get ingested
    return results
