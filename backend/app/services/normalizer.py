"""Player name normalization.

Every source uses a slightly different name convention:
  DK           'Sinner J.' or 'Jannik Sinner' (varies by sport)
  PrizePicks   'Jannik Sinner'
  Kalshi       'Sinner' (often surname only)
  Tennis-data  'Sinner J.'
  ATP site     'Jannik SINNER'

We resolve all of these to one canonical_id ('jannik_sinner') in the players
table. This module owns that resolution.

Strategy:
  1. Exact alias match against existing aliases (fast path)
  2. Fuzzy match above threshold → auto-resolve, record the alias
  3. Below threshold → add to unmatched_names queue, notify Discord
"""
from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

from rapidfuzz import fuzz, process

from app.db import get_client

logger = logging.getLogger(__name__)

FUZZY_AUTO_THRESHOLD = 88  # score >= this = auto-resolve
FUZZY_SUGGEST_THRESHOLD = 70  # score >= this = suggest as best_guess but queue for review


def canonicalize(name: str) -> str:
    """Build a canonical_id from a display name.

    'Jannik Sinner' → 'jannik_sinner'
    'Carlos Alcaraz Garfia' → 'carlos_alcaraz_garfia'
    'Núñez, Aníbal' → 'nunez_anibal'

    Deterministic, slug-safe, used as primary key in players table.
    """
    # Strip diacritics
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase, normalize separators
    stripped = stripped.lower().strip()
    # Handle "Last, First" format — reverse it
    if "," in stripped:
        parts = [p.strip() for p in stripped.split(",", 1)]
        if len(parts) == 2:
            stripped = f"{parts[1]} {parts[0]}"
    # Remove anything non-alphanumeric-space-hyphen
    stripped = re.sub(r"[^a-z0-9\s\-]", "", stripped)
    # Collapse whitespace
    stripped = re.sub(r"\s+", "_", stripped.strip())
    return stripped


def normalize_for_match(name: str) -> str:
    """Normalize a name for fuzzy matching (looser than canonicalize)."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


@dataclass
class MatchResult:
    canonical_id: Optional[str]
    score: float
    auto_resolved: bool
    was_new: bool = False
    display_name: str = ""


class PlayerNormalizer:
    """In-process cache of the players table. Rebuilt on first use and
    refreshed when we add new players."""

    def __init__(self, sport: str):
        self.sport = sport
        self._loaded = False
        # canonical_id → display_name
        self._canonicals: dict[str, str] = {}
        # All known name variants → canonical_id, for fast exact match
        self._alias_lookup: dict[str, str] = {}
        # Normalized variants → canonical_id, for fuzzy match
        self._normalized_lookup: dict[str, str] = {}

    def _ensure_loaded(self):
        if self._loaded:
            return
        db = get_client()
        rows = db.table("players").select("*").eq("sport", self.sport).execute().data or []
        for row in rows:
            cid = row["canonical_id"]
            self._canonicals[cid] = row["display_name"]
            self._alias_lookup[row["display_name"]] = cid
            self._normalized_lookup[normalize_for_match(row["display_name"])] = cid
            aliases = row.get("aliases") or {}
            for _source, alias in aliases.items():
                if isinstance(alias, str):
                    self._alias_lookup[alias] = cid
                    self._normalized_lookup[normalize_for_match(alias)] = cid
                elif isinstance(alias, list):
                    for a in alias:
                        self._alias_lookup[a] = cid
                        self._normalized_lookup[normalize_for_match(a)] = cid
        self._loaded = True
        logger.info("Loaded %d %s players into normalizer", len(self._canonicals), self.sport)

    def resolve(
        self,
        raw_name: str,
        source: str,
        context: Optional[dict] = None,
        create_if_missing: bool = True,
    ) -> MatchResult:
        """Resolve a raw name to a canonical_id.

        create_if_missing: if True and no match found, create a new player row
        using the raw name as both display_name and seed for canonical_id.
        Set to False when you want to be strict (e.g. Kalshi integration
        shouldn't invent players — they need to exist from DK first).
        """
        self._ensure_loaded()
        raw_name = raw_name.strip()
        if not raw_name:
            return MatchResult(None, 0, False)

        # Fast path: exact alias hit
        if raw_name in self._alias_lookup:
            cid = self._alias_lookup[raw_name]
            return MatchResult(cid, 100, True, display_name=self._canonicals[cid])

        # Normalized exact hit
        norm = normalize_for_match(raw_name)
        if norm in self._normalized_lookup:
            cid = self._normalized_lookup[norm]
            self._record_alias(cid, source, raw_name)
            return MatchResult(cid, 100, True, display_name=self._canonicals[cid])

        # Fuzzy match
        candidates = list(self._normalized_lookup.keys())
        if candidates:
            # token_set_ratio handles reordered parts ("Sinner J." vs "Jannik Sinner")
            best = process.extractOne(
                norm, candidates, scorer=fuzz.token_set_ratio, score_cutoff=FUZZY_SUGGEST_THRESHOLD
            )
            if best:
                matched_norm, score, _ = best
                cid = self._normalized_lookup[matched_norm]
                if score >= FUZZY_AUTO_THRESHOLD:
                    self._record_alias(cid, source, raw_name)
                    return MatchResult(
                        cid, score, True, display_name=self._canonicals[cid]
                    )
                # Below auto threshold — queue for review with best guess
                self._queue_unmatched(raw_name, source, context, cid, score)
                return MatchResult(cid, score, False, display_name=self._canonicals[cid])

        # Nothing close. Either create or queue.
        if create_if_missing:
            cid = self._create_player(raw_name, source)
            return MatchResult(cid, 0, True, was_new=True, display_name=raw_name)
        self._queue_unmatched(raw_name, source, context, None, 0)
        return MatchResult(None, 0, False)

    def _create_player(self, display_name: str, source: str) -> str:
        """Create a new player row."""
        base_cid = canonicalize(display_name)
        cid = base_cid
        db = get_client()
        # Handle collisions: jannik_sinner, jannik_sinner_2, ...
        suffix = 2
        while cid in self._canonicals:
            cid = f"{base_cid}_{suffix}"
            suffix += 1

        db.table("players").insert(
            {
                "canonical_id": cid,
                "display_name": display_name,
                "sport": self.sport,
                "aliases": {source: display_name},
            }
        ).execute()

        # Update caches
        self._canonicals[cid] = display_name
        self._alias_lookup[display_name] = cid
        self._normalized_lookup[normalize_for_match(display_name)] = cid

        logger.info("Created new %s player: %s (%s)", self.sport, display_name, cid)
        return cid

    def _record_alias(self, canonical_id: str, source: str, raw_name: str):
        """Add a new alias to an existing player."""
        if raw_name in self._alias_lookup:
            return  # already known
        db = get_client()
        # Merge into aliases JSONB
        row = (
            db.table("players")
            .select("aliases")
            .eq("canonical_id", canonical_id)
            .single()
            .execute()
        )
        aliases = (row.data or {}).get("aliases") or {}
        existing = aliases.get(source)
        if existing is None:
            aliases[source] = raw_name
        elif isinstance(existing, str) and existing != raw_name:
            aliases[source] = [existing, raw_name]
        elif isinstance(existing, list) and raw_name not in existing:
            existing.append(raw_name)
            aliases[source] = existing
        else:
            return  # no change

        db.table("players").update({"aliases": aliases}).eq(
            "canonical_id", canonical_id
        ).execute()
        self._alias_lookup[raw_name] = canonical_id
        self._normalized_lookup[normalize_for_match(raw_name)] = canonical_id

    def _queue_unmatched(
        self,
        raw_name: str,
        source: str,
        context: Optional[dict],
        best_guess: Optional[str],
        score: float,
    ):
        """Add to the unmatched queue for manual review."""
        db = get_client()
        try:
            db.table("unmatched_names").upsert(
                {
                    "source": source,
                    "sport": self.sport,
                    "raw_name": raw_name,
                    "context": context or {},
                    "best_guess_id": best_guess,
                    "best_guess_score": score,
                },
                on_conflict="source,sport,raw_name",
            ).execute()
        except Exception as e:
            logger.error("Failed to queue unmatched name %r: %s", raw_name, e)
