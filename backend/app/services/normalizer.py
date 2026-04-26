"""Player name normalization.

Every source uses a slightly different name convention:
  DK            'Sinner J.' or 'Jannik Sinner' (varies by sport)
  PrizePicks    'Jannik Sinner'
  Kalshi        'Sinner' (often surname only)
  Tennis-data   'Sinner J.'
  ATP site      'Jannik SINNER'

We resolve all of these to one canonical_id ('jannik_sinner') in the
players table. This module owns that resolution.

Strategy:
  1. Exact alias match against existing aliases (fast path)
  2. Fuzzy match above AUTO threshold → auto-resolve, record the alias
  3. Below AUTO threshold AND create_if_missing → create new player
     (PP/DK regularly introduce qualifiers and ITF crossovers we don't
     have yet; treating them as fuzzy-match candidates against existing
     players caused the Arthur Rinderknech → arthur_fils corruption)
  4. Below AUTO threshold AND not create_if_missing → queue for review
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

# Bumped from 88 → 92. "arthur rinderknech" vs "arthur fils" scores ~75
# on token_set_ratio, so 88 was already safe — but we also tightened the
# rule below: AUTO requires both a high score AND a non-trivial last-name
# token overlap, so two players sharing only a first name can never auto-
# alias to each other.
FUZZY_AUTO_THRESHOLD = 92
FUZZY_SUGGEST_THRESHOLD = 70


def canonicalize(name: str) -> str:
    """Build a canonical_id from a display name.

    'Jannik Sinner'         → 'jannik_sinner'
    'Carlos Alcaraz Garfia' → 'carlos_alcaraz_garfia'
    'Núñez, Aníbal'         → 'nunez_anibal'

    Deterministic, slug-safe, used as primary key in players table.
    """
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    stripped = stripped.lower().strip()
    if "," in stripped:
        parts = [p.strip() for p in stripped.split(",", 1)]
        if len(parts) == 2:
            stripped = f"{parts[1]} {parts[0]}"
    stripped = re.sub(r"[^a-z0-9\s\-]", "", stripped)
    stripped = re.sub(r"\s+", "_", stripped.strip())
    return stripped


def normalize_for_match(name: str) -> str:
    """Normalize a name for fuzzy matching (looser than canonicalize)."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


def _last_token(norm: str) -> str:
    """Pull the last whitespace-delimited token (rough surname proxy)."""
    parts = [p for p in re.split(r"\s+", norm.strip()) if p]
    return parts[-1] if parts else ""


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
        self._canonicals: dict[str, str] = {}
        self._alias_lookup: dict[str, str] = {}
        self._normalized_lookup: dict[str, str] = {}

    def _ensure_loaded(self):
        if self._loaded:
            return
        db = get_client()
        rows = (
            db.table("players").select("*").eq("sport", self.sport).execute().data
            or []
        )
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
        logger.info(
            "Loaded %d %s players into normalizer", len(self._canonicals), self.sport
        )

    def resolve(
        self,
        raw_name: str,
        source: str,
        context: Optional[dict] = None,
        create_if_missing: bool = True,
    ) -> MatchResult:
        """Resolve a raw name to a canonical_id.

        create_if_missing: if True and no AUTO-confidence match found,
        create a new player row using the raw name as both display_name
        and seed for canonical_id. PP scrapers should pass True; Kalshi
        ingestion that joins back to existing slate players should pass
        False (Kalshi can't invent players that DK doesn't have).
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
        best_cid = None
        best_score = 0.0
        if candidates:
            best = process.extractOne(
                norm,
                candidates,
                scorer=fuzz.token_set_ratio,
                score_cutoff=FUZZY_SUGGEST_THRESHOLD,
            )
            if best:
                matched_norm, best_score, _ = best
                best_cid = self._normalized_lookup[matched_norm]

                # AUTO-resolve only if BOTH the global token-set score is
                # high AND the surname tokens substantially overlap. This
                # is the fix for the Arthur Rinderknech → arthur_fils
                # corruption: same first name was inflating token_set
                # while the actual surnames disagreed entirely.
                surname_score = fuzz.ratio(_last_token(norm), _last_token(matched_norm))
                if best_score >= FUZZY_AUTO_THRESHOLD and surname_score >= 80:
                    self._record_alias(best_cid, source, raw_name)
                    return MatchResult(
                        best_cid,
                        best_score,
                        True,
                        display_name=self._canonicals[best_cid],
                    )

        # No high-confidence match.
        # If we're allowed to create, do so — never return a low-confidence
        # fuzzy guess as if it were authoritative. Callers that pass
        # create_if_missing=False want strict mode and accept None.
        if create_if_missing:
            cid = self._create_player(raw_name, source)
            return MatchResult(cid, 0, True, was_new=True, display_name=raw_name)

        # Strict mode: queue and return the suggestion (auto_resolved=False
        # so callers know not to treat canonical_id as authoritative).
        self._queue_unmatched(raw_name, source, context, best_cid, best_score)
        return MatchResult(
            best_cid,
            best_score,
            False,
            display_name=self._canonicals.get(best_cid, "") if best_cid else "",
        )

    def _create_player(self, display_name: str, source: str) -> str:
        """Create a new player row."""
        base_cid = canonicalize(display_name)
        cid = base_cid
        db = get_client()
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
        self._canonicals[cid] = display_name
        self._alias_lookup[display_name] = cid
        self._normalized_lookup[normalize_for_match(display_name)] = cid
        logger.info(
            "Created new %s player: %s (%s)", self.sport, display_name, cid
        )
        return cid

    def _record_alias(self, canonical_id: str, source: str, raw_name: str):
        """Add a new alias to an existing player."""
        if raw_name in self._alias_lookup:
            return
        db = get_client()
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
            return
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
