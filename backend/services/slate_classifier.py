"""Classifier tests. No DB required — pure logic.

Run with: pytest backend/tests/test_slate_classifier.py -v
"""
from datetime import datetime

from app.models import DKDraftable, DKDraftGroup
from app.services.slate_classifier import (
    SlateType,
    classify_slate,
    pick_slates_to_ingest,
)


# ── Fixture builders ─────────────────────────────────────────────────

def _dg(
    dgid: int = 1,
    contest_type: str = "Classic",
    slate_label: str | None = None,
    sport: str = "tennis",
) -> DKDraftGroup:
    return DKDraftGroup(
        draft_group_id=dgid,
        sport=sport,
        contest_type=contest_type,
        slate_label=slate_label,
        salary_cap=50000,
        lock_time=datetime(2026, 4, 23, 16, 0, 0),
    )


def _draftable(pos: str = "P", pid: int = 100, name: str = "Test Player") -> DKDraftable:
    return DKDraftable(
        dk_player_id=pid,
        display_name=name,
        salary=8000,
        roster_position=pos,
        competition_id=1,
        competition_name="A vs B",
    )


def _classic_draftables() -> list[DKDraftable]:
    """Standard tennis Classic: all roster_position='P'."""
    return [_draftable(pos="P", pid=i, name=f"P{i}") for i in range(1, 5)]


def _showdown_draftables() -> list[DKDraftable]:
    """Showdown: each player appears with CPT and FLEX roster positions."""
    out = []
    for i in range(1, 4):
        out.append(_draftable(pos="CPT", pid=i, name=f"P{i}"))
        out.append(_draftable(pos="FLEX", pid=i + 100, name=f"P{i}"))
    return out


# ── classify_slate tests ────────────────────────────────────────────

def test_classic_basic():
    """Standard tennis Classic → CLASSIC."""
    assert classify_slate(_dg(contest_type="Classic"), _classic_draftables()) == SlateType.CLASSIC


def test_classic_by_structure_only():
    """No keyword match, roster=P only → CLASSIC (the default)."""
    assert classify_slate(_dg(contest_type=""), _classic_draftables()) == SlateType.CLASSIC


def test_showdown_by_keyword():
    """contest_type='Showdown' → SHOWDOWN regardless of roster."""
    assert classify_slate(_dg(contest_type="Showdown"), _classic_draftables()) == SlateType.SHOWDOWN


def test_showdown_by_label():
    """Keyword in slate_label → SHOWDOWN."""
    assert classify_slate(
        _dg(contest_type="Classic", slate_label="Single Game"),
        _classic_draftables(),
    ) == SlateType.SHOWDOWN


def test_showdown_by_roster_positions():
    """CPT/FLEX roster positions → SHOWDOWN, even without keyword match."""
    assert classify_slate(_dg(contest_type=""), _showdown_draftables()) == SlateType.SHOWDOWN


def test_captain_mode_keyword():
    """'Captain Mode' → SHOWDOWN."""
    assert classify_slate(
        _dg(contest_type="Captain Mode"), _classic_draftables()
    ) == SlateType.SHOWDOWN


def test_tiers_is_other():
    """Tiers → OTHER."""
    assert classify_slate(_dg(contest_type="Tiers"), _classic_draftables()) == SlateType.OTHER


def test_pickem_is_other():
    """Pick'Em → OTHER."""
    assert classify_slate(
        _dg(contest_type="Pick'Em"), _classic_draftables()
    ) == SlateType.OTHER


def test_express_is_other():
    """Express → OTHER."""
    assert classify_slate(_dg(contest_type="Express"), _classic_draftables()) == SlateType.OTHER


def test_turbo_is_other():
    """Turbo → OTHER."""
    assert classify_slate(_dg(slate_label="Turbo"), _classic_draftables()) == SlateType.OTHER


def test_other_takes_precedence_over_showdown():
    """A slate that's both Tiers AND showdown should classify as OTHER
    (we reject all non-standard formats before showdown check)."""
    assert classify_slate(
        _dg(contest_type="Tiers Showdown"), _showdown_draftables()
    ) == SlateType.OTHER


# ── pick_slates_to_ingest tests ─────────────────────────────────────

def test_pick_ingests_classic_only_by_default():
    """Classic + Showdown → ingest Classic only, skip Showdown."""
    classic = (_dg(dgid=1), _classic_draftables(), SlateType.CLASSIC)
    showdown = (_dg(dgid=2, contest_type="Showdown"), _showdown_draftables(), SlateType.SHOWDOWN)
    result = pick_slates_to_ingest(
        [classic, showdown],
        allowed_types={SlateType.CLASSIC},
        fallback_to_showdown=True,
    )
    assert len(result) == 1
    assert result[0][0].draft_group_id == 1
    assert result[0][2] == SlateType.CLASSIC
    assert result[0][3] is False  # not a fallback


def test_pick_falls_back_to_showdown_when_no_classic():
    """No Classic + Showdown exists + fallback on → ingest Showdown as fallback."""
    showdown = (_dg(dgid=2, contest_type="Showdown"), _showdown_draftables(), SlateType.SHOWDOWN)
    result = pick_slates_to_ingest(
        [showdown],
        allowed_types={SlateType.CLASSIC},
        fallback_to_showdown=True,
    )
    assert len(result) == 1
    assert result[0][0].draft_group_id == 2
    assert result[0][2] == SlateType.SHOWDOWN
    assert result[0][3] is True  # is_fallback = True


def test_pick_skips_showdown_when_fallback_disabled():
    """No Classic + Showdown exists + fallback OFF → ingest nothing."""
    showdown = (_dg(dgid=2, contest_type="Showdown"), _showdown_draftables(), SlateType.SHOWDOWN)
    result = pick_slates_to_ingest(
        [showdown],
        allowed_types={SlateType.CLASSIC},
        fallback_to_showdown=False,
    )
    assert result == []


def test_pick_ingests_both_when_allowed():
    """Classic + Showdown, both in allowed_types → ingest both, neither is fallback."""
    classic = (_dg(dgid=1), _classic_draftables(), SlateType.CLASSIC)
    showdown = (_dg(dgid=2, contest_type="Showdown"), _showdown_draftables(), SlateType.SHOWDOWN)
    result = pick_slates_to_ingest(
        [classic, showdown],
        allowed_types={SlateType.CLASSIC, SlateType.SHOWDOWN},
        fallback_to_showdown=True,
    )
    assert len(result) == 2
    # Neither is fallback — both were explicitly allowed
    assert all(r[3] is False for r in result)


def test_pick_skips_other_always():
    """Tiers/PickEm slates never ingest, even if OTHER is somehow 'allowed'."""
    tiers = (_dg(dgid=3, contest_type="Tiers"), _classic_draftables(), SlateType.OTHER)
    result = pick_slates_to_ingest(
        [tiers],
        allowed_types={SlateType.CLASSIC, SlateType.SHOWDOWN},
        fallback_to_showdown=True,
    )
    # OTHER is excluded from pick_slates_to_ingest regardless of allowed set
    assert result == []


def test_pick_multiple_classics_all_ingest():
    """Early Classic + Main Classic both in same day → ingest both."""
    early = (_dg(dgid=1, slate_label="Early"), _classic_draftables(), SlateType.CLASSIC)
    main = (_dg(dgid=2, slate_label="Main"), _classic_draftables(), SlateType.CLASSIC)
    result = pick_slates_to_ingest(
        [early, main],
        allowed_types={SlateType.CLASSIC},
        fallback_to_showdown=True,
    )
    assert len(result) == 2


def test_empty_input():
    """No draft groups at all → nothing to ingest."""
    assert pick_slates_to_ingest([], {SlateType.CLASSIC}, True) == []
