"""Unit tests for name canonicalization. No DB required."""
from app.services.normalizer import canonicalize, normalize_for_match


def test_canonicalize_basic():
    assert canonicalize("Jannik Sinner") == "jannik_sinner"
    assert canonicalize("Carlos Alcaraz") == "carlos_alcaraz"


def test_canonicalize_strips_diacritics():
    assert canonicalize("Núñez Aníbal") == "nunez_anibal"
    assert canonicalize("Sébastien Grosjean") == "sebastien_grosjean"


def test_canonicalize_handles_last_first():
    assert canonicalize("Sinner, Jannik") == "jannik_sinner"


def test_canonicalize_handles_triple_names():
    assert canonicalize("Carlos Alcaraz Garfia") == "carlos_alcaraz_garfia"


def test_canonicalize_is_stable():
    # Same input always produces same output
    assert canonicalize("  Jannik  Sinner  ") == canonicalize("Jannik Sinner")


def test_normalize_for_match_loose():
    # Different surface forms normalize to the same string for fuzzy matching
    a = normalize_for_match("Sinner J.")
    b = normalize_for_match("Jannik Sinner")
    # They're not equal but share tokens — fuzzy matcher handles that
    assert "sinner" in a
    assert "sinner" in b
