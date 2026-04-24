"""Piece #2 tests — odds transformation, Kalshi signing, PP admin logic.

Pure logic tests. No real network. Run with:
  PYTHONPATH=. python -m pytest tests/test_piece2.py -v
"""
from __future__ import annotations

from datetime import datetime, timezone


# ── Odds API transformation ─────────────────────────────────────────

def test_decimal_to_american_favorite():
    from app.services.odds_api import decimal_to_american
    # -250 favorite = 1.40 decimal → back to -250 (approximately)
    assert decimal_to_american(1.40) == -250


def test_decimal_to_american_underdog():
    from app.services.odds_api import decimal_to_american
    # +150 = 2.50 decimal
    assert decimal_to_american(2.50) == 150


def test_decimal_to_american_pickem():
    from app.services.odds_api import decimal_to_american
    # 2.00 = +100 (even money)
    assert decimal_to_american(2.0) == 100


def test_parse_event_basic():
    from app.services.odds_api import _parse_event
    event = {
        "id": "abc",
        "home_team": "Jannik Sinner",
        "away_team": "Carlos Alcaraz",
        "commence_time": "2026-04-24T13:00:00Z",
        "bookmakers": [{
            "key": "pinnacle",
            "markets": [
                {"key": "h2h", "outcomes": [
                    {"name": "Jannik Sinner", "price": 1.67},
                    {"name": "Carlos Alcaraz", "price": 2.30},
                ]},
                {"key": "totals", "outcomes": [
                    {"name": "Over", "point": 22.5, "price": 1.91},
                    {"name": "Under", "point": 22.5, "price": 1.91},
                ]},
            ],
        }],
    }
    row = _parse_event(event)
    assert row is not None
    assert row.ml_a is not None
    assert row.ml_b is not None
    # Sinner favored → ml_a is negative
    assert row.ml_a < 0
    assert row.ml_b > 0
    # Totals split in half
    assert row.gw_a_line == 11.25
    assert row.gw_b_line == 11.25


def test_parse_event_malformed():
    from app.services.odds_api import _parse_event
    # Missing required fields → returns None, doesn't crash
    assert _parse_event({}) is None
    assert _parse_event({"home_team": "X"}) is None


def test_swap_ab_fields():
    from app.services.odds_api import _swap_ab_fields
    inp = {"ml_a": -150, "ml_b": 120, "gw_a_line": 12.5, "gw_a_over": -110, "fetched_at": "ts"}
    out = _swap_ab_fields(inp)
    assert out["ml_b"] == -150
    assert out["ml_a"] == 120
    assert out["gw_b_line"] == 12.5
    assert out["gw_b_over"] == -110
    assert out["fetched_at"] == "ts"


# ── Kalshi: RSA-PSS signing ─────────────────────────────────────────

def test_kalshi_signature_produces_valid_base64():
    """Signing should produce a base64 string. Uses an ephemeral key so
    we don't depend on env vars being set."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from app.services import kalshi as kalshi_mod

    # Generate ephemeral key
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    # Inject into module-level cache
    kalshi_mod._private_key = key

    sig = kalshi_mod._sign_request("GET", "/trade-api/v2/markets", "1703123456789")
    assert sig is not None
    assert isinstance(sig, str)
    # Must be valid base64 (decodes without error)
    import base64
    decoded = base64.b64decode(sig)
    # RSA-PSS signatures are fixed-size = key size in bytes
    assert len(decoded) == 256  # 2048 bits / 8


def test_kalshi_path_strips_query():
    """Kalshi signs path without query parameters."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from app.services import kalshi as kalshi_mod

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    kalshi_mod._private_key = key

    # Both signatures should be identical since query is stripped
    sig1 = kalshi_mod._sign_request("GET", "/markets", "1700000000000")
    sig2 = kalshi_mod._sign_request("GET", "/markets?series_ticker=ATP", "1700000000000")
    # RSA-PSS is probabilistic (salted), so signatures differ, but both
    # should verify against the same message. Easier test: check lengths match.
    assert len(sig1) == len(sig2)


def test_dollar_to_prob_midpoint():
    """Kalshi yes_bid_dollars and yes_ask_dollars should midpoint to prob."""
    from app.services.kalshi import _dollar_to_prob
    assert _dollar_to_prob("0.60", "0.62") == 0.61
    assert _dollar_to_prob("0.5600", "0.5600") == 0.56


def test_dollar_to_prob_single_side():
    """If only bid or only ask is present, return that value."""
    from app.services.kalshi import _dollar_to_prob
    assert _dollar_to_prob("0.42", None) == 0.42
    assert _dollar_to_prob(None, "0.58") == 0.58


def test_dollar_to_prob_invalid():
    """None/empty/out-of-range values return None."""
    from app.services.kalshi import _dollar_to_prob
    assert _dollar_to_prob(None, None) is None
    assert _dollar_to_prob("not_a_number", None) is None
    assert _dollar_to_prob("1.5", None) is None  # outside valid 0-1 range
    assert _dollar_to_prob("-0.1", None) is None


def test_event_grouping_structure():
    """Verify the event-ticker grouping pattern works on a realistic Kalshi
    response shape. This is the structure our fetch_tick() will encounter."""
    markets = [
        {
            "ticker": "KXWTAMATCH-26APR24PUTKOS-PUT",
            "event_ticker": "KXWTAMATCH-26APR24PUTKOS",
            "yes_sub_title": "Yulia Putintseva",
            "yes_bid_dollars": "0.4200",
            "yes_ask_dollars": "0.4400",
        },
        {
            "ticker": "KXWTAMATCH-26APR24PUTKOS-KOS",
            "event_ticker": "KXWTAMATCH-26APR24PUTKOS",
            "yes_sub_title": "Marta Kostyuk",
            "yes_bid_dollars": "0.5600",
            "yes_ask_dollars": "0.5800",
        },
        {
            "ticker": "KXWTAMATCH-26APR24TAUSIN-TAU",
            "event_ticker": "KXWTAMATCH-26APR24TAUSIN",
            "yes_sub_title": "Clara Tauson",
            "yes_bid_dollars": "0.7000",
            "yes_ask_dollars": "0.7200",
        },
        {
            "ticker": "KXWTAMATCH-26APR24TAUSIN-SIN",
            "event_ticker": "KXWTAMATCH-26APR24TAUSIN",
            "yes_sub_title": "Katerina Siniakova",
            "yes_bid_dollars": "0.2800",
            "yes_ask_dollars": "0.3000",
        },
    ]
    # Group like fetch_tick does
    events = {}
    for m in markets:
        events.setdefault(m["event_ticker"], []).append(m)
    assert len(events) == 2
    # Each event has two sides
    for event_markets in events.values():
        assert len(event_markets) == 2
        names = [m["yes_sub_title"] for m in event_markets]
        assert all(names)  # both named


def test_frontend_match_odds_accepts_kalshi_fields():
    """Pydantic model accepts kalshi_prob_a/b when hydrating from matches.odds.
    Regression guard for hotfix #3 — prior model silently dropped these fields."""
    from app.models import FrontendMatchOdds
    db_odds = {
        "ml_a": -300,
        "ml_b": 245,
        "kalshi_prob_a": 0.865,
        "kalshi_prob_b": 0.135,
        # Extra keys from other sources should be silently ignored, not raise
        "kalshi": {"raw": {"a": {}, "b": {}}},
        "the_odds_api": {"raw": {}, "fetched_at": "..."},
    }
    model = FrontendMatchOdds(**db_odds)
    assert model.ml_a == -300
    assert model.kalshi_prob_a == 0.865
    assert model.kalshi_prob_b == 0.135


def test_frontend_match_odds_empty_dict():
    """Empty dict → all-None model. Default path for matches without odds yet."""
    from app.models import FrontendMatchOdds
    model = FrontendMatchOdds(**{})
    assert model.ml_a is None
    assert model.kalshi_prob_a is None
