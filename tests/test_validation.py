"""Unit test for is_valid_trade: the valid/invalid filter logic, tested as a
pure function so it doesn't require a Spark cluster (plan section 7.1).

Spark's own filter uses VALID_FILTER_SQL / INVALID_FILTER_SQL (same module),
so this test also protects against the two rules drifting apart.
"""
from common.schema import is_valid_trade

BASE_TRADE = {
    "event_time": "2025-07-13T10:15:32.100Z",
    "symbol": "BTCUSDT",
    "price": 109000.20,
    "quantity": 0.3,
    "trade_time": "2025-07-13T10:15:32.095Z",
}


def test_valid_trade_passes():
    assert is_valid_trade(BASE_TRADE) is True


def test_null_price_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, price=None)) is False


def test_negative_price_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, price=-1.0)) is False


def test_zero_price_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, price=0)) is False


def test_negative_quantity_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, quantity=-0.5)) is False


def test_null_quantity_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, quantity=None)) is False


def test_missing_event_time_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, event_time=None)) is False


def test_non_numeric_price_is_invalid():
    assert is_valid_trade(dict(BASE_TRADE, price="not-a-number")) is False
