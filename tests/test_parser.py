"""Unit test for transform_raw_trade: mock raw Binance payload -> Data Contract.

Pure function - no WebSocket, no Kafka, no Spark required (plan section 7.1).
"""
from common.schema import CONTRACT_FIELDS, transform_raw_trade

RAW_TRADE = {
    "e": "trade",
    "E": 1752401732100,
    "s": "BTCUSDT",
    "t": 12345,
    "p": "109000.20",
    "q": "0.30000000",
    "b": 88,
    "a": 50,
    "T": 1752401732095,
    "m": True,
    "M": True,
}

EXPECTED_CONTRACT = {
    "event_time": "2025-07-13T10:15:32.100Z",
    "symbol": "BTCUSDT",
    "price": 109000.20,
    "quantity": 0.3,
    "trade_time": "2025-07-13T10:15:32.095Z",
}


def test_transform_raw_trade_matches_data_contract():
    result = transform_raw_trade(RAW_TRADE)
    assert result == EXPECTED_CONTRACT


def test_transform_raw_trade_casts_price_and_quantity_to_float():
    result = transform_raw_trade(RAW_TRADE)
    assert isinstance(result["price"], float)
    assert isinstance(result["quantity"], float)


def test_transform_raw_trade_only_produces_contract_fields():
    result = transform_raw_trade(RAW_TRADE)
    assert set(result.keys()) == set(CONTRACT_FIELDS)


def test_transform_raw_trade_raises_on_missing_field():
    incomplete_raw = {k: v for k, v in RAW_TRADE.items() if k != "p"}
    try:
        transform_raw_trade(incomplete_raw)
        assert False, "expected KeyError for missing 'p' field"
    except KeyError:
        pass


def test_transform_raw_trade_raises_on_unparsable_price():
    bad_raw = dict(RAW_TRADE, p="not-a-number")
    try:
        transform_raw_trade(bad_raw)
        assert False, "expected ValueError for unparsable price"
    except ValueError:
        pass
