"""Single source of truth for the Binance trade Data Contract.

Both the producer (plain dict -> JSON) and the Spark streaming job
(StructType) derive their view of the message shape from this module.
If the contract changes, it changes here once - not in two places that
can silently drift apart.

See docs / README "Data Contract" section for the full field mapping.
"""
from dataclasses import dataclass, fields
from typing import Any, Dict

from .utils import epoch_ms_to_iso

KAFKA_TOPIC_TRADES = "crypto_trades"
KAFKA_TOPIC_TRADES_DLQ = "crypto_trades_dlq"

# Raw Binance trade-stream field -> Data Contract field name.
# 'e' (event type) is intentionally dropped: the topic is already scoped
# to trade events, so the field carries no extra information for us.
RAW_TO_CONTRACT_FIELD = {
    "E": "event_time",
    "s": "symbol",
    "p": "price",
    "q": "quantity",
    "T": "trade_time",
}

# The validity rule lives here as a single SQL-compatible expression so
# Spark's DataFrame filter and the pure-Python check below can never
# describe two different rules.
VALID_FILTER_SQL = "price > 0 AND quantity > 0 AND event_time IS NOT NULL"
INVALID_FILTER_SQL = f"NOT ({VALID_FILTER_SQL})"


@dataclass(frozen=True)
class Trade:
    """The Data Contract message published to the `crypto_trades` topic."""

    event_time: str  # ISO 8601, converted from Binance epoch ms ("E")
    symbol: str  # e.g. "BTCUSDT"
    price: float  # Binance sends this as a string - must be cast to float
    quantity: float  # Binance sends this as a string - must be cast to float
    trade_time: str  # ISO 8601, converted from Binance epoch ms ("T")

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}


CONTRACT_FIELDS = [f.name for f in fields(Trade)]


def transform_raw_trade(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a raw Binance `@trade` WebSocket message into the Data Contract.

    Raises KeyError/ValueError if a required raw field is missing or not
    castable to the expected type - callers decide how to handle that
    (the producer logs and drops the message rather than crashing).
    """
    return {
        "event_time": epoch_ms_to_iso(raw["E"]),
        "symbol": raw["s"],
        "price": float(raw["p"]),
        "quantity": float(raw["q"]),
        "trade_time": epoch_ms_to_iso(raw["T"]),
    }


def is_valid_trade(record: Dict[str, Any]) -> bool:
    """Pure-Python mirror of VALID_FILTER_SQL, used by producer-side checks and tests.

    Rule: price > 0 AND quantity > 0 AND event_time IS NOT NULL.
    """
    price = record.get("price")
    quantity = record.get("quantity")
    event_time = record.get("event_time")
    if price is None or quantity is None or event_time is None:
        return False
    try:
        return float(price) > 0 and float(quantity) > 0
    except (TypeError, ValueError):
        return False


def spark_trade_schema():
    """Build the Spark StructType matching the Data Contract.

    Imported lazily so this module has no hard dependency on pyspark -
    the producer (which never touches Spark) can import schema.py without
    pyspark being installed.
    """
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

    return StructType(
        [
            StructField("event_time", TimestampType(), nullable=True),
            StructField("symbol", StringType(), nullable=True),
            StructField("price", DoubleType(), nullable=True),
            StructField("quantity", DoubleType(), nullable=True),
            StructField("trade_time", TimestampType(), nullable=True),
        ]
    )
