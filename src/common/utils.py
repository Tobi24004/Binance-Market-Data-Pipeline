"""Small stateless helpers shared by the producer and the Spark job."""
from datetime import datetime, timezone
from typing import Iterator


def epoch_ms_to_iso(epoch_ms) -> str:
    """Convert a Binance epoch-millisecond timestamp to an ISO 8601 UTC string.

    Example: 1752401732100 -> "2026-07-13T10:15:32.100Z"
    """
    dt = datetime.fromtimestamp(int(epoch_ms) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def exponential_backoff(base: float = 1.0, factor: float = 2.0, max_delay: float = 30.0) -> Iterator[float]:
    """Yield an unbounded sequence of increasing delays, capped at max_delay.

    Used to back off WebSocket / Kafka reconnect attempts instead of hammering
    the remote endpoint in a tight loop.
    """
    delay = base
    while True:
        yield min(delay, max_delay)
        delay = min(delay * factor, max_delay)
