"""Stage 2.2 - transform raw Binance trade payloads into the Data Contract and
publish them to Kafka.

Field names always come from common/schema.py (the single source of truth)
so this file never hardcodes a raw Binance field name a second time.
"""
import json
from typing import Any, Dict, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common.logger import get_logger
from common.schema import transform_raw_trade

logger = get_logger(__name__)


def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )


class TradePublisher:
    """raw trade -> Data Contract -> Kafka, with simple received/sent/failed counters.

    Kafka client libraries don't expose these counts out of the box, so this
    is the "custom producer metric" layer called out in the plan's
    Monitoring stage - main.py logs them periodically.
    """

    def __init__(self, producer: KafkaProducer, topic: str):
        self.producer = producer
        self.topic = topic
        self.received_count = 0
        self.sent_count = 0
        self.failed_count = 0

    def publish_raw_trade(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.received_count += 1
        try:
            contract_message = transform_raw_trade(raw)
        except (KeyError, ValueError, TypeError) as exc:
            self.failed_count += 1
            logger.warning("Dropping unparsable raw trade message: %s (%s)", raw, exc)
            return None

        try:
            future = self.producer.send(self.topic, key=contract_message["symbol"], value=contract_message)
        except KafkaError as exc:
            # Raised synchronously: serialization error, buffer full, etc.
            self.failed_count += 1
            logger.error("Failed to enqueue message for topic %s: %s", self.topic, exc)
            return None

        # Broker-level failures (leader not available, timeout, ...) surface
        # asynchronously once the background sender thread flushes the batch.
        #
        # kafka-python's Future.add_errback(f, *args) wraps f as
        # functools.partial(f, *args) and later calls it as f(exception) -
        # i.e. partial_f(exception), so any *args given to add_errback/
        # add_callback land BEFORE the value/exception, not after. Passing
        # contract_message as an add_errback arg would silently swap the
        # (exc, message) parameters below. A closure sidesteps that footgun
        # entirely by capturing contract_message and taking only the
        # exception from kafka-python.
        future.add_callback(lambda _meta: self._on_send_success())
        future.add_errback(lambda exc: self._on_send_error(exc, contract_message))
        return contract_message

    def _on_send_success(self):
        self.sent_count += 1

    def _on_send_error(self, exc, message: Dict[str, Any]):
        self.failed_count += 1
        logger.error("Kafka broker rejected message for topic %s: %s (message=%s)", self.topic, exc, message)

    def flush(self, timeout: Optional[float] = None):
        self.producer.flush(timeout=timeout)
