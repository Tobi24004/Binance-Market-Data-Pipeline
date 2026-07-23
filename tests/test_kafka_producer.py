"""Regression test for TradePublisher's use of kafka-python's Future callbacks.

kafka.future.Future.add_errback(f, *args) wraps f as functools.partial(f,
*args) and later calls it as f(exception) - i.e. the exception lands AFTER
any extra args, not before. Passing contract_message as an add_errback arg
(future.add_errback(self._on_send_error, contract_message)) silently swapped
_on_send_error's (exc, message) parameters: exc received contract_message and
message received the real exception. This test fakes kafka-python's actual
callback semantics (not a real broker) to catch that class of bug without
needing Kafka - but still needs the real `kafka` package importable, since
kafka_producer.py imports it at module load time; skipped where it isn't
(e.g. kafka-python 2.0.2 does not yet support Python 3.12+).
"""
import functools

import pytest

pytest.importorskip("kafka")

from producer.kafka_producer import TradePublisher  # noqa: E402


class FakeFuture:
    """Mirrors kafka.future.Future's add_callback/add_errback exactly: extra
    args are prepended via functools.partial, so the eventual value/exception
    is appended LAST when the callback fires.
    """

    def __init__(self):
        self._callbacks = []
        self._errbacks = []

    def add_callback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        self._callbacks.append(f)
        return self

    def add_errback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        self._errbacks.append(f)
        return self

    def trigger_success(self, value):
        for f in self._callbacks:
            f(value)

    def trigger_failure(self, exc):
        for f in self._errbacks:
            f(exc)


class FakeKafkaProducer:
    def __init__(self):
        self.sent = []
        self.futures = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        future = FakeFuture()
        self.futures.append(future)
        return future


RAW_TRADE = {
    "e": "trade",
    "E": 1752401732100,
    "s": "BTCUSDT",
    "p": "109000.20",
    "q": "0.30000000",
    "T": 1752401732095,
}


def test_successful_send_increments_sent_count():
    fake_producer = FakeKafkaProducer()
    publisher = TradePublisher(fake_producer, "crypto_trades")

    publisher.publish_raw_trade(dict(RAW_TRADE))
    fake_producer.futures[0].trigger_success(object())

    assert publisher.sent_count == 1
    assert publisher.failed_count == 0


def test_broker_failure_passes_the_real_exception_and_message_uncorrupted():
    fake_producer = FakeKafkaProducer()
    publisher = TradePublisher(fake_producer, "crypto_trades")

    contract_message = publisher.publish_raw_trade(dict(RAW_TRADE))
    assert contract_message is not None

    captured = {}

    def fake_on_send_error(exc, message):
        captured["exc"] = exc
        captured["message"] = message

    publisher._on_send_error = fake_on_send_error

    boom = RuntimeError("broker unavailable")
    fake_producer.futures[0].trigger_failure(boom)

    assert captured["exc"] is boom
    assert captured["message"] == contract_message


def test_broker_failure_increments_failed_count():
    fake_producer = FakeKafkaProducer()
    publisher = TradePublisher(fake_producer, "crypto_trades")

    publisher.publish_raw_trade(dict(RAW_TRADE))
    fake_producer.futures[0].trigger_failure(RuntimeError("broker unavailable"))

    assert publisher.failed_count == 1
    assert publisher.sent_count == 0
