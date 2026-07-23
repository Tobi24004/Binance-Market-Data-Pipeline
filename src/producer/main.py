"""Entry point: wires BinanceTradeClient (2.1) -> TradePublisher (2.2) together
and keeps the process alive.

This is the one long-running service in the pipeline that Airflow does NOT
manage - see plan section 2 / 8. Run with `python -m producer.main` (PYTHONPATH
must include src/, see the producer Dockerfile).
"""
import signal
import threading

from producer.client import BinanceTradeClient
from producer.kafka_producer import TradePublisher, build_kafka_producer

from common.config import get_kafka_bootstrap_servers, load_settings
from common.logger import get_logger
from common.schema import KAFKA_TOPIC_TRADES

logger = get_logger(__name__)


def _log_metrics_periodically(publisher: TradePublisher, interval_seconds: int, stop_event: threading.Event):
    while not stop_event.wait(interval_seconds):
        logger.info(
            "producer metrics | received=%d sent=%d failed=%d",
            publisher.received_count,
            publisher.sent_count,
            publisher.failed_count,
        )


def main():
    settings = load_settings()
    symbols = settings["symbols"]
    ws_base_url = settings["binance"]["ws_base_url"]
    bootstrap_servers = get_kafka_bootstrap_servers(settings)
    topic = settings.get("kafka", {}).get("topic_trades", KAFKA_TOPIC_TRADES)
    producer_cfg = settings.get("producer", {})
    ping_interval = producer_cfg.get("ping_interval_seconds", 20)
    metrics_interval = producer_cfg.get("metrics_log_interval_seconds", 30)

    kafka_producer = build_kafka_producer(bootstrap_servers)
    publisher = TradePublisher(kafka_producer, topic)

    client = BinanceTradeClient(
        symbols=symbols,
        ws_base_url=ws_base_url,
        on_message=publisher.publish_raw_trade,
        ping_interval=ping_interval,
    )

    stop_event = threading.Event()
    metrics_thread = threading.Thread(
        target=_log_metrics_periodically,
        args=(publisher, metrics_interval, stop_event),
        daemon=True,
    )
    metrics_thread.start()

    def _shutdown(signum, _frame):
        logger.info("Received signal %s, shutting down producer...", signum)
        stop_event.set()
        client.stop()
        publisher.flush(timeout=5)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Starting Binance producer for symbols=%s -> kafka topic=%s", symbols, topic)
    client.run_forever()


if __name__ == "__main__":
    main()
