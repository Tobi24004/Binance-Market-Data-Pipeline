"""Stage 2.1 - Binance WebSocket -> console.

Owns the WebSocket connection only: knows nothing about Kafka or the Data
Contract. kafka_producer.py (stage 2.2) is responsible for transforming the
raw payload this module hands back through on_message.
"""
import json
import threading
from typing import Callable, Iterable

import websocket

from common.logger import get_logger
from common.utils import exponential_backoff

logger = get_logger(__name__)


def build_stream_url(symbols: Iterable[str], ws_base_url: str) -> str:
    """Combined-stream URL, e.g. wss://.../stream?streams=btcusdt@trade/ethusdt@trade."""
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    return f"{ws_base_url}/stream?streams={streams}"


class BinanceTradeClient:
    """Long-lived WebSocket client with automatic reconnect + exponential backoff.

    on_message receives the *raw* Binance trade payload (fields e, E, s, p,
    q, T, ...), unwrapped from the combined-stream envelope.
    """

    def __init__(
        self,
        symbols: Iterable[str],
        ws_base_url: str,
        on_message: Callable[[dict], None],
        ping_interval: int = 20,
    ):
        self.symbols = list(symbols)
        self.ws_base_url = ws_base_url
        self.on_message = on_message
        self.ping_interval = ping_interval
        self._stop = threading.Event()
        self._ws = None

    def _handle_message(self, _ws, message: str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("Dropping non-JSON WebSocket message: %s", message[:200])
            return
        # Combined-stream envelope is {"stream": "...", "data": {...}};
        # a single-stream connection sends the raw payload directly.
        raw = payload.get("data", payload)
        self.on_message(raw)

    def _handle_error(self, _ws, error):
        logger.warning("WebSocket error: %s", error)

    def _handle_close(self, _ws, status_code, msg):
        logger.warning("WebSocket closed (code=%s, msg=%s)", status_code, msg)

    def run_forever(self):
        """Blocks, reconnecting with exponential backoff until stop() is called."""
        url = build_stream_url(self.symbols, self.ws_base_url)
        backoff = exponential_backoff()
        while not self._stop.is_set():
            logger.info("Connecting to Binance WebSocket: %s", url)
            self._ws = websocket.WebSocketApp(
                url,
                on_message=self._handle_message,
                on_error=self._handle_error,
                on_close=self._handle_close,
            )
            self._ws.run_forever(ping_interval=self.ping_interval)
            if self._stop.is_set():
                break
            delay = next(backoff)
            logger.warning("WebSocket disconnected, reconnecting in %.1fs", delay)
            self._stop.wait(delay)

    def stop(self):
        """Signal run_forever() to stop and unblock it if a connection is open.

        Without closing the live WebSocketApp, run_forever() stays blocked
        until the socket happens to disconnect on its own - setting the flag
        alone would not shut the producer down promptly.
        """
        self._stop.set()
        if self._ws is not None:
            self._ws.close()


def _debug_main():
    """Manual stage-2.1 check: `python -m producer.client` prints raw trades, no Kafka."""
    from common.config import load_settings

    settings = load_settings()
    client = BinanceTradeClient(
        symbols=settings["symbols"],
        ws_base_url=settings["binance"]["ws_base_url"],
        on_message=print,
    )
    client.run_forever()


if __name__ == "__main__":
    _debug_main()
