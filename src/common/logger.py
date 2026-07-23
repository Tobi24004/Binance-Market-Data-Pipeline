"""Unified logging setup shared by the producer and the Spark job.

Both sides call get_logger(__name__) so log lines from every component
have the same shape and can be grepped/aggregated the same way, whether
they came from the long-running producer process or a Spark executor.
"""
import logging
import os
import sys

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    global _CONFIGURED
    if not _CONFIGURED:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            stream=sys.stdout,
        )
        _CONFIGURED = True
    return logging.getLogger(name)
