"""Stage 8 - daily batch job, fully independent of the streaming pipeline.

Pulls the live symbol list from Binance's public REST API and upserts it
into dim_symbol. Airflow never starts/stops/monitors the producer or the
Spark streaming job - those are long-running services managed directly by
Docker Compose (see plan section 2 / 8, "Airflow khong tham gia streaming").
"""
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow.decorators import dag, task
from psycopg2.extras import execute_values

# src/common/ is bind-mounted alongside this DAG file (see docker-compose.yml
# `airflow` service) so it is importable without duplicating config/logging code.
sys.path.insert(0, os.path.dirname(__file__))

from common.config import get_postgres_conn_params, load_settings  # noqa: E402
from common.logger import get_logger  # noqa: E402

logger = get_logger(__name__)

BINANCE_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"


def _get_pg_conn():
    settings = load_settings()
    return psycopg2.connect(**get_postgres_conn_params(settings))


@dag(
    dag_id="update_dim_symbol",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["binance", "batch"],
)
def update_dim_symbol_dag():
    @task
    def fetch_and_upsert_symbols():
        response = requests.get(BINANCE_EXCHANGE_INFO_URL, timeout=30)
        response.raise_for_status()
        symbols_payload = response.json()["symbols"]

        rows = [
            (item["symbol"], item["baseAsset"], item["quoteAsset"])
            for item in symbols_payload
            if item.get("status") == "TRADING"
        ]

        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO dim_symbol (symbol, base_asset, quote_asset) VALUES %s "
                    "ON CONFLICT (symbol) DO UPDATE SET "
                    "base_asset = EXCLUDED.base_asset, quote_asset = EXCLUDED.quote_asset",
                    rows,
                )
            conn.commit()
            logger.info("Upserted %d symbols into dim_symbol", len(rows))
        finally:
            conn.close()

    fetch_and_upsert_symbols()


update_dim_symbol_dag()
