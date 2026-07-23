"""Stage 8 - daily aggregate report (max/min/avg price per symbol), independent
of the streaming pipeline. See dags/update_dim_symbol.py for the sibling DAG
and the shared "why is common/ importable here" note.
"""
import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task

sys.path.insert(0, os.path.dirname(__file__))

from common.config import get_postgres_conn_params, load_settings  # noqa: E402
from common.logger import get_logger  # noqa: E402

logger = get_logger(__name__)

_AGGREGATE_QUERY = """
    SELECT d.symbol,
           MIN(f.price) AS min_price,
           MAX(f.price) AS max_price,
           AVG(f.price) AS avg_price,
           COUNT(*)     AS trade_count
    FROM fact_price f
    JOIN dim_symbol d ON d.symbol_id = f.symbol_id
    WHERE f.event_time >= NOW() - INTERVAL '1 day'
    GROUP BY d.symbol
    ORDER BY d.symbol
"""


def _get_pg_conn():
    settings = load_settings()
    return psycopg2.connect(**get_postgres_conn_params(settings))


@dag(
    dag_id="daily_aggregate_report",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["binance", "batch"],
)
def daily_aggregate_report_dag():
    @task
    def compute_and_log_aggregates():
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(_AGGREGATE_QUERY)
                rows = cur.fetchall()
        finally:
            conn.close()

        for symbol, min_price, max_price, avg_price, trade_count in rows:
            logger.info(
                "daily aggregate | symbol=%s min=%.8f max=%.8f avg=%.8f trades=%d",
                symbol,
                min_price,
                max_price,
                avg_price,
                trade_count,
            )

    compute_and_log_aggregates()


daily_aggregate_report_dag()
