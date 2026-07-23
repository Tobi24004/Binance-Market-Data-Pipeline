"""Stage 5/6 - Spark Structured Streaming: Kafka -> parse/validate -> Postgres + DLQ.

Run via spark-submit with --py-files dependencies.zip (see entrypoint.sh) -
Spark executors cannot see src/common/ otherwise, since it lives outside
this file (see plan section 4.2 / 7 "Gioi han ky thuat").

Structured Streaming has no built-in JDBC sink, so writing to PostgreSQL
goes through foreachBatch. Each writeStream below has its OWN
checkpointLocation - sharing one between the valid and DLQ branches breaks
on restart.
"""
import os

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct

from common.config import get_kafka_bootstrap_servers, get_postgres_conn_params, get_postgres_jdbc_url, load_settings
from common.logger import get_logger
from common.schema import (
    INVALID_FILTER_SQL,
    KAFKA_TOPIC_TRADES,
    KAFKA_TOPIC_TRADES_DLQ,
    VALID_FILTER_SQL,
    spark_trade_schema,
)

logger = get_logger(__name__)

_KNOWN_QUOTE_ASSETS = ("USDT", "BUSD", "USDC", "BTC", "ETH", "BNB")

_FALLBACK_DDL = """
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id   SERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL UNIQUE,
    base_asset  VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_price (
    id          BIGSERIAL PRIMARY KEY,
    symbol_id   INTEGER NOT NULL REFERENCES dim_symbol (symbol_id),
    price       DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,
    trade_time  TIMESTAMPTZ NOT NULL
);
"""


def build_spark_session(app_name: str) -> SparkSession:
    # --master is supplied by `spark-submit`, not hardcoded here.
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_trade_stream(spark: SparkSession, bootstrap_servers: str, topic: str, starting_offsets: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def parse_trade_stream(raw_df: DataFrame) -> DataFrame:
    """Kafka `value` bytes -> Data Contract columns, using the StructType from schema.py."""
    schema = spark_trade_schema()
    return (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )


def ensure_tables_exist(pg_params: dict) -> None:
    """Fallback safety net only - sql/001_create_tables.sql (Stage 4) is authoritative."""
    conn = psycopg2.connect(**pg_params)
    try:
        with conn.cursor() as cur:
            cur.execute(_FALLBACK_DDL)
        conn.commit()
    finally:
        conn.close()


def _guess_base_quote(symbol: str):
    """Best-effort split for symbols first seen in the stream.

    dags/update_dim_symbol.py (Stage 8) is the authoritative source for
    accurate base/quote asset metadata - this is only a placeholder so the
    foreign key never breaks for a brand-new symbol.
    """
    for quote in _KNOWN_QUOTE_ASSETS:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, ""


def _upsert_missing_symbols(pg_params: dict, table_dim_symbol: str, symbols) -> None:
    if not symbols:
        return
    rows = [(symbol, *_guess_base_quote(symbol)) for symbol in symbols]
    conn = psycopg2.connect(**pg_params)
    try:
        with conn.cursor() as cur:
            query = sql.SQL(
                "INSERT INTO {table} (symbol, base_asset, quote_asset) VALUES %s "
                "ON CONFLICT (symbol) DO NOTHING"
            ).format(table=sql.Identifier(table_dim_symbol))
            execute_values(cur, query.as_string(cur), rows)
        conn.commit()
    finally:
        conn.close()


def make_postgres_writer(jdbc_url: str, pg_params: dict, table_fact_price: str, table_dim_symbol: str):
    """Build the foreachBatch function: upsert new symbols, then append fact rows.

    foreachBatch closures run on the driver, so opening a psycopg2 connection
    here (rather than inside a Python UDF on the executors) is safe and simple.
    """

    jdbc_props = {
        "user": pg_params["user"],
        "password": pg_params["password"],
        "driver": "org.postgresql.Driver",
    }

    def write_to_postgres(batch_df: DataFrame, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        batch_df.persist()
        try:
            symbols_in_batch = [row.symbol for row in batch_df.select("symbol").distinct().collect()]
            _upsert_missing_symbols(pg_params, table_dim_symbol, symbols_in_batch)

            symbol_map_df = batch_df.sparkSession.read.jdbc(
                url=jdbc_url, table=table_dim_symbol, properties=jdbc_props
            ).select("symbol_id", "symbol")

            enriched_df = batch_df.join(symbol_map_df, on="symbol", how="inner").select(
                "symbol_id", "price", "quantity", "event_time", "trade_time"
            )
            row_count = enriched_df.count()
            enriched_df.write.jdbc(url=jdbc_url, table=table_fact_price, mode="append", properties=jdbc_props)
            logger.info("batch=%d wrote %d row(s) to %s", batch_id, row_count, table_fact_price)
        finally:
            batch_df.unpersist()

    return write_to_postgres


def main():
    settings = load_settings()
    kafka_bootstrap = get_kafka_bootstrap_servers(settings)
    pg_params = get_postgres_conn_params(settings)
    jdbc_url = get_postgres_jdbc_url(settings)

    kafka_cfg = settings.get("kafka", {})
    spark_cfg = settings.get("spark", {})
    postgres_cfg = settings.get("postgres", {})

    topic_trades = kafka_cfg.get("topic_trades", KAFKA_TOPIC_TRADES)
    topic_dlq = kafka_cfg.get("topic_trades_dlq", KAFKA_TOPIC_TRADES_DLQ)
    table_fact_price = postgres_cfg.get("table_fact_price", "fact_price")
    table_dim_symbol = postgres_cfg.get("table_dim_symbol", "dim_symbol")
    trigger_interval = f"{spark_cfg.get('trigger_interval_seconds', 10)} seconds"

    spark = build_spark_session(spark_cfg.get("app_name", "binance-streaming"))
    spark.sparkContext.setLogLevel("WARN")

    ensure_tables_exist(pg_params)

    raw_df = read_trade_stream(spark, kafka_bootstrap, topic_trades, spark_cfg.get("starting_offsets", "latest"))
    parsed_df = parse_trade_stream(raw_df)

    valid_df = parsed_df.filter(VALID_FILTER_SQL)
    invalid_df = parsed_df.filter(INVALID_FILTER_SQL)

    active_queries = []

    active_queries.append(
        valid_df.writeStream.outputMode("append")
        .option("checkpointLocation", spark_cfg.get("checkpoint_valid", "data/checkpoints/valid"))
        .trigger(processingTime=trigger_interval)
        .foreachBatch(make_postgres_writer(jdbc_url, pg_params, table_fact_price, table_dim_symbol))
        .start()
    )

    active_queries.append(
        invalid_df.select(to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", topic_dlq)
        .option("checkpointLocation", spark_cfg.get("checkpoint_dlq", "data/checkpoints/dlq"))
        .trigger(processingTime=trigger_interval)
        .start()
    )

    # Optional Stage-5 debug step: raw parsed rows to console, own checkpoint.
    if os.environ.get("SPARK_DEBUG_CONSOLE") == "1":
        active_queries.append(
            parsed_df.writeStream.format("console")
            .option("truncate", False)
            .option("checkpointLocation", spark_cfg.get("checkpoint_console", "data/checkpoints/console"))
            .trigger(processingTime=trigger_interval)
            .start()
        )

    logger.info(
        "Spark streaming started: valid -> postgres.%s, invalid -> kafka topic %s",
        table_fact_price,
        topic_dlq,
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in active_queries:
            if query.isActive:
                query.stop()


if __name__ == "__main__":
    main()
