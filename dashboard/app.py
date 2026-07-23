"""Stage 10 - minimal dashboard: price-over-time chart + top-movers table.

Queries PostgreSQL directly (no Kafka/Spark dependency at read time) using
the same common/config.py as every other service, so there is exactly one
place that knows how to build a Postgres connection.
"""
import sys
from pathlib import Path

# Lets `streamlit run dashboard/app.py` work from a plain checkout too, not
# just inside the container where PYTHONPATH=/app/src is already set.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
import psycopg2
import streamlit as st

from common.config import get_postgres_conn_params, load_settings

st.set_page_config(page_title="Binance Market Data Pipeline", layout="wide")


@st.cache_resource
def get_settings():
    return load_settings()


def get_connection():
    return psycopg2.connect(**get_postgres_conn_params(get_settings()))


@st.cache_data(ttl=5)
def load_symbols():
    conn = get_connection()
    try:
        return pd.read_sql("SELECT symbol FROM dim_symbol ORDER BY symbol", conn)["symbol"].tolist()
    finally:
        conn.close()


@st.cache_data(ttl=5)
def load_price_history(symbol: str, lookback_minutes: int) -> pd.DataFrame:
    query = """
        SELECT f.event_time, f.price, f.quantity
        FROM fact_price f
        JOIN dim_symbol d ON d.symbol_id = f.symbol_id
        WHERE d.symbol = %s AND f.event_time >= NOW() - (%s::text || ' minutes')::interval
        ORDER BY f.event_time
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn, params=(symbol, lookback_minutes))
    finally:
        conn.close()


@st.cache_data(ttl=5)
def load_top_movers(lookback_minutes: int) -> pd.DataFrame:
    query = """
        WITH bounds AS (
            SELECT d.symbol,
                   (ARRAY_AGG(f.price ORDER BY f.event_time ASC))[1]  AS first_price,
                   (ARRAY_AGG(f.price ORDER BY f.event_time DESC))[1] AS last_price,
                   MIN(f.price) AS min_price,
                   MAX(f.price) AS max_price,
                   COUNT(*) AS trade_count
            FROM fact_price f
            JOIN dim_symbol d ON d.symbol_id = f.symbol_id
            WHERE f.event_time >= NOW() - (%s::text || ' minutes')::interval
            GROUP BY d.symbol
        ),
        changes AS (
            SELECT symbol, first_price, last_price, min_price, max_price, trade_count,
                   ROUND((100.0 * (last_price - first_price) / NULLIF(first_price, 0))::numeric, 3) AS pct_change
            FROM bounds
        )
        SELECT *
        FROM changes
        ORDER BY ABS(pct_change) DESC NULLS LAST
    """
    conn = get_connection()
    try:
        return pd.read_sql(query, conn, params=(lookback_minutes,))
    finally:
        conn.close()


def main():
    st.title("Binance Market Data Pipeline")
    st.caption("Binance WebSocket -> Kafka -> Spark Structured Streaming -> PostgreSQL -> Dashboard")

    settings = get_settings()
    default_lookback = settings.get("dashboard", {}).get("default_lookback_minutes", 60)

    lookback_minutes = st.sidebar.slider("Lookback window (minutes)", 5, 24 * 60, default_lookback)

    symbols = load_symbols()
    if not symbols:
        st.warning("dim_symbol is empty - run sql/003_seed_dim_symbol.sql or wait for the pipeline to seed it.")
        return

    selected_symbol = st.sidebar.selectbox("Symbol", symbols)

    st.subheader(f"{selected_symbol} price over time")
    history_df = load_price_history(selected_symbol, lookback_minutes)
    if history_df.empty:
        st.info("No trades recorded yet for this symbol/window.")
    else:
        st.line_chart(history_df.set_index("event_time")["price"])
        st.caption(f"{len(history_df):,} trades in the last {lookback_minutes} minutes")

    st.subheader(f"Top movers (last {lookback_minutes} minutes)")
    movers_df = load_top_movers(lookback_minutes)
    st.dataframe(movers_df, use_container_width=True)


if __name__ == "__main__":
    main()
