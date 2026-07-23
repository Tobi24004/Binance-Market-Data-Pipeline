-- Star schema: Dim_Symbol (dimension) + Fact_Price (fact).
-- This is the authoritative schema source; Spark only CREATE TABLE IF NOT
-- EXISTS as a fallback on startup (see src/streaming/spark_stream.py).

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
