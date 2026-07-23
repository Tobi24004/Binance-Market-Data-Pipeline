-- Dashboard queries filter/group by symbol + time range constantly, so this
-- is the index that matters most. symbol_id is the normalized stand-in for
-- symbol here (fact_price has no symbol column - it references dim_symbol).
CREATE INDEX IF NOT EXISTS idx_fact_price_symbol_event_time
    ON fact_price (symbol_id, event_time);
