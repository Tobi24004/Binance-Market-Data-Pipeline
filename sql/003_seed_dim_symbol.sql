-- Initial symbol list (kept in sync with config/settings.yaml `symbols`).
-- dags/update_dim_symbol.py keeps this table current afterwards.
INSERT INTO dim_symbol (symbol, base_asset, quote_asset) VALUES
    ('BTCUSDT', 'BTC', 'USDT'),
    ('ETHUSDT', 'ETH', 'USDT'),
    ('BNBUSDT', 'BNB', 'USDT')
ON CONFLICT (symbol) DO NOTHING;
