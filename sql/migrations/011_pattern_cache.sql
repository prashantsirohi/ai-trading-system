CREATE TABLE IF NOT EXISTS pattern_cache (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL DEFAULT 'NSE',
    pattern_family VARCHAR NOT NULL,
    pattern_state VARCHAR NOT NULL,
    stage2_score DOUBLE,
    stage2_label VARCHAR,
    signal_date DATE NOT NULL,
    breakout_level DOUBLE,
    watchlist_trigger_level DOUBLE,
    invalidation_price DOUBLE,
    pattern_score DOUBLE,
    setup_quality DOUBLE,
    width_bars INTEGER,
    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scan_run_id VARCHAR,
    payload_json VARCHAR,
    PRIMARY KEY (symbol_id, exchange, pattern_family, pattern_state, signal_date)
);

CREATE INDEX IF NOT EXISTS idx_pattern_cache_signal_date
    ON pattern_cache (signal_date);

CREATE INDEX IF NOT EXISTS idx_pattern_cache_stage2
    ON pattern_cache (stage2_score, pattern_state);
