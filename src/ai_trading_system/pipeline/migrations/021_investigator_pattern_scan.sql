-- Migration 021: persist investigator-owned S1 pattern scan history

CREATE TABLE IF NOT EXISTS investigator_pattern_scan (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    trade_date DATE,
    symbol_id VARCHAR,
    pattern_family VARCHAR,
    pattern_state VARCHAR,
    pattern_lifecycle_state VARCHAR,
    pattern_score DOUBLE,
    setup_quality DOUBLE,
    stage2_score DOUBLE,
    stage2_label VARCHAR,
    breakout_level DOUBLE,
    watchlist_trigger_level DOUBLE,
    invalidation_price DOUBLE,
    is_strong_volume_confirmation BOOLEAN,
    is_combined_volume_confirmation BOOLEAN,
    breakout_volume_ratio DOUBLE,
    s1_promotion_state VARCHAR,
    promotion_reason VARCHAR,
    trigger_reason VARCHAR,
    investigator_status VARCHAR,
    investigator_verdict VARCHAR,
    investigator_final_score DOUBLE,
    source_investigator BOOLEAN,
    source_ranked BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_investigator_pattern_scan_symbol_date
    ON investigator_pattern_scan(symbol_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_pattern_scan_run_attempt
    ON investigator_pattern_scan(run_id, attempt_number);
