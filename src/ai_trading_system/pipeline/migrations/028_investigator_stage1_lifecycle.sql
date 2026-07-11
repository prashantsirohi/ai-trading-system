CREATE TABLE IF NOT EXISTS investigator_stage1_state (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR,
    trade_date DATE NOT NULL,
    stage1_lifecycle_state VARCHAR NOT NULL,
    stage1_previous_lifecycle_state VARCHAR,
    stage1_substate VARCHAR,
    stage1_previous_substate VARCHAR,
    stage1_maturity_score DOUBLE,
    stage1_score_peak DOUBLE,
    stage1_score_delta_5d DOUBLE,
    stage1_score_delta_20d DOUBLE,
    stage1_emerging_score DOUBLE,
    stage1_emerging_rank INTEGER,
    stage1_emerging_rank_best INTEGER,
    emerging_rank_improvement_5d DOUBLE,
    emerging_rank_improvement_20d DOUBLE,
    stage1_first_seen_date DATE,
    stage1_last_seen_date DATE,
    stage1_state_entry_date DATE,
    stage1_last_transition_date DATE,
    pattern_promotion_state VARCHAR,
    pattern_state VARCHAR,
    pattern_score DOUBLE,
    golden_cross_status VARCHAR,
    golden_cross_status_previous VARCHAR,
    sma50_sma200_gap_pct DOUBLE,
    sma50_sma200_gap_delta_20d DOUBLE,
    distance_to_pivot_pct DOUBLE,
    invalidation_price DOUBLE,
    invalidation_reason VARCHAR,
    stale_reason VARCHAR,
    regression_reason VARCHAR,
    stage1_eligible BOOLEAN,
    stage1_block_reasons VARCHAR,
    stage1_evaluation_status VARCHAR,
    stage1_lifecycle_reason_codes VARCHAR,
    candidate_sources VARCHAR,
    primary_candidate_source VARCHAR,
    stage1_lifecycle_model_version VARCHAR,
    stage1_lifecycle_config_hash VARCHAR,
    execution_eligible BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS investigator_stage1_transition (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR,
    trade_date DATE NOT NULL,
    from_lifecycle_state VARCHAR,
    to_lifecycle_state VARCHAR NOT NULL,
    from_stage1_substate VARCHAR,
    to_stage1_substate VARCHAR,
    stage1_score_before DOUBLE,
    stage1_score_after DOUBLE,
    emerging_rank_before INTEGER,
    emerging_rank_after INTEGER,
    golden_cross_status_before VARCHAR,
    golden_cross_status_after VARCHAR,
    pattern_promotion_state_before VARCHAR,
    pattern_promotion_state_after VARCHAR,
    transition_reason_codes VARCHAR,
    transition_summary VARCHAR,
    candidate_sources VARCHAR,
    transition_type VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_stage1_state_symbol_date
    ON investigator_stage1_state(symbol_id, trade_date);
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_stage1_transition_event
    ON investigator_stage1_transition(symbol_id, trade_date, from_lifecycle_state, to_lifecycle_state, transition_type);
CREATE INDEX IF NOT EXISTS idx_investigator_stage1_state_active
    ON investigator_stage1_state(stage1_lifecycle_state, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_stage1_transition_symbol_date
    ON investigator_stage1_transition(symbol_id, trade_date);
