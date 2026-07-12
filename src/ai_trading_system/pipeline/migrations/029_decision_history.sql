-- Durable decision-layer history.  Feature/OHLCV storage remains unchanged.

CREATE TABLE IF NOT EXISTS rank_history (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    universe_id VARCHAR NOT NULL,
    rank_position INTEGER,
    rank_percentile DOUBLE,
    composite_score DOUBLE,
    rs_score DOUBLE,
    volume_score DOUBLE,
    trend_score DOUBLE,
    proximity_score DOUBLE,
    sector_score DOUBLE,
    rank_model_version VARCHAR NOT NULL,
    rank_formula_name VARCHAR NOT NULL,
    rank_config_hash VARCHAR NOT NULL,
    pipeline_run_id VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rank_history
    ON rank_history(symbol_id, exchange, trade_date, universe_id, rank_model_version);

CREATE TABLE IF NOT EXISTS stage_history (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    stage_family VARCHAR,
    stage_label VARCHAR,
    stage_confidence DOUBLE,
    stage_input_complete BOOLEAN,
    stage_input_completeness_pct DOUBLE,
    stage_input_missing_fields VARCHAR,
    stage_input_confidence VARCHAR,
    stage_sma200_source VARCHAR,
    stage_sma50_slope_source VARCHAR,
    stage_sma200_slope_source VARCHAR,
    stage_near_high_source VARCHAR,
    close DOUBLE,
    sma_50 DOUBLE,
    sma_200 DOUBLE,
    sma50_slope_20d_pct DOUBLE,
    sma200_slope_20d_pct DOUBLE,
    near_52w_high_pct DOUBLE,
    stage_reason VARCHAR,
    stage_model_version VARCHAR NOT NULL,
    stage_config_hash VARCHAR NOT NULL,
    pipeline_run_id VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stage_history
    ON stage_history(symbol_id, exchange, trade_date, stage_model_version);

CREATE TABLE IF NOT EXISTS stage1_history (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    stage1_formula_name VARCHAR,
    stage1_model_version VARCHAR NOT NULL,
    stage1_config_hash VARCHAR NOT NULL,
    stage1_model_status VARCHAR,
    stage1_maturity_score DOUBLE,
    stage1_score_band VARCHAR,
    stage1_substate VARCHAR,
    stage1_eligible BOOLEAN,
    stage1_block_reasons VARCHAR,
    stage1_emerging_score DOUBLE,
    stage1_emerging_rank INTEGER,
    structural_repair_score DOUBLE,
    accumulation_score DOUBLE,
    rs_acceleration_score DOUBLE,
    base_quality_score DOUBLE,
    sector_rotation_score DOUBLE,
    pattern_readiness_score DOUBLE,
    golden_cross_progression_score DOUBLE,
    stage1_bonus_score DOUBLE,
    stage1_penalty_score DOUBLE,
    stage1_adjustment_reasons VARCHAR,
    stage1_data_completeness_pct DOUBLE,
    stage1_missing_components VARCHAR,
    stage1_score_confidence VARCHAR,
    golden_cross_status VARCHAR,
    golden_cross_quality DOUBLE,
    golden_cross_imminent BOOLEAN,
    golden_cross_days_since INTEGER,
    sma50_above_sma200 BOOLEAN,
    ma_gap_pct DOUBLE,
    ma_gap_delta_5d DOUBLE,
    ma_gap_delta_20d DOUBLE,
    ma_gap_delta_60d DOUBLE,
    ma_gap_closing_flag BOOLEAN,
    ma_gap_quality_flag VARCHAR,
    pattern_family VARCHAR,
    pattern_state VARCHAR,
    pattern_promotion_state VARCHAR,
    pattern_score DOUBLE,
    setup_quality VARCHAR,
    promotion_eligibility BOOLEAN,
    promotion_block_reasons VARCHAR,
    stage1_operational_status VARCHAR,
    distance_to_pivot_pct DOUBLE,
    relative_strength DOUBLE,
    relative_strength_delta_5d DOUBLE,
    relative_strength_delta_20d DOUBLE,
    relative_strength_delta_60d DOUBLE,
    accumulation_day_count_20d INTEGER,
    distribution_day_count_20d INTEGER,
    up_down_volume_ratio_20d DOUBLE,
    sector_strength DOUBLE,
    sector_strength_delta_20d DOUBLE,
    sector_rank INTEGER,
    sector_rank_delta_20d INTEGER,
    pipeline_run_id VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stage1_history
    ON stage1_history(symbol_id, exchange, trade_date, stage1_model_version);

CREATE TABLE IF NOT EXISTS pattern_history (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    pattern_family VARCHAR NOT NULL,
    pattern_state VARCHAR,
    pattern_score DOUBLE,
    setup_quality VARCHAR,
    pattern_promotion_state VARCHAR,
    pivot_price DOUBLE,
    distance_to_pivot_pct DOUBLE,
    breakout_status VARCHAR,
    breakout_attempt_flag BOOLEAN,
    pattern_model_version VARCHAR NOT NULL,
    pattern_config_hash VARCHAR NOT NULL,
    pipeline_run_id VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pattern_history
    ON pattern_history(symbol_id, exchange, trade_date, pattern_family, pattern_model_version);

-- The existing table remains the dated lifecycle snapshot history.
DROP INDEX IF EXISTS uq_investigator_stage1_state_symbol_date;
DROP INDEX IF EXISTS idx_investigator_stage1_state_active;
ALTER TABLE investigator_stage1_state ADD COLUMN IF NOT EXISTS stage1_lifecycle_model_version VARCHAR;
ALTER TABLE investigator_stage1_state ADD COLUMN IF NOT EXISTS stage1_lifecycle_config_hash VARCHAR;
ALTER TABLE investigator_stage1_state ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE investigator_stage1_state ADD COLUMN IF NOT EXISTS stage1_days_in_lifecycle_state INTEGER;
ALTER TABLE investigator_stage1_state ADD COLUMN IF NOT EXISTS stage1_days_since_first_seen INTEGER;
UPDATE investigator_stage1_state SET exchange = 'NSE' WHERE exchange IS NULL OR exchange = '';
UPDATE investigator_stage1_state SET stage1_lifecycle_model_version = 'legacy'
    WHERE stage1_lifecycle_model_version IS NULL OR stage1_lifecycle_model_version = '';
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_stage1_state_history
    ON investigator_stage1_state(symbol_id, exchange, trade_date, stage1_lifecycle_model_version);

CREATE TABLE IF NOT EXISTS investigator_stage1_current (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    as_of_trade_date DATE NOT NULL,
    stage1_lifecycle_state VARCHAR NOT NULL,
    stage1_previous_lifecycle_state VARCHAR,
    stage1_substate VARCHAR,
    stage1_previous_substate VARCHAR,
    stage1_maturity_score DOUBLE,
    stage1_score_peak DOUBLE,
    stage1_score_floor_since_entry DOUBLE,
    stage1_emerging_score DOUBLE,
    stage1_emerging_rank INTEGER,
    stage1_emerging_rank_best INTEGER,
    stage1_first_seen_date DATE,
    stage1_last_seen_date DATE,
    stage1_state_entry_date DATE,
    stage1_last_transition_date DATE,
    stage1_days_in_lifecycle_state INTEGER,
    stage1_days_since_first_seen INTEGER,
    stage1_evaluation_status VARCHAR,
    golden_cross_status VARCHAR,
    golden_cross_status_previous VARCHAR,
    golden_cross_status_change_date DATE,
    pattern_promotion_state VARCHAR,
    pattern_state VARCHAR,
    distance_to_pivot_pct DOUBLE,
    distance_to_pivot_best DOUBLE,
    invalidation_date DATE,
    invalidation_price DOUBLE,
    invalidation_reason VARCHAR,
    stale_since_date DATE,
    stale_reason VARCHAR,
    regression_reason VARCHAR,
    candidate_sources VARCHAR,
    primary_candidate_source VARCHAR,
    lifecycle_model_version VARCHAR NOT NULL,
    lifecycle_config_hash VARCHAR NOT NULL,
    pipeline_run_id VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(symbol_id, exchange)
);

INSERT INTO investigator_stage1_current (
    symbol_id, exchange, as_of_trade_date, stage1_lifecycle_state,
    stage1_previous_lifecycle_state, stage1_substate, stage1_previous_substate,
    stage1_maturity_score, stage1_score_peak, stage1_emerging_score,
    stage1_emerging_rank, stage1_emerging_rank_best, stage1_first_seen_date,
    stage1_last_seen_date, stage1_state_entry_date, stage1_last_transition_date,
    stage1_days_in_lifecycle_state, stage1_days_since_first_seen,
    stage1_evaluation_status, golden_cross_status, golden_cross_status_previous,
    pattern_promotion_state, pattern_state, distance_to_pivot_pct,
    invalidation_price, invalidation_reason, stale_reason, regression_reason,
    candidate_sources, primary_candidate_source, lifecycle_model_version,
    lifecycle_config_hash, pipeline_run_id, source_attempt
)
SELECT history.symbol_id, history.exchange, history.trade_date, history.stage1_lifecycle_state,
       stage1_previous_lifecycle_state, stage1_substate, stage1_previous_substate,
       stage1_maturity_score, stage1_score_peak, stage1_emerging_score,
       stage1_emerging_rank, stage1_emerging_rank_best, stage1_first_seen_date,
       stage1_last_seen_date, stage1_state_entry_date, stage1_last_transition_date,
       stage1_days_in_lifecycle_state, stage1_days_since_first_seen,
       stage1_evaluation_status, golden_cross_status, golden_cross_status_previous,
       pattern_promotion_state, pattern_state, distance_to_pivot_pct,
       invalidation_price, invalidation_reason, stale_reason, regression_reason,
       candidate_sources, primary_candidate_source, stage1_lifecycle_model_version,
       COALESCE(stage1_lifecycle_config_hash, 'legacy'), COALESCE(pipeline_run_id, run_id),
       COALESCE(attempt_number, 0)
FROM (
    SELECT * FROM investigator_stage1_state
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol_id, exchange ORDER BY trade_date DESC, updated_at DESC NULLS LAST
    ) = 1
) AS history
WHERE NOT EXISTS (
    SELECT 1 FROM investigator_stage1_current current_state
    WHERE current_state.symbol_id = history.symbol_id
      AND current_state.exchange = history.exchange
);

ALTER TABLE investigator_stage1_transition ADD COLUMN IF NOT EXISTS transition_id VARCHAR;
ALTER TABLE investigator_stage1_transition ADD COLUMN IF NOT EXISTS stage1_lifecycle_model_version VARCHAR;
ALTER TABLE investigator_stage1_transition ADD COLUMN IF NOT EXISTS stage1_lifecycle_config_hash VARCHAR;
ALTER TABLE investigator_stage1_transition ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR;
ALTER TABLE investigator_stage1_transition ADD COLUMN IF NOT EXISTS distance_to_pivot_pct DOUBLE;
ALTER TABLE investigator_stage1_transition ADD COLUMN IF NOT EXISTS invalidation_price DOUBLE;
UPDATE investigator_stage1_transition SET exchange = 'NSE' WHERE exchange IS NULL OR exchange = '';
UPDATE investigator_stage1_transition SET stage1_lifecycle_model_version = 'legacy'
    WHERE stage1_lifecycle_model_version IS NULL OR stage1_lifecycle_model_version = '';
UPDATE investigator_stage1_transition SET transition_id =
    md5(symbol_id || '|' || exchange || '|' || CAST(trade_date AS VARCHAR) || '|' ||
        COALESCE(from_lifecycle_state, '') || '|' || to_lifecycle_state || '|' || transition_type || '|' ||
        stage1_lifecycle_model_version || '|legacy|' || COALESCE(run_id, '') || '|' ||
        COALESCE(CAST(attempt_number AS VARCHAR), ''))
    WHERE transition_id IS NULL
       OR transition_id IN (
            SELECT duplicate_id FROM (
                SELECT transition_id AS duplicate_id
                FROM investigator_stage1_transition
                WHERE transition_id IS NOT NULL
                GROUP BY transition_id
                HAVING COUNT(*) > 1
            ) duplicated
       );
DROP INDEX IF EXISTS uq_investigator_stage1_transition_event;
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_stage1_transition_id
    ON investigator_stage1_transition(transition_id);

CREATE OR REPLACE VIEW investigator_stage1_current_derived AS
SELECT * EXCLUDE (_rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY symbol_id, exchange ORDER BY trade_date DESC, updated_at DESC NULLS LAST
    ) AS _rn
    FROM investigator_stage1_state
) WHERE _rn = 1;
