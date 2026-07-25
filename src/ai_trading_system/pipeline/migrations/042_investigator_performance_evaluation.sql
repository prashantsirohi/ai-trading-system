-- Point-in-time Investigator attribution and two-event performance evaluation.

ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS stage_label VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS stage_confidence DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS pattern_family VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS pattern_state VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS setup_quality_bucket VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS breakout_type VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS candidate_tier VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS qualified_breakout BOOLEAN;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS confirmed_regime VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS raw_regime VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS regime_confidence DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS breadth_velocity_bucket VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS breadth_velocity_quantile VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS regime_score_chg_5d DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS sector_relative_strength_bucket VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_context_as_of TIMESTAMP;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_attribution_mode VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_missing_fields_json VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_context_json VARCHAR;

CREATE TABLE IF NOT EXISTS investigator_performance_event (
    event_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    sector_name VARCHAR,
    overlap_group_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    event_at TIMESTAMP NOT NULL,
    session_date DATE NOT NULL,
    anchor_price DOUBLE,
    anchor_price_basis VARCHAR NOT NULL,
    source_snapshot_id VARCHAR NOT NULL,
    source_transition_id VARCHAR,
    attribution_mode VARCHAR NOT NULL,
    primary_eligible BOOLEAN NOT NULL,
    lifecycle_evaluable BOOLEAN NOT NULL DEFAULT TRUE,
    context_as_of TIMESTAMP,
    context_json VARCHAR NOT NULL,
    source_run_id VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    data_quality_status VARCHAR NOT NULL,
    data_quality_reason VARCHAR,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_performance_event_idempotency
    ON investigator_performance_event(idempotency_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_performance_event_episode_type
    ON investigator_performance_event(candidate_id, event_type);
CREATE INDEX IF NOT EXISTS idx_investigator_performance_event_date
    ON investigator_performance_event(session_date, event_type);
CREATE INDEX IF NOT EXISTS idx_investigator_performance_event_symbol
    ON investigator_performance_event(exchange, symbol_id, session_date);

CREATE TABLE IF NOT EXISTS investigator_performance_horizon (
    event_id VARCHAR NOT NULL,
    horizon_sessions INTEGER NOT NULL,
    target_session_date DATE,
    close_to_close_return_pct DOUBLE,
    next_open_entry_return_pct DOUBLE,
    maximum_favourable_excursion_pct DOUBLE,
    maximum_adverse_excursion_pct DOUBLE,
    days_to_2pct INTEGER,
    days_to_5pct INTEGER,
    drawdown_before_2pct_pct DOUBLE,
    drawdown_before_5pct_pct DOUBLE,
    benchmark_symbol VARCHAR,
    benchmark_return_pct DOUBLE,
    benchmark_relative_return_pct DOUBLE,
    sector_index_code VARCHAR,
    sector_return_pct DOUBLE,
    sector_relative_return_pct DOUBLE,
    lifecycle_outcome VARCHAR,
    data_quality_status VARCHAR NOT NULL,
    data_quality_reason VARCHAR,
    matured_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    PRIMARY KEY (event_id, horizon_sessions)
);
CREATE INDEX IF NOT EXISTS idx_investigator_performance_horizon_status
    ON investigator_performance_horizon(horizon_sessions, data_quality_status);

ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS stage_confidence DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS confirmed_regime VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS raw_regime VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS regime_confidence DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS breadth_velocity_bucket VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS breadth_velocity_quantile VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS regime_score_chg_5d DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS sector_relative_strength_bucket VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS context_as_of TIMESTAMP;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS attribution_mode VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS missing_fields_json VARCHAR;
