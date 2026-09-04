-- Phase 3.5 P2: immutable convergence observations and shadow-only outcome
-- evaluation. These tables have no ranking, admission, lifecycle, candidate,
-- execution, or broker consumer.

CREATE TABLE IF NOT EXISTS opportunity_convergence_observation (
    observation_id VARCHAR PRIMARY KEY,
    exchange VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    session_date DATE NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    policy_snapshot_id VARCHAR NOT NULL,
    convergence_policy_version VARCHAR NOT NULL,
    convergence_cohort VARCHAR NOT NULL,
    investigator_member BOOLEAN NOT NULL,
    fundamental_member BOOLEAN NOT NULL,
    pattern_member BOOLEAN NOT NULL,
    sector_name VARCHAR,
    sector_evaluation_state VARCHAR NOT NULL,
    invalidation_price DOUBLE,
    evidence_snapshot_json VARCHAR NOT NULL,
    evidence_hash VARCHAR NOT NULL,
    source_run_id VARCHAR NOT NULL,
    source_stage_attempt INTEGER NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_convergence_observation_key
    ON opportunity_convergence_observation(
        exchange, symbol_id, session_date, policy_snapshot_id
    );
CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_convergence_observation_idempotency
    ON opportunity_convergence_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_opportunity_convergence_observation_session
    ON opportunity_convergence_observation(session_date, convergence_cohort);

CREATE TABLE IF NOT EXISTS opportunity_convergence_anchor (
    anchor_id VARCHAR PRIMARY KEY,
    observation_id VARCHAR NOT NULL,
    anchor_type VARCHAR NOT NULL,
    candidate_id VARCHAR,
    source_event_id VARCHAR,
    anchor_session_date DATE NOT NULL,
    anchor_price DOUBLE,
    anchor_price_basis VARCHAR NOT NULL,
    invalidation_price DOUBLE,
    fill_policy_version VARCHAR,
    source_run_id VARCHAR NOT NULL,
    source_data_hash VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_convergence_anchor_idempotency
    ON opportunity_convergence_anchor(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_opportunity_convergence_anchor_observation
    ON opportunity_convergence_anchor(observation_id, anchor_type);

CREATE TABLE IF NOT EXISTS opportunity_convergence_horizon (
    anchor_id VARCHAR NOT NULL,
    horizon_sessions INTEGER NOT NULL,
    observed_sessions INTEGER NOT NULL,
    target_session_date DATE,
    partial_return_pct DOUBLE,
    return_pct DOUBLE,
    maximum_favourable_excursion_pct DOUBLE,
    maximum_adverse_excursion_pct DOUBLE,
    days_to_2pct INTEGER,
    days_to_5pct INTEGER,
    days_to_stop INTEGER,
    benchmark_symbol VARCHAR,
    benchmark_return_pct DOUBLE,
    benchmark_relative_return_pct DOUBLE,
    sector_index_code VARCHAR,
    sector_return_pct DOUBLE,
    sector_relative_return_pct DOUBLE,
    data_quality_status VARCHAR NOT NULL,
    data_quality_reason VARCHAR,
    outcome_source_hash VARCHAR,
    matured_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    PRIMARY KEY (anchor_id, horizon_sessions)
);
CREATE INDEX IF NOT EXISTS idx_opportunity_convergence_horizon_status
    ON opportunity_convergence_horizon(horizon_sessions, data_quality_status);
