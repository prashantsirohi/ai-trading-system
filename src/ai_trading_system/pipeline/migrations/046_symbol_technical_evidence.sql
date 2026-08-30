-- Shadow-only, symbol/session technical labels shared by independent setup families.
-- These observations have no execution, publish, admission, or lifecycle authority.
CREATE TABLE IF NOT EXISTS symbol_technical_evidence_observation (
    technical_evidence_observation_id VARCHAR PRIMARY KEY,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    observed_session DATE NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    price DOUBLE,
    sma20 DOUBLE,
    high_52w DOUBLE,
    distance_from_52w_high_pct DOUBLE,
    weekly_gainer_state VARCHAR NOT NULL,
    near_52w_high_10_state VARCHAR NOT NULL,
    above_sma20_state VARCHAR NOT NULL,
    entry_confirmed_state VARCHAR NOT NULL,
    sma20_break_state VARCHAR NOT NULL,
    missing_reasons_json VARCHAR NOT NULL,
    price_basis VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    source_run_id VARCHAR NOT NULL,
    source_stage VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    source_artifact_type VARCHAR NOT NULL,
    source_artifact_path VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    snapshot_json VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_symbol_technical_evidence_idempotency
    ON symbol_technical_evidence_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_symbol_technical_evidence_history
    ON symbol_technical_evidence_observation(
        exchange, symbol_id, observed_session, observed_at
    );

ALTER TABLE candidate_snapshot
    ADD COLUMN IF NOT EXISTS technical_evidence_observation_id VARCHAR;
CREATE INDEX IF NOT EXISTS idx_candidate_snapshot_technical_evidence
    ON candidate_snapshot(technical_evidence_observation_id);
