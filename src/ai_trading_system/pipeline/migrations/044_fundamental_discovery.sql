-- Shadow-only fundamental thesis observations. This table is append-only and
-- does not participate in operational candidate or execution selection.
CREATE TABLE IF NOT EXISTS candidate_fundamental_observation (
    observation_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    primary_thesis VARCHAR NOT NULL,
    secondary_theses_json VARCHAR NOT NULL,
    evaluations_json VARCHAR NOT NULL,
    evidence_json VARCHAR NOT NULL,
    blockers_json VARCHAR NOT NULL,
    source_data_hash VARCHAR NOT NULL,
    statement_basis VARCHAR NOT NULL,
    source_report_date DATE,
    source_available_at DATE,
    taxonomy_version VARCHAR NOT NULL,
    rule_version VARCHAR NOT NULL,
    admission_version VARCHAR NOT NULL,
    policy_snapshot_id VARCHAR,
    source_run_id VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_fundamental_observation_idempotency
    ON candidate_fundamental_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_fundamental_observation_candidate
    ON candidate_fundamental_observation(candidate_id, observed_at);
