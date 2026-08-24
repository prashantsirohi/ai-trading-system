CREATE TABLE IF NOT EXISTS jcurve_seed_run (
    run_id VARCHAR PRIMARY KEY,
    screen_id BIGINT NOT NULL,
    screen_url VARCHAR NOT NULL,
    as_of_date DATE NOT NULL,
    policy_version VARCHAR NOT NULL,
    policy_hash VARCHAR NOT NULL,
    query_text VARCHAR NOT NULL,
    query_hash VARCHAR NOT NULL,
    source_artifact_id VARCHAR NOT NULL,
    snapshot_hash VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    source_row_count BIGINT NOT NULL,
    evaluated_count BIGINT NOT NULL,
    resolved_count BIGINT NOT NULL,
    candidate_count BIGINT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS jcurve_seed_candidate (
    run_id VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    company_name VARCHAR,
    screen_member BOOLEAN NOT NULL,
    supplemental_member BOOLEAN NOT NULL,
    company_id VARCHAR,
    security_id VARCHAR,
    listing_id VARCHAR,
    identity_status VARCHAR NOT NULL,
    statement_basis VARCHAR,
    basis_resolution_reason VARCHAR NOT NULL,
    annual_period_count INTEGER NOT NULL,
    aligned_quarter_count INTEGER NOT NULL,
    accounting_visible BOOLEAN NOT NULL,
    build_signal BOOLEAN NOT NULL,
    commissioning_signal BOOLEAN NOT NULL,
    clean_transfer_signal BOOLEAN NOT NULL,
    ramp_signal BOOLEAN NOT NULL,
    disposition VARCHAR NOT NULL,
    accepted BOOLEAN NOT NULL,
    reason_codes_json VARCHAR NOT NULL,
    metrics_json VARCHAR NOT NULL,
    source_artifact_id VARCHAR NOT NULL,
    decision_hash VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, symbol)
);

CREATE INDEX IF NOT EXISTS jcurve_seed_candidate_company_idx
    ON jcurve_seed_candidate(company_id);
