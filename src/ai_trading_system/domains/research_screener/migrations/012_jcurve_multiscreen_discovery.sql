CREATE TABLE IF NOT EXISTS jcurve_discovery_run (
    run_id VARCHAR PRIMARY KEY,
    as_of_date DATE NOT NULL,
    universe_run_id VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    policy_hash VARCHAR NOT NULL,
    cohort_version VARCHAR NOT NULL,
    cohort_hash VARCHAR NOT NULL,
    snapshot_hash VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    union_count BIGINT NOT NULL,
    universe_eligible_count BIGINT NOT NULL,
    focus_count BIGINT NOT NULL,
    evaluated_count BIGINT NOT NULL,
    primary_queue_count BIGINT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS jcurve_discovery_screen (
    run_id VARCHAR NOT NULL,
    screen_id BIGINT NOT NULL,
    lane VARCHAR NOT NULL,
    screen_url VARCHAR NOT NULL,
    query_text VARCHAR NOT NULL,
    query_hash VARCHAR NOT NULL,
    source_artifact_id VARCHAR NOT NULL,
    source_row_count BIGINT NOT NULL,
    acquisition_mode VARCHAR NOT NULL,
    PRIMARY KEY (run_id, screen_id)
);

CREATE TABLE IF NOT EXISTS jcurve_discovery_candidate (
    run_id VARCHAR NOT NULL,
    candidate_key VARCHAR NOT NULL,
    company_name VARCHAR,
    isin VARCHAR,
    nse_symbol VARCHAR,
    bse_code VARCHAR,
    company_id VARCHAR,
    security_id VARCHAR,
    listing_id VARCHAR,
    identity_status VARCHAR NOT NULL,
    matched_screen_ids_json VARCHAR NOT NULL,
    matched_lanes_json VARCHAR NOT NULL,
    screen_match_count INTEGER NOT NULL,
    baseline_member BOOLEAN NOT NULL,
    baseline_case_role VARCHAR,
    universe_status VARCHAR NOT NULL,
    focus_eligible BOOLEAN NOT NULL,
    statement_basis VARCHAR,
    basis_resolution_reason VARCHAR,
    accounting_disposition VARCHAR,
    queue_disposition VARCHAR NOT NULL,
    queue_rank INTEGER,
    reason_codes_json VARCHAR NOT NULL,
    metrics_json VARCHAR NOT NULL,
    decision_hash VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, candidate_key)
);

CREATE INDEX IF NOT EXISTS jcurve_discovery_candidate_company_idx
    ON jcurve_discovery_candidate(company_id);
