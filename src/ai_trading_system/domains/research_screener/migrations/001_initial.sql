CREATE TABLE IF NOT EXISTS schema_migration (
    version VARCHAR PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS source_registry (
    source_key VARCHAR NOT NULL, registry_version VARCHAR NOT NULL, policy_json VARCHAR NOT NULL,
    policy_hash VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, PRIMARY KEY(source_key, registry_version)
);
CREATE TABLE IF NOT EXISTS ingestion_run (
    ingestion_run_id VARCHAR PRIMARY KEY, screening_run_id VARCHAR, source_key VARCHAR NOT NULL,
    effective_date DATE NOT NULL, started_at TIMESTAMP NOT NULL, ended_at TIMESTAMP,
    status VARCHAR NOT NULL, error_code VARCHAR, error_message VARCHAR
);
CREATE TABLE IF NOT EXISTS source_artifact (
    artifact_id VARCHAR PRIMARY KEY, ingestion_run_id VARCHAR, source_key VARCHAR NOT NULL,
    provider VARCHAR NOT NULL, source_url VARCHAR, local_dataset_id VARCHAR, effective_date DATE,
    published_at TIMESTAMP, retrieved_at TIMESTAMP NOT NULL, content_hash VARCHAR NOT NULL,
    byte_count BIGINT NOT NULL, row_count BIGINT, parser_version VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL, validation_status VARCHAR NOT NULL, parent_artifact_id VARCHAR,
    metadata_json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS dataset_snapshot (
    snapshot_id VARCHAR PRIMARY KEY, screening_run_id VARCHAR NOT NULL, dataset_name VARCHAR NOT NULL,
    as_of_date DATE NOT NULL, snapshot_hash VARCHAR NOT NULL, row_count BIGINT NOT NULL,
    artifact_ids_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS data_quality_issue (
    issue_id VARCHAR PRIMARY KEY, screening_run_id VARCHAR NOT NULL, company_id VARCHAR,
    security_id VARCHAR, domain VARCHAR NOT NULL, code VARCHAR NOT NULL, severity VARCHAR NOT NULL,
    state VARCHAR NOT NULL, message VARCHAR NOT NULL, source_artifact_id VARCHAR, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS data_repair_queue (
    repair_id VARCHAR PRIMARY KEY, screening_run_id VARCHAR NOT NULL, company_id VARCHAR,
    domain VARCHAR NOT NULL, reason_code VARCHAR NOT NULL, status VARCHAR NOT NULL,
    required_action VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS company_master (
    company_id VARCHAR PRIMARY KEY, legal_name VARCHAR NOT NULL, company_type VARCHAR NOT NULL,
    valid_from DATE NOT NULL, valid_to DATE, source_artifact_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS security_master (
    security_id VARCHAR PRIMARY KEY, company_id VARCHAR NOT NULL, isin VARCHAR NOT NULL,
    security_name VARCHAR NOT NULL, instrument_type VARCHAR NOT NULL, face_value DOUBLE,
    currency VARCHAR NOT NULL, valid_from DATE NOT NULL, valid_to DATE, source_artifact_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS listing_master (
    listing_id VARCHAR PRIMARY KEY, security_id VARCHAR NOT NULL, exchange VARCHAR NOT NULL,
    exchange_security_id VARCHAR, symbol VARCHAR, bse_code VARCHAR, series VARCHAR, board VARCHAR,
    active_flag BOOLEAN NOT NULL, listing_date DATE, delisting_date DATE, valid_from DATE NOT NULL,
    valid_to DATE, source_artifact_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS security_identifier_history (
    identifier_history_id VARCHAR PRIMARY KEY, security_id VARCHAR NOT NULL, identifier_type VARCHAR NOT NULL,
    identifier_value VARCHAR NOT NULL, exchange VARCHAR, valid_from DATE NOT NULL, valid_to DATE,
    source_artifact_id VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS corporate_action_version (
    version_id VARCHAR PRIMARY KEY, security_id VARCHAR NOT NULL, through_date DATE NOT NULL,
    adjustment_version VARCHAR, validation_status VARCHAR NOT NULL, snapshot_hash VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS financial_statement_version (
    version_id VARCHAR PRIMARY KEY, company_id VARCHAR NOT NULL, security_id VARCHAR NOT NULL,
    period_type VARCHAR NOT NULL, period_end DATE NOT NULL, statement_scope VARCHAR NOT NULL,
    available_from DATE NOT NULL, source_artifact_id VARCHAR, source_row_hash VARCHAR,
    completeness DOUBLE, normalization_status VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS screen_definition (
    definition_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL UNIQUE, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS screen_definition_version (
    definition_version_id VARCHAR PRIMARY KEY, definition_id VARCHAR NOT NULL, semantic_version VARCHAR NOT NULL,
    rules_json VARCHAR NOT NULL, rules_hash VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL,
    UNIQUE(definition_id, semantic_version)
);
CREATE TABLE IF NOT EXISTS screening_run (
    run_id VARCHAR PRIMARY KEY, definition_version_id VARCHAR NOT NULL, run_mode VARCHAR NOT NULL,
    as_of_date DATE NOT NULL, financial_cutoff DATE NOT NULL, price_cutoff DATE NOT NULL,
    min_market_cap_cr DOUBLE NOT NULL, max_market_cap_cr DOUBLE NOT NULL,
    input_snapshot_hash VARCHAR, code_version VARCHAR, status VARCHAR NOT NULL,
    eligible_count BIGINT, evaluated_count BIGINT, started_at TIMESTAMP NOT NULL, ended_at TIMESTAMP,
    error_code VARCHAR, error_message VARCHAR, supersedes_run_id VARCHAR
);
CREATE TABLE IF NOT EXISTS universe_snapshot (
    universe_snapshot_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL UNIQUE, snapshot_hash VARCHAR NOT NULL,
    member_count BIGINT NOT NULL, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS universe_member (
    member_id VARCHAR PRIMARY KEY, universe_snapshot_id VARCHAR NOT NULL, company_id VARCHAR NOT NULL,
    security_id VARCHAR NOT NULL, fixture_symbol VARCHAR NOT NULL, identity_status VARCHAR NOT NULL,
    market_cap_cr DOUBLE, market_cap_as_of DATE, market_cap_status VARCHAR NOT NULL,
    statement_scope VARCHAR NOT NULL, annual_completeness DOUBLE, quarterly_completeness DOUBLE,
    corporate_action_status VARCHAR NOT NULL, data_confidence DOUBLE NOT NULL,
    technical_status VARCHAR NOT NULL, disposition VARCHAR NOT NULL, input_json VARCHAR NOT NULL,
    UNIQUE(universe_snapshot_id, fixture_symbol)
);
CREATE TABLE IF NOT EXISTS factor_definition (
    factor_definition_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, version VARCHAR NOT NULL,
    domain VARCHAR NOT NULL, formula_json VARCHAR NOT NULL, UNIQUE(name, version)
);
CREATE TABLE IF NOT EXISTS factor_snapshot (
    factor_snapshot_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, member_id VARCHAR NOT NULL,
    factor_definition_id VARCHAR NOT NULL, raw_value DOUBLE, value_state VARCHAR NOT NULL,
    score DOUBLE, confidence DOUBLE NOT NULL, source_artifact_ids_json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS archetype_definition (
    archetype_definition_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, version VARCHAR NOT NULL,
    rules_json VARCHAR NOT NULL, UNIQUE(name, version)
);
CREATE TABLE IF NOT EXISTS archetype_match (
    archetype_match_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, member_id VARCHAR NOT NULL,
    archetype_definition_id VARCHAR NOT NULL, matched BOOLEAN NOT NULL, evidence_json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS archetype_score (
    archetype_score_id VARCHAR PRIMARY KEY, archetype_match_id VARCHAR NOT NULL, score DOUBLE,
    confidence DOUBLE NOT NULL, score_state VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS candidate_decision (
    decision_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, member_id VARCHAR NOT NULL UNIQUE,
    fundamental_score DOUBLE, data_confidence_score DOUBLE NOT NULL, technical_status VARCHAR NOT NULL,
    disposition VARCHAR NOT NULL, decision_hash VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS decision_reason (
    reason_id VARCHAR PRIMARY KEY, decision_id VARCHAR NOT NULL, ordinal INTEGER NOT NULL,
    reason_code VARCHAR NOT NULL, message VARCHAR NOT NULL, source_artifact_ids_json VARCHAR NOT NULL,
    UNIQUE(decision_id, ordinal)
);
CREATE TABLE IF NOT EXISTS boundary_review (
    review_id VARCHAR PRIMARY KEY, decision_id VARCHAR NOT NULL, review_status VARCHAR NOT NULL,
    trigger_codes_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS screening_run_comparison (
    comparison_id VARCHAR PRIMARY KEY, left_run_id VARCHAR NOT NULL, right_run_id VARCHAR NOT NULL,
    comparison_hash VARCHAR NOT NULL, summary_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL
);

-- Extension-ready research history. Qualitative automation is intentionally absent.
CREATE TABLE IF NOT EXISTS research_document (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS research_evidence (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS management_statement (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS investment_thesis (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS thesis_version (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS thesis_risk (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS thesis_invalidation (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS forward_estimate (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS valuation_scenario (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS quarterly_kpi_definition (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS quarterly_kpi_observation (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS quarterly_review (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS technical_episode (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
CREATE TABLE IF NOT EXISTS company_status_history (record_id VARCHAR PRIMARY KEY, company_id VARCHAR, effective_at TIMESTAMP, payload_json VARCHAR, source_artifact_id VARCHAR);
