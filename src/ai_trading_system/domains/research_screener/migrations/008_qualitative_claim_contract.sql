CREATE TABLE IF NOT EXISTS qualitative_claim (
    claim_id VARCHAR PRIMARY KEY,
    research_run_id VARCHAR NOT NULL,
    document_id VARCHAR NOT NULL,
    company_id VARCHAR NOT NULL,
    security_id VARCHAR NOT NULL,
    isin VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    topic VARCHAR NOT NULL,
    metric_name VARCHAR NOT NULL,
    claim_kind VARCHAR NOT NULL,
    claim_text VARCHAR NOT NULL,
    exact_excerpt VARCHAR NOT NULL,
    page INTEGER NOT NULL,
    source_artifact_id VARCHAR NOT NULL,
    source_content_hash VARCHAR NOT NULL,
    published_at TIMESTAMP NOT NULL,
    fiscal_period VARCHAR NOT NULL,
    statement_scope VARCHAR NOT NULL,
    value DOUBLE,
    unit VARCHAR,
    currency VARCHAR,
    materiality VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    claim_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS qualitative_claim_review (
    review_id VARCHAR PRIMARY KEY,
    claim_id VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    reviewer_role VARCHAR NOT NULL,
    decision VARCHAR NOT NULL,
    reviewer_model VARCHAR NOT NULL,
    prompt_version VARCHAR NOT NULL,
    independent_context BOOLEAN NOT NULL,
    normalized_claim_hash VARCHAR NOT NULL,
    issue_codes_json VARCHAR NOT NULL,
    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    request_id VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    review_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (claim_id, reviewer_role, reviewer_model, prompt_version)
);

CREATE TABLE IF NOT EXISTS qualitative_claim_policy_decision (
    decision_id VARCHAR PRIMARY KEY,
    claim_id VARCHAR NOT NULL,
    contract_version VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    reason_codes_json VARCHAR NOT NULL,
    extraction_review_id VARCHAR,
    verification_review_id VARCHAR,
    decided_at TIMESTAMP NOT NULL
);
