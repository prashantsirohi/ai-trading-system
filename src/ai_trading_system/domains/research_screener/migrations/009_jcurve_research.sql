CREATE TABLE IF NOT EXISTS jcurve_research_run (
    run_id VARCHAR PRIMARY KEY,
    run_type VARCHAR NOT NULL,
    parent_run_id VARCHAR,
    as_of_date DATE NOT NULL,
    policy_version VARCHAR NOT NULL,
    policy_hash VARCHAR NOT NULL,
    snapshot_hash VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    announcement_count BIGINT NOT NULL,
    claim_count BIGINT NOT NULL,
    episode_count BIGINT NOT NULL,
    degraded_reason VARCHAR,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS jcurve_announcement (
    announcement_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    upstream_raw_event_id BIGINT,
    upstream_event_hash VARCHAR NOT NULL,
    company_id VARCHAR,
    security_id VARCHAR,
    listing_id VARCHAR,
    identity_status VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    external_id VARCHAR,
    category VARCHAR,
    title VARCHAR NOT NULL,
    description VARCHAR,
    published_at TIMESTAMP NOT NULL,
    event_at TIMESTAMP,
    retrieved_at TIMESTAMP NOT NULL,
    source_artifact_id VARCHAR NOT NULL,
    attachment_artifact_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (run_id, upstream_event_hash)
);

CREATE TABLE IF NOT EXISTS jcurve_agent_request (
    request_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    announcement_id VARCHAR NOT NULL,
    role VARCHAR NOT NULL,
    route VARCHAR NOT NULL,
    model_id VARCHAR NOT NULL,
    provider VARCHAR,
    prompt_version VARCHAR NOT NULL,
    prompt_hash VARCHAR NOT NULL,
    source_page_hashes_json VARCHAR NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cost_usd DOUBLE,
    schema_valid BOOLEAN NOT NULL,
    attempt_count INTEGER NOT NULL,
    retry_history_json VARCHAR NOT NULL,
    response_hash VARCHAR,
    status VARCHAR NOT NULL,
    error_code VARCHAR,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (announcement_id, role, model_id, prompt_hash)
);

CREATE TABLE IF NOT EXISTS jcurve_claim (
    claim_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    announcement_id VARCHAR NOT NULL,
    company_id VARCHAR NOT NULL,
    security_id VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    claim_type VARCHAR NOT NULL,
    evidence_state VARCHAR NOT NULL,
    numeric_value DOUBLE,
    boolean_value BOOLEAN,
    text_value VARCHAR,
    unit VARCHAR,
    project_name VARCHAR,
    project_location VARCHAR,
    exact_excerpt VARCHAR,
    page INTEGER,
    source_artifact_id VARCHAR NOT NULL,
    source_content_hash VARCHAR NOT NULL,
    published_at TIMESTAMP NOT NULL,
    effective_at TIMESTAMP,
    confidence DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    claim_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS jcurve_claim_review (
    review_id VARCHAR PRIMARY KEY,
    claim_id VARCHAR NOT NULL,
    request_id VARCHAR NOT NULL,
    decision VARCHAR NOT NULL,
    reviewer_model VARCHAR NOT NULL,
    provider VARCHAR,
    prompt_version VARCHAR NOT NULL,
    independent_context BOOLEAN NOT NULL,
    normalized_claim_hash VARCHAR NOT NULL,
    issue_codes_json VARCHAR NOT NULL,
    review_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (claim_id, reviewer_model, prompt_version)
);

CREATE TABLE IF NOT EXISTS jcurve_episode (
    episode_id VARCHAR PRIMARY KEY,
    company_id VARCHAR NOT NULL,
    archetype VARCHAR NOT NULL,
    project_key VARCHAR NOT NULL,
    project_name VARCHAR,
    project_location VARCHAR,
    opened_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    status VARCHAR NOT NULL,
    first_trigger_artifact_id VARCHAR NOT NULL,
    clustering_policy_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (company_id, archetype, project_key)
);

CREATE TABLE IF NOT EXISTS jcurve_episode_evidence (
    episode_id VARCHAR NOT NULL,
    claim_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    attached_at TIMESTAMP NOT NULL,
    PRIMARY KEY (episode_id, claim_id)
);

CREATE TABLE IF NOT EXISTS jcurve_stage_observation (
    observation_id VARCHAR PRIMARY KEY,
    episode_id VARCHAR NOT NULL,
    as_of_date DATE NOT NULL,
    stage VARCHAR NOT NULL,
    score DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    materiality_passed BOOLEAN NOT NULL,
    materiality_metrics_json VARCHAR NOT NULL,
    reason_codes_json VARCHAR NOT NULL,
    evidence_ids_json VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    policy_hash VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    UNIQUE (episode_id, as_of_date, policy_version)
);

