CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id VARCHAR PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    run_date DATE NOT NULL,
    trigger VARCHAR,
    status VARCHAR NOT NULL,
    current_stage VARCHAR,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    error_class VARCHAR,
    error_message VARCHAR,
    metadata_json VARCHAR
);

CREATE TABLE IF NOT EXISTS pipeline_stage_run (
    stage_run_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    error_class VARCHAR,
    error_message VARCHAR,
    metadata_json VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_stage_attempt
ON pipeline_stage_run (run_id, stage_name, attempt_number);

CREATE TABLE IF NOT EXISTS pipeline_artifact (
    artifact_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL,
    artifact_type VARCHAR NOT NULL,
    uri VARCHAR NOT NULL,
    content_hash VARCHAR,
    row_count BIGINT,
    created_at TIMESTAMP,
    metadata_json VARCHAR
);

CREATE TABLE IF NOT EXISTS dq_rule (
    rule_id VARCHAR PRIMARY KEY,
    stage_name VARCHAR NOT NULL,
    dataset_name VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    description VARCHAR,
    owner VARCHAR,
    enabled BOOLEAN DEFAULT TRUE,
    rollout_date DATE
);

CREATE TABLE IF NOT EXISTS dq_result (
    result_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    rule_id VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    failed_count BIGINT DEFAULT 0,
    message VARCHAR,
    sample_uri VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_registry (
    model_id VARCHAR PRIMARY KEY,
    model_name VARCHAR NOT NULL,
    model_version VARCHAR NOT NULL,
    artifact_uri VARCHAR NOT NULL,
    feature_schema_hash VARCHAR NOT NULL,
    training_snapshot_ref VARCHAR NOT NULL,
    status VARCHAR DEFAULT 'registered',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata_json VARCHAR
);

CREATE TABLE IF NOT EXISTS model_eval (
    eval_id VARCHAR PRIMARY KEY,
    model_id VARCHAR NOT NULL,
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE NOT NULL,
    dataset_ref VARCHAR,
    notes VARCHAR
);

CREATE TABLE IF NOT EXISTS model_deployment (
    deployment_id VARCHAR PRIMARY KEY,
    model_id VARCHAR NOT NULL,
    environment VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    approved_by VARCHAR,
    approved_at TIMESTAMP,
    deployed_at TIMESTAMP,
    rollback_model_id VARCHAR,
    notes VARCHAR
);
