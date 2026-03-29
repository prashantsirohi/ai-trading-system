ALTER TABLE dq_rule ADD COLUMN IF NOT EXISTS rule_sql VARCHAR;
ALTER TABLE dq_rule ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE;

ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS train_snapshot_ref VARCHAR;
ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS approval_status VARCHAR DEFAULT 'pending';

CREATE TABLE IF NOT EXISTS publisher_delivery_log (
    delivery_log_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    channel VARCHAR NOT NULL,
    artifact_uri VARCHAR NOT NULL,
    artifact_hash VARCHAR,
    dedupe_key VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    external_message_id VARCHAR,
    external_report_id VARCHAR,
    error_message VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata_json VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_publisher_delivery_dedupe
ON publisher_delivery_log (dedupe_key, status);
