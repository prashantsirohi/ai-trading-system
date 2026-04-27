CREATE TABLE IF NOT EXISTS operator_task (
    task_id VARCHAR PRIMARY KEY,
    task_type VARCHAR NOT NULL,
    label VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    result_json VARCHAR,
    error VARCHAR,
    metadata_json VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS operator_task_log (
    task_id VARCHAR NOT NULL,
    log_order BIGINT NOT NULL,
    message VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (task_id, log_order)
);

CREATE INDEX IF NOT EXISTS idx_operator_task_started_at
ON operator_task (started_at);

CREATE INDEX IF NOT EXISTS idx_operator_task_status
ON operator_task (status);
