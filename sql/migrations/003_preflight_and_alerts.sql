CREATE TABLE IF NOT EXISTS pipeline_alert (
    alert_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    alert_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    stage_name VARCHAR,
    message VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
