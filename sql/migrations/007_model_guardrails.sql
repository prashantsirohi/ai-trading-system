CREATE TABLE IF NOT EXISTS drift_metric (
    drift_metric_id VARCHAR PRIMARY KEY,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    prediction_date DATE,
    model_id VARCHAR,
    deployment_mode VARCHAR,
    horizon INTEGER,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE NOT NULL,
    threshold_value DOUBLE,
    status VARCHAR NOT NULL,
    metadata_json VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_drift_metric_scope
ON drift_metric (model_id, deployment_mode, horizon, prediction_date, metric_name);

CREATE TABLE IF NOT EXISTS promotion_gate_result (
    gate_result_id VARCHAR PRIMARY KEY,
    model_id VARCHAR NOT NULL,
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    gate_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    metric_value DOUBLE,
    threshold_value DOUBLE,
    metadata_json VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_promotion_gate_model
ON promotion_gate_result (model_id, evaluated_at, gate_name);
