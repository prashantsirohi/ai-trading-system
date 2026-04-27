CREATE TABLE IF NOT EXISTS prediction_log (
    prediction_log_id VARCHAR PRIMARY KEY,
    prediction_date DATE NOT NULL,
    model_id VARCHAR,
    model_name VARCHAR,
    model_version VARCHAR,
    deployment_mode VARCHAR NOT NULL,
    horizon INTEGER NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    score DOUBLE,
    probability DOUBLE,
    prediction INTEGER,
    rank INTEGER,
    artifact_uri VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata_json VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_log_scope
ON prediction_log (prediction_date, deployment_mode, horizon, symbol_id, exchange);

CREATE TABLE IF NOT EXISTS shadow_eval (
    shadow_eval_id VARCHAR PRIMARY KEY,
    prediction_log_id VARCHAR NOT NULL,
    prediction_date DATE NOT NULL,
    model_id VARCHAR,
    deployment_mode VARCHAR NOT NULL,
    horizon INTEGER NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    future_date DATE,
    realized_return DOUBLE,
    hit BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata_json VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_eval_prediction_horizon
ON shadow_eval (prediction_log_id, horizon);
