CREATE TABLE IF NOT EXISTS model_shadow_prediction (
    prediction_id VARCHAR PRIMARY KEY,
    prediction_date DATE NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    close DOUBLE,
    technical_score DOUBLE,
    technical_rank INTEGER,
    technical_top_decile BOOLEAN,
    ml_5d_prob DOUBLE,
    ml_5d_rank INTEGER,
    ml_5d_top_decile BOOLEAN,
    ml_20d_prob DOUBLE,
    ml_20d_rank INTEGER,
    ml_20d_top_decile BOOLEAN,
    blend_5d_score DOUBLE,
    blend_5d_rank INTEGER,
    blend_5d_top_decile BOOLEAN,
    blend_20d_score DOUBLE,
    blend_20d_rank INTEGER,
    blend_20d_top_decile BOOLEAN,
    artifact_uri VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata_json VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_prediction_date_symbol
ON model_shadow_prediction (prediction_date, symbol_id, exchange);

CREATE TABLE IF NOT EXISTS model_shadow_outcome (
    outcome_id VARCHAR PRIMARY KEY,
    prediction_id VARCHAR NOT NULL,
    prediction_date DATE NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    horizon INTEGER NOT NULL,
    future_date DATE,
    realized_return DOUBLE,
    hit BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_outcome_prediction_horizon
ON model_shadow_outcome (prediction_id, horizon);
