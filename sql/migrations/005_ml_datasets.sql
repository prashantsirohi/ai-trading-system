CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id VARCHAR PRIMARY KEY,
    dataset_ref VARCHAR NOT NULL,
    dataset_uri VARCHAR NOT NULL,
    data_domain VARCHAR NOT NULL,
    engine_name VARCHAR,
    feature_schema_version VARCHAR,
    feature_schema_hash VARCHAR,
    label_version VARCHAR,
    target_column VARCHAR,
    from_date DATE,
    to_date DATE,
    horizon INTEGER,
    row_count BIGINT,
    symbol_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata_json VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_registry_ref
ON dataset_registry (dataset_ref);
