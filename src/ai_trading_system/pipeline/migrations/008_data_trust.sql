CREATE TABLE IF NOT EXISTS data_repair_run (
    repair_run_id VARCHAR PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    from_date DATE,
    to_date DATE,
    exchange VARCHAR,
    status VARCHAR NOT NULL,
    repaired_row_count BIGINT DEFAULT 0,
    unresolved_symbol_count BIGINT DEFAULT 0,
    unresolved_date_count BIGINT DEFAULT 0,
    report_uri VARCHAR,
    metadata_json VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_data_repair_run_created
ON data_repair_run (exchange, created_at);
