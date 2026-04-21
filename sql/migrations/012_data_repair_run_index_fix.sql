DROP INDEX IF EXISTS idx_data_repair_run_created;

CREATE INDEX IF NOT EXISTS idx_data_repair_run_created
ON data_repair_run (exchange, created_at);
