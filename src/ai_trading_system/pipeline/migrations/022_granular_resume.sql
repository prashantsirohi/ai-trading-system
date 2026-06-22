ALTER TABLE pipeline_stage_run ADD COLUMN IF NOT EXISTS resumable_key VARCHAR;
ALTER TABLE pipeline_stage_run ADD COLUMN IF NOT EXISTS parent_stage_name VARCHAR;
ALTER TABLE pipeline_stage_run ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP;
ALTER TABLE pipeline_stage_run ADD COLUMN IF NOT EXISTS interrupted_at TIMESTAMP;
ALTER TABLE pipeline_stage_run ADD COLUMN IF NOT EXISTS resume_policy VARCHAR;
ALTER TABLE pipeline_stage_run ADD COLUMN IF NOT EXISTS checkpoint_json VARCHAR;

DROP INDEX IF EXISTS idx_pipeline_stage_run_status;
DROP INDEX IF EXISTS idx_pipeline_stage_parent_status;

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_run_lookup
ON pipeline_stage_run (run_id, stage_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_parent_lookup
ON pipeline_stage_run (run_id, parent_stage_name);
