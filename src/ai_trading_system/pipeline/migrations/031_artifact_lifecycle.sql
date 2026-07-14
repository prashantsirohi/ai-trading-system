ALTER TABLE pipeline_artifact
ADD COLUMN IF NOT EXISTS lifecycle_status VARCHAR DEFAULT 'written';

ALTER TABLE pipeline_artifact
ADD COLUMN IF NOT EXISTS dq_passed_at TIMESTAMP;

ALTER TABLE pipeline_artifact
ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMP;

UPDATE pipeline_artifact AS artifact
SET lifecycle_status = 'promoted',
    dq_passed_at = COALESCE(artifact.dq_passed_at, stage.ended_at),
    promoted_at = COALESCE(artifact.promoted_at, stage.ended_at)
FROM pipeline_stage_run AS stage
WHERE stage.run_id = artifact.run_id
  AND stage.stage_name = artifact.stage_name
  AND stage.attempt_number = artifact.attempt_number
  AND stage.status = 'completed'
  AND COALESCE(artifact.lifecycle_status, 'written') <> 'promoted';

UPDATE pipeline_artifact
SET lifecycle_status = 'written'
WHERE lifecycle_status IS NULL;
