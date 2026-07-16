-- ADR-0006 Amendment A3: immutable policy snapshot registry and record stamping.
-- The registry binds each human-readable policy version label to exactly one
-- canonical content hash. The nullable columns stamp new registry records with
-- the composite policy snapshot outside semantic payload JSON, so legacy rows
-- and pre-A3 replay hashes are unchanged.

CREATE TABLE IF NOT EXISTS policy_version_registry (
    version_label VARCHAR PRIMARY KEY,
    policy_snapshot_id VARCHAR NOT NULL,
    content_json VARCHAR NOT NULL,
    first_registered_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    first_run_id VARCHAR
);

ALTER TABLE candidate_episode ADD COLUMN IF NOT EXISTS policy_snapshot_id VARCHAR;
ALTER TABLE candidate_episode ADD COLUMN IF NOT EXISTS closed_policy_snapshot_id VARCHAR;
ALTER TABLE candidate_transition ADD COLUMN IF NOT EXISTS policy_snapshot_id VARCHAR;
ALTER TABLE candidate_decision_context ADD COLUMN IF NOT EXISTS policy_snapshot_id VARCHAR;
