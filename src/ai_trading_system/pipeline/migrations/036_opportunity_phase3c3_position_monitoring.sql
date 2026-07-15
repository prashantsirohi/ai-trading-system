-- Phase 3C-3 additive alert-incident and controlled position-recovery state.

CREATE TABLE IF NOT EXISTS pipeline_alert_incident (
    incident_id VARCHAR PRIMARY KEY,
    dedupe_key VARCHAR NOT NULL,
    alert_type VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    first_run_id VARCHAR NOT NULL,
    last_run_id VARCHAR NOT NULL,
    stage_name VARCHAR,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    payload_json VARCHAR NOT NULL,
    opened_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    last_seen_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    resolved_at TIMESTAMP,
    resolution_json VARCHAR
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_alert_incident_dedupe
ON pipeline_alert_incident(dedupe_key);

CREATE TABLE IF NOT EXISTS position_recovery_proposal (
    recovery_proposal_id VARCHAR PRIMARY KEY,
    position_cycle_id VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    recovery_mode VARCHAR NOT NULL,
    proposal_status VARCHAR NOT NULL,
    compatibility_status VARCHAR NOT NULL,
    payload_hash VARCHAR NOT NULL,
    payload_json VARCHAR NOT NULL,
    created_run_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_recovery_cycle_policy
ON position_recovery_proposal(position_cycle_id, recovery_mode, payload_hash);

CREATE TABLE IF NOT EXISTS position_recovery_action (
    recovery_action_id VARCHAR PRIMARY KEY,
    recovery_proposal_id VARCHAR NOT NULL,
    position_cycle_id VARCHAR NOT NULL,
    candidate_id VARCHAR NOT NULL,
    recovery_mode VARCHAR NOT NULL,
    reviewed_by VARCHAR,
    reviewed_at TIMESTAMP,
    review_notes VARCHAR,
    payload_json VARCHAR NOT NULL,
    created_run_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_recovery_action_proposal
ON position_recovery_action(recovery_proposal_id);
