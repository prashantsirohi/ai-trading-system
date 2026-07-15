-- Phase 3C-1 append-only sector membership and stage correction governance.

CREATE TABLE IF NOT EXISTS sector_membership_history (
    membership_observation_id VARCHAR PRIMARY KEY,
    exchange VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    sector_id VARCHAR NOT NULL,
    sector_name VARCHAR NOT NULL,
    industry_name VARCHAR,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    membership_trust VARCHAR NOT NULL,
    point_in_time_valid BOOLEAN NOT NULL,
    source_type VARCHAR NOT NULL,
    source_hash VARCHAR NOT NULL,
    supersedes_membership_observation_id VARCHAR,
    policy_version VARCHAR NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sector_membership_semantic
ON sector_membership_history(exchange, symbol_id, valid_from, valid_to, membership_trust, source_hash);
CREATE INDEX IF NOT EXISTS idx_sector_membership_asof
ON sector_membership_history(exchange, symbol_id, valid_from, valid_to, recorded_at);

CREATE TABLE IF NOT EXISTS stage_observation_governance (
    governance_event_id VARCHAR PRIMARY KEY,
    observation_scope VARCHAR NOT NULL,
    observation_id VARCHAR NOT NULL,
    governance_action VARCHAR NOT NULL,
    supersedes_observation_id VARCHAR,
    membership_trust VARCHAR NOT NULL,
    authoritative BOOLEAN NOT NULL,
    correction_reason VARCHAR,
    correction_authority VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    event_hash VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stage_governance_event_hash
ON stage_observation_governance(event_hash);
CREATE INDEX IF NOT EXISTS idx_stage_governance_observation
ON stage_observation_governance(observation_scope, observation_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_stage_governance_supersession
ON stage_observation_governance(observation_scope, supersedes_observation_id, recorded_at);

CREATE TABLE IF NOT EXISTS stage_observation_dependency (
    dependency_id VARCHAR PRIMARY KEY,
    sector_observation_id VARCHAR NOT NULL,
    dependency_type VARCHAR NOT NULL,
    dependency_observation_id VARCHAR NOT NULL,
    dependency_source_hash VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stage_observation_dependency
ON stage_observation_dependency(sector_observation_id, dependency_type, dependency_observation_id);

CREATE TABLE IF NOT EXISTS stage_correction_impact (
    impact_id VARCHAR PRIMARY KEY,
    correction_governance_event_id VARCHAR NOT NULL,
    corrected_observation_scope VARCHAR NOT NULL,
    corrected_observation_id VARCHAR NOT NULL,
    affected_record_type VARCHAR NOT NULL,
    affected_record_id VARCHAR NOT NULL,
    candidate_id VARCHAR,
    impact_status VARCHAR NOT NULL,
    impact_reason VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    run_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_stage_correction_impact
ON stage_correction_impact(correction_governance_event_id, affected_record_type, affected_record_id);
CREATE INDEX IF NOT EXISTS idx_stage_correction_impact_candidate
ON stage_correction_impact(candidate_id, impact_status, detected_at);
