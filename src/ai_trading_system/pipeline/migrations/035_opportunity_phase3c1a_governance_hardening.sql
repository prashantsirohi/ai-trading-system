-- Phase 3C-1A governance hardening: explicit authority metadata and legacy link status.

ALTER TABLE stage_observation_governance
ADD COLUMN IF NOT EXISTS authority_reference VARCHAR;
ALTER TABLE stage_observation_governance
ADD COLUMN IF NOT EXISTS authority_recorded_at TIMESTAMP;
ALTER TABLE stage_observation_governance
ADD COLUMN IF NOT EXISTS governance_policy_version VARCHAR;

CREATE INDEX IF NOT EXISTS idx_stage_governance_authority
ON stage_observation_governance(observation_scope, correction_authority, authority_recorded_at);

ALTER TABLE stage_correction_impact
ADD COLUMN IF NOT EXISTS match_count INTEGER;
ALTER TABLE stage_correction_impact
ADD COLUMN IF NOT EXISTS match_rule_version VARCHAR;
ALTER TABLE stage_correction_impact
ADD COLUMN IF NOT EXISTS match_evidence VARCHAR;
ALTER TABLE stage_correction_impact
ADD COLUMN IF NOT EXISTS authoritative_calibration_eligible BOOLEAN;
ALTER TABLE stage_correction_impact
ADD COLUMN IF NOT EXISTS review_required BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_stage_correction_impact_review
ON stage_correction_impact(impact_status, review_required, authoritative_calibration_eligible);
