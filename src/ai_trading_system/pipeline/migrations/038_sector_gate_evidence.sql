-- ADR-0006 Amendment A2: completed-week locked-sector gate evidence.
-- Columns are nullable and live outside semantic payload JSON so legacy rows
-- and gate-untouched replay identities remain unchanged.

ALTER TABLE candidate_decision_context
    ADD COLUMN IF NOT EXISTS sector_locked_stage_prior_completed_week VARCHAR;
ALTER TABLE candidate_decision_context
    ADD COLUMN IF NOT EXISTS sector_provisional_stage_current_week VARCHAR;
ALTER TABLE candidate_decision_context
    ADD COLUMN IF NOT EXISTS sector_stage_velocity_current_week DOUBLE;
ALTER TABLE candidate_decision_context
    ADD COLUMN IF NOT EXISTS sector_gate_taxonomy VARCHAR;
ALTER TABLE candidate_decision_context
    ADD COLUMN IF NOT EXISTS sector_gate_cohort VARCHAR;
