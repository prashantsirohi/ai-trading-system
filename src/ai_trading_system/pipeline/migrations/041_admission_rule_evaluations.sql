-- ADR-0006 Amendment A4: structured evaluate-all admission record.
-- Nullable columns outside semantic payload JSON; legacy rows and replay
-- identities unchanged. primary_admission_reason / primary_setup_family are
-- the existing opening_reason / setup_family columns.

ALTER TABLE candidate_episode
    ADD COLUMN IF NOT EXISTS satisfied_admission_rules_json VARCHAR;
ALTER TABLE candidate_episode
    ADD COLUMN IF NOT EXISTS rule_evaluations_json VARCHAR;
