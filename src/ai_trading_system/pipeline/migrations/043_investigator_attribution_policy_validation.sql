-- Phase 3.5B: frozen Investigator policy, complete evidence, lifecycle,
-- executable-entry anchors, and daily attribution coverage receipts.

ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS pattern_score DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS setup_quality_score DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS breakout_tier VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS move_tag VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS trigger_reason VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS final_score DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS attribution_score DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS review_lane VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS review_eligible BOOLEAN;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS sector_leadership VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_price DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_volume DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_sma20 DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_sma50 DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_sma200 DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_high_52w DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_breakout_level DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_invalidation_price DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS distance_from_breakout_pct DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS distance_from_sma50_pct DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS distance_from_52w_high_pct DOUBLE;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_source_lineage_json VARCHAR;
ALTER TABLE candidate_snapshot ADD COLUMN IF NOT EXISTS investigator_evaluation_states_json VARCHAR;

ALTER TABLE investigator_performance_event ADD COLUMN IF NOT EXISTS next_session_open DOUBLE;
ALTER TABLE investigator_performance_event ADD COLUMN IF NOT EXISTS simulated_fill_price DOUBLE;
ALTER TABLE investigator_performance_event ADD COLUMN IF NOT EXISTS invalidation_price DOUBLE;
ALTER TABLE investigator_performance_event ADD COLUMN IF NOT EXISTS fill_policy_version VARCHAR;
ALTER TABLE investigator_performance_event ADD COLUMN IF NOT EXISTS policy_version VARCHAR;

ALTER TABLE investigator_performance_horizon ADD COLUMN IF NOT EXISTS days_to_stop INTEGER;

CREATE TABLE IF NOT EXISTS investigator_evaluation_transition (
    evaluation_transition_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    from_state VARCHAR NOT NULL,
    to_state VARCHAR NOT NULL,
    transitioned_at TIMESTAMP NOT NULL,
    session_date DATE NOT NULL,
    reason_code VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    source_event_id VARCHAR NOT NULL,
    source_snapshot_id VARCHAR,
    source_transition_id VARCHAR,
    originating_run_id VARCHAR NOT NULL,
    confirming_run_id VARCHAR,
    price_anchor DOUBLE,
    price_anchor_basis VARCHAR,
    evidence_snapshot_json VARCHAR NOT NULL,
    evidence_snapshot_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_evaluation_transition_idempotency
    ON investigator_evaluation_transition(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_investigator_evaluation_transition_candidate
    ON investigator_evaluation_transition(candidate_id, transitioned_at);

CREATE TABLE IF NOT EXISTS investigator_attribution_coverage_receipt (
    receipt_id VARCHAR PRIMARY KEY,
    as_of_date DATE NOT NULL,
    source_run_id VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    policy_snapshot_id VARCHAR,
    metric_name VARCHAR NOT NULL,
    numerator INTEGER NOT NULL,
    denominator INTEGER NOT NULL,
    coverage_pct DOUBLE NOT NULL,
    target_pct DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    exclusion_reasons_json VARCHAR NOT NULL,
    unexplained_unknown_count INTEGER NOT NULL DEFAULT 0,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigator_coverage_receipt_idempotency
    ON investigator_attribution_coverage_receipt(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_investigator_coverage_receipt_date
    ON investigator_attribution_coverage_receipt(as_of_date, metric_name);
