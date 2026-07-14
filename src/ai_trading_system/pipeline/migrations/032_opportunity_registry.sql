-- Canonical opportunity episode history. Existing tracker and pipeline artifacts are unchanged.

CREATE TABLE IF NOT EXISTS opportunity_registry_schema (
    schema_name VARCHAR PRIMARY KEY,
    schema_version VARCHAR NOT NULL,
    installed_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
INSERT INTO opportunity_registry_schema (schema_name, schema_version)
VALUES ('opportunity_registry', 'opportunity-registry-schema-v1')
ON CONFLICT (schema_name) DO UPDATE SET schema_version = excluded.schema_version;

CREATE TABLE IF NOT EXISTS candidate_episode (
    candidate_id VARCHAR PRIMARY KEY,
    setup_id VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    episode_number INTEGER NOT NULL,
    episode_type VARCHAR NOT NULL,
    setup_family VARCHAR NOT NULL,
    admission_identity VARCHAR NOT NULL,
    episode_started_at TIMESTAMP NOT NULL,
    episode_closed_at TIMESTAMP,
    episode_status VARCHAR NOT NULL,
    opening_reason VARCHAR NOT NULL,
    closing_reason VARCHAR,
    created_run_id VARCHAR NOT NULL,
    created_stage VARCHAR NOT NULL,
    created_artifact_hash VARCHAR NOT NULL,
    closed_run_id VARCHAR,
    closed_stage VARCHAR,
    contract_version VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_episode_setup ON candidate_episode(exchange, symbol_id, setup_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_episode_number ON candidate_episode(exchange, symbol_id, episode_number);
CREATE INDEX IF NOT EXISTS idx_candidate_episode_lookup ON candidate_episode(exchange, symbol_id, setup_family);

CREATE TABLE IF NOT EXISTS candidate_snapshot (
    snapshot_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_type VARCHAR NOT NULL,
    source_artifact_path VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    lifecycle_state VARCHAR NOT NULL,
    followthrough_status VARCHAR NOT NULL,
    opportunity_score DOUBLE,
    rank_position INTEGER,
    rank_percentile DOUBLE,
    rank_velocity DOUBLE,
    evidence_score DOUBLE,
    evidence_verdict VARCHAR,
    days_in_state INTEGER NOT NULL,
    days_without_progress INTEGER NOT NULL,
    progress_status VARCHAR,
    active_position BOOLEAN NOT NULL,
    latest_action VARCHAR NOT NULL,
    eligibility VARCHAR NOT NULL,
    stock_stage_observation_id VARCHAR,
    sector_stage_observation_id VARCHAR,
    contract_version VARCHAR NOT NULL,
    serialization_version VARCHAR NOT NULL,
    snapshot_json VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_snapshot_idempotency ON candidate_snapshot(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_snapshot_history ON candidate_snapshot(candidate_id, as_of, observed_at, created_at);
CREATE INDEX IF NOT EXISTS idx_candidate_snapshot_lineage ON candidate_snapshot(run_id, stage_name, source_artifact_hash);

CREATE TABLE IF NOT EXISTS candidate_stage_observation (
    stage_observation_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    scope VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    entity_name VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    provisional_stage VARCHAR NOT NULL,
    locked_stage VARCHAR NOT NULL,
    effective_stage VARCHAR NOT NULL,
    stage_status VARCHAR NOT NULL,
    confidence_score DOUBLE NOT NULL,
    confidence_band VARCHAR NOT NULL,
    confidence_components_json VARCHAR NOT NULL,
    stage_locked_at TIMESTAMP,
    source_week_start DATE NOT NULL,
    source_week_end DATE NOT NULL,
    previous_locked_stage VARCHAR,
    weeks_in_locked_stage INTEGER NOT NULL,
    provisional_persistence_days INTEGER NOT NULL,
    transition_reason VARCHAR NOT NULL,
    classifier_version VARCHAR NOT NULL,
    confidence_formula_version VARCHAR NOT NULL,
    contract_version VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_type VARCHAR NOT NULL,
    source_artifact_path VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    observation_json VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_stage_idempotency ON candidate_stage_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_stage_history ON candidate_stage_observation(candidate_id, scope, as_of, observed_at, created_at);
CREATE INDEX IF NOT EXISTS idx_candidate_stage_lineage ON candidate_stage_observation(run_id, stage_name, source_artifact_hash);

CREATE TABLE IF NOT EXISTS candidate_evidence_observation (
    evidence_observation_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    evidence_type VARCHAR NOT NULL,
    source_module VARCHAR NOT NULL,
    source_component VARCHAR NOT NULL,
    score DOUBLE NOT NULL,
    verdict VARCHAR NOT NULL,
    positive_evidence_json VARCHAR NOT NULL,
    negative_evidence_json VARCHAR NOT NULL,
    missing_evidence_json VARCHAR NOT NULL,
    details_json VARCHAR NOT NULL,
    evidence_model_version VARCHAR NOT NULL,
    contract_version VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_type VARCHAR NOT NULL,
    source_artifact_path VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    observation_json VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_evidence_idempotency ON candidate_evidence_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_evidence_history ON candidate_evidence_observation(candidate_id, as_of, observed_at, created_at);
CREATE INDEX IF NOT EXISTS idx_candidate_evidence_lineage ON candidate_evidence_observation(run_id, stage_name, source_artifact_hash);

CREATE TABLE IF NOT EXISTS candidate_opportunity_observation (
    opportunity_observation_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    opportunity_score DOUBLE NOT NULL,
    rank_position INTEGER NOT NULL,
    rank_percentile DOUBLE NOT NULL,
    rank_velocity DOUBLE,
    rank_velocity_state VARCHAR NOT NULL,
    factor_scores_json VARCHAR NOT NULL,
    rank_model_version VARCHAR NOT NULL,
    contract_version VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_type VARCHAR NOT NULL,
    source_artifact_path VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    observation_json VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_opportunity_idempotency ON candidate_opportunity_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_opportunity_history ON candidate_opportunity_observation(candidate_id, as_of, observed_at, created_at);
CREATE INDEX IF NOT EXISTS idx_candidate_opportunity_lineage ON candidate_opportunity_observation(run_id, stage_name, source_artifact_hash);

CREATE TABLE IF NOT EXISTS candidate_transition (
    transition_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    from_state VARCHAR NOT NULL,
    to_state VARCHAR NOT NULL,
    transition_reason VARCHAR NOT NULL,
    transitioned_at TIMESTAMP NOT NULL,
    triggering_snapshot_id VARCHAR NOT NULL,
    rule_version VARCHAR NOT NULL,
    metadata_json VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_transition_idempotency ON candidate_transition(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_transition_history ON candidate_transition(candidate_id, transitioned_at, created_at);

CREATE TABLE IF NOT EXISTS candidate_progress_observation (
    progress_observation_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    progress_status VARCHAR NOT NULL,
    rank_velocity_improved BOOLEAN,
    evidence_score_improved BOOLEAN,
    base_contraction_improved BOOLEAN,
    volume_dry_up_improved BOOLEAN,
    weekly_ma_slope_improved BOOLEAN,
    distance_to_pivot_narrowed BOOLEAN,
    relative_strength_improved BOOLEAN,
    sector_alignment_improved BOOLEAN,
    days_without_progress INTEGER NOT NULL,
    details_json VARCHAR NOT NULL,
    rule_version VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    observation_json VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_progress_idempotency ON candidate_progress_observation(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_progress_history ON candidate_progress_observation(candidate_id, as_of, observed_at, created_at);

CREATE TABLE IF NOT EXISTS candidate_decision_context (
    decision_context_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    decided_at TIMESTAMP NOT NULL,
    action VARCHAR NOT NULL,
    eligibility VARCHAR NOT NULL,
    decision_confidence DOUBLE NOT NULL,
    size_multiplier DOUBLE NOT NULL,
    decision_stage VARCHAR NOT NULL,
    decision_stage_status VARCHAR NOT NULL,
    decision_stage_as_of TIMESTAMP NOT NULL,
    decision_locked_stage VARCHAR NOT NULL,
    decision_provisional_stage VARCHAR NOT NULL,
    decision_stage_confidence DOUBLE NOT NULL,
    decision_sector_stage VARCHAR NOT NULL,
    decision_sector_stage_status VARCHAR NOT NULL,
    decision_sector_stage_confidence DOUBLE NOT NULL,
    opportunity_score DOUBLE NOT NULL,
    evidence_score DOUBLE NOT NULL,
    lifecycle_state VARCHAR NOT NULL,
    followthrough_status VARCHAR NOT NULL,
    market_regime VARCHAR NOT NULL,
    sector_regime VARCHAR NOT NULL,
    rank_model_version VARCHAR NOT NULL,
    evidence_model_version VARCHAR NOT NULL,
    stage_classifier_version VARCHAR NOT NULL,
    action_policy_version VARCHAR NOT NULL,
    execution_policy_version VARCHAR NOT NULL,
    portfolio_context_json VARCHAR NOT NULL,
    reasons_json VARCHAR NOT NULL,
    blockers_json VARCHAR NOT NULL,
    warnings_json VARCHAR NOT NULL,
    next_required_event VARCHAR,
    contract_version VARCHAR NOT NULL,
    decision_json VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_decision_idempotency ON candidate_decision_context(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_decision_history ON candidate_decision_context(candidate_id, decided_at, created_at);

CREATE TABLE IF NOT EXISTS candidate_outcome_attribution (
    attribution_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    setup_id VARCHAR NOT NULL,
    attribution_category VARCHAR NOT NULL,
    attribution_subcategory VARCHAR,
    attribution_confidence DOUBLE NOT NULL,
    attribution_rule_version VARCHAR NOT NULL,
    supporting_evidence_json VARCHAR NOT NULL,
    counterfactual_notes VARCHAR,
    resolved_at TIMESTAMP NOT NULL,
    contract_version VARCHAR NOT NULL,
    attribution_json VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_attribution_idempotency ON candidate_outcome_attribution(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_candidate_attribution_history ON candidate_outcome_attribution(candidate_id, resolved_at, created_at);

CREATE OR REPLACE VIEW candidate_current_state AS
WITH
latest_snapshot AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT s.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY as_of DESC, observed_at DESC, created_at DESC, snapshot_id DESC
        ) rn FROM candidate_snapshot s
    ) WHERE rn = 1
),
latest_opportunity AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT o.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY as_of DESC, observed_at DESC, created_at DESC, opportunity_observation_id DESC
        ) rn FROM candidate_opportunity_observation o
    ) WHERE rn = 1
),
latest_evidence AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT e.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY as_of DESC, observed_at DESC, created_at DESC, evidence_observation_id DESC
        ) rn FROM candidate_evidence_observation e
    ) WHERE rn = 1
),
latest_stock_stage AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT s.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY as_of DESC, observed_at DESC, created_at DESC, stage_observation_id DESC
        ) rn FROM candidate_stage_observation s WHERE scope = 'STOCK'
    ) WHERE rn = 1
),
latest_sector_stage AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT s.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY as_of DESC, observed_at DESC, created_at DESC, stage_observation_id DESC
        ) rn FROM candidate_stage_observation s WHERE scope = 'SECTOR'
    ) WHERE rn = 1
),
latest_progress AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT p.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY as_of DESC, observed_at DESC, created_at DESC, progress_observation_id DESC
        ) rn FROM candidate_progress_observation p
    ) WHERE rn = 1
),
latest_decision AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT d.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY decided_at DESC, created_at DESC, decision_context_id DESC
        ) rn FROM candidate_decision_context d
    ) WHERE rn = 1
),
latest_transition AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT t.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY transitioned_at DESC, created_at DESC, transition_id DESC
        ) rn FROM candidate_transition t
    ) WHERE rn = 1
),
latest_lineage AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT u.*, ROW_NUMBER() OVER (
            PARTITION BY candidate_id ORDER BY event_at DESC, observed_at DESC, created_at DESC, stable_id DESC
        ) rn
        FROM (
            SELECT candidate_id, run_id, as_of AS event_at, observed_at, created_at, snapshot_id AS stable_id FROM candidate_snapshot
            UNION ALL SELECT candidate_id, run_id, as_of, observed_at, created_at, opportunity_observation_id FROM candidate_opportunity_observation
            UNION ALL SELECT candidate_id, run_id, as_of, observed_at, created_at, evidence_observation_id FROM candidate_evidence_observation
            UNION ALL SELECT candidate_id, run_id, as_of, observed_at, created_at, stage_observation_id FROM candidate_stage_observation
            UNION ALL SELECT candidate_id, run_id, as_of, observed_at, created_at, progress_observation_id FROM candidate_progress_observation
            UNION ALL SELECT candidate_id, run_id, transitioned_at, transitioned_at, created_at, transition_id FROM candidate_transition
            UNION ALL SELECT candidate_id, run_id, decided_at, decided_at, created_at, decision_context_id FROM candidate_decision_context
            UNION ALL SELECT candidate_id, run_id, resolved_at, resolved_at, created_at, attribution_id FROM candidate_outcome_attribution
        ) u
    ) WHERE rn = 1
)
SELECT
    ep.candidate_id, ep.setup_id, ep.symbol_id, ep.exchange,
    ep.episode_status, ep.episode_started_at, ep.episode_closed_at,
    CASE WHEN t.transitioned_at IS NOT NULL AND (s.as_of IS NULL OR t.transitioned_at > s.as_of)
         THEN t.to_state ELSE s.lifecycle_state END AS current_lifecycle_state,
    s.followthrough_status AS current_followthrough_status,
    COALESCE(o.opportunity_score, s.opportunity_score) AS latest_opportunity_score,
    COALESCE(o.rank_position, s.rank_position) AS latest_rank_position,
    COALESCE(o.rank_percentile, s.rank_percentile) AS latest_rank_percentile,
    COALESCE(o.rank_velocity, s.rank_velocity) AS latest_rank_velocity,
    COALESCE(e.score, s.evidence_score) AS latest_evidence_score,
    COALESCE(e.verdict, s.evidence_verdict) AS latest_evidence_verdict,
    stock.effective_stage AS current_stock_stage,
    stock.stage_status AS current_stock_stage_status,
    stock.confidence_score AS current_stock_stage_confidence,
    sector.effective_stage AS current_sector_stage,
    sector.stage_status AS current_sector_stage_status,
    sector.confidence_score AS current_sector_stage_confidence,
    p.progress_status AS current_progress_status,
    s.days_in_state,
    COALESCE(p.days_without_progress, s.days_without_progress) AS days_without_progress,
    COALESCE(d.action, s.latest_action) AS latest_action,
    COALESCE(d.eligibility, s.eligibility) AS current_eligibility,
    s.as_of AS last_snapshot_at,
    t.transitioned_at AS last_transition_at,
    COALESCE(lineage.run_id, ep.created_run_id) AS last_observed_run_id
FROM candidate_episode ep
LEFT JOIN latest_snapshot s USING (candidate_id)
LEFT JOIN latest_opportunity o USING (candidate_id)
LEFT JOIN latest_evidence e USING (candidate_id)
LEFT JOIN latest_stock_stage stock USING (candidate_id)
LEFT JOIN latest_sector_stage sector USING (candidate_id)
LEFT JOIN latest_progress p USING (candidate_id)
LEFT JOIN latest_decision d USING (candidate_id)
LEFT JOIN latest_transition t USING (candidate_id)
LEFT JOIN latest_lineage lineage USING (candidate_id);
