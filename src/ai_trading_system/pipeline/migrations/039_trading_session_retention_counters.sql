-- ADR-0006 A5: trading-session retention counters.
-- Operational counter lineage remains outside snapshot semantic payload JSON,
-- preserving all pre-A5 replay identities.

ALTER TABLE candidate_snapshot
    ADD COLUMN IF NOT EXISTS last_progress_at TIMESTAMP;
ALTER TABLE candidate_snapshot
    ADD COLUMN IF NOT EXISTS last_retention_counted_session DATE;

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
    s.last_progress_at,
    s.last_retention_counted_session,
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
