CREATE TABLE IF NOT EXISTS decision_model_deployment (
    decision_domain VARCHAR NOT NULL,
    model_version VARCHAR NOT NULL,
    config_hash VARCHAR NOT NULL,
    environment VARCHAR NOT NULL DEFAULT 'production',
    effective_from DATE NOT NULL,
    effective_to DATE,
    status VARCHAR NOT NULL DEFAULT 'approved',
    approved_by VARCHAR,
    approved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(decision_domain, model_version, config_hash, environment, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_decision_model_deployment_lookup
    ON decision_model_deployment(decision_domain, environment, status, effective_from);

ALTER TABLE investigator_stage1_current ADD COLUMN IF NOT EXISTS stage1_score_delta_5d DOUBLE;
ALTER TABLE investigator_stage1_current ADD COLUMN IF NOT EXISTS stage1_score_delta_20d DOUBLE;
ALTER TABLE investigator_stage1_current ADD COLUMN IF NOT EXISTS emerging_rank_improvement_20d DOUBLE;
ALTER TABLE investigator_stage1_current ADD COLUMN IF NOT EXISTS stage1_eligible BOOLEAN;
ALTER TABLE investigator_stage1_current ADD COLUMN IF NOT EXISTS promotion_eligibility BOOLEAN;
ALTER TABLE investigator_stage1_current ADD COLUMN IF NOT EXISTS execution_eligible BOOLEAN DEFAULT FALSE;
ALTER TABLE stage1_history ADD COLUMN IF NOT EXISTS stage1_score_delta_5d DOUBLE;
ALTER TABLE stage1_history ADD COLUMN IF NOT EXISTS stage1_score_delta_20d DOUBLE;

UPDATE investigator_stage1_current AS current_state
SET stage1_score_delta_5d = history.stage1_score_delta_5d,
    stage1_score_delta_20d = history.stage1_score_delta_20d,
    emerging_rank_improvement_20d = history.emerging_rank_improvement_20d,
    stage1_eligible = history.stage1_eligible,
    execution_eligible = COALESCE(history.execution_eligible, FALSE)
FROM investigator_stage1_state AS history
WHERE current_state.symbol_id = history.symbol_id
  AND current_state.exchange = history.exchange
  AND current_state.as_of_trade_date = history.trade_date;

UPDATE investigator_stage1_current AS current_state
SET promotion_eligibility = analytics.promotion_eligibility
FROM stage1_history AS analytics
WHERE current_state.symbol_id = analytics.symbol_id
  AND current_state.exchange = analytics.exchange
  AND current_state.as_of_trade_date = analytics.trade_date;

-- Bootstrap versions already produced by successful pipeline runs. Operators can
-- supersede these records explicitly; reads never infer ordering from version text.
INSERT OR IGNORE INTO decision_model_deployment
    (decision_domain, model_version, config_hash, environment, effective_from, status, approved_by, notes)
SELECT 'rank', rank_model_version, rank_config_hash, 'production', MIN(trade_date),
       'approved', 'migration-030', 'Bootstrapped from persisted rank history'
FROM rank_history GROUP BY rank_model_version, rank_config_hash;

INSERT OR IGNORE INTO decision_model_deployment
    (decision_domain, model_version, config_hash, environment, effective_from, status, approved_by, notes)
SELECT 'stage', stage_model_version, stage_config_hash, 'production', MIN(trade_date),
       'approved', 'migration-030', 'Bootstrapped from persisted stage history'
FROM stage_history GROUP BY stage_model_version, stage_config_hash;

INSERT OR IGNORE INTO decision_model_deployment
    (decision_domain, model_version, config_hash, environment, effective_from, status, approved_by, notes)
SELECT 'stage1', stage1_model_version, stage1_config_hash, 'production', MIN(trade_date),
       'approved', 'migration-030', 'Bootstrapped from persisted Stage-1 history'
FROM stage1_history GROUP BY stage1_model_version, stage1_config_hash;

INSERT OR IGNORE INTO decision_model_deployment
    (decision_domain, model_version, config_hash, environment, effective_from, status, approved_by, notes)
SELECT 'pattern', pattern_model_version, pattern_config_hash, 'production', MIN(trade_date),
       'approved', 'migration-030', 'Bootstrapped from persisted pattern history'
FROM pattern_history GROUP BY pattern_model_version, pattern_config_hash;

INSERT OR IGNORE INTO decision_model_deployment
    (decision_domain, model_version, config_hash, environment, effective_from, status, approved_by, notes)
SELECT 'stage1_lifecycle', lifecycle_model_version, lifecycle_config_hash, 'production', MIN(as_of_trade_date),
       'approved', 'migration-030', 'Bootstrapped from persisted Stage-1 current state'
FROM investigator_stage1_current GROUP BY lifecycle_model_version, lifecycle_config_hash;
