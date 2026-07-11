ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_substate VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_maturity_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_emerging_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_emerging_rank INTEGER;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_eligible BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_block_reasons VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_data_completeness_pct DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_score_confidence VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS pattern_promotion_state VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_operational_status VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS promotion_eligibility BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS promotion_block_reasons VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_model_version VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_config_hash VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_formula_name VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS model_status VARCHAR;

ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_substate VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_maturity_score DOUBLE;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_emerging_score DOUBLE;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_emerging_rank INTEGER;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_operational_status VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_model_version VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_config_hash VARCHAR;

ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_substate VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_maturity_score DOUBLE;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_emerging_score DOUBLE;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_emerging_rank INTEGER;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_operational_status VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_model_version VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_config_hash VARCHAR;

CREATE INDEX IF NOT EXISTS idx_investigator_scores_stage1_rank
    ON investigator_scores(stage1_emerging_rank);
