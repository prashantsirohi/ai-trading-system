ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_score_band VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_bonus_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_penalty_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage1_adjustment_reasons VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS golden_cross_status VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS golden_cross_status_legacy VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS golden_cross_quality DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS ma_gap_quality_flag VARCHAR;

ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS stage1_score_band VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS promotion_eligibility BOOLEAN;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS promotion_block_reasons VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS golden_cross_status VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS ma_gap_quality_flag VARCHAR;

ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS stage1_score_band VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS promotion_eligibility BOOLEAN;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS promotion_block_reasons VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS golden_cross_status VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS ma_gap_quality_flag VARCHAR;
