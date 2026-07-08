-- Migration 024: Investigator stage/pattern/breakout context.

ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage_label VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS stage_reason VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS price_above_sma50 BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS price_above_sma200 BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS sma50_slope_positive BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS sma200_slope_positive BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS near_52w_high_pct DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS base_age_days DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS volume_dryup_flag BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS accumulation_flag BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS distribution_flag BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS pattern_family VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS pattern_state VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS pattern_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS pattern_rank DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS setup_quality VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS setup_quality_bucket VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS breakout_type VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS breakout_score DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS breakout_rank DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS candidate_tier VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS qualified_breakout BOOLEAN;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS breakout_state VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS final_score_bucket VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS relative_strength DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS volume_intensity DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS trend_persistence DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS proximity_to_highs DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS sector_strength DOUBLE;

ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS stage_label VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS pattern_family VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS pattern_state VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS setup_quality_bucket VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS breakout_type VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS candidate_tier VARCHAR;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS qualified_breakout BOOLEAN;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS stage_score DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS pattern_score DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS breakout_score DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS composite_score DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS relative_strength DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS volume_intensity DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS trend_persistence DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS proximity_to_highs DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS delivery_pct DOUBLE;
ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS sector_strength DOUBLE;

CREATE INDEX IF NOT EXISTS idx_investigator_scores_stage_label
    ON investigator_scores(stage_label);
CREATE INDEX IF NOT EXISTS idx_investigator_scores_pattern_family
    ON investigator_scores(pattern_family);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_stage_label
    ON investigator_cohort_performance(stage_label);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_pattern_family
    ON investigator_cohort_performance(pattern_family);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_candidate_tier
    ON investigator_cohort_performance(candidate_tier);
