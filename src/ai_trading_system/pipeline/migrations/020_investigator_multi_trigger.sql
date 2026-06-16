-- Migration 020: multi-trigger investigator intake columns

ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS avg_volume_5 DOUBLE;
ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS volume_ratio_5d DOUBLE;
ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS return_5d DOUBLE;
ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS return_10d DOUBLE;
ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS return_20d DOUBLE;
ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS max_daily_gain_5d DOUBLE;
ALTER TABLE investigator_daily_log ADD COLUMN IF NOT EXISTS green_days_5d DOUBLE;

ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS trigger_reason VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS volume_ratio_5d DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS daily_return_pct DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS return_5d DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS return_10d DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS return_20d DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS max_daily_gain_5d DOUBLE;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS green_days_5d DOUBLE;

ALTER TABLE investigator_repeat_tracker ADD COLUMN IF NOT EXISTS daily_gainer_count_20d INTEGER;
ALTER TABLE investigator_repeat_tracker ADD COLUMN IF NOT EXISTS weekly_gainer_count_20d INTEGER;
ALTER TABLE investigator_repeat_tracker ADD COLUMN IF NOT EXISTS stealth_count_20d INTEGER;

ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS trigger_reason VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS trigger_reason VARCHAR;
