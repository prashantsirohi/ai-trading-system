DROP INDEX IF EXISTS idx_investigator_scores_symbol_date;
DROP INDEX IF EXISTS idx_investigator_scores_stage_label;
DROP INDEX IF EXISTS idx_investigator_scores_pattern_family;
DROP INDEX IF EXISTS idx_investigator_lifecycle_status;
DROP INDEX IF EXISTS idx_investigator_archive_symbol;

ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS candidate_sources VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS primary_candidate_source VARCHAR;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS candidate_source_count INTEGER;
ALTER TABLE investigator_scores ADD COLUMN IF NOT EXISTS new_candidate_today BOOLEAN;

ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS candidate_sources VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS primary_candidate_source VARCHAR;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS candidate_source_count INTEGER;
ALTER TABLE investigator_lifecycle ADD COLUMN IF NOT EXISTS new_candidate_today BOOLEAN;

ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS candidate_sources VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS primary_candidate_source VARCHAR;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS candidate_source_count INTEGER;
ALTER TABLE investigator_archive ADD COLUMN IF NOT EXISTS new_candidate_today BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_investigator_scores_symbol_date
    ON investigator_scores(symbol_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_scores_stage_label
    ON investigator_scores(stage_label);
CREATE INDEX IF NOT EXISTS idx_investigator_scores_pattern_family
    ON investigator_scores(pattern_family);
CREATE INDEX IF NOT EXISTS idx_investigator_lifecycle_status
    ON investigator_lifecycle(status);
CREATE INDEX IF NOT EXISTS idx_investigator_archive_symbol
    ON investigator_archive(symbol_id);
