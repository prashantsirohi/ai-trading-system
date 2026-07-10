-- Migration 023: investigator forward-return tracking foundation.
-- TODO: add forward-return maturation/backfill mirroring research/perf_tracker
-- once investigator cohorts are ready to be matured operationally.

CREATE TABLE IF NOT EXISTS investigator_cohort_performance (
    trade_date DATE NOT NULL,
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL DEFAULT 'NSE',
    trigger_reason VARCHAR,
    verdict VARCHAR,
    final_score DOUBLE,
    hard_trap_flag BOOLEAN,
    credible_trigger BOOLEAN,
    move_tag VARCHAR,
    sector VARCHAR,
    close DOUBLE,
    fwd_3d_return DOUBLE,
    fwd_5d_return DOUBLE,
    fwd_10d_return DOUBLE,
    fwd_20d_return DOUBLE,
    fwd_3d_matured_at DATE,
    fwd_5d_matured_at DATE,
    fwd_10d_matured_at DATE,
    fwd_20d_matured_at DATE,
    data_quality_status VARCHAR DEFAULT 'PENDING',
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, symbol_id, exchange)
);

ALTER TABLE investigator_cohort_performance ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- DuckDB rejects ALTER COLUMN when secondary indexes depend on the table.
-- Re-running this migration after a partial/previous initialization may find
-- these indexes already present, so drop only this migration's indexes before
-- setting the default and recreate them below.
DROP INDEX IF EXISTS idx_investigator_cohort_trade_date;
DROP INDEX IF EXISTS idx_investigator_cohort_symbol_id;
DROP INDEX IF EXISTS idx_investigator_cohort_verdict;
DROP INDEX IF EXISTS idx_investigator_cohort_trigger_reason;
DROP INDEX IF EXISTS idx_investigator_cohort_sector;
DROP INDEX IF EXISTS idx_investigator_cohort_data_quality_status;
DROP INDEX IF EXISTS idx_investigator_cohort_symbol_date;
DROP INDEX IF EXISTS idx_investigator_cohort_date_verdict;
DROP INDEX IF EXISTS idx_investigator_cohort_stage_label;
DROP INDEX IF EXISTS idx_investigator_cohort_pattern_family;
DROP INDEX IF EXISTS idx_investigator_cohort_candidate_tier;

ALTER TABLE investigator_cohort_performance ALTER COLUMN data_quality_status SET DEFAULT 'PENDING';

CREATE INDEX IF NOT EXISTS idx_investigator_cohort_trade_date
    ON investigator_cohort_performance(trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_symbol_id
    ON investigator_cohort_performance(symbol_id);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_verdict
    ON investigator_cohort_performance(verdict);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_trigger_reason
    ON investigator_cohort_performance(trigger_reason);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_sector
    ON investigator_cohort_performance(sector);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_data_quality_status
    ON investigator_cohort_performance(data_quality_status);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_symbol_date
    ON investigator_cohort_performance(symbol_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_date_verdict
    ON investigator_cohort_performance(trade_date, verdict);
