-- Migration 023: schema-only scaffold for investigator forward-return tracking.
-- TODO: add a backfill/service mirroring research/perf_tracker once investigator
-- cohorts are ready to be matured operationally.

CREATE TABLE IF NOT EXISTS investigator_cohort_performance (
    trade_date DATE,
    symbol_id VARCHAR,
    exchange VARCHAR,
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
    data_quality_status VARCHAR DEFAULT 'trusted',
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, symbol_id, exchange)
);

CREATE INDEX IF NOT EXISTS idx_investigator_cohort_symbol_date
    ON investigator_cohort_performance(symbol_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_cohort_date_verdict
    ON investigator_cohort_performance(trade_date, verdict);
