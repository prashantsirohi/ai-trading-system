-- Migration 016: top-1000 liquid equal-weight universe index
--
-- Persists membership snapshots (monthly rebalance) and per-bar diagnostics
-- for the derived UNIV_TOP1000 index. The canonical price series lives in
-- _index_catalog (one row per trading day); diagnostics here explain how
-- each bar was built so coverage issues can be audited.

CREATE TABLE IF NOT EXISTS _universe_membership (
    rebalance_date DATE NOT NULL,
    symbol_id TEXT NOT NULL,
    rank_by_turnover INTEGER NOT NULL,
    median_turnover DOUBLE,
    recent_days INTEGER,
    sparse_history BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    PRIMARY KEY (rebalance_date, symbol_id)
);

CREATE INDEX IF NOT EXISTS idx_universe_membership_rebalance
    ON _universe_membership(rebalance_date);

CREATE TABLE IF NOT EXISTS _universe_index_diagnostics (
    index_code TEXT NOT NULL,
    date DATE NOT NULL,
    rebalance_date DATE NOT NULL,
    n_members INTEGER NOT NULL,
    n_used INTEGER NOT NULL,
    n_missing INTEGER NOT NULL,
    used_ratio DOUBLE,
    daily_return DOUBLE,
    index_level DOUBLE NOT NULL,
    quality_flag TEXT NOT NULL DEFAULT 'ok',  -- 'ok' | 'low_coverage' | 'sparse_membership' | 'gap'
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
    PRIMARY KEY (index_code, date)
);

CREATE INDEX IF NOT EXISTS idx_universe_index_diagnostics_date
    ON _universe_index_diagnostics(date);
CREATE INDEX IF NOT EXISTS idx_universe_index_diagnostics_quality
    ON _universe_index_diagnostics(quality_flag);

-- Register UNIV_TOP1000 in _index_metadata if the table already exists.
-- This is wrapped in DO/EXCEPTION-style guards via INSERT OR IGNORE pattern
-- since _index_metadata is created by an earlier migration / runtime init.
-- (DuckDB's INSERT ... ON CONFLICT DO NOTHING covers the "already there" case.)
