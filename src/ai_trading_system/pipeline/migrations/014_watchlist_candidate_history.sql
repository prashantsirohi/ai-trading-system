-- Migration 014: watchlist_candidate_history
--
-- Stores the final operator-facing watchlist rows in queryable form while
-- preserving the CSV/JSON artifacts as the audit source for each rank attempt.

CREATE TABLE IF NOT EXISTS watchlist_candidate_history (
    watchlist_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    symbol_id TEXT NOT NULL,
    rank INTEGER,
    previous_rank INTEGER,
    rank_change INTEGER,
    days_on_watchlist INTEGER NOT NULL DEFAULT 1,
    is_new_entry BOOLEAN NOT NULL DEFAULT TRUE,
    sector TEXT,
    sector_status TEXT,
    stage TEXT,
    momentum_tags TEXT,
    setup_label TEXT,
    watchlist_score DOUBLE,
    composite_score DOUBLE,
    action TEXT,
    technical_catalyst_summary TEXT,
    catalyst_tags TEXT,
    catalyst_confidence TEXT,
    bull_case TEXT,
    risk_flags TEXT,
    watchlist_reason TEXT,
    data_trust_status TEXT,
    artifact_uri TEXT,
    metadata_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (watchlist_date, run_id, symbol_id)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_candidate_history_symbol
    ON watchlist_candidate_history(symbol_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_candidate_history_date
    ON watchlist_candidate_history(watchlist_date);
CREATE INDEX IF NOT EXISTS idx_watchlist_candidate_history_score
    ON watchlist_candidate_history(watchlist_score);
