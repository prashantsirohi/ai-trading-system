-- Migration 013: events_enrichment_log
--
-- Records the output of the events pipeline stage so that:
--   1. Re-runs of the same (run_id, symbol, trigger_type) are deduped against
--      previous attempts and the publish layer's build_dedupe_key() can
--      include event_hashes for per-symbol-per-category dedup.
--   2. Operators can inspect (via Streamlit / SQL) which triggers fired,
--      which were suppressed by the noise filters, and what the materiality
--      verdict was.
--
-- Companion read paths:
--   - dashboard/operator console reads recent rows for the events tab
--   - publish/delivery_manager joins on event_hashes when computing dedup keys

CREATE TABLE IF NOT EXISTS events_enrichment_log (
    run_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    as_of_date DATE,
    trigger_strength DOUBLE,
    trigger_metadata_json TEXT,
    event_hashes_json TEXT,           -- JSON list of market_intel event_hashes
    materiality_label TEXT,           -- low | medium | high | critical
    top_category TEXT,                -- e.g. capex_expansion, demerger
    event_count INTEGER NOT NULL DEFAULT 0,
    suppressed BOOLEAN NOT NULL DEFAULT FALSE,
    suppress_reason TEXT,
    severity TEXT,                    -- low-info | medium | high
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (run_id, symbol, trigger_type)
);

CREATE INDEX IF NOT EXISTS idx_events_enrichment_log_symbol
    ON events_enrichment_log(symbol);
CREATE INDEX IF NOT EXISTS idx_events_enrichment_log_as_of
    ON events_enrichment_log(as_of_date);
CREATE INDEX IF NOT EXISTS idx_events_enrichment_log_severity
    ON events_enrichment_log(severity);
