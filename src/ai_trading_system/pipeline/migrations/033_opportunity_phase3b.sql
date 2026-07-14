-- Phase 3B append-only universal stage and scan-routing history.

CREATE TABLE IF NOT EXISTS weekly_stock_stage_history (
    observation_id VARCHAR PRIMARY KEY,
    exchange VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    sector_id VARCHAR,
    sector_name VARCHAR,
    as_of TIMESTAMP NOT NULL,
    source_week_start DATE NOT NULL,
    source_week_end DATE NOT NULL,
    stage_status VARCHAR NOT NULL,
    effective_stage VARCHAR NOT NULL,
    classifier_version VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    observation_json VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_weekly_stock_stage_semantic
ON weekly_stock_stage_history(exchange, symbol_id, source_week_end, stage_status, classifier_version, source_artifact_hash);
CREATE INDEX IF NOT EXISTS idx_weekly_stock_stage_asof
ON weekly_stock_stage_history(exchange, symbol_id, as_of, source_week_end);

CREATE TABLE IF NOT EXISTS weekly_sector_stage_history (
    observation_id VARCHAR PRIMARY KEY,
    sector_id VARCHAR NOT NULL,
    sector_name VARCHAR NOT NULL,
    as_of TIMESTAMP NOT NULL,
    source_week_start DATE NOT NULL,
    source_week_end DATE NOT NULL,
    stage_status VARCHAR NOT NULL,
    effective_stage VARCHAR NOT NULL,
    aggregation_rule_version VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    observation_json VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_weekly_sector_stage_semantic
ON weekly_sector_stage_history(sector_id, source_week_end, stage_status, aggregation_rule_version, source_artifact_hash);

CREATE TABLE IF NOT EXISTS opportunity_scan_routing_history (
    decision_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage_attempt INTEGER NOT NULL,
    as_of DATE NOT NULL,
    exchange VARCHAR NOT NULL,
    symbol_id VARCHAR NOT NULL,
    scan_tier VARCHAR NOT NULL,
    reasons_json VARCHAR NOT NULL,
    policy_version VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    decision_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_scan_routing_replay
ON opportunity_scan_routing_history(run_id, stage_attempt, exchange, symbol_id, policy_version, source_artifact_hash);
