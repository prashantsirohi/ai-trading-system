CREATE TABLE IF NOT EXISTS investigator_daily_log (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    prev_close DOUBLE,
    volume DOUBLE,
    avg_volume_20 DOUBLE,
    volume_ratio_20 DOUBLE,
    daily_return_pct DOUBLE,
    delivery_pct DOUBLE,
    composite_score DOUBLE,
    rank_position DOUBLE,
    sector VARCHAR,
    trigger_reason VARCHAR
);

CREATE TABLE IF NOT EXISTS investigator_scores (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR,
    trade_date DATE,
    close DOUBLE,
    volume_ratio_20 DOUBLE,
    delivery_pct DOUBLE,
    composite_score DOUBLE,
    rank_position DOUBLE,
    sector VARCHAR,
    price_structure_score DOUBLE,
    volume_delivery_score DOUBLE,
    fundamental_score DOUBLE,
    trigger_quality_score DOUBLE,
    sector_support_score DOUBLE,
    buyer_fingerprint_score DOUBLE,
    ranking_overlay_score DOUBLE,
    final_score DOUBLE,
    verdict VARCHAR,
    move_tag VARCHAR,
    credible_trigger BOOLEAN,
    hard_trap_flag BOOLEAN,
    long_upper_wick_trap BOOLEAN,
    low_delivery_flag BOOLEAN,
    fa_missing BOOLEAN,
    fa_improvement BOOLEAN,
    sector_rotation_active BOOLEAN,
    execution_eligible BOOLEAN
);

CREATE TABLE IF NOT EXISTS investigator_repeat_tracker (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR,
    first_seen_date DATE,
    last_seen_date DATE,
    days_since_last_seen INTEGER,
    appearance_count_5d INTEGER,
    appearance_count_10d INTEGER,
    appearance_count_15d INTEGER,
    appearance_count_20d INTEGER,
    avg_volume_ratio DOUBLE,
    volume_escalation BOOLEAN,
    price_progression_pct DOUBLE,
    rank_current DOUBLE,
    rank_change_20d DOUBLE,
    score_current DOUBLE,
    score_peak DOUBLE,
    sector_cluster_count INTEGER,
    repeat_score DOUBLE,
    high_priority_repeat BOOLEAN
);

CREATE TABLE IF NOT EXISTS investigator_lifecycle (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR,
    trade_date DATE,
    status VARCHAR,
    first_seen_date DATE,
    last_seen_date DATE,
    days_since_last_seen INTEGER,
    appearance_count_20d INTEGER,
    score_current DOUBLE,
    score_peak DOUBLE,
    rank_current DOUBLE,
    rank_change_20d DOUBLE,
    price_progression_pct DOUBLE,
    final_score DOUBLE,
    verdict VARCHAR,
    drop_reason VARCHAR,
    archived_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS investigator_final_gate (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR,
    trade_date DATE,
    verdict VARCHAR,
    final_score DOUBLE,
    thesis VARCHAR,
    invalidation_level VARCHAR,
    exit_plan VARCHAR,
    gate_status VARCHAR
);

CREATE TABLE IF NOT EXISTS investigator_archive (
    run_id VARCHAR,
    attempt_number INTEGER,
    artifact_uri VARCHAR,
    symbol_id VARCHAR,
    trade_date DATE,
    status VARCHAR,
    first_seen_date DATE,
    last_seen_date DATE,
    days_since_last_seen INTEGER,
    appearance_count_20d INTEGER,
    score_current DOUBLE,
    score_peak DOUBLE,
    rank_current DOUBLE,
    rank_change_20d DOUBLE,
    price_vs_first_trigger_pct DOUBLE,
    price_progression_pct DOUBLE,
    final_score DOUBLE,
    verdict VARCHAR,
    drop_reason VARCHAR,
    archived_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_investigator_daily_symbol_date ON investigator_daily_log(symbol_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_scores_symbol_date ON investigator_scores(symbol_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_investigator_lifecycle_status ON investigator_lifecycle(status);
CREATE INDEX IF NOT EXISTS idx_investigator_archive_symbol ON investigator_archive(symbol_id);
