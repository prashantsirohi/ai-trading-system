-- Complete analytical rank cross-section before regime top-N truncation.
-- Operational consumers continue to read rank_history/ranked_signals only.
CREATE TABLE IF NOT EXISTS rank_universe_history (
    symbol_id VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    universe_id VARCHAR NOT NULL,
    rank_position INTEGER,
    rank_percentile DOUBLE,
    composite_score DOUBLE,
    composite_score_adjusted DOUBLE,
    rs_score DOUBLE,
    volume_score DOUBLE,
    trend_score DOUBLE,
    proximity_score DOUBLE,
    sector_score DOUBLE,
    momentum_acceleration_score DOUBLE,
    delivery_score DOUBLE,
    rank_confidence DOUBLE,
    rank_eligible BOOLEAN,
    rejection_reasons VARCHAR,
    liquidity_score DOUBLE,
    avg_value_traded_20 DOUBLE,
    delivery_pct_20d_avg DOUBLE,
    delivery_trend_score DOUBLE,
    rank_model_version VARCHAR NOT NULL,
    rank_formula_name VARCHAR NOT NULL,
    rank_config_hash VARCHAR NOT NULL,
    selection_policy VARCHAR,
    effective_min_score DOUBLE,
    effective_top_n INTEGER,
    market_regime VARCHAR,
    regime_as_of DATE,
    regime_age_days INTEGER,
    regime_freshness_status VARCHAR,
    regime_freshness_policy_version VARCHAR,
    pipeline_run_id VARCHAR NOT NULL,
    source_attempt INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rank_universe_history
    ON rank_universe_history(
        symbol_id, exchange, trade_date, universe_id, rank_model_version
    );
