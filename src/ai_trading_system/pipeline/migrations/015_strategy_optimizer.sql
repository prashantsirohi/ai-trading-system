-- Migration 015: strategy_optimizer
--
-- Stores rule packs, optimisation runs, per-trial results (with per-fold
-- breakdown), and the trades produced by each backtest. The optimizer is
-- research-only and never touches operational rank/execute paths.

CREATE TABLE IF NOT EXISTS strategy_rule_pack (
    rule_pack_id TEXT NOT NULL,              -- SHA256 of canonical YAML; uniqueness enforced via index + ON CONFLICT
    parent_rule_pack_id TEXT,
    strategy_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    rule_yaml TEXT NOT NULL,                 -- canonical YAML for full provenance
    rule_json TEXT NOT NULL,                 -- canonical JSON (sorted keys)
    lifecycle_status TEXT NOT NULL DEFAULT 'draft',
                                             -- draft -> backtested -> walkforward_passed
                                             -- -> shadow -> paper_approved
                                             -- -> production_candidate -> active
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_rule_pack_id
    ON strategy_rule_pack(rule_pack_id);
CREATE INDEX IF NOT EXISTS idx_strategy_rule_pack_strategy
    ON strategy_rule_pack(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_rule_pack_status
    ON strategy_rule_pack(lifecycle_status);

CREATE TABLE IF NOT EXISTS strategy_optimization_run (
    optimization_run_id TEXT NOT NULL,       -- UUID per study; uniqueness enforced via index
    recipe_name TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    baseline_rule_pack_id TEXT NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    seed INTEGER NOT NULL,
    max_trials INTEGER NOT NULL,
    status TEXT NOT NULL,                    -- pending | running | completed | failed | cancelled
    champion_rule_pack_id TEXT,
    recipe_json TEXT NOT NULL,               -- full recipe snapshot
    error TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_optimization_run_id
    ON strategy_optimization_run(optimization_run_id);
CREATE INDEX IF NOT EXISTS idx_strategy_optimization_run_strategy
    ON strategy_optimization_run(strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_optimization_run_status
    ON strategy_optimization_run(status);

CREATE TABLE IF NOT EXISTS strategy_iteration_result (
    optimization_run_id TEXT NOT NULL,
    iteration INTEGER NOT NULL,              -- Optuna trial number
    rule_pack_id TEXT NOT NULL,
    fold_index INTEGER NOT NULL,             -- -1 for aggregate row
    fold_role TEXT,                          -- 'train' | 'val' | 'aggregate'
    fitness DOUBLE,
    cagr DOUBLE,
    sharpe DOUBLE,
    sortino DOUBLE,
    max_drawdown_pct DOUBLE,
    win_rate DOUBLE,
    profit_factor DOUBLE,
    trade_count INTEGER,
    trades_per_year DOUBLE,
    total_return_pct DOUBLE,
    nifty_return_pct DOUBLE,
    accepted BOOLEAN,
    rejection_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_iteration_result
    ON strategy_iteration_result(optimization_run_id, iteration, fold_index);
CREATE INDEX IF NOT EXISTS idx_strategy_iteration_result_pack
    ON strategy_iteration_result(rule_pack_id);

CREATE TABLE IF NOT EXISTS strategy_backtest_trade (
    optimization_run_id TEXT NOT NULL,
    iteration INTEGER NOT NULL,
    fold_index INTEGER NOT NULL,
    rule_pack_id TEXT NOT NULL,
    symbol_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    entry_date DATE NOT NULL,
    entry_price DOUBLE NOT NULL,
    entry_reason TEXT,
    exit_date DATE,
    exit_price DOUBLE,
    exit_reason TEXT,
    bars_held INTEGER,
    pnl DOUBLE,
    pnl_pct DOUBLE,
    sector TEXT,
    rank_at_entry INTEGER,
    score_at_entry DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_strategy_backtest_trade_run
    ON strategy_backtest_trade(optimization_run_id, iteration);
