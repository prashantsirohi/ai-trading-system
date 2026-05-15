"""End-to-end smoke test: small Optuna study runs against synthetic data,
persists rule packs + trials + trades, and produces a champion (or NULL).
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.optimization.acceptance import AcceptanceThresholds
from ai_trading_system.research.optimization.evaluator import FitnessWeights
from ai_trading_system.research.optimization.recipe import (
    OptimizationRecipe,
    StoppingConfig,
    WalkForwardConfig,
)
from ai_trading_system.research.optimization.runner import run_optimization


V1_YAML = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "strategies"
    / "momentum_breakout_v1.yaml"
)


def _seed_research_db(tmp_path: Path) -> tuple[date, date]:
    """Two years of synthetic OHLCV so two folds fit (12m train + 3m val + 3m step)."""
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            security_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            parquet_file VARCHAR,
            ingestion_version BIGINT,
            ingestion_ts TIMESTAMP
        )
        """
    )
    start = date(2022, 1, 3)
    rows = []
    for i in range(560):
        d = start + timedelta(days=i)
        # Two divergent symbols + benchmark.
        rows.append(("AAA", None, "NSE", d, 100 + i * 0.3, 102 + i * 0.3, 99 + i * 0.3, 101 + i * 0.3, 1000 + i, None, 1, d))
        rows.append(("BBB", None, "NSE", d, 200 - i * 0.1, 201 - i * 0.1, 199 - i * 0.1, 200 - i * 0.1, 900 + i, None, 1, d))
        rows.append(("NIFTY50", None, "NSE", d, 1000 + i * 0.15, 1001 + i * 0.15, 999 + i * 0.15, 1000 + i * 0.15, 5000 + i, None, 1, d))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    master = sqlite3.connect(paths.root_dir / "masterdata.db")
    master.execute("CREATE TABLE stock_details (Symbol TEXT PRIMARY KEY, Sector TEXT)")
    master.execute("INSERT INTO stock_details VALUES ('AAA', 'TECH')")
    master.execute("INSERT INTO stock_details VALUES ('BBB', 'BANKS')")
    master.commit()
    master.close()
    # Allow one fold: 12m train + 3m val = 15m. Start 2022-01-03, val ends ~2023-03-31.
    return date(2022, 1, 3), date(2023, 7, 31)


def test_runner_persists_baseline_trials_and_writes_champion_or_none(tmp_path):
    from_d, to_d = _seed_research_db(tmp_path)

    # Tiny study: 3 trials, no patience, no realistic acceptance — we want to
    # prove plumbing works, not produce a real champion on synthetic data.
    recipe = OptimizationRecipe(
        name="smoke_test",
        strategy_id="momentum_breakout",
        baseline_pack_path=str(V1_YAML),
        from_date=from_d,
        to_date=to_d,
        exchange="NSE",
        starting_equity=1_000_000.0,
        seed=42,
        walkforward=WalkForwardConfig(train_months=12, validation_months=3, step_months=3),
        # Relax acceptance so we can prove the persistence path even when
        # synthetic data produces few/no trades.
        acceptance=AcceptanceThresholds(
            min_fitness_improvement=0.0,
            max_mdd_ratio_vs_champion=100.0,
            min_trades_per_year=0.0,
            min_fold_improvement_rate=0.0,
            worst_fold_min_return_vs_benchmark=False,
            worst_fold_max_mdd_ratio_vs_baseline=100.0,
            require_no_zero_trade_fold=False,
        ),
        stopping=StoppingConfig(max_trials=3, patience=100, max_runtime_minutes=10),
        fitness_weights=FitnessWeights(),
    )

    result = run_optimization(recipe, project_root=tmp_path)
    assert result["trials"] == 3

    # Inspect the control DB.
    from ai_trading_system.pipeline.registry import RegistryStore

    db_path = RegistryStore(project_root=tmp_path).db_path
    con = duckdb.connect(str(db_path))
    try:
        runs = con.execute(
            "SELECT optimization_run_id, status, strategy_id FROM strategy_optimization_run"
        ).fetchall()
        assert len(runs) == 1
        assert runs[0][1] == "completed"

        # 3 trial × N folds × 2 rows (per-fold + aggregate) + 1 baseline × N folds × 2 rows.
        n_iter_rows = con.execute(
            "SELECT COUNT(*) FROM strategy_iteration_result WHERE optimization_run_id = ?",
            [result["optimization_run_id"]],
        ).fetchone()[0]
        assert n_iter_rows > 0

        # At least the baseline and one trial pack rows.
        n_packs = con.execute("SELECT COUNT(*) FROM strategy_rule_pack").fetchone()[0]
        assert n_packs >= 2

        # Aggregate rows tagged correctly.
        n_agg = con.execute(
            "SELECT COUNT(*) FROM strategy_iteration_result WHERE fold_index = -1 AND fold_role = 'aggregate'"
        ).fetchone()[0]
        assert n_agg >= 4  # baseline + 3 trials
    finally:
        con.close()
