"""Reports and lifecycle promotion against a real Optuna run."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.research.optimization.acceptance import AcceptanceThresholds
from ai_trading_system.research.optimization.evaluator import FitnessWeights
from ai_trading_system.research.optimization.promote import (
    LIFECYCLE_ORDER,
    _allowed_transition,
    main as promote_main,
)
from ai_trading_system.research.optimization.recipe import (
    OptimizationRecipe,
    StoppingConfig,
    WalkForwardConfig,
)
from ai_trading_system.research.optimization.reports import (
    build_markdown_report,
    load_run_summary,
)
from ai_trading_system.research.optimization.runner import run_optimization
from ai_trading_system.research.optimization.store import OptimizationStore


V1_YAML = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "strategies"
    / "momentum_breakout_v1.yaml"
)


def _seed_research_db(tmp_path: Path) -> tuple[date, date]:
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR, security_id VARCHAR, exchange VARCHAR,
            timestamp TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, parquet_file VARCHAR,
            ingestion_version BIGINT, ingestion_ts TIMESTAMP
        )
        """
    )
    start = date(2022, 1, 3)
    rows = []
    for i in range(560):
        d = start + timedelta(days=i)
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
    return start, date(2023, 7, 31)


def _run_short_study(tmp_path: Path) -> dict:
    from_d, to_d = _seed_research_db(tmp_path)
    recipe = OptimizationRecipe(
        name="report_smoke",
        strategy_id="momentum_breakout",
        baseline_pack_path=str(V1_YAML),
        from_date=from_d,
        to_date=to_d,
        walkforward=WalkForwardConfig(train_months=12, validation_months=3, step_months=3),
        acceptance=AcceptanceThresholds(
            min_fitness_improvement=0.0,
            max_mdd_ratio_vs_champion=100.0,
            min_trades_per_year=0.0,
            min_fold_improvement_rate=0.0,
            worst_fold_min_return_vs_nifty=False,
            worst_fold_max_mdd_ratio_vs_baseline=100.0,
            require_no_zero_trade_fold=False,
        ),
        stopping=StoppingConfig(max_trials=2, patience=100, max_runtime_minutes=10),
        fitness_weights=FitnessWeights(),
    )
    return run_optimization(recipe, project_root=tmp_path)


def test_lifecycle_transition_allowed():
    assert _allowed_transition("backtested", "shadow")
    assert _allowed_transition("shadow", "active")
    assert not _allowed_transition("active", "backtested")
    assert not _allowed_transition("unknown", "shadow")


def test_report_renders_for_completed_run(tmp_path):
    result = _run_short_study(tmp_path)
    summary = load_run_summary(tmp_path, result["optimization_run_id"])
    assert summary.status == "completed"

    text = build_markdown_report(tmp_path, result["optimization_run_id"])
    assert "Strategy optimisation report" in text
    assert "Baseline (per-fold)" in text
    assert "Top trials by fitness" in text
    assert summary.optimization_run_id in text


def test_promote_cli_advances_status(tmp_path):
    result = _run_short_study(tmp_path)
    # Find any pack in the run.
    db_path = RegistryStore(project_root=tmp_path).db_path
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        pack_id = con.execute(
            "SELECT rule_pack_id FROM strategy_rule_pack LIMIT 1"
        ).fetchone()[0]
    finally:
        con.close()

    # Bump straight to backtested (allowed even from draft).
    rc = promote_main(
        ["--rule-pack-id", pack_id, "--to", "backtested", "--project-root", str(tmp_path)]
    )
    assert rc == 0
    # Backwards rejected.
    rc = promote_main(
        ["--rule-pack-id", pack_id, "--to", "draft", "--project-root", str(tmp_path)]
    )
    assert rc == 2

    # Verify persisted state.
    OptimizationStore(project_root=tmp_path)  # ensure migrations applied
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        status = con.execute(
            "SELECT lifecycle_status FROM strategy_rule_pack WHERE rule_pack_id = ?",
            [pack_id],
        ).fetchone()[0]
    finally:
        con.close()
    # Champion may have been promoted past backtested by champion_guards; we
    # just require status is one of the legitimate stages and forward of draft.
    assert status in LIFECYCLE_ORDER
    assert LIFECYCLE_ORDER.index(status) >= LIFECYCLE_ORDER.index("backtested")
