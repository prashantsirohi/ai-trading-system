from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.research.backtest_pipeline import (
    build_strategy_returns_from_equity_curve,
    compute_benchmark_comparison,
    load_benchmark_close_history,
)


def test_compute_benchmark_comparison_returns_expected_metrics() -> None:
    strategy_returns = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
            "strategy_return": [0.02, 0.01, -0.005],
        }
    )
    benchmark_history = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
            "benchmark_close": [100.0, 101.0, 100.5],
        }
    )

    report = compute_benchmark_comparison(
        strategy_returns,
        benchmark_history,
        benchmark_symbol="NIFTY_50",
    )

    assert report["status"] == "ok"
    assert report["benchmark_symbol"] == "NIFTY_50"
    assert report["observations"] == 2
    assert set(report["metrics"]) == {"alpha", "beta", "information_ratio", "tracking_error"}
    assert report["metrics"]["tracking_error"] >= 0


def test_compute_benchmark_comparison_handles_missing_benchmark_gracefully() -> None:
    strategy_returns = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-01", "2026-04-02"]),
            "strategy_return": [0.01, 0.02],
        }
    )

    report = compute_benchmark_comparison(
        strategy_returns,
        pd.DataFrame(columns=["date", "benchmark_close"]),
        benchmark_symbol="NIFTY_50",
    )

    assert report["status"] == "benchmark_unavailable"
    assert report["metrics"] == {}


def test_load_benchmark_history_and_strategy_returns_helpers(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('NIFTY_50', 'NSE', '2026-04-01 15:30:00', 100, 101, 99, 100, 1000),
            ('NIFTY_50', 'NSE', '2026-04-02 15:30:00', 101, 102, 100, 101, 1000),
            ('NIFTY_50', 'NSE', '2026-04-03 15:30:00', 102, 103, 101, 103, 1000)
            """
        )
    finally:
        conn.close()

    benchmark = load_benchmark_close_history(
        ohlcv_db_path=db_path,
        benchmark_symbol="NIFTY_50",
        from_date="2026-04-01",
        to_date="2026-04-03",
    )
    equity_curve = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
            "capital": [100000.0, 101000.0, 102010.0],
        }
    )

    strategy_returns = build_strategy_returns_from_equity_curve(equity_curve)

    assert benchmark["benchmark_close"].tolist() == pytest.approx([100.0, 101.0, 103.0])
    assert strategy_returns.columns.tolist() == ["date", "strategy_return"]
    assert strategy_returns["strategy_return"].tolist() == pytest.approx([0.01, 0.01])
