from __future__ import annotations

import pandas as pd
import pytest

from ai_trading_system.analytics.pattern_calibration import (
    compute_pattern_setup_quality_calibration,
)
from ai_trading_system.analytics.rank_backtester import RankBacktester
from analytics.stage_gate_backtest import evaluate_stage2_freshness


def _backtester() -> RankBacktester:
    bt = RankBacktester.__new__(RankBacktester)
    bt.top_n = 2
    bt.rebalance_days = 2
    return bt


def test_rank_backtester_default_grid_includes_sector_and_normalizes() -> None:
    bt = _backtester()

    grid = bt._default_weight_grid()

    assert grid
    assert all("sector_strength" in weights for weights in grid)
    assert all(sum(weights.values()) == pytest.approx(1.0) for weights in grid)


def test_rank_backtester_run_backtest_reports_gross_and_net_cost_schema() -> None:
    bt = _backtester()
    dates = pd.date_range("2026-01-01", periods=5, freq="B")
    prices = pd.DataFrame(
        {
            "AAA": [100.0, 102.0, 104.0, 106.0, 108.0],
            "BBB": [100.0, 99.0, 98.0, 97.0, 96.0],
        },
        index=dates,
    )
    signals = pd.DataFrame(True, index=dates, columns=prices.columns)

    metrics, equity, trades = bt.run_backtest(
        prices,
        signals,
        initial_cash=100_000,
        fees=0.001,
        slippage_bps=5,
    )

    expected_metric_keys = {
        "gross_total_return",
        "net_total_return",
        "total_return",
        "transaction_cost",
        "slippage_bps",
        "total_cost_paid",
    }
    assert expected_metric_keys.issubset(metrics)
    assert "gross_equity" in equity.columns
    assert {"avg_gross_return", "avg_net_return", "total_cost_paid"}.issubset(trades.columns)
    assert metrics["gross_total_return"] > metrics["net_total_return"]


def test_pattern_setup_quality_calibration_groups_by_family_and_bucket() -> None:
    prices = pd.DataFrame(
        {
            "date": list(pd.date_range("2026-01-01", periods=70, freq="B")) * 2,
            "symbol_id": ["AAA"] * 70 + ["BBB"] * 70,
            "close": list(range(100, 170)) + list(range(200, 130, -1)),
        }
    )
    signals = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "signal_date": "2026-01-05",
                "pattern_family": "vcp",
                "setup_quality": 82,
            },
            {
                "symbol_id": "BBB",
                "signal_date": "2026-01-05",
                "pattern_family": "vcp",
                "setup_quality": 55,
            },
        ]
    )

    detail, summary = compute_pattern_setup_quality_calibration(
        signals,
        prices,
        horizons=(20,),
    )

    assert not detail.empty
    assert {"pattern_family", "setup_quality_bucket", "hit_rate_20d"}.issubset(summary.columns)
    hits = summary.set_index("setup_quality_bucket")["hit_rate_20d"].to_dict()
    assert hits["80-90"] == pytest.approx(1.0)
    assert hits["50-65"] == pytest.approx(0.0)


def test_stage2_freshness_report_schema_covers_required_buckets() -> None:
    dates = pd.date_range("2026-01-02", periods=70, freq="B")
    close_pivot = pd.DataFrame(
        {
            "AAA": range(100, 170),
            "BBB": range(200, 270),
            "CCC": range(300, 370),
            "DDD": range(400, 470),
        },
        index=dates,
    )
    snapshots = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "week_end_date": dates[0],
                "stage_label": "S2",
                "stage_transition": "NONE",
                "bars_in_stage": 3,
            },
            {
                "symbol": "BBB",
                "week_end_date": dates[0],
                "stage_label": "S2",
                "stage_transition": "NONE",
                "bars_in_stage": 20,
            },
            {
                "symbol": "CCC",
                "week_end_date": dates[0],
                "stage_label": "S2",
                "stage_transition": "S1_TO_S2",
                "bars_in_stage": 1,
            },
            {
                "symbol": "DDD",
                "week_end_date": dates[0],
                "stage_label": "S1",
                "stage_transition": "NONE",
                "bars_in_stage": 10,
            },
        ]
    )

    detail, summary = evaluate_stage2_freshness(snapshots, close_pivot)

    assert {"fresh_s2", "mature_s2", "S1_TO_S2", "non_s2"} == set(detail["bucket"])
    assert {"bucket", "horizon", "n", "mean_return", "median_return", "win_rate"}.issubset(summary.columns)
