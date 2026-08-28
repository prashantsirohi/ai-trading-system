"""Tests for realized-market, ranked-cohort, and strategy backtest MCP tools."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.tools.performance import (
    get_backtest_result,
    get_backtest_runs,
    get_backtest_trades,
    get_market_winners,
    get_rank_performance_summary,
    get_ranked_winners,
    get_symbol_backtest_history,
)


def test_market_winners_use_adjusted_prices(ctx: McpContext) -> None:
    result = get_market_winners(ctx, period="week", as_of="2026-01-09")
    assert result["meta"]["price_basis"] == "adjusted"
    assert result["meta"]["evidence_type"] == "realized_market_return_hindsight"
    assert result["data"][0]["symbol_id"] == "AAA"
    assert result["data"][0]["return_pct"] == pytest.approx(20.0)


def test_ranked_winners_require_maturity_and_recording_by_cutoff(ctx: McpContext) -> None:
    result = get_ranked_winners(
        ctx, period="month", horizon_days=20, as_of="2026-01-09"
    )
    assert [row["symbol_id"] for row in result["data"]] == ["AAA"]
    assert result["data"][0]["return_pct"] == 12.0
    assert result["data"][0]["matured_at"] == "2026-01-09"


def test_rank_performance_summary_and_symbol_history(ctx: McpContext) -> None:
    summary = get_rank_performance_summary(
        ctx, period="month", horizon_days=20, as_of="2026-01-09"
    )["data"]
    assert summary["cohort_rows"] == 2
    assert summary["winning_rows"] == 1
    assert summary["win_rate_pct"] == 50.0
    history = get_symbol_backtest_history(
        ctx, "AAA", horizon_days=20, as_of="2026-01-09"
    )["data"]
    assert len(history) == 1
    assert history[0]["source_run_id"] == "run-1"


def test_invalid_horizon_and_period_are_rejected(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="horizon_days"):
        get_ranked_winners(ctx, horizon_days=30)
    with pytest.raises(ValueError, match="period"):
        get_market_winners(ctx, period="fortnight")


def test_persisted_backtest_runs_results_and_trades(ctx: McpContext) -> None:
    runs = get_backtest_runs(ctx)["data"]
    assert runs[0]["optimization_run_id"] == "opt-1"
    assert runs[0]["trial_count"] == 1
    result = get_backtest_result(ctx, "opt-1")["data"]
    assert result["selected_iteration"] == 3
    assert result["metrics"][0]["total_return_pct"] == 30.0
    winners = get_backtest_trades(ctx, "opt-1", outcome="winner")["data"]
    assert [row["symbol_id"] for row in winners] == ["AAA"]
    losers = get_backtest_trades(ctx, "opt-1", outcome="loser")["data"]
    assert [row["symbol_id"] for row in losers] == ["BBB"]


def test_backtest_as_of_excludes_later_completed_run(ctx: McpContext) -> None:
    assert get_backtest_runs(ctx, as_of="2026-01-08")["data"] == []
    assert get_backtest_result(ctx, "opt-1", as_of="2026-01-08")["data"] == {}
    assert get_backtest_trades(ctx, "opt-1", as_of="2026-01-08")["data"] == []
