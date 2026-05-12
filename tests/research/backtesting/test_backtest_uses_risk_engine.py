"""Engine-driven backtest produces full-provenance trade rows."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from ai_trading_system.domains.risk import RiskPolicyConfig
from ai_trading_system.research.backtesting import EngineBacktestRunner


def _row(symbol_id: str, close: float, **overrides) -> dict:
    row = {
        "symbol_id": symbol_id,
        "exchange": "NSE",
        "close": close,
        "composite_score": 80.0,
        "eligible_rank": 1,
        "is_stage2_uptrend": True,
        "sector_name": "TECH",
        "sector_strength_score": 0.7,
        "sma_11": close * 0.99,
        "sma_20": close * 0.97,
        "sma_50": close * 0.92,
        "sma_200": close * 0.80,
        "atr_14": 2.0,
        "volume_ratio_20": 2.0,
        "swing_low_20": close * 0.94,
        "delivery_pct": 60.0,
    }
    row.update(overrides)
    return row


def test_entry_and_dma_exit_round_trip():
    d0 = date(2026, 1, 5)
    # Day 1: ACME enters
    # Day 2-5: holds
    # Day 6: close drops below sma_20 → 20DMA exit
    ranked_by_date = {
        d0: pd.DataFrame([_row("ACME", 100.0)]),
        d0 + timedelta(days=1): pd.DataFrame([_row("ACME", 102.0)]),
        d0 + timedelta(days=2): pd.DataFrame([_row("ACME", 104.0)]),
        d0 + timedelta(days=3): pd.DataFrame([_row("ACME", 103.0)]),
        d0 + timedelta(days=4): pd.DataFrame([_row("ACME", 102.0)]),
        # Day 1 entry: close 100, atr 2 → stop 96. To fire DMA-exit not hard-stop
        # we keep close above 96 but below sma_20=100*0.995=99.5.
        d0 + timedelta(days=5): pd.DataFrame(
            [_row("ACME", 97.0, sma_20=100.0, sma_50=95.0, sma_200=80.0)]
        ),
    }

    runner = EngineBacktestRunner(risk_config=RiskPolicyConfig(name="test"), starting_equity=1_000_000.0)
    result = runner.run(ranked_by_date)

    df = result.to_trades_df()
    assert not df.empty
    # On day 6 ACME exits AND re-enters in the same bar (exits-before-entries
    # frees the slot). The re-entry stays open till backtest end. Filter to the
    # one with the real DMA-exit reason.
    dma_exits = df[df["exit_reason"] == "close_below_20dma"]
    assert len(dma_exits) == 1
    row = dma_exits.iloc[0]
    assert row["symbol_id"] == "ACME"
    assert row["entry_reason"] == "entry_confirmed"
    assert row["stop_price"] is not None
    assert row["rank_at_entry"] == 1
    assert row["score_at_entry"] == 80.0
    assert row["pnl"] is not None


def test_hard_stop_wins_over_dma():
    d0 = date(2026, 2, 1)
    ranked_by_date = {
        d0: pd.DataFrame([_row("ACME", 100.0)]),
        # Day 2: collapse below stop AND below 20-DMA — hard_stop must win
        d0 + timedelta(days=1): pd.DataFrame(
            [_row("ACME", 50.0, sma_20=95.0, sma_50=92.0, sma_200=80.0)]
        ),
    }
    runner = EngineBacktestRunner(risk_config=RiskPolicyConfig(name="test"), starting_equity=500_000.0)
    result = runner.run(ranked_by_date)
    closed = [t for t in result.trades if t.exit_reason]
    assert len(closed) == 1
    assert closed[0].exit_reason == "hard_stop"


def test_no_entry_when_volume_below_threshold():
    d0 = date(2026, 3, 1)
    ranked_by_date = {
        d0: pd.DataFrame([_row("ACME", 100.0, volume_ratio_20=0.5)]),
        d0 + timedelta(days=1): pd.DataFrame([_row("ACME", 102.0, volume_ratio_20=0.5)]),
    }
    runner = EngineBacktestRunner(risk_config=RiskPolicyConfig(name="test"), starting_equity=500_000.0)
    result = runner.run(ranked_by_date)
    assert result.trades == []


def test_open_trade_closes_at_latest_close_on_backtest_end():
    d0 = date(2026, 4, 1)
    ranked_by_date = {
        d0: pd.DataFrame([_row("ACME", 100.0)]),
        d0 + timedelta(days=1): pd.DataFrame([_row("ACME", 120.0)]),
    }
    runner = EngineBacktestRunner(risk_config=RiskPolicyConfig(name="test"), starting_equity=500_000.0)
    result = runner.run(ranked_by_date)

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == "backtest_end"
    assert trade.exit_price == 120.0
    assert trade.pnl and trade.pnl > 0
    assert result.equity_curve[-1]["equity"] == 500_000.0 + trade.pnl
