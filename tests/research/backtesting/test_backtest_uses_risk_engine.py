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


def test_hard_stop_fires_intrabar_and_fills_at_stop_price():
    """Bar pierces stop intrabar (low < stop) but closes above: stop must fire,
    and the fill price must be the stop price, not the close."""
    d0 = date(2026, 2, 1)
    ranked_by_date = {
        d0: pd.DataFrame([_row("ACME", 100.0, open=100.0, high=101.0, low=99.0)]),
        # Day 2: bar dips to low=90 (below stop ~96) but closes at 99.
        d0
        + timedelta(days=1): pd.DataFrame(
            [_row("ACME", 99.0, open=99.5, high=100.0, low=90.0)]
        ),
    }
    runner = EngineBacktestRunner(
        risk_config=RiskPolicyConfig(name="test"),
        starting_equity=500_000.0,
        commission_bps=0.0,
        slippage_bps=0.0,
    )
    result = runner.run(ranked_by_date)
    closed = [t for t in result.trades if t.exit_reason == "hard_stop"]
    assert len(closed) == 1
    trade = closed[0]
    # Fill at stop price, not at close (99.0).
    assert trade.stop_price is not None
    assert trade.exit_price == trade.stop_price


def test_hard_stop_gap_down_fills_at_open():
    """Bar gaps down below stop at the open: fill at open, not at stop."""
    d0 = date(2026, 2, 10)
    ranked_by_date = {
        d0: pd.DataFrame([_row("ACME", 100.0, open=100.0, high=101.0, low=99.0)]),
        # Day 2: gap-down opens at 85 (well below stop ~96); low even lower.
        d0
        + timedelta(days=1): pd.DataFrame(
            [_row("ACME", 86.0, open=85.0, high=88.0, low=82.0)]
        ),
    }
    runner = EngineBacktestRunner(
        risk_config=RiskPolicyConfig(name="test"),
        starting_equity=500_000.0,
        commission_bps=0.0,
        slippage_bps=0.0,
    )
    result = runner.run(ranked_by_date)
    closed = [t for t in result.trades if t.exit_reason == "hard_stop"]
    assert len(closed) == 1
    trade = closed[0]
    assert trade.exit_price == 85.0


def test_costs_applied_on_round_trip():
    """Zero price move with 10 bps commission + 20 bps slippage per side must
    produce a negative P&L. Round-trip cost ~ 2 * (commission + slippage)."""
    d0 = date(2026, 5, 1)
    # Many bars at same close → trade closes at backtest_end with no price move.
    ranked_by_date = {
        d0 + timedelta(days=i): pd.DataFrame(
            [_row("ACME", 100.0, open=100.0, high=100.0, low=100.0)]
        )
        for i in range(3)
    }
    runner = EngineBacktestRunner(
        risk_config=RiskPolicyConfig(name="test"),
        starting_equity=500_000.0,
        commission_bps=10.0,
        slippage_bps=20.0,
    )
    result = runner.run(ranked_by_date)
    assert len(result.trades) == 1
    trade = result.trades[0]
    # Expected drag per side: 20 bps slip + 10 bps commission = 30 bps.
    # Round-trip ≈ 60 bps. P&L pct should be negative ~ -0.006.
    assert trade.pnl < 0
    assert -0.0070 < trade.pnl_pct < -0.0050


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
    runner = EngineBacktestRunner(
        risk_config=RiskPolicyConfig(name="test"),
        starting_equity=500_000.0,
        commission_bps=0.0,
        slippage_bps=0.0,
    )
    result = runner.run(ranked_by_date)

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == "backtest_end"
    assert trade.exit_price == 120.0
    assert trade.pnl and trade.pnl > 0
    assert result.equity_curve[-1]["equity"] == 500_000.0 + trade.pnl
