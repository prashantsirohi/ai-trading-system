"""Phase 0 golden backtest fixture.

Locks the numerical output of the engine-driven backtest against a deterministic
synthetic universe. Any unintentional change to entry/exit/sizing/cost math will
regress this test. Update ``EXPECTED`` deliberately when behavior changes.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.domains.risk import RiskPolicyConfig
from ai_trading_system.research.backtesting import EngineBacktestRunner


FIXTURE_PATH = Path(__file__).resolve().parent.parent.parent / "fixtures" / "backtest_golden.json"


def _build_synthetic_universe(seed: int = 42) -> dict[date, pd.DataFrame]:
    """Three symbols, 60 trading days, deterministic drift + small noise."""
    import random

    rng = random.Random(seed)
    symbols = [("ACME", "TECH"), ("BETA", "BANKS"), ("GAMMA", "TECH")]
    d0 = date(2024, 1, 2)
    days = [d0 + timedelta(days=i) for i in range(60)]

    # Per-symbol drift + noise so all three move differently.
    price_paths: dict[str, list[float]] = {}
    for sym, _ in symbols:
        drift = {"ACME": 0.004, "BETA": -0.001, "GAMMA": 0.002}[sym]
        start_price = {"ACME": 100.0, "BETA": 200.0, "GAMMA": 150.0}[sym]
        prices: list[float] = [start_price]
        for _ in range(1, 60):
            shock = rng.uniform(-0.015, 0.015)
            prices.append(prices[-1] * (1.0 + drift + shock))
        price_paths[sym] = prices

    ranked_by_date: dict[date, pd.DataFrame] = {}
    for i, d in enumerate(days):
        rows = []
        for rank_idx, (sym, sector) in enumerate(symbols, start=1):
            close = price_paths[sym][i]
            rows.append({
                "symbol_id": sym,
                "exchange": "NSE",
                "open": close * 0.999,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "composite_score": 80.0 - rank_idx * 5.0,
                "eligible_rank": rank_idx,
                "is_stage2_uptrend": True,
                "sector_name": sector,
                "sector_strength_score": 0.7,
                "sma_11": close * 0.99,
                "sma_20": close * 0.97,
                "sma_50": close * 0.92,
                "sma_200": close * 0.80,
                "atr_14": close * 0.02,
                "volume_ratio_20": 2.0,
                "swing_low_20": close * 0.94,
                "delivery_pct": 60.0,
            })
        ranked_by_date[d] = pd.DataFrame(rows)
    return ranked_by_date


def _run_golden_backtest() -> dict:
    ranked = _build_synthetic_universe(seed=42)
    runner = EngineBacktestRunner(
        risk_config=RiskPolicyConfig(name="golden"),
        starting_equity=1_000_000.0,
        commission_bps=10.0,
        slippage_bps=20.0,
    )
    result = runner.run(ranked)
    return {
        "trade_count": len(result.trades),
        "final_equity": round(result.equity_curve[-1]["equity"], 2)
        if result.equity_curve
        else 1_000_000.0,
        "total_pnl_pct": round(
            (result.equity_curve[-1]["equity"] - 1_000_000.0) / 1_000_000.0 * 100.0,
            4,
        )
        if result.equity_curve
        else 0.0,
        "exit_reasons": sorted(
            {t.exit_reason for t in result.trades if t.exit_reason}
        ),
    }


def test_golden_backtest_matches_fixture():
    """Snapshot the engine's numerical output. If this fails, the engine math
    changed — confirm intentional and regenerate via :func:`regenerate_fixture`.
    """
    actual = _run_golden_backtest()

    if not FIXTURE_PATH.exists():
        FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE_PATH.write_text(json.dumps(actual, indent=2, sort_keys=True))
        pytest.skip(f"golden fixture initialised at {FIXTURE_PATH}; re-run to assert")

    expected = json.loads(FIXTURE_PATH.read_text())

    assert actual["trade_count"] == expected["trade_count"]
    assert actual["exit_reasons"] == expected["exit_reasons"]
    # Floating-point: 4 decimal places.
    assert abs(actual["final_equity"] - expected["final_equity"]) < 0.01
    assert abs(actual["total_pnl_pct"] - expected["total_pnl_pct"]) < 0.0001


if __name__ == "__main__":
    # Regenerate the fixture: python -m tests.research.backtesting.test_golden_backtest
    actual = _run_golden_backtest()
    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(json.dumps(actual, indent=2, sort_keys=True))
    print(f"wrote {FIXTURE_PATH}")
    print(json.dumps(actual, indent=2))
