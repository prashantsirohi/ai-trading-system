"""Parity: identical engine inputs in backtest and paper paths produce identical decisions.

This guards the core acceptance criterion: "A close below 20 DMA produces the
same ExitDecision in backtest and paper mode."
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.domains.execution.policies import build_trade_actions
from ai_trading_system.domains.execution.portfolio import PositionSnapshot as ExecPositionSnapshot
from ai_trading_system.domains.risk import (
    PortfolioSnapshot,
    PositionSnapshot,
    RiskPolicyConfig,
    TradingRuleEngine,
)
from ai_trading_system.domains.risk.adapters import (
    candidate_from_row,
    market_from_row,
)


def _ranked_row(symbol_id: str, **overrides) -> dict:
    row = {
        "symbol_id": symbol_id,
        "exchange": "NSE",
        "close": 90.0,
        "composite_score": 75.0,
        "eligible_rank": 5,
        "is_stage2_uptrend": True,
        "sector_name": "TECH",
        "sector_strength_score": 0.6,
        "sma_11": 95.0,
        "sma_20": 100.0,  # close 90 < sma_20 → 20DMA exit
        "sma_50": 95.0,
        "sma_200": 80.0,
        "atr_14": 2.0,
        "volume_ratio_20": 1.8,
        "swing_low_20": 86.0,
        "delivery_pct": 55.0,
    }
    row.update(overrides)
    return row


def test_engine_directly_and_via_paper_path_emit_same_exit():
    config = RiskPolicyConfig(name="parity_test")
    bar_date = date(2026, 4, 15)
    row = _ranked_row("HELD")

    # --- A. Engine-direct (research backtest style)
    candidate = candidate_from_row(row)
    market = market_from_row(row, as_of=bar_date)
    held_position = PositionSnapshot(
        symbol_id="HELD",
        exchange="NSE",
        entry_date=date(2026, 3, 1),
        entry_price=95.0,
        shares=100,
        sector="TECH",
        stop_price=80.0,
        stop_method="atr",
        rank_at_entry=3,
        score_at_entry=85.0,
        bars_held=30,
    )
    portfolio = PortfolioSnapshot(
        cash=900_000.0,
        equity=1_000_000.0,
        positions=(held_position,),
        sector_exposure={"TECH": 0.10},
    )
    engine = TradingRuleEngine(config)
    intents_direct = engine.generate_order_intents([candidate], {"HELD": market}, portfolio)
    direct_exit = next(i for i in intents_direct if i.intent_kind == "exit")

    # --- B. Same input via paper trading path
    exec_position = ExecPositionSnapshot(
        symbol_id="HELD",
        exchange="NSE",
        quantity=100,
        avg_entry_price=95.0,
        last_fill_price=95.0,
    )
    actions = build_trade_actions(
        ranked_df=pd.DataFrame([row]),
        positions={"HELD": exec_position},
        risk_config=config,
        equity=1_000_000.0,
        stop_records={
            "HELD": {
                "stop_price": 80.0,
                "entry_price": 95.0,
                "created_at": "2026-03-01T00:00:00",
                "metadata": {"sector": "TECH", "stop_method": "atr", "bars_held": 30},
            }
        },
    )
    paper_exits = [a for a in actions if a.side == "SELL"]
    assert len(paper_exits) == 1
    paper_exit = paper_exits[0]

    # The reason and quantity are the single source of truth.
    assert paper_exit.reason == direct_exit.reason == "close_below_20dma"
    assert paper_exit.quantity == direct_exit.quantity == 100
    assert paper_exit.symbol_id == direct_exit.symbol_id == "HELD"
