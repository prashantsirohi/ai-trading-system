"""Verify paper-trade build_trade_actions delegates to the shared engine
when a RiskPolicyConfig is supplied."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.execution.policies import build_trade_actions
from ai_trading_system.domains.execution.portfolio import PositionSnapshot
from ai_trading_system.domains.risk import RiskPolicyConfig
from ai_trading_system.domains.risk.config import EntryConfig, ExitConfig


def _ranked_row(symbol_id: str, **overrides) -> dict:
    row = {
        "symbol_id": symbol_id,
        "exchange": "NSE",
        "close": 100.0,
        "composite_score": 80.0,
        "eligible_rank": 1,
        "is_stage2_uptrend": True,
        "sector_name": "TECH",
        "sector_strength_score": 0.7,
        "sma_20": 96.0,
        "sma_50": 92.0,
        "sma_200": 80.0,
        "sma_11": 98.0,
        "atr_14": 2.0,
        "volume_ratio_20": 2.0,
        "swing_low_20": 94.0,
        "delivery_pct": 60.0,
    }
    row.update(overrides)
    return row


def _config(**overrides) -> RiskPolicyConfig:
    cfg = RiskPolicyConfig(name="test")
    if "entry" in overrides:
        cfg = RiskPolicyConfig(
            name=cfg.name,
            entry=overrides["entry"],
            stop=cfg.stop,
            exit=overrides.get("exit", cfg.exit),
            sizing=cfg.sizing,
            constraints=cfg.constraints,
        )
    return cfg


def test_engine_path_emits_entry_action_for_top_ranked():
    ranked = pd.DataFrame([_ranked_row("ACME")])
    actions = build_trade_actions(
        ranked_df=ranked,
        positions={},
        risk_config=_config(),
        equity=1_000_000.0,
    )
    buys = [a for a in actions if a.side == "BUY"]
    assert len(buys) == 1
    assert buys[0].symbol_id == "ACME"
    assert buys[0].quantity and buys[0].quantity > 0
    assert buys[0].reason == "entry_confirmed"
    assert buys[0].metadata.get("intent_kind") == "entry"
    assert buys[0].metadata.get("stop_method") in {"atr", "hybrid", "swing_low", "percent"}
    assert buys[0].metadata.get("initial_stop") is not None


def test_engine_path_emits_close_below_20dma_exit_for_held_position():
    # Held position will exit on close < sma_20 (20DMA exit, default config)
    held = PositionSnapshot(
        symbol_id="HELD",
        exchange="NSE",
        quantity=100,
        avg_entry_price=110.0,
        last_fill_price=110.0,
    )
    # Bar shows close 90, sma_20 100 → close below 20DMA
    ranked = pd.DataFrame(
        [
            _ranked_row("HELD", close=90.0, sma_20=100.0, sma_50=95.0, sma_200=80.0),
            _ranked_row("NEW", eligible_rank=2),
        ]
    )
    actions = build_trade_actions(
        ranked_df=ranked,
        positions={"HELD": held},
        risk_config=_config(),
        equity=1_000_000.0,
        stop_records={
            "HELD": {
                "stop_price": 80.0,
                "entry_price": 110.0,
                "created_at": "2026-04-01T00:00:00",
                "metadata": {"sector": "TECH"},
            }
        },
    )
    sells = [a for a in actions if a.side == "SELL"]
    assert len(sells) == 1
    assert sells[0].symbol_id == "HELD"
    assert sells[0].reason == "close_below_20dma"
    assert actions[0].side == "SELL"  # exits emitted before entries


def test_engine_path_skips_held_symbol_for_entry():
    held = PositionSnapshot(
        symbol_id="ACME", exchange="NSE", quantity=100, avg_entry_price=95.0, last_fill_price=100.0
    )
    ranked = pd.DataFrame([_ranked_row("ACME"), _ranked_row("NEW", eligible_rank=2)])
    actions = build_trade_actions(
        ranked_df=ranked,
        positions={"ACME": held},
        risk_config=_config(),
        equity=1_000_000.0,
        stop_records={
            "ACME": {
                "stop_price": 80.0,
                "entry_price": 95.0,
                "created_at": "2026-04-01T00:00:00",
                "metadata": {"sector": "TECH"},
            }
        },
    )
    buys = [a for a in actions if a.side == "BUY"]
    # ACME already held → only NEW should buy
    assert {a.symbol_id for a in buys} == {"NEW"}


def test_engine_path_rejects_volume_below_threshold():
    ranked = pd.DataFrame([_ranked_row("ACME", volume_ratio_20=0.5)])
    actions = build_trade_actions(
        ranked_df=ranked,
        positions={},
        risk_config=_config(),
        equity=1_000_000.0,
    )
    assert [a for a in actions if a.side == "BUY"] == []


def test_legacy_path_unchanged_when_no_risk_config():
    """Smoke test: omit risk_config and verify legacy diff logic still runs."""
    ranked = pd.DataFrame([_ranked_row("ACME")])
    actions = build_trade_actions(
        ranked_df=ranked,
        positions={},
        target_position_count=1,
    )
    assert any(a.side == "BUY" and a.symbol_id == "ACME" for a in actions)
    # Legacy path tags reason="target_entry", not "entry_confirmed"
    assert any(a.reason == "target_entry" for a in actions if a.side == "BUY")
