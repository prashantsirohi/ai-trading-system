"""End-to-end persistence of engine-emitted fields through the autotrader path."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.execution import (
    AutoTrader,
    ExecutionService,
    ExecutionStore,
    PaperExecutionAdapter,
    PortfolioManager,
)
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.risk import RiskPolicyConfig


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
        "sma_11": 99.0,
        "sma_20": 97.0,
        "sma_50": 92.0,
        "sma_200": 80.0,
        "atr_14": 2.0,
        "volume_ratio_20": 2.0,
        "swing_low_20": 94.0,
        "delivery_pct": 60.0,
    }
    row.update(overrides)
    return row


def test_engine_entry_persists_reason_and_stop_in_fill_metadata(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    portfolio = PortfolioManager(store)

    ranked = pd.DataFrame([_ranked_row("ACME")])
    AutoTrader(service, portfolio).run(
        ranked_df=ranked,
        strategy_mode="technical",
        capital=1_000_000.0,
        risk_config=RiskPolicyConfig(name="test"),
        execution_enabled=True,
    )

    fills = store.list_fills()
    buy_fills = [f for f in fills if f["side"] == "BUY"]
    assert len(buy_fills) == 1
    meta = buy_fills[0]["metadata"]
    assert meta.get("reason") == "entry_confirmed"
    assert meta.get("intent_kind") == "entry"
    assert meta.get("initial_stop") is not None
    assert meta.get("stop_method") in {"atr", "hybrid", "swing_low", "percent", "breakout_candle_low"}
    assert meta.get("rank_at_entry") == 1
    assert meta.get("score_at_entry") == 80.0


def test_engine_entry_persists_stop_via_engine_method_not_local_atr(tmp_path: Path) -> None:
    """When engine supplies initial_stop, service must use it verbatim (no ATR recompute)."""
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    portfolio = PortfolioManager(store)

    ranked = pd.DataFrame([_ranked_row("ACME")])
    AutoTrader(service, portfolio).run(
        ranked_df=ranked,
        strategy_mode="technical",
        capital=1_000_000.0,
        risk_config=RiskPolicyConfig(name="test"),  # default = atr stop, multiple=2
        execution_enabled=True,
    )

    # Default ATR config: close=100, atr=2, mult=2 → stop=96
    stop_record = store.get_position_stop("NSE:ACME")
    assert stop_record is not None
    assert abs(float(stop_record["stop_price"]) - 96.0) < 0.01

    metadata = json.loads(stop_record["metadata_json"])
    assert metadata.get("stop_method") == "atr"
    assert metadata.get("reason") == "entry_confirmed"
    # Streak counters initialised to 0 on a fresh position.
    assert metadata.get("rank_above_threshold_streak", 0) == 0


def test_engine_exit_persists_reason_in_fill_and_deactivates_stop(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    portfolio = PortfolioManager(store)

    # Day 1: enter via engine
    ranked_d1 = pd.DataFrame([_ranked_row("ACME", close=100.0)])
    AutoTrader(service, portfolio).run(
        ranked_df=ranked_d1,
        capital=1_000_000.0,
        risk_config=RiskPolicyConfig(name="test"),
        execution_enabled=True,
    )
    assert store.get_position_stop("NSE:ACME") is not None
    assert store.get_position_stop("NSE:ACME")["status"] == "ACTIVE"

    # Day 2: close below 20DMA AND below 200DMA → engine emits exit and
    # re-entry is blocked (below_sma200 gate fails), so the stop stays deactivated.
    ranked_d2 = pd.DataFrame(
        [_ranked_row("ACME", close=97.0, sma_20=100.0, sma_50=95.0, sma_200=110.0)]
    )
    AutoTrader(service, portfolio).run(
        ranked_df=ranked_d2,
        capital=1_000_000.0,
        risk_config=RiskPolicyConfig(name="test"),
        execution_enabled=True,
    )

    sells = [f for f in store.list_fills() if f["side"] == "SELL"]
    assert len(sells) == 1
    # Highest-priority exit when both fire is close_below_200dma (priority 1)
    # over close_below_20dma (priority 2).
    assert sells[0]["metadata"].get("reason") in {"close_below_200dma", "close_below_20dma"}
    assert sells[0]["metadata"].get("intent_kind") == "exit"

    # Stop record for the closed position must be deactivated (no re-entry).
    assert store.get_position_stop("NSE:ACME")["status"] == "INACTIVE"


def test_streak_counters_increment_across_ticks(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    portfolio = PortfolioManager(store)

    cfg = RiskPolicyConfig(name="test")  # default max_hold_rank=50

    # Day 1: enter ACME at rank 1
    AutoTrader(service, portfolio).run(
        ranked_df=pd.DataFrame([_ranked_row("ACME", eligible_rank=1)]),
        capital=1_000_000.0,
        risk_config=cfg,
        execution_enabled=True,
    )
    # Day 2 + 3: rank slips to 80 (> max_hold_rank=50)
    for day in range(2):
        AutoTrader(service, portfolio).run(
            ranked_df=pd.DataFrame([_ranked_row("ACME", eligible_rank=80, close=99.0)]),
            capital=1_000_000.0,
            risk_config=cfg,
            execution_enabled=True,
        )

    record = store.get_position_stop("NSE:ACME")
    metadata = json.loads(record["metadata_json"])
    # After 2 bad-rank ticks: streak = 2.
    assert metadata["rank_above_threshold_streak"] >= 2
    assert metadata["bars_held"] >= 2


def test_legacy_path_unchanged_when_no_risk_config(tmp_path: Path) -> None:
    """Without risk_config, legacy ATR-based stop logic still works."""

    class _Rm:
        def compute_position_size(self, *args, **kwargs):
            return {"shares": 10, "atr": 1.5, "stop_loss": 95.0}

    store = ExecutionStore(tmp_path)
    service = ExecutionService(
        store, PaperExecutionAdapter(slippage_bps=0), risk_manager=_Rm()
    )
    portfolio = PortfolioManager(store)

    ranked = pd.DataFrame(
        [{"symbol_id": "ACME", "exchange": "NSE", "close": 100.0, "composite_score": 90.0}]
    )
    AutoTrader(service, portfolio).run(
        ranked_df=ranked,
        strategy_mode="technical",
        target_position_count=1,
        capital=1_000_000.0,
        execution_enabled=True,
    )
    fills = store.list_fills()
    assert any(f["side"] == "BUY" for f in fills)
    # Legacy path: intent_kind absent; reason is whatever the legacy diff set it to.
    buy_meta = next(f["metadata"] for f in fills if f["side"] == "BUY")
    assert buy_meta.get("intent_kind") is None
    # Legacy diff uses "target_entry" — engine path uses "entry_confirmed".
    assert buy_meta.get("reason") in {None, "target_entry"}
