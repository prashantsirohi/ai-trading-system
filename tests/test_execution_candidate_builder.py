from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.domains.execution.candidate_builder import (
    ExecutionCandidateBuilder,
    attach_execution_weight,
    prioritize_execution_candidates,
    ExecutionRequest,
)


def _stage_context(
    tmp_path: Path,
    *,
    params: dict | None = None,
    artifacts: dict | None = None,
) -> StageContext:
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-15-execute-builder",
        run_date="2026-04-15",
        stage_name="execute",
        attempt_number=1,
        params={"data_domain": "operational", **(params or {})},
        artifacts=artifacts or {},
    )


def test_execution_request_normalizes_context_params(tmp_path: Path) -> None:
    context = _stage_context(
        tmp_path,
        params={
            "strategy_mode": "hybrid_confirm",
            "execution_enabled": False,
            "execution_preview": True,
            "execution_top_n": 7,
            "execution_ml_horizon": 10,
            "execution_ml_confirm_threshold": 0.61,
            "execution_capital": 250000,
            "execution_fixed_quantity": "12",
            "execution_breakout_linkage": " soft_gate ",
            "execution_regime": "RANGE",
            "execution_regime_multiplier": 0.8,
            "paper_slippage_bps": 0,
            "execution_order_type": "LIMIT",
            "execution_product_type": "CNC",
            "execution_validity": "IOC",
        },
    )

    request = ExecutionRequest.from_context(context)

    assert request.strategy_mode == "hybrid_confirm"
    assert request.execution_enabled is False
    assert request.preview_only is True
    assert request.target_position_count == 7
    assert request.ml_horizon == 10
    assert request.ml_confirm_threshold == pytest.approx(0.61)
    assert request.capital == pytest.approx(250000.0)
    assert request.buy_quantity == 12
    assert request.breakout_linkage_mode == "soft_gate"
    assert request.regime == "RANGE"
    assert request.regime_multiplier == pytest.approx(0.8)
    assert request.paper_slippage_bps == pytest.approx(0.0)
    assert request.order_type == "LIMIT"
    assert request.product_type == "CNC"
    assert request.validity == "IOC"
    assert request.entry_policy_name == "breakout"
    assert request.exit_atr_multiple == pytest.approx(2.0)
    assert request.exit_max_holding_days == 20
    assert request.use_portfolio_constraints is False
    assert request.max_positions == 10
    assert request.max_sector_exposure == pytest.approx(0.20)
    assert request.max_single_stock_weight == pytest.approx(0.10)
    assert request.use_atr_position_sizing is False


def test_execution_candidate_priority_and_weight_scaffolding() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 90.0, "rank_confidence": 0.9, "signal_decay_score": 0.8},
            {"symbol_id": "BBB", "composite_score": 95.0, "rank_confidence": 0.7, "signal_decay_score": 0.9},
        ]
    )

    out = attach_execution_weight(prioritize_execution_candidates(frame))

    assert out["symbol_id"].tolist() == ["BBB", "AAA"]
    assert out["execution_weight"].tolist() == [0.7, 0.9]


def test_execution_candidate_builder_applies_soft_gate_and_loads_overlay(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 92.0},
            {"symbol_id": "BBB", "exchange": "NSE", "close": 99.0, "composite_score": 91.0},
        ]
    ).to_csv(ranked_path, index=False)

    breakout_path = tmp_path / "breakout_scan.csv"
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "breakout_state": "qualified", "candidate_tier": "A"},
            {"symbol_id": "BBB", "breakout_state": "qualified", "candidate_tier": "B"},
        ]
    ).to_csv(breakout_path, index=False)

    overlay_path = tmp_path / "ml_overlay.csv"
    pd.DataFrame(
        [{"symbol_id": "AAA", "exchange": "NSE", "ml_5d_prob": 0.72, "blend_5d_rank": 1}]
    ).to_csv(overlay_path, index=False)

    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps({"summary": {"data_trust_status": "trusted"}}),
        encoding="utf-8",
    )

    context = _stage_context(
        tmp_path,
        params={"execution_breakout_linkage": "soft_gate"},
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact("ranked_signals", str(ranked_path)),
                "breakout_scan": StageArtifact("breakout_scan", str(breakout_path)),
                "ml_overlay": StageArtifact("ml_overlay", str(overlay_path)),
                "dashboard_payload": StageArtifact("dashboard_payload", str(dashboard_path)),
            }
        },
    )

    request = ExecutionRequest.from_context(context)
    bundle = ExecutionCandidateBuilder().build(context, request=request)

    assert bundle.data_trust_status == "trusted"
    assert bundle.breakout_linkage_mode == "soft_gate"
    assert bundle.ranked_rows_before_linkage == 2
    assert bundle.ranked_rows_after_linkage == 1
    assert bundle.breakout_candidates_count == 2
    assert bundle.breakout_qualified_count == 1
    assert bundle.breakout_tier_a_count == 1
    assert bundle.ranked_df["symbol_id"].tolist() == ["AAA"]
    assert bundle.ml_overlay_df["symbol_id"].tolist() == ["AAA"]


def test_execution_candidate_builder_respects_untrusted_execution_override(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "composite_score": 90.0}]).to_csv(ranked_path, index=False)

    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps({"summary": {"data_trust_status": "degraded"}}),
        encoding="utf-8",
    )

    context = _stage_context(
        tmp_path,
        params={
            "block_degraded_execution": True,
            "allow_untrusted_execution": True,
        },
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact("ranked_signals", str(ranked_path)),
                "dashboard_payload": StageArtifact("dashboard_payload", str(dashboard_path)),
            }
        },
    )

    request = ExecutionRequest.from_context(context)
    bundle = ExecutionCandidateBuilder().build(context, request=request)

    assert bundle.data_trust_status == "degraded"
    assert bundle.ranked_rows_after_linkage == 1
