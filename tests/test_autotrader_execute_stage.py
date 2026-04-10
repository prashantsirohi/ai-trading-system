from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution import AutoTrader, ExecutionService, ExecutionStore, PaperExecutionAdapter, PortfolioManager
from execution.models import OrderIntent
from run.stages import ExecuteStage
from run.stages.base import StageArtifact, StageContext


class _StaticRiskManager:
    def compute_position_size(
        self,
        symbol_id: str,
        exchange: str = "NSE",
        capital: float = 1_000_000,
        regime: str = "TREND",
        regime_multiplier: float = 1.0,
    ) -> dict:
        return {
            "symbol_id": symbol_id,
            "shares": 10,
            "position_value": 1000.0,
            "risk_amount": 50.0,
            "stop_loss": 95.0,
            "atr": 2.0,
            "close": 100.0,
            "regime": regime,
            "regime_multiplier": regime_multiplier,
        }


def test_autotrader_rebalances_positions_in_technical_mode(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0), risk_manager=_StaticRiskManager())
    portfolio = PortfolioManager(store)
    service.submit_order(
        OrderIntent(symbol_id="AAA", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "BBB", "exchange": "NSE", "close": 101.0, "composite_score": 90.0},
            {"symbol_id": "CCC", "exchange": "NSE", "close": 102.0, "composite_score": 89.0},
        ]
    )

    result = AutoTrader(service, portfolio).run(
        ranked_df=ranked_df,
        strategy_mode="technical",
        target_position_count=2,
    )

    actions = pd.DataFrame(result["actions"])
    assert set(actions["action"]) == {"BUY", "SELL"}
    assert set(actions["symbol_id"]) == {"AAA", "BBB", "CCC"}
    positions_after = {row["symbol_id"]: row["quantity"] for row in result["positions_after"]}
    assert positions_after["BBB"] == 10
    assert positions_after["CCC"] == 10
    assert "AAA" not in positions_after


def test_execute_stage_runs_hybrid_confirm_and_writes_artifacts(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    ranked_df = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 92.0},
            {"symbol_id": "BBB", "exchange": "NSE", "close": 98.0, "composite_score": 90.0},
        ]
    )
    ranked_df.to_csv(ranked_path, index=False)

    overlay_path = tmp_path / "ml_overlay.csv"
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "ml_5d_prob": 0.72, "blend_5d_rank": 1},
            {"symbol_id": "BBB", "exchange": "NSE", "ml_5d_prob": 0.44, "blend_5d_rank": 2},
        ]
    ).to_csv(overlay_path, index=False)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-05-execute",
        run_date="2026-04-05",
        stage_name="execute",
        attempt_number=1,
        params={
            "data_domain": "operational",
            "strategy_mode": "hybrid_confirm",
            "execution_top_n": 2,
            "execution_ml_confirm_threshold": 0.55,
            "execution_fixed_quantity": 10,
            "paper_slippage_bps": 0,
        },
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact("ranked_signals", str(ranked_path)),
                "ml_overlay": StageArtifact("ml_overlay", str(overlay_path)),
            }
        },
    )

    result = ExecuteStage().run(context)

    assert result.metadata["strategy_mode"] == "hybrid_confirm"
    assert result.metadata["actions_count"] == 1
    assert any(artifact.artifact_type == "trade_actions" for artifact in result.artifacts)
    assert any(artifact.artifact_type == "execute_summary" for artifact in result.artifacts)

    actions = pd.read_csv(context.output_dir() / "trade_actions.csv")
    assert actions["symbol_id"].tolist() == ["AAA"]
    assert actions["action"].tolist() == ["BUY"]

    summary = json.loads((context.output_dir() / "execute_summary.json").read_text(encoding="utf-8"))
    assert summary["summary"]["open_position_count"] == 1
    assert summary["run_date"] == "2026-04-05"
    assert summary["parameters"]["strategy_mode"] == "hybrid_confirm"
    assert summary["parameters"]["execution_fixed_quantity"] == 10


def test_execute_stage_preview_mode_does_not_create_fills(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame(
        [{"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 92.0}]
    ).to_csv(ranked_path, index=False)

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-05-preview",
        run_date="2026-04-05",
        stage_name="execute",
        attempt_number=1,
        params={
            "data_domain": "operational",
            "strategy_mode": "technical",
            "execution_top_n": 1,
            "execution_fixed_quantity": 10,
            "execution_preview": True,
            "execution_enabled": False,
            "paper_slippage_bps": 0,
        },
        artifacts={"rank": {"ranked_signals": StageArtifact("ranked_signals", str(ranked_path))}},
    )

    result = ExecuteStage().run(context)

    assert result.metadata["execution_status"] == "preview"
    assert result.metadata["preview_only"] is True
    assert result.metadata["fill_count"] == 0
    orders = pd.read_csv(context.output_dir() / "executed_orders.csv")
    assert orders.iloc[0]["symbol_id"] == "AAA"
    fills = pd.read_csv(context.output_dir() / "executed_fills.csv")
    assert fills.empty
    summary = json.loads((context.output_dir() / "execute_summary.json").read_text(encoding="utf-8"))
    assert summary["parameters"]["execution_preview"] is True


def test_execute_stage_soft_gate_uses_qualified_breakouts_only(tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 92.0},
            {"symbol_id": "BBB", "exchange": "NSE", "close": 101.0, "composite_score": 91.0},
        ]
    ).to_csv(ranked_path, index=False)

    breakout_path = tmp_path / "breakout_scan.csv"
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "breakout_state": "qualified", "candidate_tier": "A"},
            {"symbol_id": "BBB", "breakout_state": "qualified", "candidate_tier": "B"},
        ]
    ).to_csv(breakout_path, index=False)

    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps({"summary": {"data_trust_status": "trusted"}}),
        encoding="utf-8",
    )

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-05-soft-gate",
        run_date="2026-04-05",
        stage_name="execute",
        attempt_number=1,
        params={
            "data_domain": "operational",
            "strategy_mode": "technical",
            "execution_top_n": 2,
            "execution_fixed_quantity": 10,
            "execution_preview": True,
            "execution_enabled": False,
            "execution_breakout_linkage": "soft_gate",
            "paper_slippage_bps": 0,
        },
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact("ranked_signals", str(ranked_path)),
                "breakout_scan": StageArtifact("breakout_scan", str(breakout_path)),
                "dashboard_payload": StageArtifact("dashboard_payload", str(dashboard_path)),
            }
        },
    )

    result = ExecuteStage().run(context)

    assert result.metadata["breakout_linkage_mode"] == "soft_gate"
    assert result.metadata["ranked_rows_before_linkage"] == 2
    assert result.metadata["ranked_rows_after_linkage"] == 1
    assert result.metadata["breakout_tier_a_count"] == 1
    actions = pd.read_csv(context.output_dir() / "trade_actions.csv")
    assert actions["symbol_id"].tolist() == ["AAA"]
