"""Optional auto-execution stage for paper trading."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd

from analytics.risk_manager import RiskManager
from execution import AutoTrader, ExecutionService, ExecutionStore, PaperExecutionAdapter, PortfolioManager
from run.stages.base import StageArtifact, StageContext, StageResult
from utils.data_domains import ensure_domain_layout


class ExecuteStage:
    """Convert ranked signals into paper orders and fills."""

    name = "execute"
    EMPTY_COLUMNS = {
        "trade_actions": [
            "action",
            "symbol_id",
            "exchange",
            "side",
            "quantity",
            "requested_price",
            "strategy_mode",
            "reason",
        ],
        "executed_orders": [
            "symbol_id",
            "exchange",
            "side",
            "quantity",
            "requested_price",
            "strategy_mode",
            "reason",
        ],
        "executed_fills": [
            "fill_id",
            "order_id",
            "symbol_id",
            "exchange",
            "side",
            "quantity",
            "price",
        ],
        "positions": [
            "symbol_id",
            "exchange",
            "quantity",
            "avg_entry_price",
            "last_fill_price",
        ],
    }
    PARAMETER_KEYS = [
        "data_domain",
        "ml_mode",
        "strategy_mode",
        "execution_enabled",
        "execution_preview",
        "execution_top_n",
        "execution_ml_horizon",
        "execution_ml_confirm_threshold",
        "execution_capital",
        "execution_fixed_quantity",
        "execution_regime",
        "execution_regime_multiplier",
        "paper_slippage_bps",
    ]

    def run(self, context: StageContext) -> StageResult:
        rank_artifact = context.require_artifact("rank", "ranked_signals")
        ranked_df = pd.read_csv(rank_artifact.uri) if Path(rank_artifact.uri).exists() else pd.DataFrame()

        ml_artifact = context.artifact_for("rank", "ml_overlay")
        ml_overlay_df = pd.read_csv(ml_artifact.uri) if ml_artifact and Path(ml_artifact.uri).exists() else pd.DataFrame()

        paths = ensure_domain_layout(
            project_root=context.project_root,
            data_domain=context.params.get("data_domain", "operational"),
        )
        risk_manager = RiskManager(
            ohlcv_db_path=str(context.db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=context.params.get("data_domain", "operational"),
        )
        store = ExecutionStore(context.project_root)
        service = ExecutionService(
            store,
            PaperExecutionAdapter(slippage_bps=float(context.params.get("paper_slippage_bps", 5.0))),
            default_order_type=str(context.params.get("execution_order_type", "MARKET")),
            default_product_type=str(context.params.get("execution_product_type", "INTRADAY")),
            default_validity=str(context.params.get("execution_validity", "DAY")),
            risk_manager=risk_manager,
        )
        autotrader = AutoTrader(service, PortfolioManager(store))
        execution_enabled = bool(context.params.get("execution_enabled", True))
        preview_only = bool(context.params.get("execution_preview", False))
        result = autotrader.run(
            ranked_df=ranked_df,
            ml_overlay_df=ml_overlay_df,
            strategy_mode=str(context.params.get("strategy_mode", "technical")),
            target_position_count=int(context.params.get("execution_top_n", context.params.get("top_n") or 5)),
            ml_horizon=int(context.params.get("execution_ml_horizon", 5)),
            ml_confirm_threshold=float(context.params.get("execution_ml_confirm_threshold", 0.55)),
            buy_quantity=(
                int(context.params.get("execution_fixed_quantity"))
                if context.params.get("execution_fixed_quantity") not in (None, "")
                else None
            ),
            capital=float(context.params.get("execution_capital", 1_000_000)),
            regime=str(context.params.get("execution_regime", "TREND")),
            regime_multiplier=float(context.params.get("execution_regime_multiplier", 1.0)),
            preview_only=preview_only,
            execution_enabled=execution_enabled,
        )

        actions_df = pd.DataFrame(result["actions"])
        cycle_orders = [item["result"].get("order", {}) for item in result["executions"] if item.get("result")]
        cycle_fills = [
            fill
            for item in result["executions"]
            for fill in item.get("result", {}).get("fills", [])
        ]
        orders_df = pd.DataFrame(cycle_orders)
        fills_df = pd.DataFrame(cycle_fills)
        positions_df = pd.DataFrame(result["positions_after"])

        output_dir = context.output_dir()
        artifacts = []
        artifact_frames: Dict[str, pd.DataFrame] = {
            "trade_actions": actions_df,
            "executed_orders": orders_df,
            "executed_fills": fills_df,
            "positions": positions_df,
        }
        metadata = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "execution_status": result.get("status", "completed"),
            "execution_enabled": execution_enabled,
            "preview_only": preview_only,
            "strategy_mode": str(context.params.get("strategy_mode", "technical")),
            "actions_count": int(len(actions_df)),
            "order_count": int(len(cycle_orders)),
            "fill_count": int(len(cycle_fills)),
            "open_position_count": int(len(positions_df)),
        }

        for artifact_type, df in artifact_frames.items():
            if df.empty:
                df = pd.DataFrame(columns=self.EMPTY_COLUMNS.get(artifact_type, []))
            path = output_dir / f"{artifact_type}.csv"
            df.to_csv(path, index=False)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(df),
                    metadata={"columns": list(df.columns)},
                    attempt_number=context.attempt_number,
                )
            )

        summary_path = output_dir / "execute_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "summary": metadata,
                    "run_date": context.run_date,
                    "parameters": {
                        key: context.params.get(key)
                        for key in self.PARAMETER_KEYS
                        if key in context.params
                    },
                    "positions_before": result["positions_before"],
                    "positions_after": result["positions_after"],
                },
                indent=2,
                sort_keys=True,
                default=str,
            ),
            encoding="utf-8",
        )
        artifacts.append(
            StageArtifact.from_file(
                "execute_summary",
                summary_path,
                row_count=metadata["actions_count"],
                metadata=metadata,
                attempt_number=context.attempt_number,
            )
        )
        return StageResult(artifacts=artifacts, metadata=metadata)
