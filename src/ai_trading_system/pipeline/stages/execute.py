"""Optional auto-execution stage for paper trading."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd

from ai_trading_system.analytics.risk_manager import RiskManager
from ai_trading_system.analytics.regime_detector import RegimeDetector
from ai_trading_system.domains.execution.adapters import PaperExecutionAdapter
from ai_trading_system.domains.execution.autotrader import AutoTrader
from ai_trading_system.domains.execution.portfolio import PortfolioManager
from ai_trading_system.domains.execution.service import ExecutionService
from ai_trading_system.domains.execution.store import ExecutionStore
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.domains.execution.candidate_builder import ExecutionCandidateBuilder, ExecutionRequest


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
        "execution_breakout_linkage",
        "execution_regime",
        "execution_regime_multiplier",
        "paper_slippage_bps",
        "execution_entry_policy",
        "execution_exit_atr_multiple",
        "execution_exit_max_holding_days",
        "execution_use_portfolio_constraints",
        "execution_max_positions",
        "execution_max_sector_exposure",
        "execution_max_single_stock_weight",
        "execution_use_atr_position_sizing",
        "execution_heat_gate_threshold",
        "execution_require_stage2",
        "execution_stage2_min_score",
    ]
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
        "execution_breakout_linkage",
        "execution_regime",
        "execution_regime_multiplier",
        "paper_slippage_bps",
        "execution_entry_policy",
        "execution_exit_atr_multiple",
        "execution_exit_max_holding_days",
        "execution_use_portfolio_constraints",
        "execution_max_positions",
        "execution_max_sector_exposure",
        "execution_max_single_stock_weight",
        "execution_use_atr_position_sizing",
        "execution_heat_gate_threshold",
        "execution_require_stage2",
        "execution_stage2_min_score",
    ]

    def run(self, context: StageContext) -> StageResult:
        context.require_artifact("rank", "ranked_signals")
        request = ExecutionRequest.from_context(context)
        candidates = ExecutionCandidateBuilder().build(context, request=request)

        paths = ensure_domain_layout(
            project_root=context.project_root,
            data_domain=request.data_domain,
        )
        risk_manager = RiskManager(
            ohlcv_db_path=str(context.db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=request.data_domain,
        )
        regime_detector = RegimeDetector(
            ohlcv_db_path=str(context.db_path),
            feature_store_dir=str(paths.feature_store_dir),
        )
        try:
            detected_regime = regime_detector.get_market_regime()
            current_regime = detected_regime.get("market_regime", request.regime or "TREND")
        except Exception:
            current_regime = request.regime or "TREND"
            detected_regime = {"market_regime": current_regime}
        store = ExecutionStore(context.project_root)
        portfolio_manager = PortfolioManager(store)
        service = ExecutionService(
            store,
            PaperExecutionAdapter(slippage_bps=request.paper_slippage_bps),
            default_order_type=request.order_type,
            default_product_type=request.product_type,
            default_validity=request.validity,
            risk_manager=risk_manager,
        )
        autotrader = AutoTrader(service, portfolio_manager)
        current_prices = {}
        if candidates.ranked_df is not None and not candidates.ranked_df.empty and "close" in candidates.ranked_df.columns:
            current_prices = {
                str(row["symbol_id"]).strip().upper(): float(row["close"])
                for row in candidates.ranked_df.to_dict(orient="records")
                if row.get("symbol_id") not in (None, "") and pd.notna(row.get("close"))
            }
        atr_by_symbol = {}
        if candidates.ranked_df is not None and not candidates.ranked_df.empty and "atr_14" in candidates.ranked_df.columns:
            atr_by_symbol = {
                str(row["symbol_id"]).strip().upper(): float(row["atr_14"])
                for row in candidates.ranked_df.to_dict(orient="records")
                if row.get("symbol_id") not in (None, "") and pd.notna(row.get("atr_14"))
            }
        result = autotrader.run(
            ranked_df=candidates.ranked_df,
            ml_overlay_df=candidates.ml_overlay_df,
            current_prices=current_prices,
            strategy_mode=request.strategy_mode,
            target_position_count=request.target_position_count,
            ml_horizon=request.ml_horizon,
            ml_confirm_threshold=request.ml_confirm_threshold,
            buy_quantity=request.buy_quantity,
            capital=request.capital,
            regime=current_regime,
            regime_multiplier=request.regime_multiplier,
            preview_only=request.preview_only,
            execution_enabled=request.execution_enabled,
            entry_policy_name=request.entry_policy_name,
            exit_atr_multiple=request.exit_atr_multiple,
            exit_max_holding_days=request.exit_max_holding_days,
            use_portfolio_constraints=request.use_portfolio_constraints,
            max_positions=request.max_positions,
            max_sector_exposure=request.max_sector_exposure,
            max_single_stock_weight=request.max_single_stock_weight,
            use_atr_position_sizing=request.use_atr_position_sizing,
            heat_gate_threshold=context.params.get("execution_heat_gate_threshold", 0.08),
        )
        trailing_summary = {"updated_count": 0, "evaluated_count": 0}
        if request.execution_enabled and not request.preview_only:
            open_symbols = set(portfolio_manager.open_positions().keys())
            trailing_summary = service.maintain_trailing_stops(
                current_prices=current_prices,
                atr_by_symbol=atr_by_symbol,
                open_symbols=open_symbols,
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
            "execution_enabled": request.execution_enabled,
            "preview_only": request.preview_only,
            "strategy_mode": request.strategy_mode,
            "data_trust_status": candidates.data_trust_status,
            "trust_confidence": candidates.trust_confidence,
            "breakout_linkage_mode": candidates.breakout_linkage_mode,
            "ranked_rows_before_linkage": candidates.ranked_rows_before_linkage,
            "ranked_rows_after_linkage": candidates.ranked_rows_after_linkage,
            "breakout_candidates_count": candidates.breakout_candidates_count,
            "breakout_qualified_count": candidates.breakout_qualified_count,
            "breakout_tier_a_count": candidates.breakout_tier_a_count,
            "stage2_gate": candidates.stage2_gate,
            "actions_count": int(len(actions_df)),
            "order_count": int(len(cycle_orders)),
            "fill_count": int(len(cycle_fills)),
            "open_position_count": int(len(positions_df)),
            "detected_regime": current_regime,
            "regime_details": detected_regime,
            "canary_blocked": bool(context.params.get("canary")) and context.params.get("canary_blocked", False),
            "trailing_stops_updated": int(trailing_summary.get("updated_count", 0) or 0),
            "trailing_stops_evaluated": int(trailing_summary.get("evaluated_count", 0) or 0),
        }

        total_position_value = sum(
            pos.get("quantity", 0) * pos.get("avg_entry_price", 0)
            for pos in result.get("positions_after", [])
        )
        portfolio_value = request.capital - total_position_value + total_position_value
        try:
            import duckdb
            conn = duckdb.connect(str(context.db_path), read_only=True)
            position_symbols = [p.get("symbol_id") for p in result.get("positions_after", []) if p.get("symbol_id")]
            if position_symbols:
                latest_prices = conn.execute("""
                    SELECT symbol_id, close
                    FROM _catalog
                    WHERE symbol_id IN (SELECT UNNEST(?))
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) = 1
                """, [position_symbols]).fetchall()
                latest_close_map = {row[0]: row[1] for row in latest_prices}
                total_position_value_mtm = sum(
                    p.get("quantity", 0) * latest_close_map.get(p.get("symbol_id"), p.get("avg_entry_price", 0))
                    for p in result.get("positions_after", [])
                )
                portfolio_value = request.capital - total_position_value + total_position_value_mtm
            conn.close()
        except Exception:
            pass

        peak_value = store.get_latest_drawdown(context.run_id)
        current_peak = peak_value["peak_value"] if peak_value and peak_value.get("peak_value") else portfolio_value
        current_heat = total_position_value / request.capital if request.capital > 0 else 0.0

        store.record_drawdown_snapshot(
            run_id=context.run_id,
            portfolio_value=portfolio_value,
            peak_value=max(current_peak, portfolio_value),
            portfolio_heat=current_heat,
            snapshot_type="intraday",
            metadata={"actions_count": len(actions_df), "fill_count": len(cycle_fills)},
        )

        is_eod = context.params.get("is_eod", False)
        if is_eod:
            store.record_drawdown_snapshot(
                run_id=context.run_id,
                portfolio_value=portfolio_value,
                peak_value=max(current_peak, portfolio_value),
                portfolio_heat=current_heat,
                snapshot_type="eod",
                metadata={"actions_count": len(actions_df), "fill_count": len(cycle_fills)},
            )

        metadata["portfolio_drawdown_pct"] = round(
            ((portfolio_value - max(current_peak, portfolio_value)) / max(current_peak, portfolio_value) * 100)
            if max(current_peak, portfolio_value) > 0 else 0.0,
            2,
        )
        metadata["portfolio_heat"] = round(current_heat, 4)

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
