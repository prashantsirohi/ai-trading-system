"""Optional auto-execution stage for paper trading."""

from __future__ import annotations

import json
import logging
import os
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
from ai_trading_system.domains.risk import RiskPolicyConfig, load_profile
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.domains.execution.candidate_builder import ExecutionCandidateBuilder, ExecutionRequest


_logger = logging.getLogger(__name__)


def _build_market_extras(ranked_df) -> dict[str, dict]:
    """Pull ATR / SMA-200 / swing-low-20 from the ranked DF when present."""
    if ranked_df is None or ranked_df.empty:
        return {}
    keys = {"atr_14", "sma_11", "sma_200", "volume_ratio_20", "swing_low_20"}
    columns = [col for col in keys if col in ranked_df.columns]
    if not columns:
        return {}
    extras: dict[str, dict] = {}
    for row in ranked_df.to_dict(orient="records"):
        sid = row.get("symbol_id")
        if not sid:
            continue
        extras[str(sid)] = {col: row.get(col) for col in columns if pd.notna(row.get(col))}
    return extras


def _resolve_risk_config(context) -> RiskPolicyConfig | None:
    """Pick a risk profile from context.params or RISK_PROFILE env var."""
    name = None
    try:
        name = context.params.get("risk_profile") if context and context.params else None
    except AttributeError:
        name = None
    if not name:
        name = os.environ.get("RISK_PROFILE")
    if not name:
        return None
    cfg = load_profile(str(name))
    _logger.info("execute stage: using risk_profile=%s", cfg.name)
    return cfg


def _extract_regime_overlay(dashboard_payload: dict) -> tuple[dict, dict, dict]:
    """Return rank-stage regime snapshot/profile/disagreement dictionaries.

    The third element is a structured disagreement payload (see
    ``regime_disagreement``) — present even when there's no divergence, with
    ``present=False`` / ``dangerous=False``.
    """
    market_regime = dashboard_payload.get("market_regime") or {}
    regime_profile = dashboard_payload.get("regime_profile") or {}
    disagreement = dashboard_payload.get("market_regime_disagreement") or {}
    return (
        market_regime if isinstance(market_regime, dict) else {},
        regime_profile if isinstance(regime_profile, dict) else {},
        disagreement if isinstance(disagreement, dict) else {},
    )


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
        rank_regime, rank_profile, regime_disagree = _extract_regime_overlay(
            candidates.dashboard_payload
        )

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
            current_regime = str(rank_regime.get("regime") or detected_regime.get("market_regime", request.regime or "TREND"))
        except Exception:
            current_regime = str(rank_regime.get("regime") or request.regime or "TREND")
            detected_regime = {"market_regime": current_regime}
        if rank_regime:
            detected_regime = {**detected_regime, "breadth_regime": rank_regime}

        effective_capital = request.capital
        effective_top_n = request.target_position_count
        effective_max_positions = request.max_positions
        effective_max_sector_exposure = request.max_sector_exposure
        effective_max_single_stock_weight = request.max_single_stock_weight
        effective_exit_atr_multiple = request.exit_atr_multiple
        effective_use_portfolio_constraints = request.use_portfolio_constraints
        if rank_profile:
            max_exposure_val = rank_profile.get("max_exposure")
            max_exposure = float(max_exposure_val) if max_exposure_val is not None else 1.0
            effective_capital = request.capital * max_exposure
            max_positions_val = rank_profile.get("max_positions")
            effective_max_positions = (
                int(max_positions_val) if max_positions_val is not None else request.max_positions
            )
            effective_top_n = (
                min(request.target_position_count, effective_max_positions)
                if "execution_top_n" in context.params
                else effective_max_positions
            )
            effective_max_sector_exposure = float(rank_profile.get("max_sector_exposure") or request.max_sector_exposure)
            effective_max_single_stock_weight = float(
                rank_profile.get("max_single_stock_weight") or request.max_single_stock_weight
            )
            effective_exit_atr_multiple = float(rank_profile.get("atr_stop_mult") or request.exit_atr_multiple)
            effective_use_portfolio_constraints = True
        # Phase 6: per-regime risk_per_trade_pct override. None means the
        # profile didn't declare one (or rank_profile itself is absent) —
        # autotrader falls through to the signal-level value, then the
        # historic 0.01 default.
        effective_risk_per_trade_pct: float | None = None
        if rank_profile:
            raw_val = rank_profile.get("risk_per_trade_pct")
            if raw_val is not None:
                try:
                    effective_risk_per_trade_pct = float(raw_val)
                except (TypeError, ValueError):
                    effective_risk_per_trade_pct = None

        # Raw-regime early-warning override. Opt-in via param. When the raw
        # breadth signal is risk_off while confirmed is still bull/strong_bull,
        # the operator may want to behave as if today is risk_off (block fresh
        # entries, no new capital deployment) without waiting for the 3-day
        # hysteresis to flip the confirmed regime. Existing positions are
        # untouched — only entry-side caps are zeroed.
        raw_override_enabled = bool(
            context.params.get("execution_raw_regime_overrides_on_disagreement", False)
        )
        raw_override_active = False
        if raw_override_enabled and regime_disagree.get("dangerous"):
            raw_override_active = True
            _logger.warning(
                "execute stage: raw_regime override active (raw=risk_off, "
                "confirmed=%s) — zeroing entry capital and max_positions",
                regime_disagree.get("confirmed", "unknown"),
            )
            effective_capital = 0.0
            effective_max_positions = 0
            effective_top_n = 0
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
            target_position_count=effective_top_n,
            ml_horizon=request.ml_horizon,
            ml_confirm_threshold=request.ml_confirm_threshold,
            buy_quantity=request.buy_quantity,
            capital=effective_capital,
            regime=current_regime,
            regime_multiplier=request.regime_multiplier,
            preview_only=request.preview_only,
            execution_enabled=request.execution_enabled,
            entry_policy_name=request.entry_policy_name,
            exit_atr_multiple=effective_exit_atr_multiple,
            exit_max_holding_days=request.exit_max_holding_days,
            use_portfolio_constraints=effective_use_portfolio_constraints,
            max_positions=effective_max_positions,
            max_sector_exposure=effective_max_sector_exposure,
            max_single_stock_weight=effective_max_single_stock_weight,
            use_atr_position_sizing=request.use_atr_position_sizing,
            heat_gate_threshold=context.params.get("execution_heat_gate_threshold", 0.08),
            risk_config=_resolve_risk_config(context),
            market_extras=_build_market_extras(candidates.ranked_df),
            # Phase 6: profile-driven risk_per_trade_pct override. None
            # preserves the legacy "signal payload sets it" behavior.
            risk_per_trade_pct=effective_risk_per_trade_pct,
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
            "market_regime": rank_regime,
            "market_regime_disagreement": regime_disagree,
            "raw_regime_override_active": raw_override_active,
            "regime_profile": rank_profile,
            "effective_risk_per_trade_pct": effective_risk_per_trade_pct,
            "effective_execution_capital": effective_capital,
            "effective_execution_top_n": effective_top_n,
            "effective_max_positions": effective_max_positions,
            "effective_max_sector_exposure": effective_max_sector_exposure,
            "effective_max_single_stock_weight": effective_max_single_stock_weight,
            "effective_exit_atr_multiple": effective_exit_atr_multiple,
            "canary_blocked": bool(context.params.get("canary")) and context.params.get("canary_blocked", False),
            "trailing_stops_updated": int(trailing_summary.get("updated_count", 0) or 0),
            "trailing_stops_evaluated": int(trailing_summary.get("evaluated_count", 0) or 0),
        }

        total_position_value = sum(
            pos.get("quantity", 0) * pos.get("avg_entry_price", 0)
            for pos in result.get("positions_after", [])
        )
        portfolio_value = effective_capital - total_position_value + total_position_value
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
                portfolio_value = effective_capital - total_position_value + total_position_value_mtm
            conn.close()
        except Exception:
            pass

        peak_value = store.get_latest_drawdown(context.run_id)
        current_peak = peak_value["peak_value"] if peak_value and peak_value.get("peak_value") else portfolio_value
        current_heat = total_position_value / effective_capital if effective_capital > 0 else 0.0

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
