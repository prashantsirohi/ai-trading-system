"""Optional Phase 3B deterministic shadow scan router."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.execution.store import ExecutionStore
from ai_trading_system.domains.opportunities.contracts import CandidateState, WeinsteinStage
from ai_trading_system.domains.opportunities.registry.service import OpportunityRegistryService
from ai_trading_system.domains.opportunities.registry.store import DuckDBOpportunityRegistryStore
from ai_trading_system.domains.opportunities.routing import (
    OpportunityScanRoutingMode,
    PositionMonitoringConfig,
    ScanRoutingConfig,
    ScanTier,
    decide_scan_route,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.pipeline.contracts import PipelineStageError, StageArtifact, StageContext, StageResult


class ScanRouterStage:
    name = "scan_router"

    def run(self, context: StageContext) -> StageResult:
        started_at = time.perf_counter()
        config = ScanRoutingConfig.from_mapping(context.params)
        if config.mode is OpportunityScanRoutingMode.OFF:
            return StageResult(metadata={"status": "skipped", "mode": "off"})
        position_config = PositionMonitoringConfig.from_mapping(context.params)
        stock_artifact = context.require_artifact("weekly_stage", "weekly_stock_stage_universe")
        stock = _read_csv(stock_artifact.uri)
        sector = _read_csv((context.artifact_for("weekly_stage", "weekly_sector_stage_universe") or stock_artifact).uri)
        promotions = _read_csv((context.artifact_for("weekly_stage", "stage_promotion_candidates") or stock_artifact).uri)
        ranked_artifact = context.require_artifact("rank", "ranked_signals")
        ranked = _read_csv(ranked_artifact.uri)
        rank_col = "rank_position" if "rank_position" in ranked else None
        ranked = ranked.copy()
        if "symbol_id" in ranked:
            ranked.loc[:, "symbol_id"] = ranked["symbol_id"].astype(str).str.upper()
        if rank_col is None:
            ranked.loc[:, "rank_position"] = range(1, len(ranked) + 1)
            rank_col = "rank_position"
        rank_selected = set(ranked.sort_values(rank_col).head(config.rank_deep_scan_limit)["symbol_id"].astype(str)) if "symbol_id" in ranked else set()
        promotion_symbols = set()
        if not promotions.empty and "symbol_id" in promotions:
            promotion_order = promotions.sort_values("light_pattern_score", ascending=False) if "light_pattern_score" in promotions else promotions
            promotion_symbols = set(
                promotion_order.head(config.stage_promoted_scan_limit)["symbol_id"].astype(str).str.upper()
            )

        paths = get_domain_paths(context.project_root, context.params.get("data_domain", "operational"))
        execution = ExecutionStore(context.project_root, db_path=paths.root_dir / "execution.duckdb", initialize=False)
        cycles = execution.list_position_cycles()
        active = {(cycle.exchange, cycle.symbol_id): cycle for cycle in cycles if cycle.active}
        stops = {
            (str(row.get("exchange") or "NSE").upper(), str(row.get("symbol_id") or "").upper()): row
            for row in (execution.list_active_stops() if execution.db_path.exists() else [])
        }
        recent = {
            (cycle.exchange, cycle.symbol_id): cycle
            for cycle in execution.list_recently_exited_positions(
                ohlcv_db_path=context.db_path,
                as_of=context.run_date,
                cooling_sessions=position_config.recent_exit_cooling_sessions,
            )
        }
        lifecycle = _open_lifecycle(context)
        stock_rows = {str(row["symbol_id"]).upper(): row for row in stock.to_dict(orient="records")}
        sector_rows = {str(row.get("sector_id") or ""): row for row in sector.to_dict(orient="records")}
        rank_rows = {str(row["symbol_id"]).upper(): row for row in ranked.to_dict(orient="records") if row.get("symbol_id")}
        all_symbols = set(stock_rows) | set(rank_selected) | set(promotion_symbols) | {key[1] for key in active} | {key[1] for key in recent} | set(lifecycle)
        decisions = []
        for symbol in sorted(all_symbols):
            stage_row = stock_rows.get(symbol, {})
            rank_row = rank_rows.get(symbol, {})
            stage = _stage(stage_row.get("effective_stage"))
            lifecycle_state = lifecycle.get(symbol)
            sector_row = sector_rows.get(str(stage_row.get("sector_id") or ""), {})
            decision = decide_scan_route(
                symbol_id=symbol,
                exchange=str(stage_row.get("exchange") or rank_row.get("exchange") or "NSE"),
                rank_position=_int(rank_row.get("rank_position")),
                rank_selected=symbol in rank_selected,
                stage_discovery=stage in {WeinsteinStage.STAGE_1, WeinsteinStage.TRANSITION_1_TO_2},
                stage_promoted=symbol in promotion_symbols,
                active_position=any(key[1] == symbol for key in active),
                recently_exited=any(key[1] == symbol for key in recent),
                triggered=lifecycle_state is CandidateState.TRIGGERED,
                pending_followthrough=lifecycle_state is CandidateState.PENDING_FOLLOWTHROUGH,
                stock_stage=stage,
                sector_stage=_stage(sector_row.get("effective_stage")),
                market_data_available=symbol in stock_rows,
                policy_version=config.scan_policy_version,
            )
            decisions.append(decision)
        missing_active = sorted({key[1] for key in active} - {item.symbol_id for item in decisions if item.scan_tier is ScanTier.POSITION_MONITOR})
        if missing_active:
            raise PipelineStageError(f"active positions missing scan routing: {missing_active}")

        rows = []
        for item in decisions:
            row = _decision_row(item)
            cycle = active.get((item.exchange, item.symbol_id))
            recent_cycle = recent.get((item.exchange, item.symbol_id))
            row["position_cycle_opened_at"] = cycle.cycle_opened_at if cycle else None
            row["last_exited_at"] = recent_cycle.last_exited_at if recent_cycle else None
            stop = stops.get((item.exchange, item.symbol_id), {})
            row["stop_price"] = stop.get("stop_price")
            close = stage_row.get("weekly_close") if (stage_row := stock_rows.get(item.symbol_id, {})) else None
            row["stop_proximity_pct"] = _stop_proximity(close, stop.get("stop_price"))
            row["weekly_ma_slope_deteriorating"] = _float(stage_row.get("weekly_ma_30_slope_acceleration"), 0.0) < 0
            row["price_below_weekly_ma"] = _float(stage_row.get("price_vs_weekly_ma_30_pct"), 0.0) < 0
            row["sector_weakening"] = item.sector_stage in {WeinsteinStage.TRANSITION_2_TO_3, WeinsteinStage.STAGE_3, WeinsteinStage.STAGE_4}
            rows.append(row)
        routing = pd.DataFrame(rows)
        output = context.output_dir()
        outputs = {
            "scan_routing": routing,
            "stage_discovery_candidates": routing.loc[routing.get("stage_selected", pd.Series(False, index=routing.index)).fillna(False)] if not routing.empty else routing,
            "deep_scan_universe": routing.loc[routing.get("scan_tier", pd.Series(dtype=str)).isin({ScanTier.FULL_INVESTIGATOR.value, ScanTier.POSITION_MONITOR.value})] if not routing.empty else routing,
            "position_monitor_universe": routing.loc[routing.get("scan_tier", pd.Series(dtype=str)).eq(ScanTier.POSITION_MONITOR.value)] if not routing.empty else routing,
            "routing_conflicts": routing.loc[
                routing.get("active_position", pd.Series(False, index=routing.index)).fillna(False)
                & ~routing.get("market_data_available", pd.Series(False, index=routing.index)).fillna(False)
            ].assign(severity="high", conflict="active_position_missing_market_data") if not routing.empty else routing,
        }
        artifacts: list[StageArtifact] = []
        for artifact_type, frame in outputs.items():
            path = output / f"{artifact_type}.csv"
            frame.to_csv(path, index=False)
            artifacts.append(StageArtifact.from_file(artifact_type, path, row_count=len(frame), attempt_number=context.attempt_number))
        old_symbols = set(ranked.get("symbol_id", pd.Series(dtype=str)).head(int(context.params.get("pattern_max_symbols", 150))).astype(str).str.upper())
        new_symbols = set(outputs["deep_scan_universe"].get("symbol_id", pd.Series(dtype=str)).astype(str))
        comparison = {
            "existing_deep_scan_count": len(old_symbols),
            "phase3b_deep_scan_count": len(new_symbols),
            "overlap_count": len(old_symbols & new_symbols),
            "newly_discovered_by_stage_routing": sorted((new_symbols - old_symbols) & promotion_symbols),
            "active_positions_missing_from_current_deep_scan": sorted({key[1] for key in active} - old_symbols),
            "current_rank_selected_structurally_blocked": sorted(set(routing.loc[routing.get("structural_long_blocked", pd.Series(False, index=routing.index)).eq(True), "symbol_id"]) & rank_selected) if not routing.empty else [],
            "incremental_compute_seconds": round(time.perf_counter() - started_at, 3),
        }
        comparison_path = context.write_json("scan_routing_comparison.json", comparison)
        artifacts.append(StageArtifact.from_file("scan_routing_comparison", comparison_path, attempt_number=context.attempt_number))
        summary = _summary(routing, len(active), comparison)
        summary_path = context.write_json("scan_coverage_summary.json", summary)
        artifacts.append(StageArtifact.from_file("scan_coverage_summary", summary_path, attempt_number=context.attempt_number))
        if context.registry is not None:
            _persist(context, rows, f"{stock_artifact.content_hash}|{ranked_artifact.content_hash}")
        return StageResult(artifacts=artifacts, metadata=summary)


def _open_lifecycle(context: StageContext) -> dict[str, CandidateState]:
    if context.registry is None:
        return {}
    service = OpportunityRegistryService(DuckDBOpportunityRegistryStore(context.registry))
    result: dict[str, CandidateState] = {}
    for state in service.list_open_candidates():
        raw = getattr(state, "current_lifecycle_state", None)
        try:
            result[str(state.symbol_id).upper()] = CandidateState(raw)
        except (TypeError, ValueError):
            continue
    return result


def _persist(context: StageContext, rows: list[dict[str, Any]], source: str) -> None:
    source_hash = hashlib.sha256(source.encode()).hexdigest()
    with context.registry._writer() as conn:  # noqa: SLF001
        for row in rows:
            decision_id = hashlib.sha256(f"{context.run_id}|{context.attempt_number}|{row['exchange']}|{row['symbol_id']}|{row['policy_version']}|{source_hash}".encode()).hexdigest()
            conn.execute(
                """INSERT INTO opportunity_scan_routing_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                   ON CONFLICT(decision_id) DO NOTHING""",
                [decision_id, context.run_id, context.attempt_number, context.run_date, row["exchange"], row["symbol_id"], row["scan_tier"], json.dumps(row["scan_reasons"]), row["policy_version"], source_hash, json.dumps(row, sort_keys=True, default=str)],
            )


def _decision_row(decision: Any) -> dict[str, Any]:
    row = asdict(decision)
    row["scan_tier"] = decision.scan_tier.value
    row["scan_reasons"] = [reason.value for reason in decision.reasons]
    row["stock_stage"] = decision.stock_stage.value
    row["sector_stage"] = decision.sector_stage.value
    return row


def _summary(routing: pd.DataFrame, active_total: int, comparison: dict[str, Any]) -> dict[str, Any]:
    tiers = routing.get("scan_tier", pd.Series(dtype=str)).value_counts().to_dict()
    active_monitored = int((routing.get("active_position", pd.Series(False, index=routing.index)).fillna(False) & routing.get("scan_tier", pd.Series(dtype=str)).eq("position_monitor")).sum())
    missing_market = int((routing.get("active_position", pd.Series(False, index=routing.index)).fillna(False) & ~routing.get("market_data_available", pd.Series(False, index=routing.index)).fillna(False)).sum())
    return {
        "eligible_full_universe": int(len(routing)),
        "stage_only": int(tiers.get("stage_only", 0)),
        "light_pattern": int(tiers.get("light_pattern", 0)),
        "full_investigator": int(tiers.get("full_investigator", 0)),
        "position_monitor": int(tiers.get("position_monitor", 0)),
        "active_positions_total": active_total,
        "active_positions_fully_monitored": active_monitored,
        "active_positions_missing_coverage": active_total - active_monitored,
        "active_positions_missing_market_data": missing_market,
        "routing_conflicts": missing_market,
        "symbols_in_multiple_selection_reasons": int(routing.get("scan_reasons", pd.Series(dtype=object)).map(lambda value: len(value) > 2).sum()) if not routing.empty else 0,
        **comparison,
    }


def _read_csv(uri: str) -> pd.DataFrame:
    path = Path(uri)
    if not path.exists() or not path.stat().st_size:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _stage(value: Any) -> WeinsteinStage:
    try:
        return WeinsteinStage(str(value))
    except ValueError:
        return WeinsteinStage.UNKNOWN


def _int(value: Any) -> int | None:
    try:
        return None if pd.isna(value) else int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return default if pd.isna(value) else float(value)
    except (TypeError, ValueError):
        return default


def _stop_proximity(close: Any, stop: Any) -> float | None:
    close_value = _float(close, 0.0)
    stop_value = _float(stop, 0.0)
    return None if close_value <= 0 or stop_value <= 0 else (close_value / stop_value - 1.0) * 100.0
