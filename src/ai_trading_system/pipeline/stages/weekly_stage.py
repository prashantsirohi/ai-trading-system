"""Optional Phase 3B full-universe weekly structural coverage stage."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pandas as pd

from ai_trading_system.domains.opportunities.coverage import (
    build_light_pattern_scan,
    build_sector_coverage,
    build_stage_coverage,
    is_completed_trading_week,
    load_daily_universe,
    load_sector_mapping,
    persist_stage_history,
)
from ai_trading_system.domains.opportunities.policy_snapshot import (
    append_policy_snapshot_event,
    compute_policy_snapshot,
    register_or_verify_policy_snapshots,
)
from ai_trading_system.domains.opportunities.routing import (
    OpportunityScanRoutingMode,
    ScanRoutingConfig,
    StageCoverageConfig,
)
from ai_trading_system.domains.opportunities.stage_governance import (
    observe_sector_mapping,
    resolve_historical_sector_mapping,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.platform.telemetry.performance import DatabasePerformanceMetric
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class WeeklyStageCoverageStage:
    name = "weekly_stage"

    def run(self, context: StageContext) -> StageResult:
        routing_config = ScanRoutingConfig.from_mapping(context.params)
        if routing_config.mode is OpportunityScanRoutingMode.OFF:
            return StageResult(metadata={"status": "skipped", "mode": "off"})
        config = StageCoverageConfig.from_mapping(context.params)
        # ADR-0006 A3: verify policy content before any stage-owned write
        # (observe_sector_mapping below is the first registry write). A
        # mismatch fails only this optional shadow stage.
        policy_snapshot = compute_policy_snapshot(context.params)
        if context.registry is not None:
            register_or_verify_policy_snapshots(context.registry, policy_snapshot, run_id=context.run_id)
            append_policy_snapshot_event(context.registry, policy_snapshot, run_id=context.run_id, stage_name=self.name)
        paths = get_domain_paths(context.project_root, context.params.get("data_domain", "operational"))
        operation_started = time.perf_counter_ns()
        latest_mapping, mapping_warnings = load_sector_mapping(paths.master_db_path)
        mapping = latest_mapping
        recorded_at = datetime.now(timezone.utc)
        if context.registry is not None:
            mapping = observe_sector_mapping(
                context.registry, mapping, exchange=str(context.params.get("exchange", "NSE")),
                as_of=context.run_date, run_id=context.run_id,
                stage_attempt=context.attempt_number, recorded_at=recorded_at,
            )
        daily = load_daily_universe(
            context.db_path,
            exchange=str(context.params.get("exchange", "NSE")),
            as_of=context.run_date,
        )
        load_duration_ms = _record_duration(context, "weekly_stage.load_price_history", operation_started, rows_out=len(daily))
        if context.performance is not None:
            context.performance.record_database_metric(DatabasePerformanceMetric(
                stage_name=self.name, operation_name="load_stage_inputs",
                query_count=2, read_query_count=2, db_read_ms=load_duration_ms,
                rows_read=len(daily) + len(mapping),
            ))
        locked = config.weekly_lock_enabled and is_completed_trading_week(
            pd.Timestamp(context.run_date).date(), paths.master_db_path
        )
        operation_started = time.perf_counter_ns()
        stock, exclusions = build_stage_coverage(
            daily,
            as_of=context.run_date,
            sector_mapping=mapping,
            config=config,
            lock_current_week=locked,
            market_regime=str(context.params.get("execution_regime") or "unknown"),
        )
        _record_duration(context, "weekly_stage.compute_stock_stages", operation_started, rows_in=len(daily), rows_out=len(stock), symbols_out=len(stock))
        operation_started = time.perf_counter_ns()
        sector = build_sector_coverage(stock, config=config) if not stock.empty else pd.DataFrame()
        _record_duration(context, "weekly_stage.aggregate_sector_stages", operation_started, rows_in=len(stock), rows_out=len(sector))
        operation_started = time.perf_counter_ns()
        light, promotions = build_light_pattern_scan(stock, config=routing_config) if not stock.empty else (pd.DataFrame(), pd.DataFrame())
        _record_duration(context, "weekly_stage.evaluate_light_pattern", operation_started, rows_in=len(stock), rows_out=len(light), symbols_out=len(light))
        history_stock = stock
        history_sector = sector
        if not locked and not daily.empty:
            week_start = pd.Timestamp(context.run_date) - pd.Timedelta(days=pd.Timestamp(context.run_date).weekday())
            prior_daily = daily.loc[pd.to_datetime(daily["timestamp"]).lt(week_start)].copy()
            prior_as_of = (week_start - pd.Timedelta(days=1)).date().isoformat()
            prior_mapping = mapping
            if context.registry is not None:
                prior_mapping = resolve_historical_sector_mapping(
                    context.registry, latest_mapping,
                    exchange=str(context.params.get("exchange", "NSE")),
                    effective_at=prior_as_of, available_at=recorded_at,
                    run_id=context.run_id, stage_attempt=context.attempt_number,
                )
            prior_stock, _ = build_stage_coverage(
                prior_daily,
                as_of=prior_as_of,
                sector_mapping=prior_mapping,
                config=config,
                lock_current_week=True,
                market_regime=str(context.params.get("execution_regime") or "unknown"),
            )
            prior_sector = build_sector_coverage(prior_stock, config=config) if not prior_stock.empty else pd.DataFrame()
            history_stock = pd.concat([prior_stock, stock], ignore_index=True, sort=False)
            history_sector = pd.concat([prior_sector, sector], ignore_index=True, sort=False)
        if context.registry is not None:
            operation_started = time.perf_counter_ns()
            persist_stage_history(
                context.registry, history_stock, history_sector, run_id=context.run_id,
                attempt=context.attempt_number, recorded_at=recorded_at,
            )
            duration_ms = _record_duration(context, "weekly_stage.persist_stage_history", operation_started, rows_out=len(history_stock) + len(history_sector))
            if context.performance is not None:
                context.performance.record_database_metric(
                    DatabasePerformanceMetric(
                        stage_name=self.name, operation_name="persist_stage_history",
                        query_count=2, write_query_count=2, transaction_count=1,
                        commit_count=1, db_write_ms=duration_ms,
                        rows_written=len(history_stock) + len(history_sector),
                    )
                )

        output = context.output_dir()
        frames = {
            "weekly_stock_stage_universe": ("weekly_stock_stage_universe.csv", stock),
            "weekly_sector_stage_universe": ("weekly_sector_stage_universe.csv", sector),
            "weekly_stage_exclusions": ("weekly_stage_exclusions.csv", exclusions),
            "light_pattern_scan": ("light_pattern_scan.csv", light),
            "stage_promotion_candidates": ("stage_promotion_candidates.csv", promotions),
        }
        artifacts: list[StageArtifact] = []
        artifact_started = time.perf_counter_ns()
        for artifact_type, (filename, frame) in frames.items():
            write_started = time.perf_counter_ns()
            path = output / filename
            frame.to_csv(path, index=False)
            artifact = StageArtifact.from_file(artifact_type, path, row_count=len(frame), attempt_number=context.attempt_number)
            artifacts.append(artifact)
            if context.performance is not None:
                context.performance.record_artifact(artifact, column_count=len(frame.columns), write_duration_ms=(time.perf_counter_ns() - write_started) / 1_000_000.0)
        summary = {
            "mode": routing_config.mode.value,
            "eligible_full_universe": int(len(stock)),
            "stage_classified": int(stock.get("effective_stage", pd.Series(dtype=str)).ne("unknown").sum()),
            "stage_provisional": int(stock.get("stage_status", pd.Series(dtype=str)).eq("provisional").sum()),
            "stage_locked": int(stock.get("stage_status", pd.Series(dtype=str)).eq("locked").sum()),
            "stage_exclusions": int(len(exclusions)),
            "sectors_classified": int(sector.get("effective_stage", pd.Series(dtype=str)).ne("unknown").sum()),
            "sector_unknown": int(sector.get("effective_stage", pd.Series(dtype=str)).eq("unknown").sum()),
            "light_pattern_scanned": int(len(light)),
            "stage_promoted_candidates": int(len(promotions)),
            "weekly_lock": bool(locked),
            "sector_mapping_symbols": int(len(mapping)),
            "sector_mapping_missing": int(
                exclusions.get("reason", pd.Series(dtype=str)).eq(
                    "missing_sector_mapping"
                ).sum()
            ),
            "sector_mapping_coverage_ratio": round(
                float(stock.get("sector_name", pd.Series(dtype=str)).notna().sum())
                / len(stock),
                6,
            ) if len(stock) else 0.0,
            "mapping_warnings": mapping_warnings,
            **policy_snapshot.metadata(),
        }
        summary_path = output / "weekly_stage_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        artifacts.append(StageArtifact.from_file("weekly_stage_summary", summary_path, attempt_number=context.attempt_number))
        _record_duration(context, "weekly_stage.write_artifacts", artifact_started, rows_out=sum(len(frame) for _, frame in frames.values()))
        return StageResult(artifacts=artifacts, metadata=summary)


def _record_duration(context: StageContext, operation: str, started_ns: int, **counts: int) -> float:
    duration_ms = max((time.perf_counter_ns() - started_ns) / 1_000_000.0, 0.0)
    if context.performance is not None:
        context.performance.record_duration(stage_name="weekly_stage", operation_name=operation, duration_ms=duration_ms, **counts)
    return duration_ms
