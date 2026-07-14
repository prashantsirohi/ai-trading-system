"""Optional Phase 3B full-universe weekly structural coverage stage."""

from __future__ import annotations

import json

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
from ai_trading_system.domains.opportunities.routing import (
    OpportunityScanRoutingMode,
    ScanRoutingConfig,
    StageCoverageConfig,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class WeeklyStageCoverageStage:
    name = "weekly_stage"

    def run(self, context: StageContext) -> StageResult:
        routing_config = ScanRoutingConfig.from_mapping(context.params)
        if routing_config.mode is OpportunityScanRoutingMode.OFF:
            return StageResult(metadata={"status": "skipped", "mode": "off"})
        config = StageCoverageConfig.from_mapping(context.params)
        paths = get_domain_paths(context.project_root, context.params.get("data_domain", "operational"))
        mapping, mapping_warnings = load_sector_mapping(paths.master_db_path)
        daily = load_daily_universe(
            context.db_path,
            exchange=str(context.params.get("exchange", "NSE")),
            as_of=context.run_date,
        )
        locked = config.weekly_lock_enabled and is_completed_trading_week(
            pd.Timestamp(context.run_date).date(), paths.master_db_path
        )
        stock, exclusions = build_stage_coverage(
            daily,
            as_of=context.run_date,
            sector_mapping=mapping,
            config=config,
            lock_current_week=locked,
            market_regime=str(context.params.get("execution_regime") or "unknown"),
        )
        sector = build_sector_coverage(stock, config=config) if not stock.empty else pd.DataFrame()
        light, promotions = build_light_pattern_scan(stock, config=routing_config) if not stock.empty else (pd.DataFrame(), pd.DataFrame())
        history_stock = stock
        history_sector = sector
        if not locked and not daily.empty:
            week_start = pd.Timestamp(context.run_date) - pd.Timedelta(days=pd.Timestamp(context.run_date).weekday())
            prior_daily = daily.loc[pd.to_datetime(daily["timestamp"]).lt(week_start)].copy()
            prior_stock, _ = build_stage_coverage(
                prior_daily,
                as_of=(week_start - pd.Timedelta(days=1)).date().isoformat(),
                sector_mapping=mapping,
                config=config,
                lock_current_week=True,
                market_regime=str(context.params.get("execution_regime") or "unknown"),
            )
            prior_sector = build_sector_coverage(prior_stock, config=config) if not prior_stock.empty else pd.DataFrame()
            history_stock = pd.concat([prior_stock, stock], ignore_index=True, sort=False)
            history_sector = pd.concat([prior_sector, sector], ignore_index=True, sort=False)
        if context.registry is not None:
            persist_stage_history(
                context.registry, history_stock, history_sector, run_id=context.run_id, attempt=context.attempt_number
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
        for artifact_type, (filename, frame) in frames.items():
            path = output / filename
            frame.to_csv(path, index=False)
            artifacts.append(StageArtifact.from_file(artifact_type, path, row_count=len(frame), attempt_number=context.attempt_number))
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
            "mapping_warnings": mapping_warnings,
        }
        summary_path = output / "weekly_stage_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        artifacts.append(StageArtifact.from_file("weekly_stage_summary", summary_path, attempt_number=context.attempt_number))
        return StageResult(artifacts=artifacts, metadata=summary)
