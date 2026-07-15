"""Optional Phase 3B deterministic shadow scan router."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.execution.store import ExecutionStore
from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.registry.service import (
    OpportunityRegistryService,
)
from ai_trading_system.domains.opportunities.registry.store import (
    DuckDBOpportunityRegistryStore,
)
from ai_trading_system.domains.opportunities.position_monitoring import (
    POSITION_COVERAGE_POLICY_VERSION,
    PositionCoverageRecord,
    PositionCoverageStatus,
)
from ai_trading_system.domains.opportunities.routing import (
    OpportunityScanRoutingMode,
    PositionMonitoringConfig,
    RoutingConflict,
    RoutingConflictCode,
    ScanRoutingConfig,
    ScanTier,
    decide_scan_route,
    validate_scan_routing_row,
)
from ai_trading_system.platform.db.paths import get_domain_paths
from ai_trading_system.pipeline.contracts import (
    PipelineStageError,
    StageArtifact,
    StageContext,
    StageResult,
)
from ai_trading_system.pipeline.alerts import AlertManager


class ScanRouterStageError(PipelineStageError):
    """Non-blocking Phase 3B/3C shadow routing failure."""


class ScanRouterStage:
    name = "scan_router"

    def run(self, context: StageContext) -> StageResult:
        started_at = time.perf_counter()
        config = ScanRoutingConfig.from_mapping(context.params)
        if config.mode is OpportunityScanRoutingMode.OFF:
            return StageResult(metadata={"status": "skipped", "mode": "off"})
        position_config = PositionMonitoringConfig.from_mapping(context.params)
        stock_artifact = context.require_artifact(
            "weekly_stage", "weekly_stock_stage_universe"
        )
        stock = _read_csv(stock_artifact.uri)
        sector = _read_csv(
            (
                context.artifact_for("weekly_stage", "weekly_sector_stage_universe")
                or stock_artifact
            ).uri
        )
        promotions = _read_csv(
            (
                context.artifact_for("weekly_stage", "stage_promotion_candidates")
                or stock_artifact
            ).uri
        )
        ranked_artifact = context.require_artifact("rank", "ranked_signals")
        ranked = _read_csv(ranked_artifact.uri)
        rank_col = "rank_position" if "rank_position" in ranked else None
        ranked = ranked.copy()
        if "symbol_id" in ranked:
            ranked.loc[:, "symbol_id"] = ranked["symbol_id"].astype(str).str.upper()
        if rank_col is None:
            ranked.loc[:, "rank_position"] = range(1, len(ranked) + 1)
            rank_col = "rank_position"
        rank_selected = (
            set(
                ranked.sort_values(rank_col)
                .head(config.rank_deep_scan_limit)["symbol_id"]
                .astype(str)
            )
            if "symbol_id" in ranked
            else set()
        )
        promotion_symbols = set()
        if not promotions.empty and "symbol_id" in promotions:
            promotion_order = (
                promotions.sort_values("light_pattern_score", ascending=False)
                if "light_pattern_score" in promotions
                else promotions
            )
            promotion_symbols = set(
                promotion_order.head(config.stage_promoted_scan_limit)["symbol_id"]
                .astype(str)
                .str.upper()
            )

        paths = get_domain_paths(
            context.project_root, context.params.get("data_domain", "operational")
        )
        execution = ExecutionStore(
            context.project_root,
            db_path=paths.root_dir / "execution.duckdb",
            initialize=False,
        )
        cycles = execution.list_position_cycles()
        active = {
            (cycle.exchange, cycle.symbol_id): cycle for cycle in cycles if cycle.active
        }
        stops = {
            (
                str(row.get("exchange") or "NSE").upper(),
                str(row.get("symbol_id") or "").upper(),
            ): row
            for row in (
                execution.list_active_stops() if execution.db_path.exists() else []
            )
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
        stock_rows = {
            str(row["symbol_id"]).upper(): row
            for row in stock.to_dict(orient="records")
        }
        sector_rows = {
            str(row.get("sector_id") or ""): row
            for row in sector.to_dict(orient="records")
        }
        rank_rows = {
            str(row["symbol_id"]).upper(): row
            for row in ranked.to_dict(orient="records")
            if row.get("symbol_id")
        }
        all_symbols = (
            set(stock_rows)
            | set(rank_selected)
            | set(promotion_symbols)
            | {key[1] for key in active}
            | {key[1] for key in recent}
            | set(lifecycle)
        )
        decisions = []
        for symbol in sorted(all_symbols):
            stage_row = stock_rows.get(symbol, {})
            rank_row = rank_rows.get(symbol, {})
            stage = _stage(stage_row.get("effective_stage"))
            lifecycle_state = lifecycle.get(symbol)
            sector_row = sector_rows.get(str(stage_row.get("sector_id") or ""), {})
            decision = decide_scan_route(
                symbol_id=symbol,
                exchange=str(
                    stage_row.get("exchange") or rank_row.get("exchange") or "NSE"
                ),
                rank_position=_int(rank_row.get("rank_position")),
                rank_selected=symbol in rank_selected,
                stage_discovery=stage
                in {WeinsteinStage.STAGE_1, WeinsteinStage.TRANSITION_1_TO_2},
                stage_promoted=symbol in promotion_symbols,
                active_position=any(key[1] == symbol for key in active),
                recently_exited=any(key[1] == symbol for key in recent),
                triggered=lifecycle_state is CandidateState.TRIGGERED,
                pending_followthrough=lifecycle_state
                is CandidateState.PENDING_FOLLOWTHROUGH,
                stock_stage=stage,
                sector_stage=_stage(sector_row.get("effective_stage")),
                market_data_available=symbol in stock_rows,
                policy_version=config.scan_policy_version,
            )
            decisions.append(decision)
        missing_active = sorted(
            {key[1] for key in active}
            - {
                item.symbol_id
                for item in decisions
                if item.scan_tier is ScanTier.POSITION_MONITOR
            }
        )
        if missing_active:
            if context.registry is not None:
                for symbol in missing_active:
                    AlertManager(context.registry).emit(
                        run_id=context.run_id,
                        alert_type="active_position_missing_routing",
                        severity="critical",
                        stage_name=self.name,
                        message=f"active position lacks validated POSITION_MONITOR routing: {symbol}",
                    )
            raise ScanRouterStageError(
                f"active positions missing scan routing: {missing_active}"
            )

        rows = []
        conflict_rows: list[dict[str, Any]] = []
        for item in decisions:
            row = _decision_row(item)
            cycle = active.get((item.exchange, item.symbol_id))
            recent_cycle = recent.get((item.exchange, item.symbol_id))
            row["position_cycle_opened_at"] = cycle.cycle_opened_at if cycle else None
            row["last_exited_at"] = (
                recent_cycle.last_exited_at if recent_cycle else None
            )
            stop = stops.get((item.exchange, item.symbol_id), {})
            row["stop_price"] = stop.get("stop_price")
            close = (
                stage_row.get("weekly_close")
                if (stage_row := stock_rows.get(item.symbol_id, {}))
                else None
            )
            row["stop_proximity_pct"] = _stop_proximity(close, stop.get("stop_price"))
            row["weekly_ma_slope_deteriorating"] = (
                _float(stage_row.get("weekly_ma_30_slope_acceleration"), 0.0) < 0
            )
            row["price_below_weekly_ma"] = (
                _float(stage_row.get("price_vs_weekly_ma_30_pct"), 0.0) < 0
            )
            row["sector_weakening"] = item.sector_stage in {
                WeinsteinStage.TRANSITION_2_TO_3,
                WeinsteinStage.STAGE_3,
                WeinsteinStage.STAGE_4,
            }
            for conflict in item.validation_conflicts:
                conflict_rows.append(conflict.as_row())
            if item.active_position and not item.market_data_available:
                conflict_rows.append(
                    RoutingConflict(
                        RoutingConflictCode.ACTIVE_POSITION_MISSING_MARKET_DATA,
                        "high",
                        "active position has no current weekly-stage market-data coverage",
                        item.symbol_id,
                        item.exchange,
                        "market_data_available",
                        str(item.market_data_available),
                    ).as_row()
                )
            validation_conflicts = validate_scan_routing_row(row)
            if validation_conflicts:
                conflict_rows.extend(
                    conflict.as_row() for conflict in validation_conflicts
                )
                continue
            rows.append(row)
        routing = pd.DataFrame(rows)
        routing_conflicts = pd.DataFrame(conflict_rows)
        coverage = _position_coverage(
            context=context,
            active=active,
            routing=routing,
            stock_rows=stock_rows,
            stock_artifact=stock_artifact,
        )
        coverage_by_key = {
            (item.exchange, item.symbol_id): item for item in coverage
        }
        if not routing.empty:
            routing.loc[:, "position_cycle_id"] = routing.apply(
                lambda row: getattr(
                    coverage_by_key.get(
                        (str(row.get("exchange") or "NSE").upper(), str(row.get("symbol_id") or "").upper())
                    ),
                    "position_cycle_id",
                    None,
                ),
                axis=1,
            )
            routing.loc[:, "market_data_complete"] = routing.apply(
                lambda row: getattr(
                    coverage_by_key.get(
                        (str(row.get("exchange") or "NSE").upper(), str(row.get("symbol_id") or "").upper())
                    ),
                    "market_data_complete",
                    bool(row.get("market_data_available")),
                ),
                axis=1,
            )
            routing.loc[:, "missing_data_fields"] = routing.apply(
                lambda row: list(
                    getattr(
                        coverage_by_key.get(
                            (str(row.get("exchange") or "NSE").upper(), str(row.get("symbol_id") or "").upper())
                        ),
                        "missing_data_fields",
                        (),
                    )
                ),
                axis=1,
            )
        coverage_frame = pd.DataFrame(item.as_row() for item in coverage)
        missing_data_frame = (
            coverage_frame.loc[
                coverage_frame.get("market_data_complete", pd.Series(dtype=bool)).eq(False)
            ]
            if not coverage_frame.empty
            else coverage_frame
        )
        reconciliation_frame = coverage_frame.copy()
        alert_counts = _reconcile_position_alerts(
            context=context,
            coverage=coverage,
            stock_artifact=stock_artifact,
        )
        output = context.output_dir()
        outputs = {
            "scan_routing": routing,
            "stage_discovery_candidates": (
                routing.loc[
                    routing.get(
                        "stage_selected", pd.Series(False, index=routing.index)
                    ).fillna(False)
                ]
                if not routing.empty
                else routing
            ),
            "deep_scan_universe": (
                routing.loc[
                    routing.get("scan_tier", pd.Series(dtype=str)).isin(
                        {
                            ScanTier.FULL_INVESTIGATOR.value,
                            ScanTier.POSITION_MONITOR.value,
                        }
                    )
                ]
                if not routing.empty
                else routing
            ),
            "position_monitor_universe": (
                routing.loc[
                    routing.get("scan_tier", pd.Series(dtype=str)).eq(
                        ScanTier.POSITION_MONITOR.value
                    )
                ]
                if not routing.empty
                else routing
            ),
            "routing_conflicts": routing_conflicts,
            "active_position_coverage": coverage_frame,
            "active_position_missing_data": missing_data_frame,
            "position_monitor_reconciliation": reconciliation_frame,
        }
        artifacts: list[StageArtifact] = []
        for artifact_type, frame in outputs.items():
            path = output / f"{artifact_type}.csv"
            frame.to_csv(path, index=False)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(frame),
                    attempt_number=context.attempt_number,
                )
            )
        old_symbols = set(
            ranked.get("symbol_id", pd.Series(dtype=str))
            .head(int(context.params.get("pattern_max_symbols", 150)))
            .astype(str)
            .str.upper()
        )
        new_symbols = set(
            outputs["deep_scan_universe"]
            .get("symbol_id", pd.Series(dtype=str))
            .astype(str)
        )
        comparison = {
            "existing_deep_scan_count": len(old_symbols),
            "phase3b_deep_scan_count": len(new_symbols),
            "overlap_count": len(old_symbols & new_symbols),
            "newly_discovered_by_stage_routing": sorted(
                (new_symbols - old_symbols) & promotion_symbols
            ),
            "active_positions_missing_from_current_deep_scan": sorted(
                {key[1] for key in active} - old_symbols
            ),
            "current_rank_selected_structurally_blocked": (
                sorted(
                    set(
                        routing.loc[
                            routing.get(
                                "structural_long_blocked",
                                pd.Series(False, index=routing.index),
                            ).eq(True),
                            "symbol_id",
                        ]
                    )
                    & rank_selected
                )
                if not routing.empty
                else []
            ),
            "incremental_compute_seconds": round(time.perf_counter() - started_at, 3),
        }
        comparison_path = context.write_json("scan_routing_comparison.json", comparison)
        artifacts.append(
            StageArtifact.from_file(
                "scan_routing_comparison",
                comparison_path,
                attempt_number=context.attempt_number,
            )
        )
        summary = _summary(
            routing,
            len(active),
            comparison,
            len(routing_conflicts),
            coverage=coverage,
            alert_counts=alert_counts,
        )
        summary_path = context.write_json("scan_coverage_summary.json", summary)
        artifacts.append(
            StageArtifact.from_file(
                "scan_coverage_summary",
                summary_path,
                attempt_number=context.attempt_number,
            )
        )
        if context.registry is not None:
            _persist(
                context,
                rows,
                f"{stock_artifact.content_hash}|{ranked_artifact.content_hash}",
            )
        return StageResult(artifacts=artifacts, metadata=summary)


def _open_lifecycle(context: StageContext) -> dict[str, CandidateState]:
    if context.registry is None:
        return {}
    service = OpportunityRegistryService(
        DuckDBOpportunityRegistryStore(context.registry)
    )
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
            conflicts = validate_scan_routing_row(row)
            if conflicts:
                raise ScanRouterStageError(
                    f"invalid scan routing persistence row for {row.get('exchange')}:{row.get('symbol_id')}: "
                    + ", ".join(conflict.code.value for conflict in conflicts)
                )
            decision_id = (
                row.get("routing_decision_id")
                or hashlib.sha256(
                    f"{context.run_id}|{context.attempt_number}|{row['exchange']}|{row['symbol_id']}|{row['policy_version']}|{source_hash}".encode()
                ).hexdigest()
            )
            conn.execute(
                """INSERT INTO opportunity_scan_routing_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                   ON CONFLICT(decision_id) DO NOTHING""",
                [
                    decision_id,
                    context.run_id,
                    context.attempt_number,
                    context.run_date,
                    row["exchange"],
                    row["symbol_id"],
                    row["scan_tier"],
                    json.dumps(row.get("all_selection_reasons") or row["scan_reasons"]),
                    row["policy_version"],
                    source_hash,
                    json.dumps(row, sort_keys=True, default=str),
                ],
            )


def _decision_row(decision: Any) -> dict[str, Any]:
    return {
        "symbol_id": decision.symbol_id,
        "exchange": decision.exchange,
        "scan_tier": decision.scan_tier.value,
        "effective_scan_tier": decision.effective_scan_tier.value,
        "winning_reason": (
            decision.winning_reason.value if decision.winning_reason else ""
        ),
        "scan_reasons": [reason.value for reason in decision.reasons],
        "all_selection_reasons": [
            reason.value for reason in decision.all_selection_reasons
        ],
        "selection_details": json.dumps(
            [dict(item) for item in decision.selection_details],
            sort_keys=True,
            default=str,
        ),
        "rank_selected": decision.rank_selected,
        "stage_selected": decision.stage_selected,
        "position_selected": decision.position_selected,
        "recent_exit_selected": decision.recent_exit_selected,
        "followthrough_selected": decision.followthrough_selected,
        "rank_position": decision.rank_position,
        "stock_stage": decision.stock_stage.value,
        "sector_stage": decision.sector_stage.value,
        "active_position": decision.active_position,
        "recently_exited": decision.recently_exited,
        "structural_long_blocked": decision.structural_long_blocked,
        "new_long_structural_blocked": decision.new_long_structural_blocked,
        "new_long_block_reasons": list(decision.new_long_block_reasons),
        "active_position_structural_risk": decision.active_position_structural_risk,
        "structural_risk_severity": decision.structural_risk_severity,
        "structural_risk_reasons": list(decision.structural_risk_reasons),
        "market_data_available": decision.market_data_available,
        "policy_version": decision.policy_version,
        "scan_policy_version": decision.policy_version,
        "routing_input_hash": decision.routing_input_hash,
        "routing_decision_id": decision.routing_decision_id,
    }


def _summary(
    routing: pd.DataFrame,
    active_total: int,
    comparison: dict[str, Any],
    conflict_count: int = 0,
    *,
    coverage: tuple[PositionCoverageRecord, ...] = (),
    alert_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    tiers = routing.get("scan_tier", pd.Series(dtype=str)).value_counts().to_dict()
    active_monitored = int(
        (
            routing.get(
                "active_position", pd.Series(False, index=routing.index)
            ).fillna(False)
            & routing.get("scan_tier", pd.Series(dtype=str)).eq("position_monitor")
        ).sum()
    )
    missing_market = int(
        (
            routing.get(
                "active_position", pd.Series(False, index=routing.index)
            ).fillna(False)
            & ~routing.get(
                "market_data_available", pd.Series(False, index=routing.index)
            ).fillna(False)
        ).sum()
    )
    fully_monitored = sum(
        item.coverage_status is PositionCoverageStatus.FULLY_MONITORED
        for item in coverage
    )
    complete_market = sum(item.market_data_complete for item in coverage)
    complete_evidence = sum(item.investigator_evidence_complete for item in coverage)
    return {
        "eligible_full_universe": int(len(routing)),
        "stage_only": int(tiers.get("stage_only", 0)),
        "light_pattern": int(tiers.get("light_pattern", 0)),
        "full_investigator": int(tiers.get("full_investigator", 0)),
        "position_monitor": int(tiers.get("position_monitor", 0)),
        "active_positions_total": active_total,
        "active_positions_with_position_monitor": active_monitored,
        "active_positions_with_complete_market_data": complete_market,
        "active_positions_with_complete_evidence": complete_evidence,
        "active_positions_fully_monitored": fully_monitored,
        "active_positions_missing_coverage": active_total - fully_monitored,
        "active_positions_missing_market_data": missing_market,
        "routing_conflicts": int(conflict_count),
        "status": "degraded" if conflict_count or active_total != fully_monitored else "completed",
        **(alert_counts or {}),
        "symbols_in_multiple_selection_reasons": (
            int(
                routing.get("scan_reasons", pd.Series(dtype=object))
                .map(lambda value: len(value) > 2)
                .sum()
            )
            if not routing.empty
            else 0
        ),
        **comparison,
    }


def _position_coverage(
    *,
    context: StageContext,
    active: dict[tuple[str, str], Any],
    routing: pd.DataFrame,
    stock_rows: dict[str, dict[str, Any]],
    stock_artifact: StageArtifact,
) -> tuple[PositionCoverageRecord, ...]:
    market = _market_session_state(context.db_path, context.run_date, active)
    route_rows = {
        (str(row.get("exchange") or "NSE").upper(), str(row.get("symbol_id") or "").upper()): row
        for row in routing.to_dict(orient="records")
    }
    as_of = datetime.fromisoformat(context.run_date).replace(tzinfo=timezone.utc)
    records: list[PositionCoverageRecord] = []
    for key, cycle in sorted(active.items()):
        route = route_rows.get(key, {})
        stock = stock_rows.get(cycle.symbol_id, {})
        state = market.get(key, {})
        missing: list[str] = []
        if state.get("last_valid_market_timestamp") is None:
            missing.append("current_close")
        if (state.get("staleness_sessions") if state.get("staleness_sessions") is not None else 999) > int(
            context.params.get("active_position_market_data_max_staleness_sessions", 0)
        ):
            missing.append("current_market_session")
        required_stock = {
            "weekly_close": stock.get("weekly_close"),
            "weekly_stock_stage": stock.get("effective_stage"),
            "price_relative_to_structural_levels": stock.get("price_vs_weekly_ma_30_pct"),
            "relative_strength_input": stock.get("weekly_rs"),
            "source_week_end": stock.get("source_week_end"),
        }
        for name, value in required_stock.items():
            if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).lower() in {"", "nan", "unknown"}:
                missing.append(name)
        source_week_end = _date_value(stock.get("source_week_end"))
        if source_week_end is not None:
            stage_staleness = sum(
                session > source_week_end for session in state.get("sessions", ())
            )
            if stage_staleness > int(
                context.params.get("active_position_market_data_max_staleness_sessions", 0)
            ):
                missing.append("current_weekly_stage_session")
        if not stock or str(stock.get("exchange") or "NSE").upper() != cycle.exchange:
            missing.append("required_symbol_mapping")
        route_valid = (
            str(route.get("effective_scan_tier") or route.get("scan_tier") or "")
            == ScanTier.POSITION_MONITOR.value
            and bool(route.get("routing_decision_id"))
        )
        cycle_id = cycle.position_cycle_id
        if not cycle_id or not cycle.cycle_opened_at:
            status = PositionCoverageStatus.HARD_EXCLUSION
            reasons = ("invalid_position_cycle_identity",)
            opened = as_of
        elif not route_valid:
            status = PositionCoverageStatus.MISSING_ROUTING
            reasons = ("validated_position_monitor_route_missing",)
            opened = _aware(cycle.cycle_opened_at)
        elif missing:
            status = PositionCoverageStatus.ROUTED_WITH_INCOMPLETE_DATA
            reasons = tuple(sorted(set(missing)))
            opened = _aware(cycle.cycle_opened_at)
        else:
            status = PositionCoverageStatus.FULLY_MONITORED
            reasons = ()
            opened = _aware(cycle.cycle_opened_at)
        records.append(
            PositionCoverageRecord(
                position_cycle_id=cycle_id or f"invalid:{cycle.exchange}:{cycle.symbol_id}",
                symbol_id=cycle.symbol_id,
                exchange=cycle.exchange,
                position_opened_at=opened,
                quantity=float(cycle.net_quantity),
                average_price=cycle.average_price,
                routing_decision_id=str(route.get("routing_decision_id") or "") or None,
                effective_scan_tier=(ScanTier.POSITION_MONITOR if route_valid else None),
                market_data_available=state.get("last_valid_market_timestamp") is not None,
                market_data_complete=not missing,
                last_valid_market_timestamp=state.get("last_valid_market_timestamp"),
                expected_market_session=state.get("expected_market_session"),
                market_data_staleness_sessions=state.get("staleness_sessions"),
                missing_data_fields=tuple(sorted(set(missing))),
                investigator_evidence_complete=False,
                opportunity_episode_id=None,
                episode_match_status="not_evaluated",
                coverage_status=status,
                coverage_reasons=reasons,
                as_of=as_of,
                policy_version=POSITION_COVERAGE_POLICY_VERSION,
            )
        )
    return tuple(records)


def _market_session_state(
    db_path: Path,
    run_date: str,
    active: dict[tuple[str, str], Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not db_path.exists() or not active:
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        sessions = [row[0] for row in conn.execute(
            """SELECT DISTINCT CAST(timestamp AS DATE) FROM _catalog
               WHERE CAST(timestamp AS DATE) <= CAST(? AS DATE) ORDER BY 1""",
            [run_date],
        ).fetchall()]
        expected = sessions[-1] if sessions else None
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for exchange, symbol in active:
            row = conn.execute(
                """SELECT MAX(timestamp) FROM _catalog
                   WHERE exchange = ? AND symbol_id = ? AND close IS NOT NULL
                     AND CAST(timestamp AS DATE) <= CAST(? AS DATE)""",
                [exchange, symbol, run_date],
            ).fetchone()
            latest = row[0] if row else None
            staleness = (
                sum(session > latest.date() for session in sessions) if latest else None
            )
            result[(exchange, symbol)] = {
                "last_valid_market_timestamp": (
                    latest.replace(tzinfo=timezone.utc) if latest and latest.tzinfo is None else latest
                ),
                "expected_market_session": expected,
                "staleness_sessions": staleness,
                "sessions": tuple(sessions),
            }
        return result
    finally:
        conn.close()


def _reconcile_position_alerts(
    *, context: StageContext, coverage: tuple[PositionCoverageRecord, ...], stock_artifact: StageArtifact
) -> dict[str, int]:
    counts = {
        "critical_missing_data_alerts_emitted": 0,
        "critical_missing_data_alerts_deduplicated": 0,
        "critical_missing_data_alerts_resolved": 0,
    }
    if context.registry is None or not bool(context.params.get("active_position_alert_enabled", True)):
        return counts
    manager = AlertManager(context.registry)
    for item in coverage:
        if item.market_data_complete:
            counts["critical_missing_data_alerts_resolved"] += manager.resolve_incidents(
                run_id=context.run_id,
                alert_type="active_position_missing_market_data",
                position_cycle_id=item.position_cycle_id,
                resolution={"restored_market_session": item.expected_market_session},
            )
            continue
        signature = hashlib.sha256("|".join(item.missing_data_fields).encode()).hexdigest()[:16]
        dedupe_key = "|".join(
            (
                "active_position_missing_market_data",
                item.position_cycle_id,
                signature,
                str(item.expected_market_session),
            )
        )
        payload = {
            **item.as_row(),
            "run_id": context.run_id,
            "artifact_lineage": {
                "uri": stock_artifact.uri,
                "content_hash": stock_artifact.content_hash,
            },
            "recommended_operator_action": (
                "Verify symbol mapping, ingest status, latest market session, and data source "
                "before relying on position-monitor recommendations."
            ),
        }
        outcome = manager.emit_incident(
            run_id=context.run_id,
            alert_type="active_position_missing_market_data",
            severity="critical",
            stage_name="scan_router",
            message=f"active position {item.exchange}:{item.symbol_id} has incomplete market data",
            dedupe_key=dedupe_key,
            payload=payload,
        )
        key = (
            "critical_missing_data_alerts_deduplicated"
            if outcome == "DEDUPLICATED"
            else "critical_missing_data_alerts_emitted"
        )
        counts[key] += 1
    return counts


def _aware(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _date_value(value: Any):
    try:
        return pd.Timestamp(value).date() if not pd.isna(value) else None
    except (TypeError, ValueError):
        return None


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
    return (
        None
        if close_value <= 0 or stop_value <= 0
        else (close_value / stop_value - 1.0) * 100.0
    )
