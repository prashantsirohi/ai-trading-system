"""Investigator service orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.investigator.buyer_fingerprint import score_buyer_fingerprint
from ai_trading_system.domains.investigator.candidate_union import (
    build_candidate_union,
    eligible_previous_watchlist,
)
from ai_trading_system.domains.investigator.cohort_performance import (
    build_performance_summary,
    build_threshold_recommendations,
    mature_investigator_cohorts,
    upsert_investigator_cohorts,
)
from ai_trading_system.domains.investigator.exit_monitor import attach_exit_monitoring
from ai_trading_system.domains.investigator.fundamentals import load_fundamental_snapshot, score_fundamentals
from ai_trading_system.domains.investigator.intake import load_investigator_intake, load_investigator_snapshot
from ai_trading_system.domains.investigator.lifecycle import apply_lifecycle
from ai_trading_system.domains.investigator.move_classifier import classify_move
from ai_trading_system.domains.investigator.pattern_scan import best_pattern_by_symbol, build_investigator_pattern_scan
from ai_trading_system.domains.investigator.payload import build_investigator_payload
from ai_trading_system.domains.investigator.price_structure import score_price_structure
from ai_trading_system.domains.investigator.repeat_tracker import build_repeat_tracker
from ai_trading_system.domains.investigator.scoring import final_gate, finalize_scores
from ai_trading_system.domains.investigator.sector_context import attach_sector_context
from ai_trading_system.domains.investigator.stage_pattern_context import (
    enrich_investigator_context,
    normalise_pattern_context,
    rank_pattern_symbols,
)
from ai_trading_system.domains.investigator.trap_summary import build_trap_summary_metrics
from ai_trading_system.domains.investigator.volume_anatomy import score_volume_anatomy
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class InvestigatorService:
    """Build post-rank investigation artifacts."""

    def run(self, context: StageContext) -> StageResult:
        ranked_artifact = context.require_artifact("rank", "ranked_signals")
        ranked = _read_csv(Path(ranked_artifact.uri))
        breakout = _read_optional(context.artifact_for("rank", "breakout_scan"))
        rank_pattern_scan = _read_optional(context.artifact_for("rank", "pattern_scan"))
        stock_scan = _read_optional(context.artifact_for("rank", "stock_scan"))
        early_accumulation = _normalise_investigator_early_accumulation(
            _read_optional(context.artifact_for("rank", "early_accumulation_scan"))
        )
        rank_context = stock_scan if not stock_scan.empty else ranked
        sector_dashboard = _read_optional(context.artifact_for("rank", "sector_dashboard"))
        gainers = load_investigator_intake(
            ohlcv_db_path=context.db_path,
            ranked_signals=rank_context,
            as_of=context.params.get("investigator_as_of") or None,
            min_return_pct=float(context.params.get("investigator_min_return_pct", 5.0)),
            min_volume_ratio=float(context.params.get("investigator_min_volume_ratio", 2.0)),
            weekly_return_pct=float(context.params.get("investigator_weekly_return_pct", 8.0)),
            stealth_5d_pct=float(context.params.get("investigator_stealth_5d_pct", 3.0)),
            stealth_20d_pct=float(context.params.get("investigator_stealth_20d_pct", 8.0)),
            min_green_days_5d=int(context.params.get("investigator_min_green_days_5d", 3)),
            min_market_cap_cr=float(context.params.get("investigator_min_market_cap_cr", 500.0)),
        )
        previous_watchlist = self._load_previous_watchlist(context)
        candidates, intake_diagnostics = build_candidate_union(
            event_intake=gainers,
            early_accumulation=early_accumulation,
            previous_watchlist=previous_watchlist,
            ranked=ranked,
            stock_scan=stock_scan,
            breakout_scan=breakout,
        )
        market_snapshot = load_investigator_snapshot(
            ohlcv_db_path=context.db_path,
            ranked_signals=rank_context,
            symbols=candidates.get("symbol_id", pd.Series(dtype=str)).astype(str).tolist(),
            as_of=context.params.get("investigator_as_of") or context.run_date,
        )
        candidates = _refresh_market_snapshot(candidates, market_snapshot)
        if not candidates.empty:
            if "trade_date" not in candidates.columns:
                candidates.loc[:, "trade_date"] = context.run_date
            else:
                missing_trade_date = pd.to_datetime(candidates["trade_date"], errors="coerce").isna()
                candidates.loc[missing_trade_date, "trade_date"] = context.run_date
        candidates, stage_pattern_context = enrich_investigator_context(
            candidates,
            ranked=ranked,
            stock_scan=stock_scan,
            breakout_scan=breakout,
            pattern_scan=rank_pattern_scan,
        )
        candidates = _mark_top_ranked_context(candidates, ranked)
        if candidates.empty:
            empty = pd.DataFrame()
            performance_frame, performance_summary, threshold_recommendations = self._performance_outputs(context)
            summary = self._summary(
                context=context,
                gainers=gainers,
                scores=empty,
                repeat=empty,
                active=empty,
                traps=empty,
                archived=empty,
                gate=empty,
                performance_summary=performance_summary,
                investigator_early_accumulation=early_accumulation,
                intake_diagnostics=intake_diagnostics,
                stage_pattern_context=_stage_pattern_summary(stage_pattern_context, performance_frame),
            )
            payload = build_investigator_payload(
                run_id=context.run_id,
                run_date=context.run_date,
                summary=summary,
                today_gainers=gainers,
                scores=empty,
                repeat_tracker=empty,
                active_watchlist=empty,
                trap_log=empty,
                archive=empty,
                final_3q_gate=empty,
                investigator_pattern_scan=empty,
                investigator_early_accumulation=early_accumulation,
                performance_summary=performance_summary,
                threshold_recommendations=threshold_recommendations,
                data_trust_status=str(context.params.get("data_trust_status", "unknown")),
                stage_status={"rank": "completed", "investigator": "completed", "publish": "pending"},
            )
            artifacts = self._write_artifacts(
                context=context,
                daily_gainer_log=gainers,
                investigator_scores=empty,
                repeat_tracker=empty,
                active_watchlist=empty,
                investigator_pattern_scan=empty,
                trap_log=empty,
                archived_investigator=empty,
                final_3q_gate=empty,
                investigator_early_accumulation=early_accumulation,
                investigator_performance_summary=performance_frame,
                investigator_summary=summary,
                investigator_payload=payload,
                investigator_performance_summary_json=performance_summary,
                investigator_threshold_recommendations=threshold_recommendations,
            )
            return StageResult(artifacts=artifacts, metadata=summary)
        candidates = score_price_structure(candidates)
        candidates = score_volume_anatomy(candidates)
        fundamentals = load_fundamental_snapshot(
            project_root=context.project_root,
            symbols=candidates.get("symbol_id", pd.Series(dtype=str)).astype(str).tolist(),
            fundamentals_db_path=Path(context.params["fundamentals_duckdb_path"]) if context.params.get("fundamentals_duckdb_path") else None,
        )
        candidates = score_fundamentals(candidates, fundamentals)
        candidates = attach_sector_context(candidates, sector_dashboard)
        candidates = classify_move(candidates)
        candidates = score_buyer_fingerprint(candidates)
        scores = finalize_scores(candidates)
        history = self._load_history(context)
        repeat = build_repeat_tracker(current_scores=scores, historical_daily_log=history)
        active, archived = apply_lifecycle(scores, repeat)
        rank_scanned_symbols = rank_pattern_symbols(rank_pattern_scan)
        active_for_pattern_scan = _without_symbols(active, rank_scanned_symbols)
        investigator_patterns = build_investigator_pattern_scan(
            context=context,
            active_watchlist=active_for_pattern_scan,
            ranked_df=ranked,
        )
        rank_patterns_for_merge = normalise_pattern_context(rank_pattern_scan)
        best_patterns = best_pattern_by_symbol(_combine_pattern_sources(rank_patterns_for_merge, investigator_patterns))
        active = _merge_best_patterns(active, best_patterns)
        scores = _merge_best_patterns(scores, best_patterns)
        traps = scores.loc[scores.get("verdict", pd.Series(dtype=str)).eq("NOISE_TRAP") | scores.get("hard_trap_flag", pd.Series(False, index=scores.index)).fillna(False)].copy()
        gate = final_gate(scores)
        gate = self._attach_exit_monitoring(context, gate)
        self._persist_cohort_performance(context, gate, scores)
        performance_frame, performance_summary, threshold_recommendations = self._performance_outputs(context)
        summary = self._summary(
            context=context,
            gainers=gainers,
            scores=scores,
            repeat=repeat,
            active=active,
            traps=traps,
            archived=archived,
            gate=gate,
            performance_summary=performance_summary,
            investigator_early_accumulation=early_accumulation,
            intake_diagnostics=intake_diagnostics,
            stage_pattern_context=_stage_pattern_summary({
                **stage_pattern_context,
                "rank_pattern_reused_rows": int(len(rank_patterns_for_merge)),
                "investigator_pattern_scanned_rows": int(len(investigator_patterns)),
                "pattern_scan_skipped_existing_rows": int(len(active) - len(active_for_pattern_scan)),
            }, performance_frame),
        )
        payload = build_investigator_payload(
            run_id=context.run_id,
            run_date=context.run_date,
            summary=summary,
            today_gainers=gainers,
            scores=scores,
            repeat_tracker=repeat,
            active_watchlist=active,
            trap_log=traps,
            archive=archived,
            final_3q_gate=gate,
            investigator_pattern_scan=investigator_patterns,
            investigator_early_accumulation=early_accumulation,
            performance_summary=performance_summary,
            threshold_recommendations=threshold_recommendations,
            data_trust_status=str(context.params.get("data_trust_status", "unknown")),
            stage_status={"rank": "completed", "investigator": "completed", "publish": "pending"},
        )
        artifacts = self._write_artifacts(
            context=context,
            daily_gainer_log=gainers,
            investigator_scores=scores,
            repeat_tracker=repeat,
            active_watchlist=active,
            investigator_pattern_scan=investigator_patterns,
            trap_log=traps,
            archived_investigator=archived,
            final_3q_gate=gate,
            investigator_early_accumulation=early_accumulation,
            investigator_performance_summary=performance_frame,
            investigator_summary=summary,
            investigator_payload=payload,
            investigator_performance_summary_json=performance_summary,
            investigator_threshold_recommendations=threshold_recommendations,
        )
        self._persist_tables(context, artifacts)
        return StageResult(artifacts=artifacts, metadata=summary)

    def _load_previous_watchlist(self, context: StageContext) -> pd.DataFrame:
        if context.registry is None:
            return pd.DataFrame()
        try:
            artifacts = context.registry.get_latest_artifact(
                stage_name="investigator",
                artifact_type="active_watchlist",
                limit=1,
                exclude_run_id=context.run_id,
            )
            if artifacts:
                previous = _read_csv(Path(artifacts[0].uri))
                if not previous.empty:
                    return eligible_previous_watchlist(previous)
        except Exception:
            pass
        try:
            with context.registry._reader() as conn:  # noqa: SLF001
                previous = conn.execute(
                    """
                    WITH latest AS (
                        SELECT * EXCLUDE (_latest_row)
                        FROM (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY symbol_id
                                    ORDER BY trade_date DESC NULLS LAST, run_id DESC, attempt_number DESC
                                ) AS _latest_row
                            FROM investigator_lifecycle
                            WHERE run_id <> ?
                        )
                        WHERE _latest_row = 1
                    )
                    SELECT
                        latest.*,
                        scores.stage_label,
                        scores.pattern_state,
                        scores.hard_trap_flag
                    FROM latest
                    LEFT JOIN investigator_scores AS scores
                      ON scores.run_id = latest.run_id
                     AND scores.attempt_number = latest.attempt_number
                     AND scores.symbol_id = latest.symbol_id
                    """,
                    [context.run_id],
                ).fetchdf()
            return eligible_previous_watchlist(previous)
        except Exception:
            return pd.DataFrame()

    def _load_history(self, context: StageContext) -> pd.DataFrame:
        if context.registry is None:
            return pd.DataFrame()
        try:
            with context.registry._reader() as conn:  # noqa: SLF001
                return conn.execute(
                    """
                    SELECT
                        symbol_id,
                        trade_date,
                        close,
                        volume_ratio_20,
                        volume_ratio_5d,
                        daily_return_pct,
                        return_5d,
                        return_20d,
                        composite_score,
                        rank_position,
                        final_score,
                        sector,
                        trigger_reason,
                        candidate_sources,
                        primary_candidate_source,
                        candidate_source_count,
                        new_candidate_today
                    FROM investigator_scores
                    WHERE trade_date >= CAST(? AS DATE) - INTERVAL 60 DAY
                      AND trade_date < CAST(? AS DATE)
                    """,
                    [context.run_date, context.run_date],
                ).fetchdf()
        except Exception:
            return pd.DataFrame()

    def _write_artifacts(self, context: StageContext, **frames_and_summary: Any) -> list[StageArtifact]:
        output_dir = context.output_dir()
        artifacts: list[StageArtifact] = []
        for artifact_type, value in frames_and_summary.items():
            if isinstance(value, dict):
                filename = {
                    "investigator_summary": "investigator_summary.json",
                    "investigator_payload": "investigator_payload.json",
                    "investigator_performance_summary_json": "investigator_performance_summary.json",
                    "investigator_threshold_recommendations": "investigator_threshold_recommendations.json",
                }.get(artifact_type, f"{artifact_type}.json")
                path = context.write_json(filename, value)
                artifacts.append(StageArtifact.from_file(artifact_type, path, row_count=1, metadata=value, attempt_number=context.attempt_number))
                continue
            assert isinstance(value, pd.DataFrame)
            filename = f"{artifact_type}.csv"
            path = output_dir / filename
            value.to_csv(path, index=False)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(value),
                    metadata={"columns": list(value.columns)},
                    attempt_number=context.attempt_number,
                )
            )
        return artifacts

    def _persist_tables(self, context: StageContext, artifacts: list[StageArtifact]) -> None:
        if context.registry is None:
            return
        mapping = {
            "daily_gainer_log": "investigator_daily_log",
            "investigator_scores": "investigator_scores",
            "repeat_tracker": "investigator_repeat_tracker",
            "active_watchlist": "investigator_lifecycle",
            "investigator_pattern_scan": "investigator_pattern_scan",
            "final_3q_gate": "investigator_final_gate",
            "archived_investigator": "investigator_archive",
        }
        by_type = {artifact.artifact_type: artifact for artifact in artifacts}
        with context.registry._writer() as conn:  # noqa: SLF001
            for artifact_type, table in mapping.items():
                artifact = by_type.get(artifact_type)
                if artifact is None:
                    continue
                frame = _read_csv(Path(artifact.uri))
                if frame.empty:
                    continue
                frame = frame.copy()
                frame.loc[:, "run_id"] = context.run_id
                frame.loc[:, "attempt_number"] = context.attempt_number
                frame.loc[:, "artifact_uri"] = artifact.uri
                conn.execute("CREATE TEMP TABLE investigator_stage_frame AS SELECT * FROM frame")
                conn.execute("DELETE FROM " + table + " WHERE run_id = ? AND attempt_number = ?", [context.run_id, context.attempt_number])
                columns = [row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]
                selected = [col for col in columns if col in frame.columns]
                if selected:
                    conn.execute(
                        f"INSERT INTO {table} ({', '.join(selected)}) SELECT {', '.join(selected)} FROM investigator_stage_frame"
                    )
                conn.execute("DROP TABLE investigator_stage_frame")

    def _persist_cohort_performance(self, context: StageContext, gate: pd.DataFrame, scores: pd.DataFrame) -> None:
        if context.registry is None or gate.empty:
            return
        with context.registry._writer() as conn:  # noqa: SLF001
            upsert_investigator_cohorts(conn, gate, scores)
            mature_investigator_cohorts(conn, ohlcv_db_path=context.db_path)

    def _attach_exit_monitoring(self, context: StageContext, gate: pd.DataFrame) -> pd.DataFrame:
        if gate.empty:
            return gate
        conn = None
        try:
            if context.registry is not None:
                conn = context.registry._connect(read_only=True)  # noqa: SLF001
            return attach_exit_monitoring(
                gate,
                ohlcv_db_path=context.db_path,
                registry_conn=conn,
                as_of=context.run_date,
            )
        finally:
            if conn is not None:
                conn.close()

    def _performance_outputs(self, context: StageContext) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
        if context.registry is None:
            summary = {
                "total_cohorts": 0,
                "pending_cohorts": 0,
                "matured_cohorts": 0,
                "matured_by_horizon": {"3d": 0, "5d": 0, "10d": 0, "20d": 0},
            }
            return pd.DataFrame(), summary, build_threshold_recommendations(pd.DataFrame(), summary)
        try:
            with context.registry._writer() as conn:  # noqa: SLF001
                mature_investigator_cohorts(conn, ohlcv_db_path=context.db_path)
                frame, summary = build_performance_summary(conn)
                recommendations = build_threshold_recommendations(frame, summary)
                return frame, summary, recommendations
        except Exception as exc:
            summary = {
                "status": "unavailable",
                "error": str(exc),
                "total_cohorts": 0,
                "pending_cohorts": 0,
                "matured_cohorts": 0,
                "matured_by_horizon": {"3d": 0, "5d": 0, "10d": 0, "20d": 0},
            }
            return pd.DataFrame(), summary, build_threshold_recommendations(pd.DataFrame(), summary)

    def _summary(
        self,
        *,
        context: StageContext,
        gainers: pd.DataFrame,
        scores: pd.DataFrame,
        repeat: pd.DataFrame,
        active: pd.DataFrame,
        traps: pd.DataFrame,
        archived: pd.DataFrame,
        gate: pd.DataFrame,
        performance_summary: dict[str, Any] | None = None,
        investigator_early_accumulation: pd.DataFrame | None = None,
        stage_pattern_context: dict[str, Any] | None = None,
        intake_diagnostics: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        stage_pattern_context = stage_pattern_context or {}
        candidate_rows = int((intake_diagnostics or {}).get("candidate_union_rows", len(scores)) or 0)
        trap_metrics = build_trap_summary_metrics(
            current_traps=traps,
            archive=archived,
            run_date=context.run_date,
            candidate_union_rows=candidate_rows,
        )
        stage_summary_keys = (
            "stage_input_complete_rows",
            "stage_input_incomplete_rows",
            "stage_input_missing_field_counts",
            "stage_missing_sma200_rows",
            "stage_missing_sma50_slope_rows",
            "stage_missing_sma200_slope_rows",
            "stage_missing_near_high_rows",
            "stage_unknown_rows",
            "stage_label_counts",
            "stage_input_confidence_counts",
        )
        verdict_counts = scores.get("verdict", pd.Series(dtype=str)).value_counts().to_dict() if not scores.empty else {}
        status_counts = active.get("status", pd.Series(dtype=str)).value_counts().to_dict() if not active.empty else {}
        trigger_counts = gainers.get("trigger_reason", pd.Series(dtype=str)).value_counts().to_dict() if not gainers.empty else {}
        return {
            "status": "completed",
            "run_id": context.run_id,
            "run_date": context.run_date,
            "trigger_counts": {str(k): int(v) for k, v in trigger_counts.items()},
            "total_intake_count": int(len(gainers)),
            "daily_gainer_count": int(trigger_counts.get("DAILY_GAINER", 0)),
            "weekly_gainer_count": int(trigger_counts.get("WEEKLY_GAINER", 0)),
            "stealth_accumulation_count": int(trigger_counts.get("STEALTH_ACCUMULATION", 0)),
            "scored_count": int(len(scores)),
            "active_count": int(len(active)),
            "trap_count": trap_metrics["unique_trap_symbols"],
            "archived_count": int(len(archived)),
            "final_gate_pending_count": int(len(gate)),
            "high_conviction_count": int(verdict_counts.get("HIGH_CONVICTION", 0)),
            "medium_conviction_count": int(verdict_counts.get("MEDIUM_CONVICTION", 0)),
            "repeat_accumulation_count": int(repeat.get("high_priority_repeat", pd.Series(dtype=bool)).sum()) if not repeat.empty else 0,
            "verdict_counts": {str(k): int(v) for k, v in verdict_counts.items()},
            "status_counts": {str(k): int(v) for k, v in status_counts.items()},
            "investigator_early_accumulation_count": int(len(investigator_early_accumulation))
            if investigator_early_accumulation is not None
            else 0,
            "stage_pattern_context": stage_pattern_context,
            "performance": performance_summary or {},
            **(intake_diagnostics or {}),
            **{key: stage_pattern_context[key] for key in stage_summary_keys if key in stage_pattern_context},
            **trap_metrics,
        }


def _refresh_market_snapshot(candidates: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty or snapshot is None or snapshot.empty or "symbol_id" not in snapshot.columns:
        return candidates
    fresh = snapshot.drop(columns=["trigger_reason"], errors="ignore").drop_duplicates("symbol_id", keep="first")
    out = candidates.merge(fresh, on="symbol_id", how="left", suffixes=("", "_snapshot"))
    for column in fresh.columns:
        if column == "symbol_id":
            continue
        snapshot_column = f"{column}_snapshot"
        if snapshot_column not in out.columns:
            continue
        out.loc[:, column] = out[snapshot_column].where(out[snapshot_column].notna(), out[column])
        out = out.drop(columns=[snapshot_column])
    return out


def _mark_top_ranked_context(candidates: pd.DataFrame, ranked: pd.DataFrame | None) -> pd.DataFrame:
    out = candidates.copy()
    if out.empty:
        out.loc[:, "in_ranked_signals"] = pd.Series(dtype=bool)
        return out
    out.loc[:, "in_ranked_signals"] = False
    if ranked is None or ranked.empty or "symbol_id" not in ranked.columns:
        return out
    ranked_symbols = set(ranked["symbol_id"].astype(str).str.upper())
    out.loc[:, "in_ranked_signals"] = out["symbol_id"].astype(str).str.upper().isin(ranked_symbols)
    return out


def _without_symbols(frame: pd.DataFrame, symbols: set[str]) -> pd.DataFrame:
    if frame is None or frame.empty or not symbols or "symbol_id" not in frame.columns:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    values = frame["symbol_id"].fillna("").astype(str).str.strip().str.upper()
    return frame.loc[~values.isin(symbols)].copy().reset_index(drop=True)


def _combine_pattern_sources(*frames: pd.DataFrame | None) -> pd.DataFrame:
    available = [frame.copy() for frame in frames if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not available:
        return pd.DataFrame()
    out = pd.concat(available, ignore_index=True, sort=False)
    if "symbol_id" in out.columns:
        out.loc[:, "symbol_id"] = out["symbol_id"].fillna("").astype(str).str.strip().str.upper()
        out = out.loc[out["symbol_id"].ne("")].copy()
    return out.reset_index(drop=True)


def _stage_pattern_summary(context: dict[str, Any], performance_frame: pd.DataFrame | None) -> dict[str, Any]:
    out = dict(context or {})
    out.setdefault("rank_pattern_reused_rows", 0)
    out.setdefault("investigator_pattern_scanned_rows", 0)
    out.setdefault("pattern_scan_skipped_existing_rows", 0)
    warnings = list(out.get("warnings") or [])
    if performance_frame is None or performance_frame.empty:
        out["top_positive_edges"] = []
        out["top_negative_edges"] = []
        warnings.append("no group has sample_count >= 20")
        out["warnings"] = sorted(set(str(item) for item in warnings))
        return out
    frame = performance_frame.copy()
    frame.loc[:, "_sample_count"] = pd.to_numeric(frame.get("sample_count"), errors="coerce").fillna(0)
    frame.loc[:, "_edge"] = pd.to_numeric(frame.get("edge_vs_baseline"), errors="coerce")
    eligible = frame.loc[frame["_sample_count"].ge(20) & frame["_edge"].notna()].copy()
    if eligible.empty:
        warnings.append("no group has sample_count >= 20")
    positive = eligible.loc[eligible["_edge"].gt(0)].sort_values(["_edge", "_sample_count"], ascending=[False, False], kind="stable").head(5)
    negative = eligible.loc[eligible["_edge"].lt(0)].sort_values(["_edge", "_sample_count"], ascending=[True, False], kind="stable").head(5)
    out["top_positive_edges"] = _edge_records(positive)
    out["top_negative_edges"] = _edge_records(negative)
    out["warnings"] = sorted(set(str(item) for item in warnings))
    return out


def _edge_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    columns = ["group_type", "group_value", "horizon", "sample_count", "avg_return", "edge_vs_baseline", "expectancy"]
    available = [column for column in columns if column in frame.columns]
    return frame[available].where(frame[available].notna(), None).to_dict(orient="records")


def _merge_best_patterns(frame: pd.DataFrame, best_patterns: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty or best_patterns is None or best_patterns.empty or "symbol_id" not in frame.columns:
        return frame
    desired = [
        "symbol_id",
        "pattern_family",
        "pattern_state",
        "pattern_lifecycle_state",
        "pattern_score",
        "pattern_rank",
        "setup_quality",
        "setup_quality_bucket",
        "s1_promotion_state",
        "promotion_reason",
        "stage2_score",
        "stage2_label",
        "breakout_level",
        "watchlist_trigger_level",
        "invalidation_price",
        "is_strong_volume_confirmation",
        "is_combined_volume_confirmation",
        "breakout_volume_ratio",
        "source_investigator",
        "source_ranked",
    ]
    available = [col for col in desired if col in best_patterns.columns]
    if len(available) <= 1:
        return frame
    out = frame.copy()
    out.loc[:, "symbol_id"] = out["symbol_id"].astype(str).str.strip().str.upper()
    pattern_cols = [col for col in available if col != "symbol_id"]
    merged = out.merge(best_patterns[available], on="symbol_id", how="left", suffixes=("", "_best"))
    for column in pattern_cols:
        best_col = f"{column}_best"
        if best_col not in merged.columns:
            continue
        if column not in merged.columns:
            merged.loc[:, column] = merged[best_col]
        else:
            current = merged[column]
            missing = current.isna() | current.astype(str).str.strip().str.upper().isin({"", "NONE", "NAN"})
            merged.loc[missing, column] = merged.loc[missing, best_col]
        merged = merged.drop(columns=[best_col])
    return merged


def _read_optional(artifact: StageArtifact | None) -> pd.DataFrame:
    if artifact is None:
        return pd.DataFrame()
    return _read_csv(Path(artifact.uri))


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


INVESTIGATOR_EARLY_ACCUMULATION_COLUMNS = [
    "symbol",
    "symbol_id",
    "sector",
    "close",
    "early_accumulation_score",
    "early_accumulation_rank",
    "early_purity_bucket",
    "pattern_family",
    "pattern_age_days",
    "base_pattern_freshness_score",
    "above_200dma_reclaim_score",
    "delivery_accumulation_score",
    "momentum_recovery_score",
    "volume_confirmation_score",
    "active_rank_pctile",
    "breakout_qualified",
    "graduation_status",
    "watchlist_reason",
    "exchange",
    "trade_date",
    "sma_200",
    "close_vs_sma200_pct",
    "sma200_slope_20d_pct",
    "days_since_200dma_reclaim",
    "trend_score",
    "adx_14",
    "sma50_slope_20d_pct",
    "delivery_pct",
    "delivery_pct_score",
    "delivery_pct_imputed",
    "volume_ratio_20",
    "volume_zscore_20",
    "momentum_acceleration",
    "return_20",
    "return_60",
    "rel_strength_score",
    "relative_strength_score_early",
    "trend_repair_score",
    "active_rank",
    "composite_score",
    "composite_score_adjusted",
    "breakout_type",
    "breakout_score",
    "pattern_state",
    "pattern_signal_date",
    "pattern_count_60d",
]


def _normalise_investigator_early_accumulation(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=INVESTIGATOR_EARLY_ACCUMULATION_COLUMNS)
    out = frame.copy()
    rename_map = {
        "sector_name": "sector",
        "top_pattern_family": "pattern_family",
        "top_pattern_state": "pattern_state",
        "top_pattern_signal_date": "pattern_signal_date",
        "top_pattern_age_days": "pattern_age_days",
        "date": "trade_date",
    }
    out = out.rename(columns={src: dst for src, dst in rename_map.items() if src in out.columns and dst not in out.columns})
    if "symbol_id" not in out.columns:
        out.loc[:, "symbol_id"] = out.get("symbol", pd.Series("", index=out.index))
    out.loc[:, "symbol_id"] = out["symbol_id"].fillna("").astype(str).str.strip().str.upper()
    out.loc[:, "symbol"] = out["symbol_id"]
    if "breakout_qualified" not in out.columns:
        status = out.get("graduation_status", pd.Series("", index=out.index)).fillna("").astype(str)
        breakout_state = out.get("breakout_state", pd.Series("", index=out.index)).fillna("").astype(str)
        out.loc[:, "breakout_qualified"] = status.eq("breakout_qualified") | breakout_state.str.lower().eq("qualified")
    for column in INVESTIGATOR_EARLY_ACCUMULATION_COLUMNS:
        if column not in out.columns:
            out.loc[:, column] = pd.NA
    for column in (
        "close",
        "early_accumulation_score",
        "early_accumulation_rank",
        "pattern_age_days",
        "base_pattern_freshness_score",
        "above_200dma_reclaim_score",
        "delivery_accumulation_score",
        "momentum_recovery_score",
        "volume_confirmation_score",
        "active_rank_pctile",
        "sma_200",
        "close_vs_sma200_pct",
        "sma200_slope_20d_pct",
        "days_since_200dma_reclaim",
        "trend_score",
        "adx_14",
        "sma50_slope_20d_pct",
        "delivery_pct",
        "delivery_pct_score",
        "volume_ratio_20",
        "volume_zscore_20",
        "momentum_acceleration",
        "return_20",
        "return_60",
        "rel_strength_score",
        "relative_strength_score_early",
        "trend_repair_score",
        "active_rank",
        "composite_score",
        "composite_score_adjusted",
        "breakout_score",
        "pattern_count_60d",
    ):
        out.loc[:, column] = pd.to_numeric(out[column], errors="coerce")
    out.loc[:, "breakout_qualified"] = out["breakout_qualified"].fillna(False).astype(bool)
    out = out.loc[:, INVESTIGATOR_EARLY_ACCUMULATION_COLUMNS].copy()
    if not out.empty:
        out = out.sort_values(
            ["early_accumulation_rank", "early_accumulation_score", "symbol_id"],
            ascending=[True, False, True],
            na_position="last",
            kind="stable",
        )
    return out.reset_index(drop=True)
