"""Investigator service orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.investigator.buyer_fingerprint import score_buyer_fingerprint
from ai_trading_system.domains.investigator.cohort_performance import (
    build_performance_summary,
    build_threshold_recommendations,
    mature_investigator_cohorts,
    upsert_investigator_cohorts,
)
from ai_trading_system.domains.investigator.exit_monitor import attach_exit_monitoring
from ai_trading_system.domains.investigator.fundamentals import load_fundamental_snapshot, score_fundamentals
from ai_trading_system.domains.investigator.intake import load_investigator_intake
from ai_trading_system.domains.investigator.lifecycle import apply_lifecycle
from ai_trading_system.domains.investigator.move_classifier import classify_move
from ai_trading_system.domains.investigator.pattern_scan import best_pattern_by_symbol, build_investigator_pattern_scan
from ai_trading_system.domains.investigator.payload import build_investigator_payload
from ai_trading_system.domains.investigator.price_structure import score_price_structure
from ai_trading_system.domains.investigator.repeat_tracker import build_repeat_tracker
from ai_trading_system.domains.investigator.scoring import final_gate, finalize_scores
from ai_trading_system.domains.investigator.sector_context import attach_sector_context
from ai_trading_system.domains.investigator.volume_anatomy import score_volume_anatomy
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult


class InvestigatorService:
    """Build post-rank investigation artifacts."""

    def run(self, context: StageContext) -> StageResult:
        ranked_artifact = context.require_artifact("rank", "ranked_signals")
        ranked = _read_csv(Path(ranked_artifact.uri))
        breakout = _read_optional(context.artifact_for("rank", "breakout_scan"))
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
        candidates = _merge_optional(gainers, breakout, ranked, stock_scan)
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
        investigator_patterns = build_investigator_pattern_scan(
            context=context,
            active_watchlist=active,
            ranked_df=ranked,
        )
        best_patterns = best_pattern_by_symbol(investigator_patterns)
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
                        trigger_reason
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
    ) -> dict[str, Any]:
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
            "trap_count": int(len(traps)),
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
            "performance": performance_summary or {},
        }


def _merge_optional(gainers: pd.DataFrame, *frames: pd.DataFrame | None) -> pd.DataFrame:
    out = gainers.copy()
    for frame in frames:
        if frame is None or frame.empty or "symbol_id" not in frame.columns:
            continue
        cols = ["symbol_id"] + [col for col in frame.columns if col != "symbol_id" and col not in out.columns]
        out = out.merge(frame[cols], on="symbol_id", how="left")
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


def _merge_best_patterns(frame: pd.DataFrame, best_patterns: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty or best_patterns is None or best_patterns.empty or "symbol_id" not in frame.columns:
        return frame
    desired = [
        "symbol_id",
        "pattern_family",
        "pattern_state",
        "pattern_lifecycle_state",
        "pattern_score",
        "setup_quality",
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
    out = out.drop(columns=pattern_cols, errors="ignore")
    return out.merge(best_patterns[available], on="symbol_id", how="left")


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
]


def _normalise_investigator_early_accumulation(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=INVESTIGATOR_EARLY_ACCUMULATION_COLUMNS)
    out = frame.copy()
    rename_map = {
        "sector_name": "sector",
        "top_pattern_family": "pattern_family",
        "top_pattern_age_days": "pattern_age_days",
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
