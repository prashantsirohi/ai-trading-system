"""Service-layer orchestration for the rank stage."""

from __future__ import annotations

import hashlib
import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
from pandas.util import hash_pandas_object

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.pipeline.contracts import TrustConfidenceEnvelope
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext, StageResult
from ai_trading_system.domains.ranking.payloads import (
    augment_dashboard_payload_with_ml,
    build_dashboard_payload,
    summarize_task_statuses,
)


TASK_FILE_MAP = {
    "rank_core": ("ranked_signals", "csv"),
    "breakout_scan": ("breakout_scan", "csv"),
    "pattern_scan": ("pattern_scan", "csv"),
    "stock_scan": ("stock_scan", "csv"),
    "sector_dashboard": ("sector_dashboard", "csv"),
    "dashboard_payload": ("dashboard_payload", "json"),
}


def attach_rank_confidence_from_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Propagate feature confidence when rank confidence is absent."""
    output = frame.copy()
    if "feature_confidence" in output.columns and "rank_confidence" not in output.columns:
        output["rank_confidence"] = pd.to_numeric(output["feature_confidence"], errors="coerce")
    return output


def _normalize_symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol_id"])
    output = frame.copy()
    if "symbol_id" not in output.columns:
        for candidate in ("Symbol", "symbol", "index"):
            if candidate in output.columns:
                output.loc[:, "symbol_id"] = output[candidate]
                break
    if "symbol_id" not in output.columns:
        return pd.DataFrame(columns=["symbol_id"])
    output.loc[:, "symbol_id"] = output["symbol_id"].astype(str)
    return output


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    series = frame[column]
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).astype(float) > 0
    text = series.astype(str).str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y"})


def _coerce_numeric_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = frame.copy()
    for column in columns:
        if column not in output.columns:
            output.loc[:, column] = pd.Series([pd.NA] * len(output), index=output.index)
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce")
    return output


def _rename_context_columns(frame: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    rename_map = {
        "exchange": f"{prefix}_exchange",
        "rel_strength_score": f"{prefix}_rel_strength_score",
        "stage2_score": f"{prefix}_stage2_score",
        "stage2_label": f"{prefix}_stage2_label",
        "close": f"{prefix}_close",
    }
    available = {source: target for source, target in rename_map.items() if source in frame.columns}
    return frame.rename(columns=available)


def _select_best_pattern_rows(pattern_df: pd.DataFrame | None) -> pd.DataFrame:
    frame = _normalize_symbol_frame(pattern_df)
    if frame.empty:
        return pd.DataFrame(columns=["symbol_id", "pattern_positive"])

    frame = _coerce_numeric_columns(
        frame,
        ["pattern_priority_score", "pattern_score", "rel_strength_score", "stage2_score"],
    )
    if "pattern_operational_tier" not in frame.columns:
        frame.loc[:, "pattern_operational_tier"] = "tier_2"
    lifecycle = (
        frame["pattern_lifecycle_state"].astype(str)
        if "pattern_lifecycle_state" in frame.columns
        else frame.get("pattern_state", pd.Series("", index=frame.index)).astype(str)
    )
    positive = frame.loc[
        frame["pattern_operational_tier"].astype(str) != "suppression_only"
    ].loc[lifecycle.isin({"watchlist", "confirmed"})]
    if positive.empty:
        return pd.DataFrame(columns=["symbol_id", "pattern_positive"])

    positive = _rename_context_columns(positive, prefix="pattern")
    positive = positive.sort_values(
        ["pattern_priority_score", "pattern_score", "pattern_rel_strength_score", "pattern_stage2_score", "symbol_id"],
        ascending=[False, False, False, False, True],
        na_position="last",
        kind="stable",
    )
    positive = positive.drop_duplicates(subset=["symbol_id"], keep="first").reset_index(drop=True)
    positive.loc[:, "pattern_positive"] = True
    return positive


def _select_best_breakout_rows(breakout_df: pd.DataFrame | None) -> pd.DataFrame:
    frame = _normalize_symbol_frame(breakout_df)
    if frame.empty:
        return pd.DataFrame(columns=["symbol_id", "breakout_positive"])

    frame = _coerce_numeric_columns(frame, ["breakout_score", "rel_strength_score", "stage2_score"])
    positive = frame.loc[
        frame.get("breakout_state", pd.Series("", index=frame.index)).astype(str).isin({"qualified", "watchlist"})
    ]
    if positive.empty:
        return pd.DataFrame(columns=["symbol_id", "breakout_positive"])

    positive = _rename_context_columns(positive, prefix="breakout")
    positive = positive.sort_values(
        ["breakout_score", "breakout_rel_strength_score", "breakout_stage2_score", "symbol_id"],
        ascending=[False, False, False, True],
        na_position="last",
        kind="stable",
    )
    positive = positive.drop_duplicates(subset=["symbol_id"], keep="first").reset_index(drop=True)
    positive.loc[:, "breakout_positive"] = True
    return positive


def build_integrated_stock_scan_view(
    *,
    ranked_df: pd.DataFrame,
    pattern_df: pd.DataFrame,
    breakout_df: pd.DataFrame,
    legacy_stock_scan_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    ranked = _normalize_symbol_frame(ranked_df)
    ranked = ranked.reset_index(drop=True)
    if "rank" not in ranked.columns:
        ranked.loc[:, "rank"] = pd.Series(range(1, len(ranked) + 1), index=ranked.index, dtype="Int64")
    ranked_top_universe = set(ranked["symbol_id"].astype(str)) if not ranked.empty else set()

    pattern_best = _select_best_pattern_rows(pattern_df)
    breakout_best = _select_best_breakout_rows(breakout_df)

    merged = ranked.copy()
    if merged.empty:
        merged = pd.DataFrame(columns=["symbol_id", "rank"])
    for extra in (pattern_best, breakout_best):
        if extra.empty:
            continue
        merged = merged.merge(extra, on="symbol_id", how="outer")

    if merged.empty:
        return merged

    legacy = _normalize_symbol_frame(legacy_stock_scan_df)
    if not legacy.empty:
        legacy = legacy.drop(columns=[col for col in ("Symbol", "symbol", "index") if col in legacy.columns])
        legacy = legacy.drop_duplicates(subset=["symbol_id"], keep="first")
        merged = merged.merge(legacy, on="symbol_id", how="left", suffixes=("", "_legacy"))

    pattern_positive_mask = _bool_series(merged, "pattern_positive")
    breakout_positive_mask = _bool_series(merged, "breakout_positive")
    merged.loc[:, "pattern_positive"] = pattern_positive_mask
    merged.loc[:, "breakout_positive"] = breakout_positive_mask
    ranked_mask = merged["symbol_id"].astype(str).isin(ranked_top_universe)
    merged.loc[:, "discovered_by_pattern_scan"] = (
        ~ranked_mask
    ) & pattern_positive_mask

    for column in ("rank", "composite_score"):
        if column not in merged.columns:
            merged.loc[:, column] = pd.Series([pd.NA] * len(merged), index=merged.index)
    merged.loc[:, "rank"] = pd.to_numeric(merged["rank"], errors="coerce").astype("Int64")
    merged.loc[:, "composite_score"] = pd.to_numeric(merged["composite_score"], errors="coerce")

    for base, pattern_col, breakout_col in (
        ("exchange", "pattern_exchange", "breakout_exchange"),
        ("rel_strength_score", "pattern_rel_strength_score", "breakout_rel_strength_score"),
        ("stage2_score", "pattern_stage2_score", "breakout_stage2_score"),
        ("stage2_label", "pattern_stage2_label", "breakout_stage2_label"),
        ("close", "pattern_close", "breakout_close"),
    ):
        current = merged[base] if base in merged.columns else pd.Series([pd.NA] * len(merged), index=merged.index, dtype="object")
        pattern_series = (
            merged[pattern_col]
            if pattern_col in merged.columns
            else pd.Series([pd.NA] * len(merged), index=merged.index, dtype="object")
        )
        breakout_series = (
            merged[breakout_col]
            if breakout_col in merged.columns
            else pd.Series([pd.NA] * len(merged), index=merged.index, dtype="object")
        )
        merged[base] = (
            current.astype("object")
            .combine_first(pattern_series.astype("object"))
            .combine_first(breakout_series.astype("object"))
        )

    merged = _coerce_numeric_columns(
        merged,
        [
            "rel_strength_score",
            "stage2_score",
            "close",
            "pattern_priority_score",
            "pattern_score",
            "breakout_score",
        ],
    )

    ranked_symbols = [symbol for symbol in ranked["symbol_id"].astype(str).tolist() if symbol in set(merged["symbol_id"].astype(str))]
    pattern_symbols = (
        merged.loc[(~ranked_mask) & pattern_positive_mask]
        .sort_values(
            ["pattern_priority_score", "rel_strength_score", "stage2_score", "symbol_id"],
            ascending=[False, False, False, True],
            na_position="last",
            kind="stable",
        )["symbol_id"]
        .astype(str)
        .tolist()
    )
    breakout_symbols = (
        merged.loc[
            (~ranked_mask)
            & breakout_positive_mask
            & (~pattern_positive_mask)
        ]
        .sort_values(
            ["breakout_score", "rel_strength_score", "stage2_score", "symbol_id"],
            ascending=[False, False, False, True],
            na_position="last",
            kind="stable",
        )["symbol_id"]
        .astype(str)
        .tolist()
    )
    ordered_symbols = ranked_symbols + pattern_symbols + breakout_symbols
    order_map = {symbol: position for position, symbol in enumerate(ordered_symbols)}
    merged = merged.loc[merged["symbol_id"].astype(str).isin(order_map)].copy()
    merged.loc[:, "_sort_order"] = merged["symbol_id"].astype(str).map(order_map)
    merged = merged.sort_values("_sort_order", kind="stable").drop(columns="_sort_order").reset_index(drop=True)
    return merged


class RankOrchestrationService:
    """Compute rank artifacts while preserving resumability and summaries."""

    def __init__(
        self,
        operation: Optional[Callable[[StageContext], Dict[str, pd.DataFrame]]] = None,
        ml_overlay_builder: Optional[Callable[[StageContext, pd.DataFrame], Dict[str, Any]]] = None,
    ):
        self.operation = operation
        self.ml_overlay_builder = ml_overlay_builder

    def run(
        self,
        context: StageContext,
        *,
        dashboard_payload_builder: Optional[Callable[..., Dict[str, object]]] = None,
    ) -> StageResult:
        outputs = self.run_default(
            context,
            dashboard_payload_builder=dashboard_payload_builder or self.build_dashboard_payload,
        )
        stage_metadata = outputs.pop("__stage_metadata__", {})
        dashboard_payload = outputs.pop("__dashboard_payload__", None)
        outputs, stage_metadata, dashboard_payload, pending_prediction_logs = self.apply_ml_overlay(
            context=context,
            outputs=outputs,
            stage_metadata=stage_metadata,
            dashboard_payload=dashboard_payload,
        )

        artifacts = []
        metadata = {"completed_at": datetime.now(timezone.utc).isoformat()}
        output_dir = context.output_dir()
        artifact_uris: Dict[str, str] = {}

        for artifact_type, df in outputs.items():
            if df is None:
                continue
            path = output_dir / f"{artifact_type}.csv"
            df.to_csv(path, index=False)
            artifact_uris[artifact_type] = str(path)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    path,
                    row_count=len(df),
                    metadata={"columns": list(df.columns)},
                    attempt_number=context.attempt_number,
                )
            )
            metadata[f"{artifact_type}_rows"] = len(df)

        if pending_prediction_logs and context.registry is not None:
            metadata["ml_prediction_log_rows"] = self.write_prediction_logs(
                context=context,
                pending_prediction_logs=pending_prediction_logs,
                artifact_uri=artifact_uris.get("ml_overlay"),
            )

        if dashboard_payload is not None:
            dashboard_path = context.write_json("dashboard_payload.json", dashboard_payload)
            artifacts.append(
                StageArtifact.from_file(
                    "dashboard_payload",
                    dashboard_path,
                    row_count=dashboard_payload.get("summary", {}).get("ranked_count"),
                    metadata={"sections": list(dashboard_payload.keys())},
                    attempt_number=context.attempt_number,
                )
            )

        ranked_signals = outputs.get("ranked_signals", pd.DataFrame())
        metadata["ranked_rows"] = len(ranked_signals)
        metadata["top_symbol"] = (
            str(ranked_signals.iloc[0]["symbol_id"])
            if not ranked_signals.empty and "symbol_id" in ranked_signals.columns
            else None
        )
        metadata.update(stage_metadata)
        summary_path = context.write_json("rank_summary.json", metadata)
        artifacts.append(
            StageArtifact.from_file(
                "rank_summary",
                summary_path,
                row_count=metadata["ranked_rows"],
                metadata=metadata,
                attempt_number=context.attempt_number,
            )
        )
        return StageResult(artifacts=artifacts, metadata=metadata)

    def run_default(
        self,
        context: StageContext,
        *,
        dashboard_payload_builder: Callable[..., Dict[str, object]],
    ) -> Dict[str, pd.DataFrame]:
        if self.operation is not None:
            return self.operation(context)

        from ai_trading_system.analytics.data_trust import load_data_trust_summary
        from ai_trading_system.analytics.patterns import PatternScanConfig, build_pattern_signals
        from ai_trading_system.analytics.patterns.data import load_pattern_frame
        from ai_trading_system.analytics.ranker import StockRanker
        from ai_trading_system.domains.ranking import sector_dashboard, stock_scan
        from ai_trading_system.domains.ranking.breakout import scan_breakouts
        from ai_trading_system.domains.ranking.patterns.universe import (
            build_pattern_seed_universe,
        )

        paths = ensure_domain_layout(
            project_root=context.project_root,
            data_domain=context.params.get("data_domain", "operational"),
        )
        trust_summary = load_data_trust_summary(context.db_path, run_date=context.run_date)
        if trust_summary.get("status") == "blocked" and not bool(context.params.get("allow_untrusted_rank", False)):
            raise RuntimeError(
                "Ranking blocked because active data quarantine remains for the current trust window."
            )
        if trust_summary.get("status") == "degraded":
            warnings = [
                "data trust degraded: "
                f"fallback_ratio={float(trust_summary.get('fallback_ratio_latest', 0.0) or 0.0) * 100:.1f}%"
            ]
        else:
            warnings = []
        previous_attempt, previous_statuses = self.previous_task_snapshot(context)
        task_status: dict[str, Any] = {}

        ranker = StockRanker(
            ohlcv_db_path=str(context.db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=context.params.get("data_domain", "operational"),
        )
        ranked, _ = self.execute_rank_task(
            context=context,
            task_name="rank_core",
            label="Build ranked_signals",
            fingerprint_payload={
                "task": "rank_core",
                "run_date": context.run_date,
                "data_domain": context.params.get("data_domain", "operational"),
                "min_score": float(context.params.get("min_score", 0.0)),
                "top_n": context.params.get("top_n"),
                "symbol_limit": context.params.get("symbol_limit"),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: ranker.rank_all(
                date=context.run_date,
                min_score=float(context.params.get("min_score", 0.0)),
                top_n=context.params.get("top_n"),
                rank_mode=str(context.params.get("rank_mode", "default")),
                apply_penalty_adjustment=bool(
                    context.params.get("rank_apply_penalty_adjustment", False)
                ),
            ),
            optional=False,
        )
        from ai_trading_system.domains.ranking.composite import (
            compute_factor_correlations,
            compute_factor_turnover,
        )

        previous_artifact = None
        previous_week_artifact = None
        if context.registry is not None:
            try:
                prev_artifacts = context.registry.get_latest_artifact(
                    stage_name="rank", artifact_type="ranked_signals", limit=8
                )
                if len(prev_artifacts) >= 2:
                    previous_artifact = prev_artifacts[1]
                if len(prev_artifacts) >= 8:
                    previous_week_artifact = prev_artifacts[7]
            except Exception:
                pass

        previous_df = None
        if previous_artifact and hasattr(previous_artifact, "uri"):
            try:
                previous_df = pd.read_csv(previous_artifact.uri)
            except Exception:
                pass

        previous_week_df = None
        if previous_week_artifact and hasattr(previous_week_artifact, "uri"):
            try:
                previous_week_df = pd.read_csv(previous_week_artifact.uri)
            except Exception:
                pass

        ranked = attach_rank_confidence_from_features(ranked)

        daily_turnover = compute_factor_turnover(ranked, previous_df)
        weekly_turnover = compute_factor_turnover(ranked, previous_week_df)
        correlation_result = compute_factor_correlations(ranked)

        outputs: Dict[str, pd.DataFrame] = {"ranked_signals": ranked}

        breakout_market_bias_allowlist = context.params.get("breakout_market_bias_allowlist", "BULLISH,NEUTRAL")
        if isinstance(breakout_market_bias_allowlist, str):
            breakout_market_bias_allowlist = [
                item.strip()
                for item in breakout_market_bias_allowlist.split(",")
                if item.strip()
            ]
        breakout_df, breakout_status = self.execute_rank_task(
            context=context,
            task_name="breakout_scan",
            label="Build breakout_scan",
            fingerprint_payload={
                "task": "breakout_scan",
                "run_date": context.run_date,
                "breakout_engine": str(context.params.get("breakout_engine", "v2")),
                "market_bias_allowlist": list(breakout_market_bias_allowlist),
                "breakout_min_breadth_score": float(context.params.get("breakout_min_breadth_score", 45.0)),
                "ranked_fingerprint": self.dataframe_fingerprint(ranked),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: scan_breakouts(
                ohlcv_db_path=str(context.db_path),
                feature_store_dir=str(paths.feature_store_dir),
                master_db_path=str(paths.master_db_path),
                date=context.run_date,
                ranked_df=ranked,
                breakout_engine=str(context.params.get("breakout_engine", "v2")),
                include_legacy_families=bool(context.params.get("breakout_include_legacy_families", True)),
                market_bias_allowlist=breakout_market_bias_allowlist,
                min_breadth_score=float(context.params.get("breakout_min_breadth_score", 45.0)),
                sector_rs_min=(
                    float(context.params.get("breakout_sector_rs_min"))
                    if context.params.get("breakout_sector_rs_min") not in (None, "")
                    else None
                ),
                sector_rs_percentile_min=(
                    float(context.params.get("breakout_sector_rs_percentile_min", 60.0))
                    if context.params.get("breakout_sector_rs_percentile_min") not in (None, "")
                    else None
                ),
                breakout_qualified_min_score=int(context.params.get("breakout_qualified_min_score", 3)),
                breakout_symbol_trend_gate_enabled=bool(
                    context.params.get("breakout_symbol_trend_gate_enabled", True)
                ),
                breakout_symbol_near_high_max_pct=float(
                    context.params.get("breakout_symbol_near_high_max_pct", 15.0)
                ),
            ),
            optional=True,
        )
        outputs["breakout_scan"] = breakout_df
        if breakout_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(
                f"breakout_scan unavailable: {breakout_status.get('error_message', breakout_status.get('detail'))}"
            )

        fallback_pattern_symbols = (
            ranked["symbol_id"].astype(str).tolist()
            if not ranked.empty and "symbol_id" in ranked.columns
            else []
        )
        pattern_seed_metadata: dict[str, Any] = {
            "seed_source_counts": {
                "cached": 0,
                "stage2_structural": 0,
                "unusual_movers": 0,
                "liquidity_remaining": 0,
            },
            "broad_universe_count": 0,
            "feature_ready_count": 0,
            "liquidity_pass_count": 0,
            "seed_symbol_count": 0,
            "latest_cached_signal_date": None,
            "pattern_seed_max_symbols": int(context.params.get("pattern_seed_max_symbols", 400) or 400),
            "pattern_min_liquidity_score": float(context.params.get("pattern_min_liquidity_score", 0.2)),
            "pattern_unusual_mover_min_vol20_avg": float(
                context.params.get("pattern_unusual_mover_min_vol20_avg", 100_000)
            ),
            "seed_symbols_digest": "empty",
            "fallback_used": False,
            "fallback_reason": None,
        }
        try:
            pattern_symbols, discovered_seed_metadata = build_pattern_seed_universe(
                project_root=context.project_root,
                ohlcv_db_path=context.db_path,
                signal_date=context.run_date,
                exchange=str(context.params.get("exchange", "NSE")),
                data_domain=str(context.params.get("data_domain", "operational")),
                max_symbols=int(context.params.get("pattern_seed_max_symbols", 400) or 400),
                min_liquidity_score=float(context.params.get("pattern_min_liquidity_score", 0.2)),
                unusual_mover_min_vol20_avg=float(
                    context.params.get("pattern_unusual_mover_min_vol20_avg", 100_000)
                ),
            )
            pattern_seed_metadata.update(discovered_seed_metadata)
            if not pattern_symbols:
                raise RuntimeError("broad seed universe resolved to zero usable symbols")
        except Exception as exc:
            pattern_symbols = fallback_pattern_symbols
            pattern_seed_metadata["fallback_used"] = True
            pattern_seed_metadata["fallback_reason"] = str(exc)
            pattern_seed_metadata["seed_symbol_count"] = len(pattern_symbols)
            pattern_seed_metadata["seed_symbols_digest"] = (
                hashlib.sha256(
                    json.dumps([str(symbol) for symbol in pattern_symbols], sort_keys=False).encode("utf-8")
                ).hexdigest()
                if pattern_symbols
                else "empty"
            )
            if pattern_symbols:
                warnings.append(f"pattern seed universe unavailable: {exc}; reverted to ranked symbols")
            else:
                warnings.append(f"pattern seed universe unavailable: {exc}")
        pattern_output_max_symbols = context.params.get("pattern_max_symbols", 150)
        pattern_enabled = bool(context.params.get("pattern_scan_enabled", True))

        def build_pattern_output() -> pd.DataFrame:
            if not pattern_symbols:
                return pd.DataFrame()
            lookback_days = int(context.params.get("pattern_lookback_days", 420))
            from_ts = (pd.Timestamp(context.run_date) - pd.Timedelta(days=lookback_days)).date().isoformat()
            started_at = time.perf_counter()

            def progress(update: dict[str, Any]) -> None:
                processed = int(update.get("processed_symbols", 0) or 0)
                total = int(update.get("total_symbols", 0) or 0)
                symbol_id = str(update.get("symbol_id", "") or "")
                elapsed = time.perf_counter() - started_at
                context.report_task(
                    task_name="pattern_scan",
                    status="running",
                    detail=f"{processed}/{total} symbols | {elapsed:.1f}s | {symbol_id}",
                    metadata={
                        "processed_symbols": processed,
                        "total_symbols": total,
                        "elapsed_seconds": round(elapsed, 2),
                    },
                )

            pattern_frame = load_pattern_frame(
                context.project_root,
                from_date=from_ts,
                to_date=context.run_date,
                exchange=str(context.params.get("exchange", "NSE")),
                symbols=pattern_symbols,
                data_domain=str(context.params.get("data_domain", "operational")),
            )
            pattern_config = PatternScanConfig(
                exchange=str(context.params.get("exchange", "NSE")),
                data_domain=str(context.params.get("data_domain", "operational")),
                symbols=tuple(pattern_symbols or ()),
                bandwidth=float(context.params.get("pattern_bandwidth", 3.0)),
                extrema_prominence=float(context.params.get("pattern_extrema_prominence", 0.02)),
                breakout_volume_ratio_min=float(context.params.get("pattern_breakout_volume_ratio_min", 1.5)),
                smoothing_method=str(context.params.get("pattern_smoothing_method", "rolling")),
            )
            return build_pattern_signals(
                project_root=context.project_root,
                signal_date=context.run_date,
                exchange=str(context.params.get("exchange", "NSE")),
                data_domain=str(context.params.get("data_domain", "operational")),
                symbols=pattern_symbols,
                config=pattern_config,
                ranked_df=ranked,
                frame=pattern_frame,
                lookback_days=lookback_days,
                progress_callback=progress,
                pattern_workers=int(context.params.get("pattern_workers", 1) or 1),
                scan_mode=str(context.params.get("pattern_scan_mode", "incremental")),
                stage2_only=bool(context.params.get("pattern_stage2_only", True)),
                min_stage2_score=float(context.params.get("pattern_min_stage2_score", 70.0)),
                pattern_seed_metadata=pattern_seed_metadata,
                pattern_watchlist_expiry_bars=int(
                    context.params.get("pattern_watchlist_expiry_bars", 10)
                ),
                pattern_confirmed_expiry_bars=int(
                    context.params.get("pattern_confirmed_expiry_bars", 20)
                ),
                pattern_invalidated_retention_bars=int(
                    context.params.get("pattern_invalidated_retention_bars", 5)
                ),
                pattern_incremental_ranked_buffer=int(
                    context.params.get("pattern_incremental_ranked_buffer", 50)
                ),
            )

        pattern_df, pattern_status = self.execute_rank_task(
            context=context,
            task_name="pattern_scan",
            label="Build pattern_scan",
            fingerprint_payload={
                "task": "pattern_scan",
                "run_date": context.run_date,
                "pattern_scan_enabled": pattern_enabled,
                "pattern_output_max_symbols": (
                    int(pattern_output_max_symbols) if pattern_output_max_symbols else None
                ),
                "pattern_seed_max_symbols": pattern_seed_metadata.get("pattern_seed_max_symbols"),
                "pattern_seed_symbol_count": len(pattern_symbols),
                "pattern_seed_symbols_digest": pattern_seed_metadata.get("seed_symbols_digest"),
                "pattern_seed_metadata": pattern_seed_metadata,
                "pattern_scan_mode": str(context.params.get("pattern_scan_mode", "incremental")),
                "pattern_watchlist_expiry_bars": int(
                    context.params.get("pattern_watchlist_expiry_bars", 10)
                ),
                "pattern_confirmed_expiry_bars": int(
                    context.params.get("pattern_confirmed_expiry_bars", 20)
                ),
                "pattern_invalidated_retention_bars": int(
                    context.params.get("pattern_invalidated_retention_bars", 5)
                ),
                "pattern_incremental_ranked_buffer": int(
                    context.params.get("pattern_incremental_ranked_buffer", 50)
                ),
                "pattern_stage2_only": bool(context.params.get("pattern_stage2_only", True)),
                "pattern_min_stage2_score": float(context.params.get("pattern_min_stage2_score", 70.0)),
                "pattern_bandwidth": float(context.params.get("pattern_bandwidth", 3.0)),
                "pattern_extrema_prominence": float(context.params.get("pattern_extrema_prominence", 0.02)),
                "breakout_volume_ratio_min": float(context.params.get("pattern_breakout_volume_ratio_min", 1.5)),
                "ranked_fingerprint": self.dataframe_fingerprint(ranked),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=build_pattern_output,
            optional=True,
            skip_reason=None if pattern_enabled else "pattern_scan disabled by config",
        )
        pattern_scan_metadata = dict(getattr(pattern_df, "attrs", {}).get("pattern_scan_metrics", {}))
        if pattern_output_max_symbols and not pattern_df.empty:
            pattern_df = pattern_df.head(int(pattern_output_max_symbols)).reset_index(drop=True)
        if pattern_scan_metadata:
            pattern_seed_metadata["pattern_scan_metrics"] = pattern_scan_metadata
        outputs["pattern_scan"] = pattern_df
        if pattern_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(
                f"pattern_scan unavailable: {pattern_status.get('error_message', pattern_status.get('detail'))}"
            )

        legacy_stock_scan_df, stock_status = self.execute_rank_task(
            context=context,
            task_name="stock_scan",
            label="Build stock_scan",
            fingerprint_payload={
                "task": "stock_scan",
                "run_date": context.run_date,
                "data_domain": context.params.get("data_domain", "operational"),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: build_integrated_stock_scan_view(
                ranked_df=ranked,
                pattern_df=pattern_df,
                breakout_df=breakout_df,
                legacy_stock_scan_df=stock_scan.scan_stocks(
                    stock_scan.load_sector_rs(),
                    stock_scan.load_stock_vs_sector(),
                    stock_scan.load_sector_mapping(),
                ).reset_index(),
            ),
            optional=True,
        )
        stock_scan_df = legacy_stock_scan_df
        outputs["stock_scan"] = stock_scan_df
        if stock_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(
                f"stock_scan unavailable: {stock_status.get('error_message', stock_status.get('detail'))}"
            )

        sector_dashboard_df, sector_status = self.execute_rank_task(
            context=context,
            task_name="sector_dashboard",
            label="Build sector_dashboard",
            fingerprint_payload={
                "task": "sector_dashboard",
                "run_date": context.run_date,
                "data_domain": context.params.get("data_domain", "operational"),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: sector_dashboard.build_dashboard(
                sector_dashboard.load_sector_rs(),
                sector_dashboard.compute_sector_momentum(
                    sector_dashboard.load_sector_rs(),
                    days=20,
                ),
            ).reset_index(),
            optional=True,
        )
        outputs["sector_dashboard"] = sector_dashboard_df
        if sector_status["status"] in {"failed", "timed_out", "degraded"}:
            warnings.append(
                f"sector_dashboard unavailable: {sector_status.get('error_message', sector_status.get('detail'))}"
            )

        dashboard_payload, _ = self.execute_rank_task(
            context=context,
            task_name="dashboard_payload",
            label="Build dashboard_payload",
            fingerprint_payload={
                "task": "dashboard_payload",
                "run_date": context.run_date,
                "ranked_fingerprint": self.dataframe_fingerprint(ranked),
                "breakout_fingerprint": self.dataframe_fingerprint(breakout_df),
                "pattern_fingerprint": self.dataframe_fingerprint(pattern_df),
                "stock_scan_fingerprint": self.dataframe_fingerprint(stock_scan_df),
                "sector_dashboard_fingerprint": self.dataframe_fingerprint(sector_dashboard_df),
                "warnings": list(warnings),
            },
            task_status=task_status,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            builder=lambda: dashboard_payload_builder(
                context=context,
                ranked_df=ranked,
                breakout_df=breakout_df,
                pattern_df=pattern_df,
                stock_scan_df=stock_scan_df,
                sector_dashboard_df=sector_dashboard_df,
                warnings=warnings,
                trust_summary=trust_summary,
                task_status=task_status,
            ),
            optional=False,
        )

        top_rank_confidence = None
        if not ranked.empty and "rank_confidence" in ranked.columns:
            try:
                top_rank_confidence = float(pd.to_numeric(ranked["rank_confidence"], errors="coerce").dropna().iloc[0])
            except Exception:
                top_rank_confidence = None
        trust_confidence = TrustConfidenceEnvelope.from_trust_summary(
            trust_summary,
            rank_confidence=top_rank_confidence,
        )

        outputs["__stage_metadata__"] = {
            "degraded_outputs": warnings,
            "degraded_output_count": len(warnings),
            "data_trust_status": trust_summary.get("status"),
            "rank_mode": str(context.params.get("rank_mode", "default")),
            "trust_confidence": trust_confidence.to_dict(),
            "task_status": task_status,
            "task_status_counts": summarize_task_statuses(task_status),
            "resumed_from_attempt": previous_attempt,
            "trust_status_at_start": trust_summary.get("status"),
            "symbol_universe_count": len(ranked),
            "canary_blocked": bool(context.params.get("canary")) and context.params.get("canary_blocked", False),
            "factor_turnover_pct": daily_turnover.get("turnover_pct", 0.0),
            "factor_turnover_symbols_changed": daily_turnover.get("symbols_changed", 0),
            "factor_turnover_weekly_pct": weekly_turnover.get("turnover_pct", 0.0),
            "factor_turnover_weekly_symbols_changed": weekly_turnover.get("symbols_changed", 0),
            "factor_correlation_violations": correlation_result.get("has_violations", False),
            "factor_correlation_details": correlation_result.get("violations", []),
            "factor_correlations_json": json.dumps(correlation_result.get("correlation_matrix", {})) if isinstance(correlation_result.get("correlation_matrix"), dict) else json.dumps({}),
            "pattern_seed_metadata": pattern_seed_metadata,
        }
        outputs["__dashboard_payload__"] = dashboard_payload
        return outputs

    def apply_ml_overlay(
        self,
        *,
        context: StageContext,
        outputs: Dict[str, pd.DataFrame],
        stage_metadata: Dict[str, Any],
        dashboard_payload: Optional[Dict[str, object]],
    ) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any], Optional[Dict[str, object]], Dict[int, Dict[str, Any]]]:
        ml_mode = str(context.params.get("ml_mode", "baseline_only"))
        stage_metadata = dict(stage_metadata or {})
        degraded_outputs = list(stage_metadata.get("degraded_outputs", []))
        stage_metadata["degraded_outputs"] = degraded_outputs
        stage_metadata["degraded_output_count"] = len(degraded_outputs)
        stage_metadata["ml_mode"] = ml_mode

        if context.params.get("smoke") or ml_mode == "baseline_only":
            stage_metadata["ml_status"] = "disabled"
            dashboard_payload = augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=outputs.get("ml_overlay", pd.DataFrame()),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        ranked_df = outputs.get("ranked_signals", pd.DataFrame())
        if ranked_df.empty:
            stage_metadata["ml_status"] = "skipped_empty_ranked"
            dashboard_payload = augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=pd.DataFrame(),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        if ml_mode != "shadow_ml":
            degraded_outputs.append(f"ml overlay unavailable: unsupported ml_mode={ml_mode}")
            stage_metadata["degraded_output_count"] = len(degraded_outputs)
            stage_metadata["ml_status"] = "unsupported_mode"
            dashboard_payload = augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=pd.DataFrame(),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        builder = self.ml_overlay_builder or self.default_ml_overlay_builder
        try:
            overlay_result = builder(context, ranked_df)
        except Exception as exc:
            degraded_outputs.append(f"ml overlay unavailable: {exc}")
            stage_metadata["degraded_output_count"] = len(degraded_outputs)
            stage_metadata["ml_status"] = "degraded"
            dashboard_payload = augment_dashboard_payload_with_ml(
                dashboard_payload,
                ml_status=stage_metadata["ml_status"],
                ml_mode=ml_mode,
                ml_overlay_df=pd.DataFrame(),
            )
            return outputs, stage_metadata, dashboard_payload, {}

        stage_metadata["ml_status"] = overlay_result.get("status", "unknown")
        overlay_df = overlay_result.get("overlay_df", pd.DataFrame())
        if overlay_df is not None and not overlay_df.empty:
            outputs["ml_overlay"] = overlay_df
            stage_metadata["ml_overlay_rows"] = int(len(overlay_df))
            stage_metadata["ml_prediction_date"] = overlay_result.get("prediction_date")
        elif overlay_result.get("reason"):
            degraded_outputs.append(f"ml overlay unavailable: {overlay_result['reason']}")
        stage_metadata["degraded_output_count"] = len(degraded_outputs)
        stage_metadata["ml_metadata"] = overlay_result.get("metadata", {})
        dashboard_payload = augment_dashboard_payload_with_ml(
            dashboard_payload,
            ml_status=stage_metadata["ml_status"],
            ml_mode=ml_mode,
            ml_overlay_df=overlay_df if overlay_df is not None else pd.DataFrame(),
        )
        return outputs, stage_metadata, dashboard_payload, overlay_result.get("prediction_logs", {})

    def default_ml_overlay_builder(self, context: StageContext, ranked_df: pd.DataFrame) -> Dict[str, Any]:
        from ai_trading_system.analytics.alpha.scoring import OperationalMLOverlayService

        service = OperationalMLOverlayService(
            project_root=context.project_root,
            registry=context.registry,
            data_domain=context.params.get("data_domain", "operational"),
        )
        prediction_logs = service.build_shadow_overlay(
            prediction_date=context.run_date,
            exchange=str(context.params.get("exchange", "NSE")),
            lookback_days=int(context.params.get("ml_lookback_days", 420)),
            technical_weight=float(context.params.get("ml_technical_weight", 0.75)),
            ml_weight=float(context.params.get("ml_weight", 0.25)),
        )
        if prediction_logs.get("prediction_logs"):
            for horizon, rows in list(prediction_logs["prediction_logs"].items()):
                model_id = rows[0].get("model_id") if rows else None
                prediction_logs["prediction_logs"][horizon] = {
                    "rows": rows,
                    "prediction_date": prediction_logs.get("prediction_date", context.run_date),
                    "deployment_mode": "shadow_ml",
                    "model_id": model_id,
                }
        return prediction_logs

    def write_prediction_logs(
        self,
        *,
        context: StageContext,
        pending_prediction_logs: Dict[int, Dict[str, Any]],
        artifact_uri: Optional[str],
    ) -> int:
        inserted = 0
        for horizon, payload in pending_prediction_logs.items():
            if isinstance(payload, dict):
                rows = payload.get("rows", [])
                prediction_date = payload.get("prediction_date", context.run_date)
                deployment_mode = payload.get("deployment_mode", "shadow_ml")
                model_id = payload.get("model_id")
            else:
                rows = list(payload)
                prediction_date = context.run_date
                deployment_mode = "shadow_ml"
                model_id = rows[0].get("model_id") if rows else None
            if not rows:
                continue
            inserted += context.registry.replace_prediction_log(
                prediction_date,
                rows,
                deployment_mode=deployment_mode,
                horizon=int(horizon),
                model_id=model_id,
                artifact_uri=artifact_uri,
            )
        return inserted

    def build_dashboard_payload(self, **kwargs) -> Dict[str, object]:
        return build_dashboard_payload(**kwargs)

    def task_status_path(self, context: StageContext) -> Path:
        return context.output_dir() / "task_status.json"

    def task_output_path(self, context: StageContext, task_name: str) -> Path:
        artifact_type, suffix = TASK_FILE_MAP[task_name]
        extension = ".json" if suffix == "json" else ".csv"
        return context.output_dir() / f"{artifact_type}{extension}"

    def previous_task_snapshot(self, context: StageContext) -> tuple[int | None, dict[str, Any]]:
        stage_dir = context.output_dir().parent
        for attempt in range(context.attempt_number - 1, 0, -1):
            snapshot_path = stage_dir / f"attempt_{attempt}" / "task_status.json"
            if not snapshot_path.exists():
                continue
            try:
                payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(payload, dict):
                return attempt, payload
        return None, {}

    def persist_task_status(self, context: StageContext, task_status: dict[str, Any]) -> None:
        self.task_status_path(context).write_text(
            json.dumps(task_status, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )

    def dataframe_fingerprint(self, frame: pd.DataFrame) -> str:
        if frame is None or frame.empty:
            return "empty"
        normalized = frame.copy()
        normalized.columns = [str(column) for column in normalized.columns]
        normalized = normalized.sort_index(axis=1)
        for col in normalized.columns:
            if normalized[col].dtype == object:
                normalized.loc[:, col] = normalized[col].map(
                    lambda x: tuple(x) if isinstance(x, list) else x
                )
        normalized = normalized.astype("object").where(pd.notna(normalized), "<NA>")
        hashed = hash_pandas_object(normalized, index=True).values.tobytes()
        return hashlib.sha256(hashed).hexdigest()

    def task_fingerprint(self, *, task_name: str, payload: dict[str, Any]) -> str:
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        return f"{task_name}:{digest}"

    def operator_task_id(self, context: StageContext, task_name: str) -> str:
        return f"{context.run_id}:{context.stage_name}:{context.attempt_number}:{task_name}"

    def start_task_tracking(
        self,
        *,
        context: StageContext,
        task_name: str,
        label: str,
        metadata: dict[str, Any],
    ) -> None:
        context.report_task(task_name=task_name, status="running", detail=label, metadata=metadata)
        if context.registry is None:
            return
        task_id = self.operator_task_id(context, task_name)
        context.registry.create_operator_task(
            task_id=task_id,
            task_type="pipeline_rank_task",
            label=label,
            status="running",
            metadata={
                "run_id": context.run_id,
                "stage_name": context.stage_name,
                "attempt_number": context.attempt_number,
                **metadata,
            },
        )

    def finish_task_tracking(
        self,
        *,
        context: StageContext,
        task_name: str,
        status: str,
        detail: str,
        metadata: dict[str, Any],
        error: str | None = None,
    ) -> None:
        context.report_task(task_name=task_name, status=status, detail=detail, metadata=metadata)
        if context.registry is None:
            return
        context.registry.update_operator_task(
            self.operator_task_id(context, task_name),
            status=status,
            finished_at=datetime.now(timezone.utc).isoformat(),
            result=metadata,
            error=error,
            metadata={
                "run_id": context.run_id,
                "stage_name": context.stage_name,
                "attempt_number": context.attempt_number,
                **metadata,
            },
        )

    def load_resumed_result(
        self,
        *,
        context: StageContext,
        task_name: str,
        previous_attempt: int | None,
        previous_statuses: dict[str, Any],
        fingerprint: str,
    ) -> tuple[bool, Any]:
        if previous_attempt is None:
            return False, None
        previous_entry = previous_statuses.get(task_name, {})
        if previous_entry.get("fingerprint") != fingerprint:
            return False, None
        if previous_entry.get("status") not in {"completed", "completed_empty", "skipped"}:
            return False, None
        previous_output = previous_entry.get("output_path")
        if not previous_output:
            return False, None
        previous_path = Path(str(previous_output))
        if not previous_path.exists():
            return False, None
        _, suffix = TASK_FILE_MAP[task_name]
        try:
            if suffix == "json":
                return True, json.loads(previous_path.read_text(encoding="utf-8"))
            return True, pd.read_csv(previous_path)
        except Exception:
            return False, None

    def persist_task_output(
        self,
        *,
        context: StageContext,
        task_name: str,
        result: Any,
    ) -> Path:
        output_path = self.task_output_path(context, task_name)
        _, suffix = TASK_FILE_MAP[task_name]
        if suffix == "json":
            output_path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str), encoding="utf-8")
        else:
            frame = result if isinstance(result, pd.DataFrame) else pd.DataFrame()
            frame.to_csv(output_path, index=False)
        return output_path

    def execute_rank_task(
        self,
        *,
        context: StageContext,
        task_name: str,
        label: str,
        fingerprint_payload: dict[str, Any],
        task_status: dict[str, Any],
        previous_attempt: int | None,
        previous_statuses: dict[str, Any],
        builder: Callable[[], Any],
        optional: bool = False,
        skip_reason: str | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        fingerprint = self.task_fingerprint(task_name=task_name, payload=fingerprint_payload)
        started_at = datetime.now(timezone.utc).isoformat()

        if skip_reason:
            record = {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "ended_at": started_at,
                "detail": skip_reason,
                "fingerprint": fingerprint,
            }
            task_status[task_name] = record
            self.persist_task_status(context, task_status)
            self.finish_task_tracking(
                context=context,
                task_name=task_name,
                status="skipped",
                detail=skip_reason,
                metadata=record,
            )
            _, suffix = TASK_FILE_MAP[task_name]
            return ({} if suffix == "json" else pd.DataFrame()), record

        resumed, previous_result = self.load_resumed_result(
            context=context,
            task_name=task_name,
            previous_attempt=previous_attempt,
            previous_statuses=previous_statuses,
            fingerprint=fingerprint,
        )
        if resumed:
            output_path = self.persist_task_output(context=context, task_name=task_name, result=previous_result)
            record = {
                "task_name": task_name,
                "status": "skipped",
                "started_at": started_at,
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "detail": f"Resumed from attempt {previous_attempt}",
                "resumed_from_attempt": previous_attempt,
                "fingerprint": fingerprint,
                "output_path": str(output_path),
            }
            if isinstance(previous_result, pd.DataFrame):
                record["rows"] = int(len(previous_result))
            task_status[task_name] = record
            self.persist_task_status(context, task_status)
            self.finish_task_tracking(
                context=context,
                task_name=task_name,
                status="skipped",
                detail=record["detail"],
                metadata=record,
            )
            return previous_result, record

        self.start_task_tracking(
            context=context,
            task_name=task_name,
            label=label,
            metadata={"fingerprint": fingerprint},
        )
        task_started_perf = time.perf_counter()
        try:
            result = builder()
            output_path = self.persist_task_output(context=context, task_name=task_name, result=result)
            status = "completed"
            rows = None
            if isinstance(result, pd.DataFrame):
                rows = int(len(result))
                if rows == 0:
                    status = "completed_empty"
            record = {
                "task_name": task_name,
                "status": status,
                "started_at": started_at,
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "detail": label,
                "fingerprint": fingerprint,
                "output_path": str(output_path),
                "elapsed_seconds": round(time.perf_counter() - task_started_perf, 3),
            }
            if rows is not None:
                record["rows"] = rows
            task_status[task_name] = record
            self.persist_task_status(context, task_status)
            self.finish_task_tracking(
                context=context,
                task_name=task_name,
                status=status,
                detail=(
                    f"{label} | {record['elapsed_seconds']:.1f}s"
                    if status == "completed"
                    else f"{label} (0 rows) | {record['elapsed_seconds']:.1f}s"
                ),
                metadata=record,
            )
            return result, record
        except TimeoutError as exc:
            status = "timed_out"
            detail = str(exc) or f"{label} timed out"
            caught_exc = exc
            error_traceback = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
        except Exception as exc:
            status = "failed"
            detail = str(exc)
            caught_exc = exc
            error_traceback = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
        if not optional:
            failure_record = {
                "task_name": task_name,
                "status": status,
                "started_at": started_at,
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "detail": detail,
                "fingerprint": fingerprint,
                "elapsed_seconds": round(time.perf_counter() - task_started_perf, 3),
                "error_class": caught_exc.__class__.__name__,
                "error_message": detail,
                "error_traceback": error_traceback,
            }
            task_status[task_name] = failure_record
            self.persist_task_status(context, task_status)
            self.finish_task_tracking(
                context=context,
                task_name=task_name,
                status=status,
                detail=detail,
                metadata=failure_record,
                error=detail,
            )
            raise caught_exc
        empty_result = {} if TASK_FILE_MAP[task_name][1] == "json" else pd.DataFrame()
        output_path = self.persist_task_output(context=context, task_name=task_name, result=empty_result)
        failure_record = {
            "task_name": task_name,
            "status": status,
            "started_at": started_at,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "detail": detail,
            "fingerprint": fingerprint,
            "output_path": str(output_path),
            "elapsed_seconds": round(time.perf_counter() - task_started_perf, 3),
            "error_class": caught_exc.__class__.__name__,
            "error_message": detail,
            "error_traceback": error_traceback,
        }
        task_status[task_name] = failure_record
        self.persist_task_status(context, task_status)
        self.finish_task_tracking(
            context=context,
            task_name=task_name,
            status=status,
            detail=detail,
            metadata=failure_record,
            error=detail,
        )
        return empty_result, failure_record
