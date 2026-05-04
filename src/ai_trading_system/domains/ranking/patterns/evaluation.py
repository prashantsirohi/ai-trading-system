"""Research-only pattern event generation, evaluation, and review rendering."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
import json
import multiprocessing as mp
from pathlib import Path
from typing import Any, Callable

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.analytics.patterns.contracts import (
    PatternBacktestConfig,
    PatternEvent,
    PatternScanConfig,
    PatternTrade,
)
from ai_trading_system.analytics.patterns.data import load_pattern_frame, load_pattern_research_frame
from ai_trading_system.analytics.patterns.detectors import (
    PatternScanStats,
    detect_pattern_signals_for_symbol,
    detect_cup_handle_events,
    detect_round_bottom_events,
)
from ai_trading_system.analytics.patterns.signal import find_local_extrema, kernel_smooth
from ai_trading_system.domains.ranking.patterns.cache import (
    ACTIVE_LIFECYCLE_STATES,
    PatternCacheStore,
)
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.platform.logging.logger import logger


SUPPORTED_SCAN_MODES = {"weekly_full", "incremental", "full"}


def _is_transient_resource_error(exc: BaseException) -> bool:
    """Return True for transient OS resource errors that should fallback to serial scan."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, BlockingIOError):
            return True
        if isinstance(current, PermissionError):
            if current.errno == 1 or "operation not permitted" in str(current).lower():
                return True
        if isinstance(current, OSError):
            if current.errno in {11, 35}:
                return True
            if current.errno == 1:
                return True
            if "resource temporarily unavailable" in str(current).lower():
                return True
            if "operation not permitted" in str(current).lower():
                return True
        if "resource temporarily unavailable" in str(current).lower():
            return True
        if "operation not permitted" in str(current).lower():
            return True
        current = current.__cause__ or current.__context__
    return False


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _safe_mean(frame: pd.DataFrame, column: str) -> float:
    series = _numeric_series(frame, column)
    return float(series.mean()) if not series.empty else float("nan")


def _safe_median(frame: pd.DataFrame, column: str) -> float:
    series = _numeric_series(frame, column)
    return float(series.median()) if not series.empty else float("nan")


def _scan_pattern_events(
    frame: pd.DataFrame,
    *,
    config: PatternBacktestConfig,
) -> tuple[pd.DataFrame, dict[str, dict[str, int]], dict[str, pd.DataFrame]]:
    events: list[PatternEvent] = []
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"candidate_count": 0, "confirmed_count": 0})
    processed: dict[str, pd.DataFrame] = {}

    if frame.empty:
        return pd.DataFrame(), {}, {}

    for symbol, symbol_frame in frame.groupby("symbol_id", sort=True):
        ordered = symbol_frame.sort_values("timestamp").reset_index(drop=True).copy()
        if len(ordered) < config.min_history_bars:
            continue
        smoothed = kernel_smooth(
            ordered["close"],
            bandwidth=config.bandwidth,
            method=getattr(config, "smoothing_method", "kernel"),
        )
        ordered.loc[:, "smoothed_close"] = smoothed
        extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)
        processed[str(symbol)] = ordered

        for detector in (detect_cup_handle_events, detect_round_bottom_events):
            detector_events, detector_stats = detector(
                ordered,
                smoothed=smoothed,
                extrema=extrema,
                config=config,
            )
            stats[detector_stats.pattern_type]["candidate_count"] += detector_stats.candidate_count
            stats[detector_stats.pattern_type]["confirmed_count"] += detector_stats.confirmed_count
            events.extend(detector_events)

    if not events:
        return pd.DataFrame(), dict(stats), processed
    events_df = pd.DataFrame([event.to_record() for event in events]).sort_values(
        ["breakout_date", "symbol_id", "pattern_type"]
    ).reset_index(drop=True)
    return events_df, dict(stats), processed


def _scan_pattern_signals(
    frame: pd.DataFrame,
    *,
    config: PatternScanConfig,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[pd.DataFrame, dict[str, dict[str, int]], dict[str, pd.DataFrame]]:
    signal_rows: list[dict[str, Any]] = []
    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"candidate_count": 0, "confirmed_count": 0, "watchlist_count": 0}
    )
    processed: dict[str, pd.DataFrame] = {}

    if frame.empty:
        return pd.DataFrame(), {}, {}

    eligible_symbols = [
        (str(symbol), symbol_frame)
        for symbol, symbol_frame in frame.groupby("symbol_id", sort=True)
        if len(symbol_frame) >= min(int(config.min_history_bars), int(getattr(config, "ipo_base_min_history_bars", 35)))
    ]
    total_symbols = len(eligible_symbols)

    for idx, (symbol, symbol_frame) in enumerate(eligible_symbols, start=1):
        ordered = symbol_frame.sort_values("timestamp").reset_index(drop=True).copy()
        smoothed = kernel_smooth(
            ordered["close"],
            bandwidth=config.bandwidth,
            method=getattr(config, "smoothing_method", "rolling"),
        )
        ordered.loc[:, "smoothed_close"] = smoothed
        extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)
        processed[str(symbol)] = ordered

        signal_df, detector_stats = detect_pattern_signals_for_symbol(
            ordered,
            smoothed=smoothed,
            extrema=extrema,
            config=config,
        )
        if not signal_df.empty:
            signal_rows.extend(signal_df.to_dict(orient="records"))
        for pattern_type, pattern_stats in detector_stats.items():
            stats[pattern_type]["candidate_count"] += pattern_stats.candidate_count
            stats[pattern_type]["confirmed_count"] += pattern_stats.confirmed_count
            stats[pattern_type]["watchlist_count"] += pattern_stats.watchlist_count
        if callable(progress_callback) and (
            idx == 1 or idx == total_symbols or idx % 25 == 0
        ):
            progress_callback(
                {
                    "processed_symbols": idx,
                    "total_symbols": total_symbols,
                    "symbol_id": str(symbol),
                }
            )

    if not signal_rows:
        return pd.DataFrame(), dict(stats), processed

    signals_df = pd.DataFrame(signal_rows)
    signals_df = signals_df.sort_values(
        ["pattern_rank", "pattern_score", "symbol_id", "signal_date"],
        ascending=[True, False, True, False],
        na_position="last",
    ).reset_index(drop=True)
    return signals_df, dict(stats), processed


def _pattern_signal_worker(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload["symbol_id"])
    ordered = pd.DataFrame(payload["frame"])
    config = PatternScanConfig(**payload["config"])
    smoothed = kernel_smooth(
        ordered["close"],
        bandwidth=config.bandwidth,
        method=getattr(config, "smoothing_method", "rolling"),
    )
    ordered.loc[:, "smoothed_close"] = smoothed
    extrema = find_local_extrema(smoothed, prominence=config.extrema_prominence)
    signal_df, detector_stats = detect_pattern_signals_for_symbol(
        ordered,
        smoothed=smoothed,
        extrema=extrema,
        config=config,
    )
    return {
        "symbol_id": symbol,
        "signal_rows": signal_df.to_dict(orient="records") if not signal_df.empty else [],
        "stats": {
            pattern_type: {
                "candidate_count": pattern_stats.candidate_count,
                "confirmed_count": pattern_stats.confirmed_count,
                "watchlist_count": pattern_stats.watchlist_count,
            }
            for pattern_type, pattern_stats in detector_stats.items()
        },
    }


def _scan_pattern_signals_parallel(
    frame: pd.DataFrame,
    *,
    config: PatternScanConfig,
    pattern_workers: int,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[pd.DataFrame, dict[str, dict[str, int]], dict[str, pd.DataFrame]]:
    if frame.empty:
        return pd.DataFrame(), {}, {}

    eligible_symbols = []
    min_scan_bars = min(int(config.min_history_bars), int(getattr(config, "ipo_base_min_history_bars", 35)))
    for symbol, symbol_frame in frame.groupby("symbol_id", sort=True):
        if len(symbol_frame) < min_scan_bars:
            continue
        ordered = symbol_frame.sort_values("timestamp").reset_index(drop=True)
        eligible_symbols.append(
            {
                "symbol_id": str(symbol),
                "frame": ordered.to_dict(orient="list"),
                "config": config.to_metadata(),
            }
        )
    total_symbols = len(eligible_symbols)
    if total_symbols == 0:
        return pd.DataFrame(), {}, {}
    if total_symbols == 1 or int(pattern_workers) <= 1:
        return _scan_pattern_signals(frame, config=config, progress_callback=progress_callback)

    signal_rows: list[dict[str, Any]] = []
    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"candidate_count": 0, "confirmed_count": 0, "watchlist_count": 0}
    )
    processed_count = 0
    worker_count = max(1, min(int(pattern_workers), total_symbols))
    ctx = mp.get_context("spawn")
    try:
        with ProcessPoolExecutor(max_workers=worker_count, mp_context=ctx) as pool:
            futures = [pool.submit(_pattern_signal_worker, payload) for payload in eligible_symbols]
            for future in as_completed(futures):
                result = future.result()
                signal_rows.extend(result.get("signal_rows", []))
                for pattern_type, pattern_stats in (result.get("stats") or {}).items():
                    stats[pattern_type]["candidate_count"] += int(pattern_stats.get("candidate_count", 0))
                    stats[pattern_type]["confirmed_count"] += int(pattern_stats.get("confirmed_count", 0))
                    stats[pattern_type]["watchlist_count"] += int(pattern_stats.get("watchlist_count", 0))
                processed_count += 1
                if callable(progress_callback):
                    progress_callback(
                        {
                            "processed_symbols": processed_count,
                            "total_symbols": total_symbols,
                            "symbol_id": str(result.get("symbol_id", "")),
                        }
                    )
    except Exception as exc:
        if _is_transient_resource_error(exc):
            logger.warning(
                "Pattern parallel scan resource-constrained; falling back to sequential scan: %s",
                exc,
            )
            return _scan_pattern_signals(frame, config=config, progress_callback=progress_callback)
        raise
    if not signal_rows:
        return pd.DataFrame(), dict(stats), {}

    signals_df = pd.DataFrame(signal_rows)
    signals_df = signals_df.sort_values(
        ["pattern_rank", "pattern_score", "symbol_id", "signal_date"],
        ascending=[True, False, True, False],
        na_position="last",
    ).reset_index(drop=True)
    return signals_df, dict(stats), {}


def build_pattern_events(
    *,
    project_root: str | Path,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    symbols: list[str] | tuple[str, ...] | None = None,
    config: PatternBacktestConfig | None = None,
    research_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build confirmed breakout events for cup-and-handle and round-bottom patterns."""

    active_config = config or PatternBacktestConfig(exchange=exchange, symbols=tuple(symbols or ()))
    frame = research_frame
    if frame is None:
        frame = load_pattern_research_frame(
            project_root,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            symbols=symbols,
        )
    events_df, _, _ = _scan_pattern_events(frame, config=active_config)
    return events_df


def build_pattern_signals(
    *,
    project_root: str | Path,
    signal_date: str,
    exchange: str = "NSE",
    data_domain: str = "operational",
    symbols: list[str] | tuple[str, ...] | None = None,
    config: PatternScanConfig | None = None,
    frame: pd.DataFrame | None = None,
    ranked_df: pd.DataFrame | None = None,
    lookback_days: int = 420,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    pattern_workers: int = 1,
    scan_mode: str = "incremental",
    stage2_only: bool = False,
    min_stage2_score: float = 70.0,
    pattern_seed_metadata: dict[str, Any] | None = None,
    pattern_watchlist_expiry_bars: int = 10,
    pattern_confirmed_expiry_bars: int = 20,
    pattern_invalidated_retention_bars: int = 5,
    pattern_incremental_ranked_buffer: int = 50,
) -> pd.DataFrame:
    """Build live bullish pattern signals for one domain and signal date."""

    normalized_scan_mode = _normalize_scan_mode(scan_mode)
    active_config = config or PatternScanConfig(
        exchange=exchange,
        data_domain=data_domain,
        symbols=tuple(symbols or ()),
    )
    active_config = PatternScanConfig(
        **{
            **active_config.to_metadata(),
            "exchange": exchange,
            "data_domain": data_domain,
            "symbols": tuple(symbols or active_config.symbols),
        }
    )
    project_root = Path(project_root)
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    working_frame = frame
    if working_frame is None:
        from_ts = (pd.Timestamp(signal_date) - pd.Timedelta(days=int(lookback_days))).date().isoformat()
        working_frame = load_pattern_frame(
            project_root,
            from_date=from_ts,
            to_date=signal_date,
            exchange=exchange,
            symbols=symbols,
            data_domain=data_domain,
        )
    working_frame = _attach_stage2_context(working_frame, ranked_df=ranked_df)
    all_symbols = list(symbols or ()) or sorted(
        working_frame.get("symbol_id", pd.Series(dtype=str)).astype(str).unique().tolist()
    )
    selected_symbols, previous_snapshot, scan_resolution = _resolve_scan_symbols(
        working_frame,
        project_root=project_root,
        signal_date=signal_date,
        exchange=exchange,
        data_domain=data_domain,
        all_symbols=all_symbols,
        ranked_df=ranked_df,
        scan_mode=normalized_scan_mode,
        ohlcv_db_path=paths.ohlcv_db_path,
        pattern_seed_metadata=pattern_seed_metadata,
        pattern_incremental_ranked_buffer=pattern_incremental_ranked_buffer,
    )
    effective_scan_mode = str(scan_resolution.get("effective_scan_mode", normalized_scan_mode))
    if selected_symbols:
        scan_frame = working_frame[working_frame["symbol_id"].astype(str).isin(selected_symbols)].copy()
    else:
        scan_frame = working_frame.iloc[0:0].copy()

    scan_frame = _apply_stage2_prescreen(
        scan_frame,
        stage2_only=stage2_only,
        min_stage2_score=min_stage2_score,
    )

    fresh_signals_df = pd.DataFrame()
    if int(pattern_workers) > 1:
        fresh_signals_df, _, _ = _scan_pattern_signals_parallel(
            scan_frame,
            config=active_config,
            pattern_workers=int(pattern_workers),
            progress_callback=progress_callback,
        )
    else:
        fresh_signals_df, _, _ = _scan_pattern_signals(
            scan_frame,
            config=active_config,
            progress_callback=progress_callback,
        )
    snapshot_df = _build_pattern_lifecycle_snapshot(
        fresh_signals_df=fresh_signals_df,
        previous_snapshot_df=previous_snapshot,
        market_frame=working_frame,
        as_of_date=signal_date,
        scan_mode=effective_scan_mode,
        exchange=exchange,
        watchlist_expiry_bars=pattern_watchlist_expiry_bars,
        confirmed_expiry_bars=pattern_confirmed_expiry_bars,
        invalidated_retention_bars=pattern_invalidated_retention_bars,
    )
    snapshot_df = _attach_rank_context_and_score(snapshot_df, ranked_df=ranked_df).reset_index(drop=True)
    output_df = snapshot_df.copy()
    if not output_df.empty and "pattern_lifecycle_state" in output_df.columns:
        output_df = output_df[
            output_df["pattern_lifecycle_state"].astype(str).str.lower() != "expired"
        ].reset_index(drop=True)
    _write_pattern_cache(
        project_root=project_root,
        data_domain=data_domain,
        exchange=exchange,
        signal_date=signal_date,
        scan_mode=effective_scan_mode,
        selected_symbols=selected_symbols,
        signals_df=snapshot_df,
    )
    output_df.attrs["pattern_scan_metrics"] = {
        **scan_resolution,
        "requested_scan_mode": normalized_scan_mode,
        "effective_scan_mode": effective_scan_mode,
        "selected_symbol_count": len(selected_symbols),
        "snapshot_row_count": int(len(snapshot_df)),
        "output_row_count": int(len(output_df)),
        "carry_forward_count": int(
            pd.to_numeric(snapshot_df.get("carry_forward_bars"), errors="coerce").fillna(0).gt(0).sum()
        )
        if not snapshot_df.empty
        else 0,
        "lifecycle_counts": (
            snapshot_df["pattern_lifecycle_state"].astype(str).value_counts().to_dict()
            if not snapshot_df.empty and "pattern_lifecycle_state" in snapshot_df.columns
            else {}
        ),
    }
    return output_df


def _latest_rank_context(ranked_df: pd.DataFrame | None) -> pd.DataFrame:
    if ranked_df is None or ranked_df.empty or "symbol_id" not in ranked_df.columns:
        return pd.DataFrame()
    ctx_cols = [
        col
        for col in [
            "symbol_id",
            "rel_strength_score",
            "sector_rs_value",
            "stage2_score",
            "stage2_label",
        ]
        if col in ranked_df.columns
    ]
    if not ctx_cols:
        return pd.DataFrame()
    ctx = ranked_df[ctx_cols].copy()
    ctx.loc[:, "symbol_id"] = ctx["symbol_id"].astype(str)
    return ctx.drop_duplicates(subset=["symbol_id"], keep="last").reset_index(drop=True)


def _attach_stage2_context(frame: pd.DataFrame, *, ranked_df: pd.DataFrame | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    ctx = _latest_rank_context(ranked_df)
    if ctx.empty:
        return frame.copy()
    merged = frame.copy()
    for col in ["stage2_score", "stage2_label", "rel_strength_score"]:
        if col in merged.columns:
            merged = merged.drop(columns=[col])
    return merged.merge(ctx, on="symbol_id", how="left")


def _attach_rank_context_and_score(signals_df: pd.DataFrame, *, ranked_df: pd.DataFrame | None) -> pd.DataFrame:
    if signals_df.empty:
        return signals_df
    output = signals_df.copy()
    ctx = _latest_rank_context(ranked_df)
    if not ctx.empty:
        if "sector_rs_value" not in ctx.columns:
            ctx.loc[:, "sector_rs_value"] = np.nan
        if "rel_strength_score" not in ctx.columns:
            ctx.loc[:, "rel_strength_score"] = np.nan
        ctx.loc[:, "sector_rs_percentile"] = (
            pd.to_numeric(ctx["sector_rs_value"], errors="coerce").rank(pct=True, method="average") * 100.0
        )
        output = output.drop(columns=["rel_strength_score", "sector_rs_percentile", "stage2_score", "stage2_label"], errors="ignore")
        output = output.merge(
            ctx[[col for col in ["symbol_id", "rel_strength_score", "sector_rs_percentile", "stage2_score", "stage2_label"] if col in ctx.columns]],
            on="symbol_id",
            how="left",
        )
    output = output.drop(columns=["pattern_rank"], errors="ignore")
    from ai_trading_system.analytics.patterns.detectors import _score_signal_rows  # local import to avoid widening public surface

    return _score_signal_rows(output)


def _build_scan_payloads(frame: pd.DataFrame) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    if frame.empty:
        return payloads
    for symbol_id, symbol_frame in frame.groupby("symbol_id", sort=True):
        ordered = symbol_frame.sort_values("timestamp").reset_index(drop=True).copy()
        payloads.append({"symbol_id": str(symbol_id), "frame": ordered})
    return payloads


def _stage2_prescreened(
    eligible_payloads: list[dict],
    *,
    stage2_only: bool = False,
    min_stage2_score: float = 70.0,
) -> tuple[list[dict], list[dict]]:
    stage2, non_stage2 = [], []
    for payload in eligible_payloads:
        frame = pd.DataFrame(payload.get("frame", {}))
        if frame.empty:
            continue
        latest_s2 = 0.0
        if "stage2_score" in frame.columns:
            latest_s2 = float(pd.to_numeric(frame["stage2_score"], errors="coerce").fillna(0.0).iloc[-1])
        if latest_s2 >= min_stage2_score:
            stage2.append(payload)
        elif not stage2_only:
            non_stage2.append(payload)
    return stage2, non_stage2


def _apply_stage2_prescreen(
    frame: pd.DataFrame,
    *,
    stage2_only: bool,
    min_stage2_score: float,
) -> pd.DataFrame:
    payloads = _build_scan_payloads(frame)
    if not payloads:
        return frame
    stage2_payloads, non_stage2_payloads = _stage2_prescreened(
        payloads,
        stage2_only=stage2_only,
        min_stage2_score=min_stage2_score,
    )
    if stage2_only and not stage2_payloads:
        has_stage2_data = any("stage2_score" in pd.DataFrame(payload.get("frame", {})).columns for payload in payloads)
        return frame if not has_stage2_data else pd.DataFrame(columns=frame.columns)
    selected_payloads = stage2_payloads + non_stage2_payloads
    selected_symbols = {str(payload["symbol_id"]) for payload in selected_payloads}
    return frame[frame["symbol_id"].astype(str).isin(selected_symbols)].copy()


def _normalize_scan_mode(scan_mode: str | None) -> str:
    normalized = str(scan_mode or "incremental").strip().lower()
    if normalized not in SUPPORTED_SCAN_MODES:
        raise ValueError(f"Unsupported pattern scan mode: {scan_mode}")
    return normalized


def _resolve_scan_symbols(
    frame: pd.DataFrame,
    *,
    project_root: Path,
    signal_date: str,
    exchange: str,
    data_domain: str,
    all_symbols: list[str],
    ranked_df: pd.DataFrame | None,
    scan_mode: str,
    ohlcv_db_path: Path,
    pattern_seed_metadata: dict[str, Any] | None,
    pattern_incremental_ranked_buffer: int,
) -> tuple[list[str], pd.DataFrame, dict[str, Any]]:
    normalized_symbols = [str(symbol).strip().upper() for symbol in all_symbols if str(symbol).strip()]
    metadata: dict[str, Any] = {
        "fallback_to_full": False,
        "previous_snapshot_date": None,
        "selected_symbol_count": len(normalized_symbols),
        "active_cached_symbol_count": 0,
        "changed_symbol_count": 0,
        "unusual_symbol_count": 0,
        "ranked_buffer_symbol_count": 0,
        "effective_scan_mode": scan_mode,
    }
    if frame.empty or str(data_domain).lower() != "operational":
        return normalized_symbols, pd.DataFrame(), metadata
    try:
        store = PatternCacheStore(project_root / "data" / "control_plane.duckdb")
    except duckdb.IOException:
        metadata["fallback_to_full"] = True
        metadata["effective_scan_mode"] = "full"
        return normalized_symbols, pd.DataFrame(), metadata
    if scan_mode in {"weekly_full", "full"}:
        return normalized_symbols, pd.DataFrame(), metadata

    previous_snapshot_date = store.latest_snapshot_date(as_of_date=signal_date, exchange=exchange)
    metadata["previous_snapshot_date"] = previous_snapshot_date
    if previous_snapshot_date is None:
        metadata["fallback_to_full"] = True
        metadata["effective_scan_mode"] = "full"
        return normalized_symbols, pd.DataFrame(), metadata

    previous_snapshot = store.read_snapshot(as_of_date=previous_snapshot_date, exchange=exchange)
    if previous_snapshot.empty:
        metadata["fallback_to_full"] = True
        metadata["effective_scan_mode"] = "full"
        return normalized_symbols, pd.DataFrame(), metadata

    active_cached = store.load_latest_active_signals_before(as_of_date=signal_date, exchange=exchange)
    active_cached_symbols = _ordered_unique(
        active_cached.get("symbol_id", pd.Series(dtype=str)).astype(str).str.upper().tolist()
        if not active_cached.empty and "symbol_id" in active_cached.columns
        else []
    )
    changed_symbols = store.symbols_needing_rescan(
        normalized_symbols,
        ohlcv_db_path=ohlcv_db_path,
        as_of_date=signal_date,
    )
    unusual_symbols = _ordered_unique(
        [
            str(symbol).strip().upper()
            for symbol in (
                ((pattern_seed_metadata or {}).get("seed_source_symbols") or {}).get("unusual_movers", [])
            )
            if str(symbol).strip()
        ]
    )
    ranked_buffer_symbols: list[str] = []
    if ranked_df is not None and not ranked_df.empty and "symbol_id" in ranked_df.columns:
        ranked_buffer_symbols = (
            ranked_df["symbol_id"]
            .astype(str)
            .str.upper()
            .dropna()
            .head(max(int(pattern_incremental_ranked_buffer or 0), 0))
            .tolist()
        )

    ordered_symbols = _ordered_unique(
        active_cached_symbols,
        changed_symbols,
        unusual_symbols,
        ranked_buffer_symbols,
    )
    metadata.update(
        {
            "selected_symbol_count": len(ordered_symbols),
            "active_cached_symbol_count": len(active_cached_symbols),
            "changed_symbol_count": len(changed_symbols),
            "unusual_symbol_count": len(unusual_symbols),
            "ranked_buffer_symbol_count": len(ranked_buffer_symbols),
        }
    )
    return ordered_symbols, previous_snapshot, metadata


def _ordered_unique(*buckets: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for bucket in buckets:
        for symbol in bucket:
            normalized = str(symbol).strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _pattern_merge_key(row: pd.Series | dict[str, Any]) -> tuple[str, str, str]:
    return (
        str((row.get("symbol_id") if isinstance(row, dict) else row["symbol_id"]) or "").strip().upper(),
        str((row.get("exchange") if isinstance(row, dict) else row["exchange"]) or "NSE").strip().upper() or "NSE",
        str((row.get("pattern_family") if isinstance(row, dict) else row["pattern_family"]) or "").strip().lower(),
    )


def _normalize_fresh_signal_snapshot(
    fresh_signals_df: pd.DataFrame,
    *,
    as_of_date: str,
    exchange: str,
    previous_by_key: dict[tuple[str, str, str], dict[str, Any]] | None = None,
) -> pd.DataFrame:
    if fresh_signals_df.empty:
        return pd.DataFrame()
    fresh = fresh_signals_df.copy()
    if "pattern_score" not in fresh.columns:
        fresh.loc[:, "pattern_score"] = np.nan
    fresh.loc[:, "exchange"] = fresh.get("exchange", pd.Series(exchange, index=fresh.index)).fillna(exchange)
    fresh.loc[:, "pattern_lifecycle_state"] = fresh["pattern_state"].astype(str).str.lower()
    fresh.loc[:, "as_of_date"] = str(as_of_date)
    fresh.loc[:, "fresh_signal_date"] = fresh["signal_date"]
    fresh.loc[:, "last_seen_date"] = str(as_of_date)
    fresh.loc[:, "invalidated_date"] = None
    fresh.loc[:, "expired_date"] = None
    fresh.loc[:, "carry_forward_bars"] = 0
    previous_by_key = previous_by_key or {}
    first_seen_dates: list[str] = []
    for _, row in fresh.iterrows():
        previous = previous_by_key.get(_pattern_merge_key(row))
        first_seen_dates.append(
            str(previous.get("first_seen_date") or previous.get("fresh_signal_date") or row["signal_date"])
            if previous
            else str(row["signal_date"])
        )
    fresh.loc[:, "first_seen_date"] = first_seen_dates
    return (
        fresh.sort_values(
            ["pattern_score", "signal_date", "symbol_id"],
            ascending=[False, False, True],
            na_position="last",
        )
        .drop_duplicates(subset=["symbol_id", "exchange", "pattern_family"], keep="first")
        .reset_index(drop=True)
    )


def _latest_close_map(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty or "symbol_id" not in frame.columns or "close" not in frame.columns:
        return {}
    latest = frame.sort_values(["symbol_id", "timestamp"]).groupby("symbol_id", sort=False).tail(1)
    closes = pd.to_numeric(latest["close"], errors="coerce")
    return {
        str(symbol).strip().upper(): float(close)
        for symbol, close in zip(latest["symbol_id"], closes)
        if pd.notna(close)
    }


def _business_bar_delta(start_date: str | None, end_date: str | None) -> int:
    if not start_date or not end_date:
        return 10_000
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if end <= start:
        return 0
    return max(0, len(pd.bdate_range(start, end)) - 1)


def _carry_forward_snapshot_row(
    previous_row: dict[str, Any],
    *,
    as_of_date: str,
    latest_close: float | None,
    watchlist_expiry_bars: int,
    confirmed_expiry_bars: int,
    invalidated_retention_bars: int,
) -> dict[str, Any] | None:
    lifecycle = str(previous_row.get("pattern_lifecycle_state") or previous_row.get("pattern_state") or "").lower()
    if lifecycle == "expired":
        return None

    row = dict(previous_row)
    row["as_of_date"] = str(as_of_date)
    carry_forward_value = pd.to_numeric(previous_row.get("carry_forward_bars"), errors="coerce")
    row["carry_forward_bars"] = int(0 if pd.isna(carry_forward_value) else carry_forward_value) + 1

    if lifecycle in ACTIVE_LIFECYCLE_STATES:
        invalidation_price = pd.to_numeric(
            pd.Series([previous_row.get("invalidation_price")]),
            errors="coerce",
        ).iloc[0]
        last_seen_date = str(
            previous_row.get("last_seen_date")
            or previous_row.get("as_of_date")
            or previous_row.get("fresh_signal_date")
            or previous_row.get("signal_date")
            or ""
        )
        expiry_bars = watchlist_expiry_bars if lifecycle == "watchlist" else confirmed_expiry_bars
        if latest_close is not None and pd.notna(invalidation_price) and float(latest_close) <= float(invalidation_price):
            row["pattern_lifecycle_state"] = "invalidated"
            row["invalidated_date"] = str(previous_row.get("invalidated_date") or as_of_date)
            row["expired_date"] = None
            return row
        if _business_bar_delta(last_seen_date, as_of_date) >= int(expiry_bars):
            row["pattern_lifecycle_state"] = "expired"
            row["expired_date"] = str(as_of_date)
            return row
        row["pattern_lifecycle_state"] = lifecycle
        return row

    if lifecycle == "invalidated":
        invalidated_date = str(previous_row.get("invalidated_date") or previous_row.get("as_of_date") or "")
        if _business_bar_delta(invalidated_date, as_of_date) >= int(invalidated_retention_bars):
            row["pattern_lifecycle_state"] = "expired"
            row["expired_date"] = str(as_of_date)
            return row
        row["pattern_lifecycle_state"] = "invalidated"
        return row

    return None


def _build_pattern_lifecycle_snapshot(
    *,
    fresh_signals_df: pd.DataFrame,
    previous_snapshot_df: pd.DataFrame,
    market_frame: pd.DataFrame,
    as_of_date: str,
    scan_mode: str,
    exchange: str,
    watchlist_expiry_bars: int,
    confirmed_expiry_bars: int,
    invalidated_retention_bars: int,
) -> pd.DataFrame:
    if scan_mode in {"weekly_full", "full"}:
        return _normalize_fresh_signal_snapshot(
            fresh_signals_df,
            as_of_date=as_of_date,
            exchange=exchange,
        )

    previous_snapshot_df = previous_snapshot_df.copy() if previous_snapshot_df is not None else pd.DataFrame()
    if not previous_snapshot_df.empty:
        previous_snapshot_df.loc[:, "exchange"] = previous_snapshot_df.get(
            "exchange",
            pd.Series(exchange, index=previous_snapshot_df.index),
        ).fillna(exchange)
    previous_by_key = (
        {_pattern_merge_key(row): row.to_dict() for _, row in previous_snapshot_df.iterrows()}
        if not previous_snapshot_df.empty
        else {}
    )
    fresh_snapshot = _normalize_fresh_signal_snapshot(
        fresh_signals_df,
        as_of_date=as_of_date,
        exchange=exchange,
        previous_by_key=previous_by_key,
    )
    fresh_keys = {_pattern_merge_key(row) for _, row in fresh_snapshot.iterrows()} if not fresh_snapshot.empty else set()
    latest_close_by_symbol = _latest_close_map(market_frame)

    carried_rows: list[dict[str, Any]] = []
    for key, previous_row in previous_by_key.items():
        if key in fresh_keys:
            continue
        carried = _carry_forward_snapshot_row(
            previous_row,
            as_of_date=as_of_date,
            latest_close=latest_close_by_symbol.get(key[0]),
            watchlist_expiry_bars=watchlist_expiry_bars,
            confirmed_expiry_bars=confirmed_expiry_bars,
            invalidated_retention_bars=invalidated_retention_bars,
        )
        if carried is not None:
            carried_rows.append(carried)

    parts: list[pd.DataFrame] = []
    if not fresh_snapshot.empty:
        parts.append(fresh_snapshot)
    if carried_rows:
        parts.append(pd.DataFrame.from_records(carried_rows))
    if not parts:
        return pd.DataFrame()
    parts = [part.dropna(axis=1, how="all") for part in parts]
    snapshot = pd.concat(parts, ignore_index=True, sort=False)
    if "pattern_score" not in snapshot.columns:
        snapshot.loc[:, "pattern_score"] = np.nan
    for public_column in ("volume_zscore_20", "volume_zscore_50"):
        if public_column not in snapshot.columns:
            snapshot.loc[:, public_column] = np.nan
    return (
        snapshot.sort_values(
            ["pattern_lifecycle_state", "pattern_score", "symbol_id", "pattern_family"],
            ascending=[True, False, True, True],
            na_position="last",
        )
        .drop_duplicates(subset=["symbol_id", "exchange", "pattern_family"], keep="first")
        .reset_index(drop=True)
    )


def _write_pattern_cache(
    *,
    project_root: Path,
    data_domain: str,
    exchange: str,
    signal_date: str,
    scan_mode: str,
    selected_symbols: list[str],
    signals_df: pd.DataFrame,
) -> None:
    if str(data_domain).lower() != "operational":
        return
    try:
        store = PatternCacheStore(project_root / "data" / "control_plane.duckdb")
    except duckdb.IOException:
        return
    normalized_mode = _normalize_scan_mode(scan_mode)
    if normalized_mode == "weekly_full":
        scan_run_id = f"weekly_full_{signal_date}"
        run_scope = None
    elif normalized_mode == "incremental":
        run_scope = f"incremental:{signal_date}:"
        scan_run_id = f"{run_scope}{len(selected_symbols)}"
    else:
        run_scope = f"full:{signal_date}:"
        scan_run_id = f"{run_scope}{len(selected_symbols)}"
    store.write_signals(
        signals_df,
        scan_run_id=scan_run_id,
        replace_date=signal_date,
        replace_run_scope=run_scope,
        as_of_date=signal_date,
    )


def _attach_event_study_metrics(
    events_df: pd.DataFrame,
    *,
    by_symbol: dict[str, pd.DataFrame],
    config: PatternBacktestConfig,
) -> pd.DataFrame:
    if events_df.empty:
        return events_df

    events = events_df.copy()
    for horizon in config.event_horizons:
        events[f"return_{horizon}d"] = np.nan
        events[f"hit_{horizon}d"] = np.nan

    for row_idx, row in events.iterrows():
        symbol_frame = by_symbol.get(str(row["symbol_id"]))
        if symbol_frame is None:
            continue
        breakout_idx = int(row["breakout_bar_index"])
        breakout_close = float(symbol_frame.iloc[breakout_idx]["close"])
        for horizon in config.event_horizons:
            future_idx = breakout_idx + horizon
            if future_idx >= len(symbol_frame):
                continue
            future_close = float(symbol_frame.iloc[future_idx]["close"])
            ret = future_close / breakout_close - 1.0
            events.at[row_idx, f"return_{horizon}d"] = round(ret, 6)
            events.at[row_idx, f"hit_{horizon}d"] = bool(ret > 0)

    return events


def simulate_pattern_trades(
    events_df: pd.DataFrame,
    *,
    by_symbol: dict[str, pd.DataFrame],
    config: PatternBacktestConfig,
) -> pd.DataFrame:
    """Run normalized breakout trade simulations for pattern events."""

    if events_df.empty:
        return pd.DataFrame()

    trades: list[PatternTrade] = []
    for _, row in events_df.iterrows():
        symbol_frame = by_symbol.get(str(row["symbol_id"]))
        if symbol_frame is None:
            continue
        breakout_idx = int(row["breakout_bar_index"])
        entry_idx = breakout_idx + 1
        if entry_idx >= len(symbol_frame):
            continue
        entry_bar = symbol_frame.iloc[entry_idx]
        entry_price = float(entry_bar["open"])
        stop_price = float(row["invalidation_price"])
        risk = entry_price - stop_price
        if risk <= 0:
            continue
        target_price = entry_price + risk * config.target_r_multiple
        end_idx = min(len(symbol_frame) - 1, entry_idx + config.max_hold_bars)
        window = symbol_frame.iloc[entry_idx : end_idx + 1].reset_index(drop=True)
        mfe = float((window["high"].max() / entry_price) - 1.0)
        mae = float((window["low"].min() / entry_price) - 1.0)

        exit_idx = end_idx
        exit_price = float(symbol_frame.iloc[end_idx]["close"])
        exit_reason = "timeout"
        for offset, (_, bar) in enumerate(symbol_frame.iloc[entry_idx : end_idx + 1].iterrows()):
            bar_low = float(bar["low"])
            bar_high = float(bar["high"])
            if bar_low <= stop_price:
                exit_idx = entry_idx + offset
                exit_price = stop_price
                exit_reason = "stop"
                break
            if bar_high >= target_price:
                exit_idx = entry_idx + offset
                exit_price = target_price
                exit_reason = "target"
                break

        gross_return = exit_price / entry_price - 1.0
        net_return = gross_return - (2.0 * config.commission_rate)
        r_multiple = (exit_price - entry_price) / risk
        trade = PatternTrade(
            event_id=str(row["event_id"]),
            symbol_id=str(row["symbol_id"]),
            pattern_type=str(row["pattern_type"]),
            breakout_date=str(row["breakout_date"]),
            entry_date=symbol_frame.iloc[entry_idx]["timestamp"].date().isoformat(),
            exit_date=symbol_frame.iloc[exit_idx]["timestamp"].date().isoformat(),
            entry_price=round(entry_price, 6),
            exit_price=round(exit_price, 6),
            stop_price=round(stop_price, 6),
            target_price=round(target_price, 6),
            exit_reason=exit_reason,
            holding_bars=int(exit_idx - entry_idx + 1),
            gross_return=round(gross_return, 6),
            net_return=round(net_return, 6),
            r_multiple=round(r_multiple, 6),
            mfe=round(mfe, 6),
            mae=round(mae, 6),
        )
        trades.append(trade)

    if not trades:
        return pd.DataFrame()
    return pd.DataFrame([trade.to_record() for trade in trades]).sort_values(
        ["entry_date", "symbol_id", "pattern_type"]
    ).reset_index(drop=True)


def _months_between(from_date: str, to_date: str) -> int:
    start = pd.Timestamp(from_date)
    end = pd.Timestamp(to_date)
    months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    return max(1, int(months))


def _build_summary_rows(
    events_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    *,
    stats: dict[str, dict[str, int]],
    from_date: str,
    to_date: str,
    config: PatternBacktestConfig,
) -> pd.DataFrame:
    months = _months_between(from_date, to_date)
    rows: list[dict[str, Any]] = []
    for pattern_type in ["cup_handle", "round_bottom", "all_patterns"]:
        if pattern_type == "all_patterns":
            event_slice = events_df.copy()
            trade_slice = trades_df.copy()
            candidate_count = int(sum(item.get("candidate_count", 0) for item in stats.values()))
            confirmed_count = int(sum(item.get("confirmed_count", 0) for item in stats.values()))
        else:
            event_slice = events_df[events_df["pattern_type"] == pattern_type].copy()
            trade_slice = trades_df[trades_df["pattern_type"] == pattern_type].copy()
            candidate_count = int(stats.get(pattern_type, {}).get("candidate_count", 0))
            confirmed_count = int(stats.get(pattern_type, {}).get("confirmed_count", 0))

        row: dict[str, Any] = {
            "pattern_type": pattern_type,
            "candidate_count": candidate_count,
            "signal_count": int(len(event_slice)),
            "trade_count": int(len(trade_slice)),
            "signals_per_month": round(len(event_slice) / months, 2),
            "confirmation_rate": round(confirmed_count / candidate_count, 4) if candidate_count else np.nan,
            "avg_cup_depth_pct": round(_safe_mean(event_slice, "cup_depth_pct"), 4) if not event_slice.empty else np.nan,
            "avg_width_bars": round(_safe_mean(event_slice, "width_bars"), 2) if not event_slice.empty else np.nan,
            "stop_out_rate": round(float((trade_slice.get("exit_reason") == "stop").mean()), 4)
            if not trade_slice.empty
            else np.nan,
            "avg_net_return": round(_safe_mean(trade_slice, "net_return"), 6) if not trade_slice.empty else np.nan,
            "median_net_return": round(_safe_median(trade_slice, "net_return"), 6) if not trade_slice.empty else np.nan,
            "expectancy_r": round(_safe_mean(trade_slice, "r_multiple"), 6) if not trade_slice.empty else np.nan,
            "avg_mfe": round(_safe_mean(trade_slice, "mfe"), 6) if not trade_slice.empty else np.nan,
            "avg_mae": round(_safe_mean(trade_slice, "mae"), 6) if not trade_slice.empty else np.nan,
        }
        for horizon in config.event_horizons:
            ret_col = f"return_{horizon}d"
            returns = _numeric_series(event_slice, ret_col)
            row[f"hit_rate_{horizon}d"] = round(float((returns > 0).mean()), 4) if not returns.empty else np.nan
            row[f"avg_return_{horizon}d"] = round(float(returns.mean()), 6) if not returns.empty else np.nan
            row[f"median_return_{horizon}d"] = round(float(returns.median()), 6) if not returns.empty else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def _build_yearly_breakdown(
    events_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    *,
    config: PatternBacktestConfig,
) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()
    yearly = events_df.copy()
    yearly.loc[:, "breakout_year"] = pd.to_datetime(yearly["breakout_date"]).dt.year
    merged = yearly.merge(
        trades_df[["event_id", "net_return", "exit_reason", "r_multiple"]] if not trades_df.empty else pd.DataFrame(columns=["event_id"]),
        on="event_id",
        how="left",
    )
    rows: list[dict[str, Any]] = []
    for (year, pattern_type), group in merged.groupby(["breakout_year", "pattern_type"], dropna=False):
        row: dict[str, Any] = {
            "breakout_year": int(year),
            "pattern_type": pattern_type,
            "signal_count": int(len(group)),
            "avg_net_return": round(_safe_mean(group, "net_return"), 6),
            "stop_out_rate": round(float((group.get("exit_reason") == "stop").mean()), 4),
            "avg_r_multiple": round(_safe_mean(group, "r_multiple"), 6),
        }
        for horizon in config.event_horizons:
            row[f"avg_return_{horizon}d"] = round(_safe_mean(group, f"return_{horizon}d"), 6)
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["breakout_year", "pattern_type"]).reset_index(drop=True)


def render_pattern_event_chart(
    *,
    output_dir: str | Path,
    event_row: pd.Series | dict[str, Any],
    symbol_frame: pd.DataFrame,
    config: PatternBacktestConfig,
) -> str:
    """Render one pattern event chart and return the HTML path."""

    import plotly.graph_objects as go

    row = dict(event_row)
    base = Path(output_dir)
    charts_dir = base / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    pattern_start_index = int(row["pattern_start_index"])
    breakout_bar_index = int(row["breakout_bar_index"])
    start_idx = max(0, pattern_start_index - 10)
    end_idx = min(len(symbol_frame) - 1, breakout_bar_index + config.max_hold_bars)
    window = symbol_frame.iloc[start_idx : end_idx + 1].copy()
    if "smoothed_close" not in window.columns:
        window.loc[:, "smoothed_close"] = kernel_smooth(window["close"], bandwidth=config.bandwidth)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=window["timestamp"], y=window["close"], name="Close", mode="lines"))
    fig.add_trace(go.Scatter(x=window["timestamp"], y=window["smoothed_close"], name="Smoothed", mode="lines"))
    pivot_dates = json.loads(str(row["pivot_dates"]))
    pivot_prices = json.loads(str(row["pivot_prices"]))
    pivot_labels = json.loads(str(row["pivot_labels"]))
    fig.add_trace(
        go.Scatter(
            x=pivot_dates,
            y=pivot_prices,
            name="Pivots",
            mode="markers+text",
            text=pivot_labels,
            textposition="top center",
        )
    )
    fig.add_hline(y=float(row["breakout_level"]), line_dash="dash", annotation_text="Breakout")
    fig.add_hline(y=float(row["invalidation_price"]), line_dash="dot", annotation_text="Invalidation")
    fig.update_layout(
        title=f"{row['symbol_id']} {row['pattern_type']} {row['breakout_date']}",
        xaxis_title="Date",
        yaxis_title="Price",
        template="plotly_white",
    )
    chart_path = charts_dir / f"{row['event_id']}.html"
    fig.write_html(chart_path, include_plotlyjs="cdn")
    return str(chart_path)


def render_pattern_review(
    *,
    output_dir: str | Path,
    events_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    by_symbol: dict[str, pd.DataFrame],
    config: PatternBacktestConfig,
    render_all: bool = False,
) -> list[str]:
    """Render pattern review charts as sampled sets or all events."""

    base = Path(output_dir)
    charts_dir = base / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    if events_df.empty:
        return []

    scored = events_df.merge(
        trades_df[["event_id", "net_return"]] if not trades_df.empty else pd.DataFrame(columns=["event_id", "net_return"]),
        on="event_id",
        how="left",
    )
    if "net_return" not in scored.columns:
        scored["net_return"] = pd.to_numeric(scored.get("return_20d"), errors="coerce")

    paths: list[str] = []
    for pattern_type in ["cup_handle", "round_bottom"]:
        pattern_rows = scored[scored["pattern_type"] == pattern_type].copy()
        if pattern_rows.empty:
            continue
        if render_all:
            selected_rows = pattern_rows.sort_values(["breakout_date", "symbol_id"]).drop_duplicates(subset=["event_id"])
        else:
            ranked = pattern_rows.sort_values("net_return", ascending=False, na_position="last")
            selected_rows = pd.concat(
                [
                    ranked.head(config.sample_charts_per_pattern),
                    ranked.tail(config.sample_charts_per_pattern),
                ],
                ignore_index=True,
            ).drop_duplicates(subset=["event_id"])
        for _, row in selected_rows.iterrows():
            symbol_frame = by_symbol.get(str(row["symbol_id"]))
            if symbol_frame is None:
                continue
            paths.append(
                render_pattern_event_chart(
                    output_dir=base,
                    event_row=row,
                    symbol_frame=symbol_frame,
                    config=config,
                )
            )

    return sorted(paths)


def ensure_pattern_event_chart(
    *,
    project_root: str | Path,
    bundle_dir: str | Path,
    event_row: pd.Series | dict[str, Any],
    config: PatternBacktestConfig,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    research_frame: pd.DataFrame | None = None,
) -> str:
    """Ensure one event chart exists, generating it on demand when needed."""

    row = dict(event_row)
    chart_path = Path(bundle_dir) / "charts" / f"{row['event_id']}.html"
    if chart_path.exists():
        return str(chart_path)

    frame = research_frame
    if frame is None:
        frame = load_pattern_research_frame(
            project_root,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            symbols=[str(row["symbol_id"])],
        )
    symbol_frame = (
        frame[frame["symbol_id"].astype(str) == str(row["symbol_id"])]
        .sort_values("timestamp")
        .reset_index(drop=True)
        .copy()
    )
    if symbol_frame.empty:
        raise ValueError(f"No research data found for symbol {row['symbol_id']}")
    symbol_frame.loc[:, "smoothed_close"] = kernel_smooth(symbol_frame["close"], bandwidth=config.bandwidth)
    return render_pattern_event_chart(
        output_dir=bundle_dir,
        event_row=row,
        symbol_frame=symbol_frame,
        config=config,
    )


def run_pattern_backtest(
    *,
    project_root: str | Path,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    symbols: list[str] | tuple[str, ...] | None = None,
    config: PatternBacktestConfig | None = None,
    research_frame: pd.DataFrame | None = None,
    output_dir: str | Path | None = None,
    precompute_all_charts: bool = False,
) -> dict[str, Any]:
    """Run the full research-only pattern backtest workflow."""

    root = Path(project_root)
    active_config = config or PatternBacktestConfig(exchange=exchange, symbols=tuple(symbols or ()))
    frame = research_frame
    if frame is None:
        frame = load_pattern_research_frame(
            root,
            from_date=from_date,
            to_date=to_date,
            exchange=exchange,
            symbols=symbols,
        )
    events_df, stats, by_symbol = _scan_pattern_events(frame, config=active_config)
    events_df = _attach_event_study_metrics(events_df, by_symbol=by_symbol, config=active_config)
    trades_df = simulate_pattern_trades(events_df, by_symbol=by_symbol, config=active_config)
    summary_df = _build_summary_rows(
        events_df,
        trades_df,
        stats=stats,
        from_date=from_date,
        to_date=to_date,
        config=active_config,
    )
    yearly_df = _build_yearly_breakdown(events_df, trades_df, config=active_config)

    paths = ensure_domain_layout(project_root=root, data_domain="research")
    if output_dir is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = paths.reports_dir / "pattern_backtests" / f"pattern_backtest_{timestamp}"
    bundle_dir = Path(output_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    event_path = bundle_dir / "pattern_events.csv"
    trade_path = bundle_dir / "pattern_trades.csv"
    summary_path = bundle_dir / "summary.csv"
    yearly_path = bundle_dir / "yearly_breakdown.csv"
    json_path = bundle_dir / "summary.json"

    events_df.to_csv(event_path, index=False)
    trades_df.to_csv(trade_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    yearly_df.to_csv(yearly_path, index=False)
    chart_paths = render_pattern_review(
        output_dir=bundle_dir,
        events_df=events_df,
        trades_df=trades_df,
        by_symbol=by_symbol,
        config=active_config,
        render_all=precompute_all_charts,
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "from_date": from_date,
        "to_date": to_date,
        "exchange": exchange,
        "symbols": list(symbols or ()),
        "config": active_config.to_metadata(),
        "precompute_all_charts": bool(precompute_all_charts),
        "scan_stats": stats,
        "bundle_dir": str(bundle_dir),
        "artifacts": {
            "pattern_events": str(event_path),
            "pattern_trades": str(trade_path),
            "summary_csv": str(summary_path),
            "yearly_breakdown_csv": str(yearly_path),
            "charts": chart_paths,
        },
        "summary_rows": summary_df.to_dict(orient="records"),
        "yearly_rows": yearly_df.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "Pattern backtest complete bundle=%s events=%s trades=%s",
        bundle_dir,
        len(events_df),
        len(trades_df),
    )
    return {
        "events": events_df,
        "trades": trades_df,
        "summary": summary_df,
        "yearly_breakdown": yearly_df,
        "scan_stats": stats,
        "paths": {
            "bundle_dir": str(bundle_dir),
            "pattern_events": str(event_path),
            "pattern_trades": str(trade_path),
            "summary_csv": str(summary_path),
            "yearly_breakdown_csv": str(yearly_path),
            "summary_json": str(json_path),
            "charts": chart_paths,
        },
        "config": asdict(active_config),
    }
