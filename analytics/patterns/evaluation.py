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

import numpy as np
import pandas as pd

from analytics.patterns.contracts import (
    PatternBacktestConfig,
    PatternEvent,
    PatternScanConfig,
    PatternTrade,
)
from analytics.patterns.data import load_pattern_frame, load_pattern_research_frame
from analytics.patterns.detectors import (
    PatternScanStats,
    detect_pattern_signals_for_symbol,
    detect_cup_handle_events,
    detect_round_bottom_events,
)
from analytics.patterns.signal import find_local_extrema, kernel_smooth
from utils.data_domains import ensure_domain_layout
from utils.logger import logger


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
        ordered = symbol_frame.sort_values("timestamp").reset_index(drop=True)
        if len(ordered) < config.min_history_bars:
            continue
        smoothed = kernel_smooth(
            ordered["close"],
            bandwidth=config.bandwidth,
            method=getattr(config, "smoothing_method", "kernel"),
        )
        ordered["smoothed_close"] = smoothed
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
        if len(symbol_frame) >= config.min_history_bars
    ]
    total_symbols = len(eligible_symbols)

    for idx, (symbol, symbol_frame) in enumerate(eligible_symbols, start=1):
        ordered = symbol_frame.sort_values("timestamp").reset_index(drop=True)
        smoothed = kernel_smooth(
            ordered["close"],
            bandwidth=config.bandwidth,
            method=getattr(config, "smoothing_method", "rolling"),
        )
        ordered["smoothed_close"] = smoothed
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
    ordered["smoothed_close"] = smoothed
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
    for symbol, symbol_frame in frame.groupby("symbol_id", sort=True):
        if len(symbol_frame) < config.min_history_bars:
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
) -> pd.DataFrame:
    """Build live bullish pattern signals for one domain and signal date."""

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
    if int(pattern_workers) > 1:
        signals_df, _, _ = _scan_pattern_signals_parallel(
            working_frame,
            config=active_config,
            pattern_workers=int(pattern_workers),
            progress_callback=progress_callback,
        )
    else:
        signals_df, _, _ = _scan_pattern_signals(
            working_frame,
            config=active_config,
            progress_callback=progress_callback,
        )
    if signals_df.empty:
        return signals_df

    if ranked_df is not None and not ranked_df.empty and "symbol_id" in ranked_df.columns:
        ctx_cols = [col for col in ["symbol_id", "rel_strength_score", "sector_rs_value"] if col in ranked_df.columns]
        ctx = ranked_df[ctx_cols].copy()
        ctx["symbol_id"] = ctx["symbol_id"].astype(str)
        if "rel_strength_score" not in ctx.columns:
            ctx["rel_strength_score"] = np.nan
        if "sector_rs_value" not in ctx.columns:
            ctx["sector_rs_value"] = np.nan
        ctx["sector_rs_percentile"] = (
            pd.to_numeric(ctx["sector_rs_value"], errors="coerce").rank(pct=True, method="average") * 100.0
        )
        signals_df = signals_df.drop(columns=["rel_strength_score", "sector_rs_percentile"], errors="ignore")
        signals_df = signals_df.merge(
            ctx[["symbol_id", "rel_strength_score", "sector_rs_percentile"]],
            on="symbol_id",
            how="left",
        )
        # Re-score after rank context is attached.
        signals_df = signals_df.drop(columns=["pattern_rank"], errors="ignore")
        from analytics.patterns.detectors import _score_signal_rows  # local import to avoid widening public surface

        signals_df = _score_signal_rows(signals_df)

    return signals_df.reset_index(drop=True)


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
    yearly["breakout_year"] = pd.to_datetime(yearly["breakout_date"]).dt.year
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
        window["smoothed_close"] = kernel_smooth(window["close"], bandwidth=config.bandwidth)

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
    symbol_frame = frame[frame["symbol_id"].astype(str) == str(row["symbol_id"])].sort_values("timestamp").reset_index(drop=True)
    if symbol_frame.empty:
        raise ValueError(f"No research data found for symbol {row['symbol_id']}")
    symbol_frame["smoothed_close"] = kernel_smooth(symbol_frame["close"], bandwidth=config.bandwidth)
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
