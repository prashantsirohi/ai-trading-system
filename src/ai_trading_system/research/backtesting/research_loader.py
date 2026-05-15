"""Dynamic backtest loader for the research OHLCV DuckDB store."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.domains.ranking.composite import compute_factor_scores, load_factor_weights
from ai_trading_system.domains.ranking.factors import (
    apply_delivery,
    apply_momentum_acceleration,
    apply_proximity_highs,
    apply_relative_strength,
    apply_trend_persistence,
    apply_volume_intensity,
    compute_penalty_score,
)
from ai_trading_system.domains.ranking.contracts import (
    STAGE2_FRESH_BARS_MAX,
    STAGE2_FRESHNESS_BONUS,
    STAGE2_MID_BARS_MAX,
    STAGE2_MID_FRESHNESS_BONUS,
    STAGE2_TRANSITION_BONUS,
    STAGE2_TRANSITION_BONUS_BARS_MAX,
)
from ai_trading_system.platform.db.paths import ensure_domain_layout


RANKING_METHOD_VERSION = "research_dynamic_v3_canonical_factor_scoring_stage2_benchmark"
# Weight of the benchmark-relative excess-return blend in `rel_strength`.
# Was NIFTY_RS_BLEND=0.4; renamed and tuned down when the default benchmark
# switched to UNIV_TOP1000 (a top-1000 PIT universe index).
RS_BENCHMARK_BLEND = 0.35
# Backwards-compat alias for any external consumer that still imports the
# legacy name. Remove after one release.
NIFTY_RS_BLEND = RS_BENCHMARK_BLEND
DEFAULT_BENCHMARK_SYMBOL = "UNIV_TOP1000"


def load_research_ranked_by_date(
    project_root: Path | str,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    exchange: str = "NSE",
    symbols: list[str] | None = None,
    warmup_days: int = 420,
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    benchmark_source: str = "index_catalog",
    weekly_stage_gate: bool = False,
    weights_override: dict[str, float] | None = None,
) -> dict[date, pd.DataFrame]:
    """Load research OHLCV and compute engine-ready ranked frames per date.

    This intentionally returns the same shape as ``pipeline_loader``:
    ``dict[date, ranked_df]``. The runner does not care whether those frames came
    from saved pipeline CSVs or from dynamic research calculations.
    """
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    if not paths.ohlcv_db_path.exists():
        return {}

    end = to_date or date.today()
    start = from_date or (end - timedelta(days=365))
    load_start = start - timedelta(days=warmup_days)
    symbol_filter = {s.strip().upper() for s in symbols or [] if s.strip()}
    load_symbols: set[str] | None = None
    if symbol_filter:
        load_symbols = set(symbol_filter)
        if benchmark_symbol:
            load_symbols.add(benchmark_symbol.strip().upper())

    df = _load_ohlcv(
        paths.ohlcv_db_path,
        exchange=exchange,
        from_date=load_start,
        to_date=end,
        symbols=sorted(load_symbols) if load_symbols else None,
    )
    if df.empty:
        return {}

    # Optionally append a benchmark series so downstream RS blending sees it
    # as a symbol_id row. ``benchmark_source='index_catalog'`` loads from
    # ``_index_catalog`` (preferred for UNIV_TOP1000, NIFTY_*, ...). If absent
    # or empty, the loader silently skips — RS blend then no-ops.
    if benchmark_symbol and benchmark_source == "index_catalog":
        bench_df = _load_benchmark_from_index_catalog(
            paths.ohlcv_db_path,
            index_code=benchmark_symbol,
            from_date=load_start,
            to_date=end,
            exchange=exchange,
        )
        if not bench_df.empty:
            df = pd.concat([df, bench_df], ignore_index=True)

    sectors = _load_sector_map(_research_master_db_path(paths))
    weekly_snapshots = _load_weekly_stage_history(paths.ohlcv_db_path)
    ranked = _compute_ranked_frame(
        df,
        sectors=sectors,
        benchmark_symbol=benchmark_symbol,
        weekly_snapshots=weekly_snapshots,
        weekly_stage_gate=weekly_stage_gate,
        weights_override=weights_override,
    )
    ranked = ranked[(ranked["date"] >= pd.Timestamp(start)) & (ranked["date"] <= pd.Timestamp(end))]
    if ranked.empty:
        return {}

    out: dict[date, pd.DataFrame] = {}
    for ts, group in ranked.groupby("date", sort=True):
        day = ts.date() if hasattr(ts, "date") else pd.to_datetime(ts).date()
        frame = group.drop(columns=["date"]).sort_values("eligible_rank", kind="stable").reset_index(drop=True)
        if not frame.empty:
            out[day] = frame
    return out


def validate_research_dynamic_data(
    project_root: Path | str,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    exchange: str = "NSE",
    warmup_days: int = 420,
) -> dict[str, object]:
    """Return lightweight data-quality facts for research dynamic backtests."""
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    end = to_date or date.today()
    start = from_date or (end - timedelta(days=365))
    load_start = start - timedelta(days=warmup_days)
    checks = {
        "status": "ok",
        "exchange": exchange,
        "from_date": start.isoformat(),
        "to_date": end.isoformat(),
        "warmup_from_date": load_start.isoformat(),
        "warnings": [],
    }
    warnings: list[str] = []
    if not paths.ohlcv_db_path.exists():
        checks.update({"status": "missing_db", "warnings": ["research_ohlcv_missing"]})
        return checks

    conn = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if "_catalog" not in tables:
            checks.update({"status": "missing_catalog", "warnings": ["catalog_table_missing"]})
            return checks
        params = [exchange, load_start, end]
        stats = conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT symbol_id) AS symbol_count,
                MIN(CAST(timestamp AS DATE)) AS min_date,
                MAX(CAST(timestamp AS DATE)) AS max_date,
                SUM(CASE WHEN close IS NULL OR high IS NULL OR low IS NULL THEN 1 ELSE 0 END) AS missing_ohlcv_rows
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) >= ?
              AND CAST(timestamp AS DATE) <= ?
            """,
            params,
        ).fetchone()
        duplicate_timestamp_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT symbol_id, exchange, timestamp
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) >= ?
                  AND CAST(timestamp AS DATE) <= ?
                GROUP BY symbol_id, exchange, timestamp
                HAVING COUNT(*) > 1
            )
            """,
            params,
        ).fetchone()[0]
        duplicate_daily_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT symbol_id, exchange, CAST(timestamp AS DATE) AS d
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) >= ?
                  AND CAST(timestamp AS DATE) <= ?
                GROUP BY symbol_id, exchange, CAST(timestamp AS DATE)
                HAVING COUNT(*) > 1
            )
            """,
            params,
        ).fetchone()[0]
        insufficient_sma200_count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT symbol_id
                FROM _catalog
                WHERE exchange = ?
                  AND CAST(timestamp AS DATE) >= ?
                  AND CAST(timestamp AS DATE) <= ?
                  AND close IS NOT NULL
                GROUP BY symbol_id
                HAVING COUNT(DISTINCT CAST(timestamp AS DATE)) < 200
            )
            """,
            params,
        ).fetchone()[0]
    finally:
        conn.close()

    row_count = int(stats[0] or 0)
    missing_rows = int(stats[4] or 0)
    if row_count == 0:
        warnings.append("no_rows_in_requested_window")
    if missing_rows:
        warnings.append("missing_ohlcv_values")
    if duplicate_timestamp_count:
        warnings.append("duplicate_symbol_exchange_timestamp")
    if duplicate_daily_count:
        warnings.append("multiple_rows_per_symbol_day")
    if insufficient_sma200_count:
        warnings.append("symbols_with_insufficient_sma200_history")

    checks.update(
        {
            "status": "warning" if warnings else "ok",
            "row_count": row_count,
            "symbol_count": int(stats[1] or 0),
            "min_date": str(stats[2]) if stats[2] else None,
            "max_date": str(stats[3]) if stats[3] else None,
            "missing_ohlcv_rows": missing_rows,
            "duplicate_timestamp_count": int(duplicate_timestamp_count or 0),
            "duplicate_daily_count": int(duplicate_daily_count or 0),
            "insufficient_sma200_symbol_count": int(insufficient_sma200_count or 0),
            "masterdata_path": str(_research_master_db_path(paths)),
            "masterdata_exists": _research_master_db_path(paths).exists(),
            "warnings": warnings,
        }
    )
    return checks


def _load_ohlcv(
    db_path: Path,
    *,
    exchange: str,
    from_date: date,
    to_date: date,
    symbols: list[str] | None,
) -> pd.DataFrame:
    clauses = ["exchange = ?", "CAST(timestamp AS DATE) >= ?", "CAST(timestamp AS DATE) <= ?"]
    params: list[object] = [exchange, from_date, to_date]
    if symbols:
        clauses.append("symbol_id IN (SELECT UNNEST(?))")
        params.append(symbols)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        try:
            return conn.execute(
                f"""
                SELECT
                    symbol_id,
                    exchange,
                    CAST(timestamp AS DATE) AS date,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM _catalog
                WHERE {' AND '.join(clauses)}
                  AND close IS NOT NULL
                  AND high IS NOT NULL
                  AND low IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange, CAST(timestamp AS DATE)
                    ORDER BY timestamp DESC
                ) = 1
                ORDER BY symbol_id, date
                """,
                params,
            ).fetchdf()
        except duckdb.Error:
            return pd.DataFrame()
    finally:
        conn.close()


def _load_benchmark_from_index_catalog(
    db_path: Path,
    *,
    index_code: str,
    from_date: date,
    to_date: date,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Load a benchmark series from ``_index_catalog`` reshaped as if it were
    a stock OHLCV row in ``_catalog``. Returns empty frame when the table is
    absent (older DBs) or the index_code has no data in range.
    """
    if not db_path.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if "_index_catalog" not in tables:
            return pd.DataFrame()
        df = conn.execute(
            """
            SELECT
                ? AS symbol_id,
                ? AS exchange,
                date,
                COALESCE(open, close) AS open,
                COALESCE(high, close) AS high,
                COALESCE(low, close) AS low,
                close,
                COALESCE(volume, 0) AS volume
              FROM _index_catalog
             WHERE index_code = ?
               AND date BETWEEN ? AND ?
               AND close IS NOT NULL
             ORDER BY date
            """,
            [index_code, exchange, index_code, from_date, to_date],
        ).fetchdf()
        return df
    except duckdb.Error:
        return pd.DataFrame()
    finally:
        conn.close()


def _research_master_db_path(paths) -> Path:
    research_master = paths.root_dir / "masterdata.db"
    return research_master if research_master.exists() else paths.master_db_path


def _load_sector_map(master_db_path: Path) -> dict[str, str]:
    if not master_db_path.exists():
        return {}
    conn = sqlite3.connect(str(master_db_path))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "stock_details" not in tables:
            return {}
        rows = conn.execute("SELECT Symbol, Sector FROM stock_details").fetchall()
        return {str(symbol).strip().upper(): str(sector or "UNKNOWN") for symbol, sector in rows}
    finally:
        conn.close()


def _compute_ranked_frame(
    df: pd.DataFrame,
    *,
    sectors: dict[str, str],
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    weekly_snapshots: pd.DataFrame | None = None,
    weekly_stage_gate: bool = False,
    weights_override: dict[str, float] | None = None,
) -> pd.DataFrame:
    # Pandas 2.x emits Copy-on-Write preview warnings on column assignments
    # in this hot path. They are cosmetic — pandas 3.0 hasn't shipped yet
    # and behaviour is unchanged today. Silence inside this function only;
    # callers outside the loader still see warnings if they hit them.
    import warnings as _warnings

    _ctx = _warnings.catch_warnings()
    _ctx.__enter__()
    _warnings.filterwarnings("ignore", category=FutureWarning)
    try:
        return _compute_ranked_frame_impl(
            df,
            sectors=sectors,
            benchmark_symbol=benchmark_symbol,
            weekly_snapshots=weekly_snapshots,
            weekly_stage_gate=weekly_stage_gate,
            weights_override=weights_override,
        )
    finally:
        _ctx.__exit__(None, None, None)


def _compute_ranked_frame_impl(
    df: pd.DataFrame,
    *,
    sectors: dict[str, str],
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    weekly_snapshots: pd.DataFrame | None = None,
    weekly_stage_gate: bool = False,
    weights_override: dict[str, float] | None = None,
) -> pd.DataFrame:
    data = df.reset_index(drop=True).copy(deep=True)
    data.loc[:, "date"] = pd.to_datetime(data["date"])
    data = data.sort_values(["symbol_id", "date"], kind="stable").reset_index(drop=True)
    grouped = data.groupby("symbol_id", group_keys=False)

    data["timestamp"] = data["date"]
    data["sma_11"] = grouped["close"].transform(lambda s: s.rolling(11, min_periods=11).mean())
    data["sma_20"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    data["sma_50"] = grouped["close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    data["sma_200"] = grouped["close"].transform(lambda s: s.rolling(200, min_periods=200).mean())
    data["ema_20"] = grouped["close"].transform(lambda s: s.ewm(span=20, adjust=False, min_periods=20).mean())
    volume_avg_20 = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    volume_std_20 = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).std())
    data["vol_20_avg"] = volume_avg_20
    data["volume_zscore_20"] = (data["volume"] - volume_avg_20) / volume_std_20.replace(0, pd.NA)
    data["volume_ratio_20"] = data["volume"] / volume_avg_20.replace(0, pd.NA)
    data["swing_low_20"] = grouped["low"].transform(lambda s: s.rolling(20, min_periods=20).min())
    data["high_52w"] = grouped["high"].transform(lambda s: s.rolling(252, min_periods=1).max())
    data["recent_high_50"] = grouped["high"].transform(lambda s: s.rolling(50, min_periods=20).max())
    data["drawdown_from_recent_high_pct"] = (
        (data["recent_high_50"] - data["close"]) / data["recent_high_50"].replace(0, pd.NA) * 100.0
    )
    data["sma50_rising_20d"] = data["sma_50"] > data.groupby("symbol_id")["sma_50"].shift(20)
    below_ema20 = data["close"] < data["ema_20"]
    data["below_ema20_days_20"] = below_ema20.groupby(data["symbol_id"]).transform(
        lambda s: s.rolling(20, min_periods=20).sum()
    )
    data["prox_lookback_days"] = _cumcount_sorted_symbols(data["symbol_id"]) + 1

    prev_close = grouped["close"].shift(1)
    tr_components = pd.concat(
        [
            data["high"] - data["low"],
            (data["high"] - prev_close).abs(),
            (data["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    data["true_range"] = tr_components.max(axis=1)
    data["atr_14"] = data.groupby("symbol_id")["true_range"].transform(
        lambda s: s.rolling(14, min_periods=14).mean()
    )
    data["return_60d"] = grouped["close"].pct_change(60)
    data["return_5"] = grouped["close"].pct_change(5).fillna(0.0) * 100.0
    data["return_10"] = grouped["close"].pct_change(10).fillna(0.0) * 100.0
    data["return_20"] = grouped["close"].pct_change(20).fillna(0.0) * 100.0
    data["return_50"] = grouped["close"].pct_change(50).fillna(0.0) * 100.0
    data["return_60"] = grouped["close"].pct_change(60).fillna(0.0) * 100.0
    data["return_120"] = grouped["close"].pct_change(120).fillna(0.0) * 100.0
    data = _blend_benchmark_relative_rs(data, benchmark_symbol=benchmark_symbol)
    benchmark_upper = (benchmark_symbol or "").strip().upper()
    if benchmark_upper:
        data = data[data["symbol_id"].astype(str).str.upper() != benchmark_upper].copy()
    data["is_stage2_uptrend"] = (
        (data["close"] > data["sma_50"])
        & (data["sma_50"] > data["sma_200"])
        & (data["sma_200"] > 0)
    )
    data["stage2_score"] = data["is_stage2_uptrend"].astype(float) * 100.0
    data["is_stage2_structural"] = data["is_stage2_uptrend"]
    data["stage2_score_bonus"] = 0.0
    data["sector_name"] = data["symbol_id"].astype(str).str.upper().map(sectors).fillna("UNKNOWN")

    data = _attach_weekly_stage_context(data, weekly_snapshots)
    if weekly_stage_gate:
        data = _filter_weekly_stage_s2(data)
    data = _apply_stage2_age_bonuses(data)

    sector_return = data.groupby(["date", "sector_name"])["return_60"].transform("median")
    data["sector_rs_value"] = (
        sector_return.groupby(data["date"]).rank(pct=True).fillna(0.5)
    )
    data["stock_vs_sector_value"] = (data["return_60"] - sector_return).fillna(0.0)

    scored_frames = [
        _score_dynamic_rank_day(group, weights_override=weights_override)
        for _, group in data.groupby("date", sort=True)
    ]
    scored = pd.concat(scored_frames, ignore_index=True) if scored_frames else pd.DataFrame()

    output_columns = [
            "date",
            "symbol_id",
            "exchange",
            "open",
            "high",
            "low",
            "close",
            "composite_score",
            "composite_score_adjusted",
            "eligible_rank",
            "rejection_reasons",
            "penalty_score",
            "is_stage2_uptrend",
            "is_stage2_structural",
            "stage2_score",
            "stage2_freshness_bonus",
            "stage2_transition_bonus",
            "stage2_age_warning",
            "weekly_stage_label",
            "weekly_stage_confidence",
            "weekly_stage_transition",
            "bars_in_stage",
            "stage_entry_date",
            "sector_name",
            "sector_strength_score",
            "sma_11",
            "sma_20",
            "sma_50",
            "sma_200",
            "ema_20",
            "atr_14",
            "volume_ratio_20",
            "swing_low_20",
            "high_52w",
            "recent_high_50",
            "drawdown_from_recent_high_pct",
            "sma50_rising_20d",
            "below_ema20_days_20",
            "volume",
            "timestamp",
            "return_5",
            "return_10",
            "return_20",
            "return_50",
            "return_60",
            "return_120",
            "rs_vs_nifty_5",
            "rs_vs_nifty_10",
            "rs_vs_nifty_20",
            "rs_vs_nifty_60",
            "rs_vs_nifty_120",
            "rs_vs_nifty_score",
            "rel_strength",
            "rel_strength_score",
            "vol_intensity",
            "volume_intensity_normalized",
            "vol_intensity_score",
            "trend_score",
            "trend_score_score",
            "momentum_acceleration",
            "momentum_acceleration_score",
            "prox_high",
            "prox_high_score",
            "delivery_pct",
            "delivery_pct_score",
            "sector_rs_value",
            "stock_vs_sector_value",
            "prox_lookback_days",
            "volume_zscore_20",
            "exhaustion_penalty",
            "exhaustion_flag",
            "pivot_distance_penalty",
            "distance_from_pivot_atr",
    ]
    for column in output_columns:
        if column not in scored.columns:
            scored[column] = pd.NA
    return scored[output_columns].dropna(subset=["close"]).reset_index(drop=True)


def _score_dynamic_rank_day(
    day_frame: pd.DataFrame,
    *,
    weights_override: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Score one historical day through canonical ranking factor functions."""
    scores = day_frame.copy(deep=True).reset_index(drop=True)
    empty = pd.DataFrame()
    scores = apply_relative_strength(scores, return_frame=empty)
    scores = _apply_benchmark_rs_blend(scores)
    scores = apply_momentum_acceleration(scores)
    scores = apply_volume_intensity(scores, volume_frame=empty)
    scores = apply_trend_persistence(scores, adx_frame=empty, sma_frame=empty)
    scores = apply_proximity_highs(scores, highs_frame=empty)
    scores = apply_delivery(scores, delivery_frame=empty)
    weights = weights_override if weights_override is not None else load_factor_weights()
    scores = compute_factor_scores(scores, weights=weights)
    scores = compute_penalty_score(scores)
    adjusted = (
        scores["composite_score"]
        + scores.get("stage2_score_bonus", 0.0)
        + scores.get("stage2_freshness_bonus", 0.0)
        + scores.get("stage2_transition_bonus", 0.0)
        - scores["penalty_score"].fillna(0.0)
    ).clip(0.0, 100.0)
    scores = scores.assign(composite_score_adjusted=adjusted)
    scores = scores.sort_values("composite_score_adjusted", ascending=False, kind="stable").reset_index(drop=True)
    scores["eligible_rank"] = scores.index + 1
    scores["rejection_reasons"] = ""
    return scores


def _cumcount_sorted_symbols(symbols: pd.Series) -> np.ndarray:
    """Return a zero-based per-symbol counter for data sorted by symbol/date."""
    values = symbols.astype(str).to_numpy()
    if len(values) == 0:
        return np.array([], dtype=int)
    changes = np.empty(len(values), dtype=bool)
    changes[0] = True
    changes[1:] = values[1:] != values[:-1]
    starts = np.flatnonzero(changes)
    lengths = np.diff(np.append(starts, len(values)))
    return np.concatenate([np.arange(length, dtype=int) for length in lengths])


def _blend_benchmark_relative_rs(data: pd.DataFrame, *, benchmark_symbol: str) -> pd.DataFrame:
    benchmark_upper = (benchmark_symbol or "").strip().upper()
    if not benchmark_upper or data.empty:
        return data.copy()
    output = data.copy()
    benchmark = output[output["symbol_id"].astype(str).str.upper() == benchmark_upper]
    if benchmark.empty:
        return output
    benchmark_by_date = benchmark.set_index("date")
    for period in (5, 10, 20, 60, 120):
        ret_col = f"return_{period}"
        rs_col = f"rs_vs_nifty_{period}"
        if ret_col not in output.columns:
            continue
        bench_returns = benchmark_by_date[ret_col]
        output[rs_col] = output[ret_col] - output["date"].map(bench_returns).fillna(0.0)
    return output


def _apply_benchmark_rs_blend(scores: pd.DataFrame) -> pd.DataFrame:
    rs_cols = [f"rs_vs_nifty_{period}" for period in (5, 10, 20, 60, 120) if f"rs_vs_nifty_{period}" in scores.columns]
    if not rs_cols or "rel_strength" not in scores.columns:
        return scores.copy()
    output = scores.copy()
    blended = output[rs_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).mean(axis=1)
    nifty_rs_score = blended.rank(pct=True) * 100.0
    output["rs_vs_nifty_score"] = nifty_rs_score
    output["rel_strength"] = (
        pd.to_numeric(output["rel_strength"], errors="coerce").fillna(0.0) * (1.0 - NIFTY_RS_BLEND)
        + nifty_rs_score * NIFTY_RS_BLEND
    )
    return output


def _load_weekly_stage_history(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if "weekly_stage_snapshot" not in tables:
            return pd.DataFrame()
        return conn.execute(
            """
            SELECT
                symbol AS symbol_id,
                CAST(week_end_date AS DATE) AS week_end_date,
                stage_label AS weekly_stage_label,
                stage_confidence AS weekly_stage_confidence,
                stage_transition AS weekly_stage_transition,
                bars_in_stage,
                stage_entry_date
            FROM weekly_stage_snapshot
            ORDER BY symbol_id, week_end_date
            """
        ).fetchdf()
    except duckdb.Error:
        return pd.DataFrame()
    finally:
        conn.close()


def _attach_weekly_stage_context(data: pd.DataFrame, weekly_snapshots: pd.DataFrame | None) -> pd.DataFrame:
    output = data.copy()
    default_columns = {
        "weekly_stage_label": pd.NA,
        "weekly_stage_confidence": pd.NA,
        "weekly_stage_transition": pd.NA,
        "bars_in_stage": pd.NA,
        "stage_entry_date": pd.NaT,
    }
    if weekly_snapshots is None or weekly_snapshots.empty:
        for column, value in default_columns.items():
            output[column] = value
        return output

    snapshots = weekly_snapshots.copy()
    snapshots["week_end_date"] = pd.to_datetime(snapshots["week_end_date"])
    merged_parts: list[pd.DataFrame] = []
    snapshot_by_symbol = {str(symbol): group.sort_values("week_end_date") for symbol, group in snapshots.groupby("symbol_id")}
    for symbol, group in output.groupby("symbol_id", sort=False):
        snap = snapshot_by_symbol.get(str(symbol))
        if snap is None or snap.empty:
            part = group.copy()
            for column, value in default_columns.items():
                part[column] = value
        else:
            part = pd.merge_asof(
                group.sort_values("date"),
                snap,
                left_on="date",
                right_on="week_end_date",
                by="symbol_id",
                direction="backward",
            ).drop(columns=["week_end_date"], errors="ignore")
        merged_parts.append(part)
    return pd.concat(merged_parts, ignore_index=True) if merged_parts else output


def _filter_weekly_stage_s2(data: pd.DataFrame) -> pd.DataFrame:
    if "weekly_stage_label" not in data.columns:
        return data.copy()
    label = data["weekly_stage_label"]
    confidence = pd.to_numeric(data.get("weekly_stage_confidence", pd.Series(1.0, index=data.index)), errors="coerce").fillna(0.0)
    has_snapshot = label.notna()
    passes = ~has_snapshot | (label.astype(str).eq("S2") & (confidence >= 0.6))
    return data[passes].copy()


def _apply_stage2_age_bonuses(data: pd.DataFrame) -> pd.DataFrame:
    output = data.copy()
    output["stage2_freshness_bonus"] = 0.0
    output["stage2_transition_bonus"] = 0.0
    output["stage2_age_warning"] = ""
    if output.empty or "weekly_stage_label" not in output.columns:
        return output

    bars = pd.to_numeric(output.get("bars_in_stage", pd.Series(pd.NA, index=output.index)), errors="coerce")
    weekly_s2 = output["weekly_stage_label"].astype(str).eq("S2")
    fresh = weekly_s2 & bars.notna() & (bars <= STAGE2_FRESH_BARS_MAX)
    mid = weekly_s2 & bars.notna() & (bars > STAGE2_FRESH_BARS_MAX) & (bars <= STAGE2_MID_BARS_MAX)
    mature = weekly_s2 & bars.notna() & (bars >= STAGE2_MID_BARS_MAX + 1)
    output.loc[fresh, "stage2_freshness_bonus"] = STAGE2_FRESHNESS_BONUS
    output.loc[mid, "stage2_freshness_bonus"] = STAGE2_MID_FRESHNESS_BONUS
    output.loc[mature, "stage2_age_warning"] = "mature_stage2"

    transition = output.get("weekly_stage_transition", pd.Series("", index=output.index)).astype(str)
    recent_transition = transition.eq("S1_TO_S2") & bars.notna() & (bars <= STAGE2_TRANSITION_BONUS_BARS_MAX)
    output.loc[recent_transition, "stage2_transition_bonus"] = STAGE2_TRANSITION_BONUS
    return output
