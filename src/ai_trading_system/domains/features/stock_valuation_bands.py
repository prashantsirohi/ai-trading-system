"""Stock-level own-history valuation bands for PE, PS, and PB."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.features.valuation_schema import ensure_valuation_schema


WINDOW_3Y = 756
WINDOW_5Y = 1260
METRICS = ("pe", "ps", "pb")


@dataclass(frozen=True)
class StockValuationBandsResult:
    rows: int
    symbols: int
    start_date: str | None
    end_date: str | None
    latest_rows: int
    latest_date: str | None


def refresh_stock_valuation_bands(
    *,
    ohlcv_db_path: str | Path,
    from_date: str | None = None,
    to_date: str | None = None,
    universe_id: str = "UNIV_TOP1000_MCAP",
    min_history_days_3y: int = 504,
    min_history_days_5y: int = 756,
    output_csv: str | Path | None = None,
) -> StockValuationBandsResult:
    """Refresh stock valuation bands from `stock_valuation_daily`."""

    universe_id = str(universe_id or "UNIV_TOP1000_MCAP").strip().upper()
    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_valuation_schema(conn)
        source = _load_source(conn, universe_id=universe_id, to_date=to_date)
        if source.empty:
            _write_output(pd.DataFrame(), output_csv)
            return StockValuationBandsResult(0, 0, None, None, 0, None)
        bands = _build_bands(
            source,
            min_history_days_3y=int(min_history_days_3y),
            min_history_days_5y=int(min_history_days_5y),
        )
        if from_date:
            bands = bands.loc[pd.to_datetime(bands["date"]).dt.date >= pd.Timestamp(from_date).date()].copy()
        if to_date:
            bands = bands.loc[pd.to_datetime(bands["date"]).dt.date <= pd.Timestamp(to_date).date()].copy()
        if bands.empty:
            _write_output(pd.DataFrame(), output_csv)
            return StockValuationBandsResult(0, 0, None, None, 0, None)
        start = str(pd.to_datetime(bands["date"]).dt.date.min())
        end = str(pd.to_datetime(bands["date"]).dt.date.max())
        conn.execute(
            """
            DELETE FROM stock_valuation_bands
            WHERE universe_id = ?
              AND date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            [universe_id, start, end],
        )
        _insert_bands(conn, bands)
    finally:
        conn.close()

    latest_date = str(pd.to_datetime(bands["date"]).dt.date.max())
    latest = bands.loc[pd.to_datetime(bands["date"]).dt.date.astype(str).eq(latest_date)].copy()
    _write_output(latest, output_csv)
    return StockValuationBandsResult(
        rows=int(len(bands)),
        symbols=int(bands["symbol"].nunique()),
        start_date=start,
        end_date=end,
        latest_rows=int(len(latest)),
        latest_date=latest_date,
    )


def _load_source(conn: duckdb.DuckDBPyConnection, *, universe_id: str, to_date: str | None) -> pd.DataFrame:
    filters = ["universe_id = ?"]
    params: list[str] = [universe_id]
    if to_date:
        filters.append("date <= CAST(? AS DATE)")
        params.append(str(to_date)[:10])
    return conn.execute(
        f"""
        SELECT
            universe_id,
            date,
            symbol,
            sector_name,
            pe_ttm,
            ps_ttm,
            pb
        FROM stock_valuation_daily
        WHERE {' AND '.join(filters)}
        ORDER BY universe_id, symbol, date
        """,
        params,
    ).df()


def _build_bands(
    source: pd.DataFrame,
    *,
    min_history_days_3y: int,
    min_history_days_5y: int,
) -> pd.DataFrame:
    frame = source.copy()
    frame.loc[:, "date"] = pd.to_datetime(frame["date"]).dt.date
    for column in ("pe_ttm", "ps_ttm", "pb"):
        frame.loc[:, column] = pd.to_numeric(frame[column], errors="coerce")
        frame.loc[~frame[column].gt(0), column] = pd.NA
    frame = frame.sort_values(["universe_id", "symbol", "date"], kind="stable").reset_index(drop=True)
    output = []
    for _, group in frame.groupby(["universe_id", "symbol"], sort=False):
        output.append(
            _build_symbol_bands(
                group.reset_index(drop=True),
                min_history_days_3y=min_history_days_3y,
                min_history_days_5y=min_history_days_5y,
            )
        )
    bands = pd.concat(output, ignore_index=True) if output else pd.DataFrame()
    bands.loc[:, "created_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return bands[_columns()]


def _build_symbol_bands(
    group: pd.DataFrame,
    *,
    min_history_days_3y: int,
    min_history_days_5y: int,
) -> pd.DataFrame:
    out = group[["universe_id", "date", "symbol", "sector_name", "pe_ttm", "ps_ttm", "pb"]].copy()
    out.loc[:, "observations_3y"] = pd.Series(range(1, len(out) + 1), index=out.index).clip(upper=WINDOW_3Y)
    out.loc[:, "observations_5y"] = pd.Series(range(1, len(out) + 1), index=out.index).clip(upper=WINDOW_5Y)
    specs = {"pe": "pe_ttm", "ps": "ps_ttm", "pb": "pb"}
    for prefix, column in specs.items():
        values = pd.to_numeric(out[column], errors="coerce")
        out.loc[:, f"{prefix}_median_3y"] = values.rolling(WINDOW_3Y, min_periods=1).median()
        out.loc[:, f"{prefix}_median_5y"] = values.rolling(WINDOW_5Y, min_periods=1).median()
        out.loc[:, f"{prefix}_pctile_3y"] = values.rolling(WINDOW_3Y, min_periods=1).apply(_last_percentile, raw=False)
        out.loc[:, f"{prefix}_pctile_5y"] = values.rolling(WINDOW_5Y, min_periods=1).apply(_last_percentile, raw=False)
        out.loc[:, f"{prefix}_vs_3y_median_pct"] = _vs_median(values, out[f"{prefix}_median_3y"])
        out.loc[:, f"{prefix}_vs_5y_median_pct"] = _vs_median(values, out[f"{prefix}_median_5y"])
    scored = out.apply(
        lambda row: _score_row(
            row,
            min_history_days_3y=min_history_days_3y,
            min_history_days_5y=min_history_days_5y,
        ),
        axis=1,
        result_type="expand",
    )
    out.loc[:, "valuation_history_score"] = scored["score"]
    out.loc[:, "valuation_history_bucket"] = scored["bucket"]
    out.loc[:, "valuation_reason"] = scored["reason"]
    return out


def _last_percentile(values: pd.Series) -> float:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    valid = valid.loc[valid.gt(0)]
    if valid.empty:
        return float("nan")
    current = pd.to_numeric(pd.Series([values.iloc[-1]]), errors="coerce").iloc[0]
    if pd.isna(current) or current <= 0:
        return float("nan")
    return float(valid.le(current).mean() * 100.0)


def _vs_median(values: pd.Series, median: pd.Series) -> pd.Series:
    median = pd.to_numeric(median, errors="coerce")
    return ((values / median.where(median.gt(0))) - 1.0) * 100.0


def _score_row(row: pd.Series, *, min_history_days_3y: int, min_history_days_5y: int) -> dict[str, object]:
    if int(row.get("observations_5y") or 0) < min_history_days_5y and int(row.get("observations_3y") or 0) < min_history_days_3y:
        return {"score": 50.0, "bucket": "INSUFFICIENT_HISTORY", "reason": "Insufficient stock valuation history"}

    scores = {metric: _metric_score(row, metric) for metric in METRICS}
    pe_available = pd.notna(row.get("pe_ttm")) and float(row.get("pe_ttm")) > 0
    if pe_available:
        score = 0.40 * _neutral(scores["pe"]) + 0.35 * _neutral(scores["ps"]) + 0.25 * _neutral(scores["pb"])
    else:
        score = 0.60 * _neutral(scores["ps"]) + 0.40 * _neutral(scores["pb"])

    low = [_is_low(row, metric) for metric in METRICS]
    below = [_is_below_median(row, metric) for metric in METRICS]
    high = [_is_high(row, metric) for metric in METRICS]
    above = [_is_above_median(row, metric) for metric in METRICS]
    if sum(high) >= 2:
        bucket = "EXPENSIVE_VS_HISTORY"
    elif sum(low) >= 2:
        bucket = "DEEPLY_BELOW_HISTORY"
    elif sum(below) >= 2:
        bucket = "BELOW_OWN_MEDIAN"
    elif 45 <= score <= 65:
        bucket = "FAIR_VALUE"
    elif sum(above) >= 2:
        bucket = "ABOVE_OWN_MEDIAN"
    else:
        bucket = "FAIR_VALUE"
    return {"score": round(float(score), 2), "bucket": bucket, "reason": _valuation_reason(row, bucket)}


def _metric_score(row: pd.Series, metric: str) -> float | None:
    current = row.get("pe_ttm" if metric == "pe" else f"{metric}_ttm" if metric == "ps" else "pb")
    median = row.get(f"{metric}_median_5y")
    pctile = row.get(f"{metric}_pctile_5y")
    if pd.isna(current) or float(current) <= 0:
        return None
    current = float(current)
    median = float(median) if pd.notna(median) and float(median) > 0 else None
    pctile = float(pctile) if pd.notna(pctile) else None
    if (pctile is not None and pctile <= 20) or (median is not None and current <= 0.80 * median):
        return 85.0
    if median is not None and current <= median:
        return 70.0
    if median is not None and current <= 1.15 * median:
        return 55.0
    if (pctile is not None and pctile >= 80) or (median is not None and current >= 1.25 * median):
        return 25.0
    return 50.0


def _neutral(value: float | None) -> float:
    return 50.0 if value is None or pd.isna(value) else float(value)


def _metric_value(row: pd.Series, metric: str) -> float | None:
    column = "pe_ttm" if metric == "pe" else "ps_ttm" if metric == "ps" else "pb"
    value = row.get(column)
    return float(value) if pd.notna(value) and float(value) > 0 else None


def _is_low(row: pd.Series, metric: str) -> bool:
    current = _metric_value(row, metric)
    median = row.get(f"{metric}_median_5y")
    pctile = row.get(f"{metric}_pctile_5y")
    return current is not None and (
        (pd.notna(pctile) and float(pctile) <= 20)
        or (pd.notna(median) and float(median) > 0 and current <= 0.80 * float(median))
    )


def _is_below_median(row: pd.Series, metric: str) -> bool:
    current = _metric_value(row, metric)
    median = row.get(f"{metric}_median_5y")
    return current is not None and pd.notna(median) and float(median) > 0 and current <= float(median)


def _is_high(row: pd.Series, metric: str) -> bool:
    current = _metric_value(row, metric)
    median = row.get(f"{metric}_median_5y")
    pctile = row.get(f"{metric}_pctile_5y")
    return current is not None and (
        (pd.notna(pctile) and float(pctile) >= 80)
        or (pd.notna(median) and float(median) > 0 and current >= 1.25 * float(median))
    )


def _is_above_median(row: pd.Series, metric: str) -> bool:
    current = _metric_value(row, metric)
    median = row.get(f"{metric}_median_5y")
    return current is not None and pd.notna(median) and float(median) > 0 and current > float(median)


def _valuation_reason(row: pd.Series, bucket: str) -> str:
    if bucket == "INSUFFICIENT_HISTORY":
        return "Insufficient stock valuation history"
    low_metrics = [metric.upper() for metric in METRICS if _is_low(row, metric)]
    below_metrics = [metric.upper() for metric in METRICS if _is_below_median(row, metric)]
    high_metrics = [metric.upper() for metric in METRICS if _is_high(row, metric)]
    if bucket == "EXPENSIVE_VS_HISTORY" and high_metrics:
        return f"Expensive: {'/'.join(high_metrics)} above 80th percentile or 1.25x own 5Y median"
    if bucket == "DEEPLY_BELOW_HISTORY" and low_metrics:
        return f"{' and '.join(low_metrics[:2])} below 20th percentile or 0.80x own 5Y median"
    if bucket == "BELOW_OWN_MEDIAN" and below_metrics:
        return f"{'/'.join(below_metrics)} below own 5Y median"
    if bucket == "ABOVE_OWN_MEDIAN":
        return "At least two valuation metrics above own 5Y median"
    return "Fair versus own valuation history"


def _insert_bands(conn: duckdb.DuckDBPyConnection, bands: pd.DataFrame) -> None:
    columns = _columns()
    conn.register("_stock_valuation_bands_frame", bands[columns])
    try:
        conn.execute(
            f"""
            INSERT INTO stock_valuation_bands ({', '.join(columns)})
            SELECT {', '.join(columns)}
            FROM _stock_valuation_bands_frame
            """
        )
    finally:
        conn.unregister("_stock_valuation_bands_frame")


def _write_output(frame: pd.DataFrame, output_csv: str | Path | None) -> None:
    if output_csv is None:
        return
    path = Path(output_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _columns() -> list[str]:
    return [
        "universe_id",
        "date",
        "symbol",
        "sector_name",
        "pe_ttm",
        "ps_ttm",
        "pb",
        "pe_median_3y",
        "pe_median_5y",
        "ps_median_3y",
        "ps_median_5y",
        "pb_median_3y",
        "pb_median_5y",
        "pe_pctile_3y",
        "pe_pctile_5y",
        "ps_pctile_3y",
        "ps_pctile_5y",
        "pb_pctile_3y",
        "pb_pctile_5y",
        "pe_vs_3y_median_pct",
        "pe_vs_5y_median_pct",
        "ps_vs_3y_median_pct",
        "ps_vs_5y_median_pct",
        "pb_vs_3y_median_pct",
        "pb_vs_5y_median_pct",
        "valuation_history_score",
        "valuation_history_bucket",
        "valuation_reason",
        "observations_3y",
        "observations_5y",
        "created_at",
    ]


__all__ = ["StockValuationBandsResult", "refresh_stock_valuation_bands"]
