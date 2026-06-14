"""Accumulation/distribution signals from NSE delivery history."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.analytics.sector_rotation.contracts import (
    ACCUMULATION_LABEL,
    DISTRIBUTION_LABEL,
    NEUTRAL_LABEL,
)
from ai_trading_system.analytics.sector_rotation.custom_indices import (
    DATE_COLUMNS,
    EXCHANGE_COLUMNS,
    SYMBOL_COLUMNS,
    _duckdb_columns,
    _first_existing,
    _table_exists,
)


DELIVERY_COLUMNS = ("delivery_pct", "Delivery %", "deliverable_pct", "delivery_percentage", "deliv_per")


def compute_accumulation_distribution(
    ohlcv_db_path: str | Path,
    ohlcv: pd.DataFrame,
    *,
    run_date: str | None = None,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Return latest delivery behavior labels per symbol."""
    base_latest = _latest_base_rows(ohlcv, run_date=run_date)
    delivery = _load_delivery(ohlcv_db_path, run_date=run_date, exchange=exchange)
    if base_latest.empty:
        return _empty()
    if delivery.empty:
        neutral = base_latest.copy()
        neutral.loc[:, "delivery_pct"] = pd.NA
        neutral.loc[:, "delivery_qty"] = pd.NA
        neutral.loc[:, "delivery_value"] = pd.NA
        neutral.loc[:, "delivery_pct_z20"] = 0.0
        neutral.loc[:, "volume_z20"] = 0.0
        neutral.loc[:, "price_return_5d"] = 0.0
        neutral.loc[:, "delivery_signal"] = NEUTRAL_LABEL
        neutral.loc[:, "accumulation_score"] = 50.0
        return neutral[_output_columns()]

    history = ohlcv.merge(delivery, on=["symbol", "date"], how="left").sort_values(["symbol", "date"], kind="stable")
    history.loc[:, "delivery_pct"] = pd.to_numeric(history["delivery_pct"], errors="coerce")
    history.loc[:, "close"] = pd.to_numeric(history["close"], errors="coerce")
    history.loc[:, "volume"] = pd.to_numeric(history["volume"], errors="coerce")
    grouped = history.groupby("symbol", group_keys=False)
    delivery_mean = grouped["delivery_pct"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    delivery_std = grouped["delivery_pct"].rolling(20, min_periods=5).std().reset_index(level=0, drop=True)
    volume_mean = grouped["volume"].rolling(20, min_periods=5).mean().reset_index(level=0, drop=True)
    volume_std = grouped["volume"].rolling(20, min_periods=5).std().reset_index(level=0, drop=True)
    history.loc[:, "delivery_pct_z20"] = (history["delivery_pct"] - delivery_mean) / delivery_std.replace(0, pd.NA)
    history.loc[:, "volume_z20"] = (history["volume"] - volume_mean) / volume_std.replace(0, pd.NA)
    history.loc[:, "price_return_5d"] = grouped["close"].pct_change(5)
    history.loc[:, "delivery_qty"] = history["volume"] * history["delivery_pct"] / 100.0
    history.loc[:, "delivery_value"] = history["delivery_qty"] * history["close"]
    history.loc[:, "_return_rank_pct"] = history.groupby("date")["price_return_5d"].rank(pct=True).fillna(0.5)
    history.loc[:, "accumulation_score"] = (
        0.40 * history["delivery_pct_z20"].clip(-3, 3).fillna(0.0)
        + 0.30 * history["volume_z20"].clip(-3, 3).fillna(0.0)
        + 0.30 * history["_return_rank_pct"].fillna(0.5)
    )
    history.loc[:, "accumulation_score"] = (50.0 + 10.0 * history["accumulation_score"]).clip(0, 100)
    history.loc[:, "delivery_signal"] = NEUTRAL_LABEL
    accumulation_mask = (
        (history["delivery_pct_z20"] >= 1)
        & (history["price_return_5d"] >= 0)
        & (history["volume_z20"] >= 0)
    )
    distribution_mask = (history["delivery_pct_z20"] >= 1) & (history["price_return_5d"] < 0)
    history.loc[accumulation_mask, "delivery_signal"] = ACCUMULATION_LABEL
    history.loc[distribution_mask, "delivery_signal"] = DISTRIBUTION_LABEL
    latest = _latest_base_rows(history, run_date=run_date)
    for column in ("delivery_pct_z20", "volume_z20", "price_return_5d"):
        latest.loc[:, column] = pd.to_numeric(latest[column], errors="coerce").fillna(0.0)
    latest.loc[:, "accumulation_score"] = pd.to_numeric(latest["accumulation_score"], errors="coerce").fillna(50.0)
    latest.loc[:, "delivery_signal"] = latest["delivery_signal"].fillna(NEUTRAL_LABEL)
    return latest[_output_columns()]


def _load_delivery(
    ohlcv_db_path: str | Path,
    *,
    run_date: str | None,
    exchange: str,
) -> pd.DataFrame:
    db_path = Path(ohlcv_db_path)
    if not db_path.exists():
        return pd.DataFrame(columns=["symbol", "date", "delivery_pct"])
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "_delivery"):
            return pd.DataFrame(columns=["symbol", "date", "delivery_pct"])
        columns = _duckdb_columns(conn, "_delivery")
        symbol_col = _first_existing(columns, SYMBOL_COLUMNS)
        date_col = _first_existing(columns, DATE_COLUMNS)
        delivery_col = _first_existing(columns, DELIVERY_COLUMNS)
        exchange_col = _first_existing(columns, EXCHANGE_COLUMNS)
        if not symbol_col or not date_col or not delivery_col:
            return pd.DataFrame(columns=["symbol", "date", "delivery_pct"])
        filters = []
        params: list[object] = []
        if run_date:
            filters.append(f'CAST("{date_col}" AS DATE) <= CAST(? AS DATE)')
            params.append(run_date)
        if exchange_col and exchange:
            filters.append(f'("{exchange_col}" IS NULL OR "{exchange_col}" = ?)')
            params.append(exchange)
        where = f" WHERE {' AND '.join(filters)}" if filters else ""
        frame = conn.execute(
            f"""
            SELECT "{symbol_col}" AS symbol,
                   CAST("{date_col}" AS DATE) AS date,
                   CAST("{delivery_col}" AS DOUBLE) AS delivery_pct
            FROM _delivery{where}
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "date", "delivery_pct"])
    frame.loc[:, "symbol"] = frame["symbol"].astype(str).str.strip()
    frame.loc[:, "date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame.loc[:, "delivery_pct"] = pd.to_numeric(frame["delivery_pct"], errors="coerce")
    return frame.dropna(subset=["symbol", "date"])


def _latest_base_rows(frame: pd.DataFrame, *, run_date: str | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    data.loc[:, "date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    if run_date:
        data = data.loc[data["date"] <= pd.Timestamp(run_date).normalize()].copy()
    return data.sort_values(["symbol", "date"], kind="stable").drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


def _output_columns() -> list[str]:
    return [
        "symbol",
        "date",
        "close",
        "volume",
        "delivery_pct",
        "delivery_qty",
        "delivery_value",
        "delivery_pct_z20",
        "volume_z20",
        "price_return_5d",
        "delivery_signal",
        "accumulation_score",
    ]


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=_output_columns())
