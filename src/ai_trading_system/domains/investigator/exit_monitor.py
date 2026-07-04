"""Operator-only exit monitoring for final investigator gate rows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


MONITORING_COLUMNS = [
    "gate_entry_date",
    "days_since_gate_entry",
    "latest_close",
    "invalidation_breached",
    "followthrough_status",
    "exit_triggered",
    "exit_reason",
]


def attach_exit_monitoring(
    final_gate: pd.DataFrame,
    *,
    ohlcv_db_path: str | Path,
    registry_conn: duckdb.DuckDBPyConnection | None = None,
    as_of: str | None = None,
) -> pd.DataFrame:
    """Attach advisory exit-monitoring fields to final-gate rows."""
    if final_gate is None or final_gate.empty:
        out = final_gate.copy() if isinstance(final_gate, pd.DataFrame) else pd.DataFrame()
        for column in MONITORING_COLUMNS:
            if column not in out.columns:
                out.loc[:, column] = pd.Series(dtype=object)
        return out

    out = final_gate.copy()
    out.loc[:, "symbol_id"] = out["symbol_id"].fillna("").astype(str).str.strip().str.upper()
    out = out.assign(
        trade_date=pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d").astype("object")
    )
    entry_dates = _load_gate_entry_dates(out, registry_conn)
    out.loc[:, "gate_entry_date"] = [
        entry_dates.get((str(row.symbol_id).upper(), str(row.trade_date)), row.trade_date)
        for row in out[["symbol_id", "trade_date"]].itertuples(index=False)
    ]

    prices = _load_prices(ohlcv_db_path, out["symbol_id"].dropna().astype(str).unique().tolist(), as_of=as_of)
    rows: list[dict[str, Any]] = []
    for row in out.to_dict(orient="records"):
        rows.append(_monitor_row(row, prices))
    monitoring = pd.DataFrame(rows, index=out.index)
    for column in MONITORING_COLUMNS:
        out.loc[:, column] = monitoring[column] if column in monitoring.columns else pd.NA
    return out


def _load_gate_entry_dates(
    final_gate: pd.DataFrame,
    registry_conn: duckdb.DuckDBPyConnection | None,
) -> dict[tuple[str, str], str]:
    if registry_conn is None or final_gate.empty:
        return {}
    symbols = sorted(final_gate["symbol_id"].dropna().astype(str).str.upper().unique().tolist())
    if not symbols:
        return {}
    placeholders = ", ".join(["?"] * len(symbols))
    out: dict[tuple[str, str], str] = {}
    for table in ("investigator_final_gate", "investigator_cohort_performance"):
        try:
            exists = registry_conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table],
            ).fetchone()[0]
            if not exists:
                continue
            frame = registry_conn.execute(
                f"""
                SELECT UPPER(symbol_id) AS symbol_id, MIN(CAST(trade_date AS DATE)) AS gate_entry_date
                FROM {table}
                WHERE UPPER(symbol_id) IN ({placeholders})
                GROUP BY UPPER(symbol_id)
                """,
                symbols,
            ).fetchdf()
        except Exception:
            continue
        for record in frame.to_dict(orient="records"):
            symbol = str(record.get("symbol_id") or "").upper()
            entry = _date_text(record.get("gate_entry_date"))
            if not symbol or not entry:
                continue
            matching_dates = final_gate.loc[final_gate["symbol_id"].eq(symbol), "trade_date"].dropna().astype(str)
            for trade_date in matching_dates:
                current = out.get((symbol, trade_date))
                out[(symbol, trade_date)] = min(current, entry) if current else entry
    return out


def _load_prices(ohlcv_db_path: str | Path, symbols: list[str], *, as_of: str | None) -> dict[str, pd.DataFrame]:
    path = Path(ohlcv_db_path)
    if not symbols or not path.exists():
        return {}
    conn: duckdb.DuckDBPyConnection | None = None
    try:
        conn = duckdb.connect(str(path), read_only=True)
        placeholders = ", ".join(["?"] * len(symbols))
        as_of_clause = "AND CAST(timestamp AS DATE) <= CAST(? AS DATE)" if as_of else ""
        params: list[Any] = list(symbols)
        if as_of:
            params.append(as_of)
        frame = conn.execute(
            f"""
            SELECT
                UPPER(symbol_id) AS symbol_id,
                exchange,
                CAST(timestamp AS DATE) AS trade_date,
                close
            FROM _catalog
            WHERE UPPER(symbol_id) IN ({placeholders})
              AND COALESCE(is_benchmark, false) = false
              {as_of_clause}
            ORDER BY symbol_id, trade_date
            """,
            params,
        ).fetchdf()
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()
    if frame.empty:
        return {}
    frame.loc[:, "trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame.loc[:, "close"] = pd.to_numeric(frame["close"], errors="coerce")
    return {symbol: group.dropna(subset=["trade_date"]).reset_index(drop=True) for symbol, group in frame.groupby("symbol_id")}


def _monitor_row(row: dict[str, Any], prices_by_symbol: dict[str, pd.DataFrame]) -> dict[str, Any]:
    symbol = str(row.get("symbol_id") or "").upper()
    entry_date = _date_text(row.get("gate_entry_date") or row.get("trade_date"))
    score = _as_float(row.get("final_score"))
    invalidation = _as_float(row.get("invalidation_level"))
    prices = prices_by_symbol.get(symbol, pd.DataFrame())
    if not symbol or not entry_date or prices.empty:
        return _unknown(row, score)

    entry_ts = pd.to_datetime(entry_date, errors="coerce")
    if pd.isna(entry_ts):
        return _unknown(row, score)
    after_entry = prices.loc[prices["trade_date"].ge(entry_ts)].copy()
    entry_or_prior = prices.loc[prices["trade_date"].le(entry_ts)].copy()
    if after_entry.empty or entry_or_prior.empty:
        return _unknown(row, score)

    gate_close = _as_float(entry_or_prior.iloc[-1].get("close"))
    latest = after_entry.iloc[-1]
    latest_close = _as_float(latest.get("close"))
    latest_date = latest.get("trade_date")
    sessions_since = max(0, int(len(after_entry) - 1))
    days_since = int((pd.Timestamp(latest_date).date() - entry_ts.date()).days) if not pd.isna(latest_date) else pd.NA
    invalidation_breached = bool(latest_close is not None and invalidation is not None and latest_close < invalidation)

    if latest_close is None or gate_close is None:
        followthrough = "UNKNOWN"
    elif sessions_since < 3:
        followthrough = "PENDING_3D"
    else:
        first_window = after_entry.head(4)
        window_close = pd.to_numeric(first_window["close"], errors="coerce")
        followthrough = "CONFIRMED" if bool(window_close.gt(gate_close).any()) and latest_close >= gate_close else "FAILED_3D"

    return _with_exit_reason(
        gate_entry_date=entry_date,
        days_since_gate_entry=days_since,
        latest_close=latest_close,
        invalidation_breached=invalidation_breached,
        followthrough_status=followthrough,
        score=score,
    )


def _unknown(row: dict[str, Any], score: float | None) -> dict[str, Any]:
    return {
        "gate_entry_date": _date_text(row.get("gate_entry_date") or row.get("trade_date")),
        "days_since_gate_entry": pd.NA,
        "latest_close": pd.NA,
        "invalidation_breached": pd.NA,
        "followthrough_status": "UNKNOWN",
        "exit_triggered": bool(score is not None and score < 55),
        "exit_reason": "SCORE_BELOW_55" if score is not None and score < 55 else "UNKNOWN_DATA",
    }


def _with_exit_reason(
    *,
    gate_entry_date: str,
    days_since_gate_entry: Any,
    latest_close: float | None,
    invalidation_breached: bool,
    followthrough_status: str,
    score: float | None,
) -> dict[str, Any]:
    if invalidation_breached:
        reason = "INVALIDATION_BREACH"
    elif followthrough_status == "FAILED_3D":
        reason = "FAILED_3D_FOLLOWTHROUGH"
    elif score is not None and score < 55:
        reason = "SCORE_BELOW_55"
    elif followthrough_status == "UNKNOWN" or latest_close is None:
        reason = "UNKNOWN_DATA"
    else:
        reason = "NONE"
    return {
        "gate_entry_date": gate_entry_date,
        "days_since_gate_entry": days_since_gate_entry,
        "latest_close": latest_close if latest_close is not None else pd.NA,
        "invalidation_breached": bool(invalidation_breached),
        "followthrough_status": followthrough_status,
        "exit_triggered": reason not in {"NONE", "UNKNOWN_DATA"},
        "exit_reason": reason,
    }


def _as_float(value: object) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(out) else out


def _date_text(value: object) -> str:
    date = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(date) else date.strftime("%Y-%m-%d")
