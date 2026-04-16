"""
Daily EOD Update Runner
=======================
Usage:
    python daily_update_runner.py                          # Full update (OHLCV + Features)
    python daily_update_runner.py --symbols-only          # OHLCV only
    python daily_update_runner.py --features-only         # Features only
    python daily_update_runner.py --nse-primary --symbols-only  # Force NSE->yfinance as primary source
    python daily_update_runner.py --force                 # Force overwrite
    python daily_update_runner.py --batch-size 500       # Custom batch size

This script is designed to run after market close (3:30 PM IST).
It performs incremental updates - only fetching rows newer than
the last date already stored in DuckDB.
"""

import os
import sys
import argparse
import time
import sqlite3
from datetime import datetime, timedelta, date
from pathlib import Path

import duckdb
import pandas as pd
import yfinance as yf

from analytics.data_trust import (
    ensure_data_trust_schema,
    load_data_trust_summary,
    quarantine_symbol_dates,
    record_provenance_rows,
    resolve_quarantine_for_rows,
)
from core.bootstrap import ensure_project_root_on_path

project_root = str(ensure_project_root_on_path(__file__))

from collectors.dhan_collector import DhanCollector, dhan_daily_window_ist
from collectors.nse_collector import NSECollector
from features.feature_store import FeatureStore
from core.paths import ensure_domain_layout
from core.logging import logger


def _load_nse_holiday_dates(masterdb_path: str | None, from_date: str, to_date: str) -> set[str]:
    if not masterdb_path or not Path(masterdb_path).exists():
        return set()
    conn = sqlite3.connect(masterdb_path)
    try:
        rows = conn.execute(
            """
            SELECT date
            FROM nse_holidays
            WHERE date BETWEEN ? AND ?
            """,
            (from_date, to_date),
        ).fetchall()
    except sqlite3.Error:
        return set()
    finally:
        conn.close()
    return {str(row[0]) for row in rows if row and row[0]}


def _business_dates(from_date: str, to_date: str, *, masterdb_path: str | None = None) -> list[str]:
    if from_date > to_date:
        return []
    business_days = [ts.date().isoformat() for ts in pd.bdate_range(from_date, to_date)]
    if not business_days:
        return []
    holidays = _load_nse_holiday_dates(masterdb_path, business_days[0], business_days[-1])
    if not holidays:
        return business_days
    return [day for day in business_days if day not in holidays]


def _downgrade_noncritical_quarantine_rows(
    *,
    db_path: str,
    masterdb_path: str | None,
    from_date: str,
    to_date: str,
    run_id: str | None = None,
) -> dict[str, int]:
    conn = duckdb.connect(db_path)
    try:
        table_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '_catalog_quarantine'
            """
        ).fetchone()[0]
        if not table_exists:
            return {
                "repair_rows_observed": 0,
                "non_trading_provider_rows_observed": 0,
            }

        note_prefix = f"[{run_id or 'manual'}]"
        repair_rows = conn.execute(
            """
            UPDATE _catalog_quarantine
            SET status = 'observed',
                resolved_at = COALESCE(resolved_at, CURRENT_TIMESTAMP),
                note = COALESCE(note, ?) 
            WHERE status = 'active'
              AND reason = 'repair_source_unavailable'
              AND trade_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            RETURNING 1
            """,
            [
                f"{note_prefix} downgraded from active to observed for repair diagnostics",
                from_date,
                to_date,
            ],
        ).fetchall()

        non_trading_dates = set()
        for ts in pd.date_range(from_date, to_date, freq="D"):
            if ts.weekday() >= 5:
                non_trading_dates.add(ts.date().isoformat())
        non_trading_dates.update(_load_nse_holiday_dates(masterdb_path, from_date, to_date))

        provider_rows = []
        if non_trading_dates:
            conn.register("non_trading_dates", pd.DataFrame({"trade_date": sorted(non_trading_dates)}))
            provider_rows = conn.execute(
                """
                UPDATE _catalog_quarantine
                SET status = 'observed',
                    resolved_at = COALESCE(resolved_at, CURRENT_TIMESTAMP),
                    note = COALESCE(note, ?)
                FROM non_trading_dates d
                WHERE _catalog_quarantine.status = 'active'
                  AND _catalog_quarantine.reason = 'provider_unavailable'
                  AND _catalog_quarantine.trade_date = CAST(d.trade_date AS DATE)
                RETURNING 1
                """,
                [f"{note_prefix} provider_unavailable moved to observed on non-trading date"],
            ).fetchall()

        return {
            "repair_rows_observed": int(len(repair_rows)),
            "non_trading_provider_rows_observed": int(len(provider_rows)),
        }
    finally:
        conn.close()


def _load_historically_trusted_symbols(db_path: str) -> set[str]:
    """Return symbols that have ever been sourced from trusted daily providers."""
    conn = duckdb.connect(db_path, read_only=True)
    try:
        table_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '_catalog'
            """
        ).fetchone()[0]
        if not table_exists:
            return set()
        provider_col_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = '_catalog'
              AND column_name = 'provider'
            """
        ).fetchone()[0]
        if not provider_col_exists:
            return {
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT DISTINCT symbol_id
                    FROM _catalog
                    WHERE exchange = 'NSE'
                    """
                ).fetchall()
                if row and row[0]
            }
        rows = conn.execute(
            """
            SELECT DISTINCT symbol_id
            FROM _catalog
            WHERE exchange = 'NSE'
              AND COALESCE(provider, '') IN ('nse_bhavcopy', 'yfinance')
            """
        ).fetchall()
        return {str(row[0]) for row in rows if row and row[0]}
    finally:
        conn.close()


def _cleanup_off_contract_unknown_rows(
    *,
    db_path: str,
    target_end_date: date,
) -> dict[str, int]:
    """
    Remove out-of-contract rows beyond ingest target window with unknown provenance.

    Daily operational ingest contract writes up to target_end_date (today-1).
    Any future-dated rows without provider/validation lineage are treated as
    stale ad-hoc leftovers and removed.
    """
    conn = duckdb.connect(db_path)
    try:
        table_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '_catalog'
            """
        ).fetchone()[0]
        if not table_exists:
            return {"deleted_unknown_future_rows": 0}

        deleted = conn.execute(
            """
            DELETE FROM _catalog
            WHERE exchange = 'NSE'
              AND CAST(timestamp AS DATE) > CAST(? AS DATE)
              AND COALESCE(provider, '') IN ('', 'unknown')
              AND COALESCE(validation_status, '') IN ('', 'legacy_unverified')
              AND COALESCE(ingest_run_id, '') = ''
            RETURNING 1
            """,
            [target_end_date.isoformat()],
        ).fetchall()
        return {"deleted_unknown_future_rows": int(len(deleted))}
    finally:
        conn.close()


def _bhavcopy_filename(trade_date: str) -> str:
    dt = date.fromisoformat(trade_date)
    return f"nse_{dt.strftime('%d%b%Y').upper()}.csv"


def _normalize_bhavcopy_frame(
    raw_df: pd.DataFrame,
    trade_date: str,
    security_map: dict[str, dict],
) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(
            columns=["symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"]
        )

    df = raw_df.copy()
    df.columns = [str(col).replace("\ufeff", "").strip().replace(" ", "") for col in df.columns]
    rename_map = {
        "SYMBOL": "symbol_id",
        "SERIES": "series",
        "OPEN_PRICE": "open",
        "HIGH_PRICE": "high",
        "LOW_PRICE": "low",
        "CLOSE_PRICE": "close",
        "TTL_TRD_QNTY": "volume",
    }
    df = df.rename(columns=rename_map)
    if "series" in df.columns:
        df = df[df["series"].astype(str).str.strip().str.upper() == "EQ"]
    df["symbol_id"] = df.get("symbol_id", pd.Series(dtype=str)).astype(str).str.strip()
    df = df[df["symbol_id"].isin(security_map.keys())].copy()
    if df.empty:
        return pd.DataFrame(
            columns=["symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"]
        )

    for field in ["open", "high", "low", "close", "volume"]:
        df[field] = pd.to_numeric(df.get(field), errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    if df.empty:
        return pd.DataFrame(
            columns=["symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"]
        )

    df["timestamp"] = pd.to_datetime(trade_date)
    df["security_id"] = df["symbol_id"].map(lambda symbol: security_map[symbol]["security_id"])
    df["exchange"] = "NSE"
    df["volume"] = df["volume"].fillna(0).astype("int64")
    return df[
        ["symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"]
    ].drop_duplicates(subset=["symbol_id", "exchange", "timestamp"])


def _rows_to_symbol_frames(rows: pd.DataFrame) -> list[pd.DataFrame]:
    if rows is None or rows.empty:
        return []
    frames: list[pd.DataFrame] = []
    for (symbol_id, security_id, exchange), part in rows.groupby(["symbol_id", "security_id", "exchange"], sort=True):
        frame = (
            part[
                [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "provider",
                    "provider_priority",
                    "validation_status",
                    "validated_against",
                    "ingest_run_id",
                    "repair_batch_id",
                ]
            ]
            .sort_values("timestamp")
            .drop_duplicates("timestamp", keep="last")
            .set_index("timestamp")
        )
        frame.attrs["symbol_info"] = {
            "symbol_id": str(symbol_id),
            "security_id": str(security_id),
            "exchange": str(exchange),
        }
        frames.append(frame)
    return frames


def _fetch_nse_bhavcopy_rows(
    *,
    raw_dir: Path,
    trade_dates: list[str],
    security_map: dict[str, dict],
) -> tuple[pd.DataFrame, list[str], list[str]]:
    collector = NSECollector(data_dir=str(raw_dir))
    normalized_frames: list[pd.DataFrame] = []
    archived_dates: list[str] = []
    missing_dates: list[str] = []

    for trade_date in trade_dates:
        df = collector.get_bhavcopy(trade_date)
        if df.empty:
            missing_dates.append(trade_date)
            continue
        out_path = raw_dir / _bhavcopy_filename(trade_date)
        if not out_path.exists():
            df.to_csv(out_path, index=False)
        normalized = _normalize_bhavcopy_frame(df, trade_date, security_map)
        if normalized.empty:
            missing_dates.append(trade_date)
            continue
        archived_dates.append(trade_date)
        normalized_frames.append(normalized)

    if not normalized_frames:
        return pd.DataFrame(), archived_dates, missing_dates
    return pd.concat(normalized_frames, ignore_index=True), archived_dates, missing_dates


def _extract_yfinance_symbol_frame(data: pd.DataFrame, symbol: str) -> pd.DataFrame:
    ticker = f"{symbol}.NS"
    if isinstance(data.columns, pd.MultiIndex):
        level0 = data.columns.get_level_values(0)
        if "Close" not in level0:
            return pd.DataFrame()
        try:
            frame = pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(data.index).tz_localize(None),
                    "open": data["Open"][ticker],
                    "high": data["High"][ticker],
                    "low": data["Low"][ticker],
                    "close": data["Close"][ticker],
                    "volume": data["Volume"][ticker],
                }
            )
        except KeyError:
            return pd.DataFrame()
    else:
        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(data.index).tz_localize(None),
                "open": data["Open"],
                "high": data["High"],
                "low": data["Low"],
                "close": data["Close"],
                "volume": data["Volume"],
            }
        )
    return frame.dropna(subset=["close"]).reset_index(drop=True)


def _fetch_yfinance_rows(
    *,
    symbol_rows: list[dict],
    trade_dates: list[str],
    batch_size: int = 100,
) -> pd.DataFrame:
    if not symbol_rows or not trade_dates:
        return pd.DataFrame()

    start = min(trade_dates)
    end = (date.fromisoformat(max(trade_dates)) + timedelta(days=1)).isoformat()
    target_dates = set(trade_dates)
    normalized_frames: list[pd.DataFrame] = []

    for i in range(0, len(symbol_rows), batch_size):
        batch = symbol_rows[i : i + batch_size]
        tickers = [f"{row['symbol_id']}.NS" for row in batch]
        data = yf.download(
            tickers,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=True,
            group_by="column",
        )
        if data is None or data.empty:
            continue

        for row in batch:
            frame = _extract_yfinance_symbol_frame(data, row["symbol_id"])
            if frame.empty:
                continue
            frame["trade_date"] = frame["timestamp"].dt.date.astype(str)
            frame = frame[frame["trade_date"].isin(target_dates)].drop(columns=["trade_date"])
            if frame.empty:
                continue
            frame["symbol_id"] = row["symbol_id"]
            frame["security_id"] = str(row["security_id"])
            frame["exchange"] = row.get("exchange", "NSE") or "NSE"
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0).astype("int64")
            normalized_frames.append(
                frame[["symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close", "volume"]]
            )

    if not normalized_frames:
        return pd.DataFrame()
    return pd.concat(normalized_frames, ignore_index=True).drop_duplicates(
        subset=["symbol_id", "exchange", "timestamp"], keep="last"
    )


def _run_nse_yfinance_daily_update(
    *,
    collector: DhanCollector,
    batch_size: int,
    symbol_limit: int | None,
    run_id: str | None = None,
    days_history: int = 7,
) -> dict:
    symbols = collector.get_symbols_from_masterdb(exchanges=["NSE"])
    if symbol_limit is not None:
        symbols = symbols[:symbol_limit]
    if not symbols:
        return {"error": "No symbols found in masterdb"}

    last_dates = collector._get_last_dates(exchanges=["NSE"])
    run_date = datetime.now().date()
    target_end_date = run_date - timedelta(days=1)
    fallback_start = target_end_date - timedelta(days=days_history)
    earliest_needed = target_end_date + timedelta(days=1)
    updated_symbols: set[str] = set()
    stale_symbols: list[str] = []
    no_data_symbols: list[str] = []
    up_to_date_symbols: list[str] = []
    required_start_by_symbol: dict[str, str] = {}

    for row in symbols:
        sym_id = row["symbol_id"]
        last = last_dates.get(sym_id)
        if last:
            start_date = datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)
            if start_date > target_end_date:
                up_to_date_symbols.append(sym_id)
            else:
                stale_symbols.append(sym_id)
        else:
            start_date = fallback_start
            no_data_symbols.append(sym_id)
        if start_date < earliest_needed:
            earliest_needed = start_date
        required_start_by_symbol[sym_id] = start_date.isoformat()

    if earliest_needed > target_end_date:
        return {
            "symbols_updated": 0,
            "symbols_errors": 0,
            "updated_symbols": [],
            "providers_used": [],
            "nse_bhavcopy_dates": [],
            "yfinance_fallback_dates": [],
            "unresolved_dates": [],
            "rows_written": 0,
            "duration_sec": 0.0,
            "stale_symbols": stale_symbols,
            "no_data_symbols": no_data_symbols,
            "up_to_date_symbols": up_to_date_symbols,
            "target_end_date": target_end_date.isoformat(),
        }

    trade_dates = _business_dates(
        earliest_needed.isoformat(),
        target_end_date.isoformat(),
        masterdb_path=collector.masterdb_path,
    )
    required_symbol_dates: dict[str, set[str]] = {}
    for symbol_id, start_date in required_start_by_symbol.items():
        required_symbol_dates[symbol_id] = {
            trade_date for trade_date in trade_dates if trade_date >= str(start_date)
        }
    raw_dir = Path(project_root) / "data" / "raw" / "NSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    security_map = {str(row["symbol_id"]): row for row in symbols}

    t0 = time.time()
    ensure_data_trust_schema(collector.db_path)
    off_contract_cleanup = _cleanup_off_contract_unknown_rows(
        db_path=collector.db_path,
        target_end_date=target_end_date,
    )
    quarantine_housekeeping = _downgrade_noncritical_quarantine_rows(
        db_path=collector.db_path,
        masterdb_path=collector.masterdb_path,
        from_date=earliest_needed.isoformat(),
        to_date=target_end_date.isoformat(),
        run_id=run_id,
    )
    nse_rows, nse_dates, missing_dates = _fetch_nse_bhavcopy_rows(
        raw_dir=raw_dir,
        trade_dates=trade_dates,
        security_map=security_map,
    )
    if not nse_rows.empty:
        nse_rows["provider"] = "nse_bhavcopy"
        nse_rows["provider_priority"] = 1
        nse_rows["validation_status"] = "trusted_primary"
        nse_rows["validated_against"] = None
        nse_rows["ingest_run_id"] = run_id
        nse_rows["repair_batch_id"] = None

    all_rows = nse_rows.copy() if not nse_rows.empty else pd.DataFrame()
    yfinance_dates: list[str] = []
    if missing_dates:
        yfinance_rows = _fetch_yfinance_rows(symbol_rows=symbols, trade_dates=missing_dates, batch_size=max(25, min(batch_size, 100)))
        if not yfinance_rows.empty:
            yfinance_rows["provider"] = "yfinance"
            yfinance_rows["provider_priority"] = 2
            yfinance_rows["validation_status"] = "trusted_fallback"
            yfinance_rows["validated_against"] = "nse_bhavcopy_missing"
            yfinance_rows["ingest_run_id"] = run_id
            yfinance_rows["repair_batch_id"] = None
            yfinance_dates = sorted(yfinance_rows["timestamp"].dt.date.astype(str).unique().tolist())
            all_rows = yfinance_rows if all_rows.empty else pd.concat([all_rows, yfinance_rows], ignore_index=True)

    resolved_symbol_dates: set[tuple[str, str]] = set()
    if not all_rows.empty:
        resolved_symbol_dates = {
            (str(row.symbol_id), str(row.trade_date))
            for row in all_rows.assign(trade_date=all_rows["timestamp"].dt.date.astype(str))
            .loc[:, ["symbol_id", "trade_date"]]
            .drop_duplicates()
            .itertuples(index=False)
        }

    unresolved_symbol_dates = sorted(
        (
            symbol_id,
            trade_date,
        )
        for symbol_id, trade_date_set in required_symbol_dates.items()
        for trade_date in sorted(trade_date_set)
        if (symbol_id, trade_date) not in resolved_symbol_dates
    )

    unresolved_dates_all = sorted({trade_date for _, trade_date in unresolved_symbol_dates})
    active_quarantine_start = target_end_date - timedelta(days=7)
    unresolved_recent_set = {
        trade_date
        for trade_date in unresolved_dates_all
        if date.fromisoformat(trade_date) >= active_quarantine_start
    }

    resolved_symbol_ids = {symbol_id for symbol_id, _ in resolved_symbol_dates}
    historically_trusted_symbols = _load_historically_trusted_symbols(collector.db_path)
    active_eligible_symbols = historically_trusted_symbols.union(resolved_symbol_ids)

    quarantined_row_count = 0
    observed_row_count = 0
    symbol_lookup = {str(row["symbol_id"]): row for row in symbols}
    unresolved_symbol_dates_active: list[tuple[str, str]] = []
    unresolved_symbol_dates_observed: list[tuple[str, str]] = []
    for symbol_id, trade_date in unresolved_symbol_dates:
        if trade_date in unresolved_recent_set and symbol_id in active_eligible_symbols:
            unresolved_symbol_dates_active.append((symbol_id, trade_date))
        else:
            unresolved_symbol_dates_observed.append((symbol_id, trade_date))

    unresolved_dates = sorted({trade_date for _, trade_date in unresolved_symbol_dates_active})
    unresolved_dates_observed = sorted({trade_date for _, trade_date in unresolved_symbol_dates_observed})

    active_symbols_by_date: dict[str, set[str]] = {}
    for symbol_id, trade_date in unresolved_symbol_dates_active:
        active_symbols_by_date.setdefault(trade_date, set()).add(symbol_id)
    observed_symbols_by_date: dict[str, set[str]] = {}
    for symbol_id, trade_date in unresolved_symbol_dates_observed:
        observed_symbols_by_date.setdefault(trade_date, set()).add(symbol_id)

    for trade_date, symbol_ids in sorted(active_symbols_by_date.items()):
        rows = [symbol_lookup[symbol_id] for symbol_id in sorted(symbol_ids) if symbol_id in symbol_lookup]
        if not rows:
            continue
        quarantined_row_count += quarantine_symbol_dates(
            collector.db_path,
            symbol_rows=rows,
            trade_dates=[trade_date],
            reason="provider_unavailable",
            status="active",
            source_run_id=run_id,
            note="NSE bhavcopy missing and yfinance fallback returned no OHLC rows.",
        )
    for trade_date, symbol_ids in sorted(observed_symbols_by_date.items()):
        rows = [symbol_lookup[symbol_id] for symbol_id in sorted(symbol_ids) if symbol_id in symbol_lookup]
        if not rows:
            continue
        observed_row_count += quarantine_symbol_dates(
            collector.db_path,
            symbol_rows=rows,
            trade_dates=[trade_date],
            reason="provider_unavailable",
            status="observed",
            source_run_id=run_id,
            note="Historical unresolved provider gap retained as observed (outside active trust window).",
        )

    rows_written = 0
    provider_counts_by_date: dict[str, dict[str, int]] = {}
    validation_counts = {
        "trusted_primary": 0,
        "trusted_fallback": 0,
        "legacy_unverified": 0,
    }
    if not all_rows.empty:
        for trade_date, provider, row_count in (
            all_rows.assign(trade_date=all_rows["timestamp"].dt.date.astype(str))
            .groupby(["trade_date", "provider"])
            .size()
            .reset_index(name="row_count")
            .itertuples(index=False)
        ):
            provider_counts_by_date.setdefault(str(trade_date), {})
            provider_counts_by_date[str(trade_date)][str(provider)] = int(row_count)
        for status, row_count in (
            all_rows.groupby("validation_status").size().reset_index(name="row_count").itertuples(index=False)
        ):
            validation_counts[str(status)] = int(row_count)

        record_provenance_rows(collector.db_path, all_rows)
        frames = _rows_to_symbol_frames(all_rows)
        rows_written = int(collector._upsert_ohlcv(frames) or 0)
        resolve_quarantine_for_rows(
            collector.db_path,
            all_rows,
            note=f"Resolved by ingest run {run_id or 'manual'}",
        )
        updated_symbols.update(all_rows["symbol_id"].astype(str).unique().tolist())

    duration = time.time() - t0
    providers_used = []
    if nse_dates:
        providers_used.append("nse_bhavcopy")
    if yfinance_dates:
        providers_used.append("yfinance")

    trust_summary = load_data_trust_summary(collector.db_path, run_date=target_end_date.isoformat())

    return {
        "symbols_updated": len(updated_symbols),
        "symbols_errors": len(unresolved_dates),
        "updated_symbols": sorted(updated_symbols),
        "providers_used": providers_used,
        "provider_counts_by_date": provider_counts_by_date,
        "nse_bhavcopy_dates": nse_dates,
        "yfinance_fallback_dates": yfinance_dates,
        "unresolved_dates": unresolved_dates,
        "unresolved_dates_all": unresolved_dates_all,
        "unresolved_date_count": len(unresolved_dates),
        "unresolved_date_count_all": len(unresolved_dates_all),
        "unresolved_symbol_date_count": len(unresolved_symbol_dates_active),
        "unresolved_symbol_date_count_all": len(unresolved_symbol_dates),
        "rows_written": rows_written,
        "quarantined_row_count": quarantined_row_count,
        "observed_row_count": observed_row_count,
        "validation_counts": validation_counts,
        "written_without_cross_source_confirmation": int(validation_counts.get("trusted_fallback", 0)),
        "duration_sec": duration,
        "stale_symbols": stale_symbols,
        "no_data_symbols": no_data_symbols,
        "up_to_date_symbols": up_to_date_symbols,
        "trust_summary": trust_summary,
        "quarantine_housekeeping": quarantine_housekeeping,
        "off_contract_cleanup": off_contract_cleanup,
        "active_eligible_symbol_count": int(len(active_eligible_symbols)),
        "target_end_date": target_end_date.isoformat(),
    }


def _load_catalog_window_rows(
    *,
    collector: DhanCollector,
    symbol_ids: list[str],
    from_date: str,
    to_date: str,
) -> pd.DataFrame:
    if not symbol_ids:
        return pd.DataFrame(
            columns=[
                "symbol_id",
                "security_id",
                "exchange",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )
    conn = duckdb.connect(collector.db_path, read_only=True)
    try:
        frame = conn.execute(
            """
            SELECT
                symbol_id,
                security_id,
                exchange,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                provider,
                provider_priority,
                validation_status,
                validated_against,
                ingest_run_id,
                repair_batch_id
            FROM _catalog
            WHERE exchange = 'NSE'
              AND symbol_id IN (
                  SELECT UNNEST(?)
              )
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
            ORDER BY symbol_id, timestamp
            """,
            [symbol_ids, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()
    return frame


def _run_dhan_primary_daily_update(
    *,
    collector: DhanCollector,
    batch_size: int,
    symbol_limit: int | None,
    run_id: str | None = None,
    validator_pct_threshold: float = 0.01,
) -> dict:
    symbols = collector.get_symbols_from_masterdb(exchanges=["NSE"])
    if symbol_limit is not None:
        symbols = symbols[:symbol_limit]
    if not symbols:
        return {"error": "No symbols found in masterdb"}

    default_from_date, default_to_date = dhan_daily_window_ist()
    t0 = time.time()
    result = collector.run_daily_update(
        exchanges=["NSE"],
        batch_size=batch_size,
        max_concurrent=10,
        symbol_limit=symbol_limit,
        compute_features=False,
        run_id=run_id,
    )
    from_date = str(result.get("window_from_date") or default_from_date)
    to_date = str(result.get("window_to_date") or default_to_date)
    duration = time.time() - t0

    symbol_ids = sorted({str(row["symbol_id"]) for row in symbols if row.get("symbol_id")})
    catalog_rows = _load_catalog_window_rows(
        collector=collector,
        symbol_ids=symbol_ids,
        from_date=from_date,
        to_date=to_date,
    )

    security_map = {str(row["symbol_id"]): row for row in symbols}
    trade_dates = _business_dates(from_date, to_date, masterdb_path=collector.masterdb_path)
    raw_dir = Path(project_root) / "data" / "raw" / "NSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    nse_rows, nse_dates, missing_dates = _fetch_nse_bhavcopy_rows(
        raw_dir=raw_dir,
        trade_dates=trade_dates,
        security_map=security_map,
    )
    yfinance_rows = _fetch_yfinance_rows(
        symbol_rows=symbols,
        trade_dates=missing_dates,
        batch_size=max(25, min(batch_size, 100)),
    ) if missing_dates else pd.DataFrame()
    yfinance_dates = sorted(yfinance_rows["timestamp"].dt.date.astype(str).unique().tolist()) if not yfinance_rows.empty else []
    unresolved_reference_dates = sorted(set(missing_dates) - set(yfinance_dates))

    reference_rows = pd.DataFrame()
    if not nse_rows.empty:
        part = nse_rows.copy()
        part["validator_source"] = "nse_bhavcopy"
        reference_rows = part
    if not yfinance_rows.empty:
        part = yfinance_rows.copy()
        part["validator_source"] = "yfinance"
        reference_rows = part if reference_rows.empty else pd.concat([reference_rows, part], ignore_index=True)

    compared_rows = 0
    mismatch_rows = 0
    mismatch_symbols: set[str] = set()
    mismatch_sample: list[dict[str, object]] = []
    if not catalog_rows.empty and not reference_rows.empty:
        left = catalog_rows.copy()
        left["trade_date"] = pd.to_datetime(left["timestamp"]).dt.date.astype(str)
        left = left[["symbol_id", "trade_date", "close"]].rename(columns={"close": "close_dhan"})
        right = reference_rows.copy()
        right["trade_date"] = pd.to_datetime(right["timestamp"]).dt.date.astype(str)
        right = right[["symbol_id", "trade_date", "close", "validator_source"]].rename(columns={"close": "close_ref"})
        merged = left.merge(right, on=["symbol_id", "trade_date"], how="inner")
        compared_rows = int(len(merged))
        if not merged.empty:
            ref_abs = merged["close_ref"].abs().replace(0, pd.NA)
            merged["abs_pct_diff"] = (merged["close_dhan"] - merged["close_ref"]).abs() / ref_abs
            merged["abs_pct_diff"] = merged["abs_pct_diff"].fillna(0.0)
            mismatch_mask = (
                merged["close_dhan"].round(4) != merged["close_ref"].round(4)
            ) & (merged["abs_pct_diff"] >= float(validator_pct_threshold))
            mismatches = merged[mismatch_mask].copy()
            mismatch_rows = int(len(mismatches))
            mismatch_symbols = set(mismatches["symbol_id"].astype(str).tolist())
            if mismatch_rows:
                mismatch_sample = (
                    mismatches.sort_values(["trade_date", "symbol_id"])
                    .head(20)[["symbol_id", "trade_date", "close_dhan", "close_ref", "abs_pct_diff", "validator_source"]]
                    .to_dict("records")
                )

    compared_symbols = int(
        len(
            set(catalog_rows["symbol_id"].astype(str).tolist()).intersection(
                set(reference_rows["symbol_id"].astype(str).tolist())
            )
        )
    ) if not catalog_rows.empty and not reference_rows.empty else 0
    mismatch_ratio = (mismatch_rows / compared_rows) if compared_rows else 0.0
    validator_status = "ok"
    if unresolved_reference_dates:
        validator_status = "degraded"
    if mismatch_ratio >= 0.05:
        validator_status = "alert"

    trust_summary = load_data_trust_summary(collector.db_path, run_date=to_date)
    providers_used = ["dhan_historical_daily"]
    if nse_dates:
        providers_used.append("validator_nse_bhavcopy")
    if yfinance_dates:
        providers_used.append("validator_yfinance")

    result = dict(result or {})
    result.update(
        {
            "ohlc_source_mode": "dhan_primary",
            "providers_used": providers_used,
            "dhan_from_date": from_date,
            "dhan_to_date": to_date,
            "duration_sec": float(result.get("duration_sec", duration)),
            "nse_validator_dates": sorted(nse_dates),
            "yfinance_validator_dates": yfinance_dates,
            "validator_unresolved_dates": unresolved_reference_dates,
            "validator_status": validator_status,
            "validator_compared_rows": compared_rows,
            "validator_compared_symbols": compared_symbols,
            "validator_mismatch_rows": mismatch_rows,
            "validator_mismatch_symbols": len(mismatch_symbols),
            "validator_mismatch_ratio": round(mismatch_ratio, 6),
            "validator_mismatch_sample": mismatch_sample,
            "validator_pct_threshold": float(validator_pct_threshold),
            "trust_summary": trust_summary,
        }
    )
    return result


def run(
    symbols_only: bool,
    features_only: bool,
    batch_size: int,
    bulk: bool,
    dhan_historical_daily: bool = False,
    nse_primary: bool = False,
    symbol_limit: int | None = None,
    canary_mode: bool = False,
    canary_symbol_limit: int | None = None,
    data_domain: str = "operational",
    symbols: list[str] | None = None,
    full_rebuild: bool = False,
    feature_tail_bars: int = 252,
    run_id: str | None = None,
):
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    incremental_features = data_domain == "operational" and not full_rebuild
    collector = DhanCollector(
        db_path=str(paths.ohlcv_db_path),
        masterdb_path=str(paths.master_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        data_domain=data_domain,
    )
    effective_symbol_limit = symbol_limit
    if canary_mode:
        requested_limit = int(canary_symbol_limit or 25)
        if effective_symbol_limit is None:
            effective_symbol_limit = requested_limit
        else:
            effective_symbol_limit = min(int(effective_symbol_limit), requested_limit)

    def bootstrap_live_dhan_access() -> None:
        access_token = collector.token_manager.ensure_valid_token(hours_before_expiry=1)
        collector.client_id = collector.client_id or collector.token_manager.client_id
        collector.api_key = collector.api_key or collector.token_manager.api_key
        collector.access_token = access_token or collector.access_token
        collector.use_api = bool(collector.client_id and collector.access_token)
        if collector.use_api and collector.dhan is None:
            collector._init_dhan_client()

    def ensure_live_dhan_access() -> None:
        bootstrap_live_dhan_access()
        if not collector.use_api or collector.dhan is None:
            raise RuntimeError(
                "Operational OHLCV ingestion requires authenticated Dhan access; synthetic fallback is disabled."
            )
        if not collector._ensure_valid_token():
            raise RuntimeError(
                "Dhan authentication is invalid or expired; aborting OHLCV ingestion."
            )

    if features_only:
        logger.info("=" * 60)
        logger.info("MODE: Features Only - recomputing all features")
        logger.info("=" * 60)

        if symbols is None:
            import duckdb

            conn = duckdb.connect(collector.db_path, read_only=True)
            try:
                syms = conn.execute(
                    "SELECT DISTINCT symbol_id FROM _catalog WHERE exchange = 'NSE'"
                ).fetchall()
                symbols = [r[0] for r in syms]
                if symbol_limit is not None:
                    symbols = symbols[:symbol_limit]
            finally:
                conn.close()

        logger.info(f"Computing features for {len(symbols)} symbols...")
        fs = FeatureStore(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=data_domain,
        )
        result = fs.compute_and_store_features(
            symbols=symbols,
            exchanges=["NSE"],
            feature_types=[
                "rsi",
                "adx",
                "sma",
                "ema",
                "macd",
                "atr",
                "bb",
                "roc",
                "supertrend",
            ],
            incremental=incremental_features,
            tail_bars=feature_tail_bars,
            full_rebuild=full_rebuild,
        )
        logger.info(f"Feature computation complete: {result}")

        logger.info("Computing sector RS and relative strength...")
        from features.compute_sector_rs import compute_all_symbols_rs

        compute_all_symbols_rs(
            db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            masterdb_path=str(paths.master_db_path),
        )
        logger.info("Sector RS computation complete")

        return {
            "mode": "features_only",
            "symbols_targeted": len(symbols or []),
            "feature_result": result,
            "full_rebuild": bool(full_rebuild),
        }

    if bulk:
        ensure_live_dhan_access()
        logger.info("=" * 60)
        logger.info("MODE: Bulk OHLC - Fast single API call for today's data")
        logger.info("=" * 60)

        result = collector.run_daily_update_bulk(
            exchanges=["NSE"],
            symbol_limit=effective_symbol_limit,
            compute_features=False,
        )
        result["canary_mode"] = bool(canary_mode)
        result["canary_symbol_limit"] = effective_symbol_limit
        if canary_mode:
            result["canary_blocked"] = False
            result["canary_status"] = "passed"
        logger.info(f"Bulk daily update result: {result}")
        return result

    if dhan_historical_daily:
        ensure_live_dhan_access()
        logger.info("=" * 60)
        logger.info("MODE: Dhan Historical Daily - fixed IST window (today-1 -> today)")
        logger.info("=" * 60)
        result = collector.run_daily_update(
            exchanges=["NSE"],
            batch_size=batch_size,
            max_concurrent=10,
            symbol_limit=effective_symbol_limit,
            compute_features=False,
            full_rebuild=full_rebuild,
            feature_tail_bars=feature_tail_bars,
        )
        result["ohlc_source_mode"] = "dhan_historical_daily"
        result["canary_mode"] = bool(canary_mode)
        result["canary_symbol_limit"] = effective_symbol_limit
        if canary_mode:
            result["canary_blocked"] = bool(result.get("symbols_errors"))
            result["canary_status"] = "blocked" if result["canary_blocked"] else "passed"
        return result

    if symbols_only:
        if nse_primary:
            logger.info("=" * 60)
            logger.info("MODE: Symbols Only - OHLCV fetch via NSE bhavcopy + yfinance fallback")
            logger.info("=" * 60)
            result = _run_nse_yfinance_daily_update(
                collector=collector,
                batch_size=batch_size,
                symbol_limit=effective_symbol_limit,
                run_id=run_id,
            )
        else:
            ensure_live_dhan_access()
            logger.info("=" * 60)
            logger.info("MODE: Symbols Only - Dhan primary with NSE/yfinance validator alerts")
            logger.info("=" * 60)
            result = _run_dhan_primary_daily_update(
                collector=collector,
                batch_size=batch_size,
                symbol_limit=effective_symbol_limit,
                run_id=run_id,
            )
        trust_summary = result.get("trust_summary") or {}
        canary_blocked = bool(
            canary_mode
            and (
                trust_summary.get("status") in {"blocked", "degraded"}
                or bool(result.get("unresolved_dates"))
                or result.get("validator_status") == "alert"
            )
        )
        result["canary_mode"] = bool(canary_mode)
        result["canary_symbol_limit"] = effective_symbol_limit
        result["canary_blocked"] = canary_blocked
        if canary_mode:
            result["canary_status"] = "blocked" if canary_blocked else "passed"
        logger.info(f"Daily update result: {result}")
        logger.info("")
        logger.info("TIP: Run features separately after OHLCV update:")
        logger.info("  python collectors/daily_update_runner.py --features-only")
        return result

    logger.info("=" * 60)
    logger.info("MODE: Full Update - OHLCV + Features")
    if nse_primary:
        logger.info("OHLC source priority: NSE bhavcopy -> yfinance fallback")
    else:
        logger.info("OHLC source priority: Dhan primary -> NSE/yfinance validator alerts")
    logger.info("=" * 60)

    if nse_primary:
        result = _run_nse_yfinance_daily_update(
            collector=collector,
            batch_size=batch_size,
            symbol_limit=effective_symbol_limit,
            run_id=run_id,
        )
    else:
        ensure_live_dhan_access()
        result = _run_dhan_primary_daily_update(
            collector=collector,
            batch_size=batch_size,
            symbol_limit=effective_symbol_limit,
            run_id=run_id,
        )
    trust_summary = result.get("trust_summary") or {}
    canary_blocked = bool(
        canary_mode
        and (
            trust_summary.get("status") in {"blocked", "degraded"}
            or bool(result.get("unresolved_dates"))
            or result.get("validator_status") == "alert"
        )
    )
    result["canary_mode"] = bool(canary_mode)
    result["canary_symbol_limit"] = effective_symbol_limit
    result["canary_blocked"] = canary_blocked
    if canary_mode:
        result["canary_status"] = "blocked" if canary_blocked else "passed"

    logger.info("=" * 60)
    logger.info("DAILY UPDATE COMPLETE")
    logger.info(f"  Symbols updated : {result.get('symbols_updated', 0)}")
    logger.info(f"  Symbols errors  : {result.get('symbols_errors', 0)}")
    logger.info(f"  Duration        : {result.get('duration_sec', 0):.1f}s")
    logger.info("=" * 60)
    updated_symbols = result.get("updated_symbols") or None
    if updated_symbols or full_rebuild:
        target_symbols = None if full_rebuild else updated_symbols
        logger.info(
            "Recomputing features for %s symbols (mode=%s, tail_bars=%s)",
            "all" if target_symbols is None else len(target_symbols),
            "incremental" if incremental_features else "full_rebuild",
            feature_tail_bars,
        )
        fs = FeatureStore(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain=data_domain,
        )
        feat_result = fs.compute_and_store_features(
            symbols=target_symbols,
            exchanges=["NSE"],
            feature_types=[
                "rsi",
                "adx",
                "sma",
                "ema",
                "macd",
                "atr",
                "bb",
                "roc",
                "supertrend",
            ],
            incremental=incremental_features,
            tail_bars=feature_tail_bars,
            full_rebuild=full_rebuild,
        )
        logger.info(f"Feature computation complete: {feat_result}")
        result["feature_result"] = feat_result

    logger.info("Computing sector RS and relative strength...")
    from features.compute_sector_rs import compute_all_symbols_rs

    compute_all_symbols_rs(
        db_path=str(paths.ohlcv_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        masterdb_path=str(paths.master_db_path),
    )
    logger.info("Sector RS computation complete")
    logger.info("")
    logger.info("TIP: Recompute features for updated symbols:")
    logger.info("  python collectors/daily_update_runner.py --features-only")
    return result


def main():
    parser = argparse.ArgumentParser(description="Daily EOD Update")
    parser.add_argument(
        "--batch-size", type=int, default=700, help="Symbols per batch (default: 700)"
    )
    parser.add_argument(
        "--symbols-only", action="store_true", help="Only fetch OHLCV, skip features"
    )
    parser.add_argument(
        "--features-only",
        action="store_true",
        help="Only recompute features, skip OHLCV fetch",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force update even if today's data exists"
    )
    parser.add_argument(
        "--bulk",
        action="store_true",
        help="Use bulk OHLC API (fast, today only). Use for quick daily updates.",
    )
    parser.add_argument(
        "--dhan-historical-daily",
        action="store_true",
        help="Use Dhan historical daily candles with fixed IST window (today-1 to today).",
    )
    parser.add_argument(
        "--nse-primary",
        action="store_true",
        help="Use NSE bhavcopy -> yfinance fallback as the primary OHLC source (legacy mode).",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=None,
        help="Limit the live symbol universe for canary/test runs.",
    )
    parser.add_argument(
        "--canary-mode",
        action="store_true",
        help="Run a limited ingest sample and return trust status for rollout checks.",
    )
    parser.add_argument(
        "--canary-symbol-limit",
        type=int,
        default=25,
        help="Maximum symbols to sample when canary mode is enabled.",
    )
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Resolved storage domain for this run.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Force full feature recomputation instead of incremental tail updates.",
    )
    parser.add_argument(
        "--feature-tail-bars",
        type=int,
        default=252,
        help="Tail window to recompute for incremental operational feature updates.",
    )
    args = parser.parse_args()

    run(
        symbols_only=args.symbols_only,
        features_only=args.features_only,
        batch_size=args.batch_size,
        bulk=args.bulk,
        dhan_historical_daily=args.dhan_historical_daily,
        nse_primary=args.nse_primary,
        symbol_limit=args.symbol_limit,
        canary_mode=args.canary_mode,
        canary_symbol_limit=args.canary_symbol_limit,
        data_domain=args.data_domain,
        full_rebuild=args.full_rebuild,
        feature_tail_bars=args.feature_tail_bars,
    )


if __name__ == "__main__":
    main()
