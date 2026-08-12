"""Backfill BSE-only equities from official BSE daily bhavcopies.

The command is preview-only unless ``--apply`` is supplied. It archives each
official source file in the canonical BSE raw cache, validates symbol identity,
backs up the live OHLCV store, records provenance, and performs targeted upserts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

from ai_trading_system.analytics.data_trust import (
    annotate_provider_reconciliation,
    ensure_data_trust_schema,
    quarantine_symbol_dates,
    record_provenance_rows,
    resolve_quarantine_for_rows,
)
from ai_trading_system.domains.features.feature_store import FeatureStore
from ai_trading_system.domains.ingest.daily_update_runner import (
    _rows_to_symbol_frames,
    _with_default_trust_metadata,
    apply_adjustment_fields,
)
from ai_trading_system.domains.ingest.price_continuity import require_no_bulk_raw_price_basis_shifts
from ai_trading_system.domains.ingest.providers.bse import BSECollector
from ai_trading_system.domains.ingest.providers.dhan import DhanCollector
from ai_trading_system.domains.ingest.validation import validate_ohlcv_frame
from ai_trading_system.platform.db.paths import get_domain_paths, require_data_root_available
from ai_trading_system.platform.utils.env import load_project_env


FEATURE_TYPES = ["rsi", "adx", "sma", "ema", "macd", "atr", "bb", "roc", "supertrend"]


def _weekday_dates(from_date: str, to_date: str) -> list[str]:
    if date.fromisoformat(from_date) > date.fromisoformat(to_date):
        return []
    return [stamp.date().isoformat() for stamp in pd.bdate_range(from_date, to_date)]


def _load_bse_symbols(master_db_path: Path, symbols: Iterable[str] | None = None) -> list[dict[str, Any]]:
    requested = sorted({str(symbol).strip().upper() for symbol in symbols or [] if str(symbol).strip()})
    conn = sqlite3.connect(f"file:{master_db_path}?mode=ro", uri=True)
    try:
        sql = """
            SELECT symbol_id, security_id, symbol_name, exchange, instrument_type, isin, bse_symbol
            FROM symbols
            WHERE exchange = ?
        """
        params: list[Any] = ["BSE"]
        if requested:
            sql += f" AND UPPER(symbol_id) IN ({', '.join(['?'] * len(requested))})"
            params.extend(requested)
        sql += " ORDER BY symbol_id"
        columns = ["symbol_id", "security_id", "symbol_name", "exchange", "instrument_type", "isin", "bse_symbol"]
        rows = [dict(zip(columns, row)) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()
    if requested:
        found = {str(row["symbol_id"]).upper() for row in rows}
        missing = sorted(set(requested) - found)
        if missing:
            raise ValueError(f"Requested symbols are not active BSE master rows: {missing}")
    return rows


def _clean_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output.columns = [str(column).replace("\ufeff", "").strip() for column in output.columns]
    return output


def _normalize_bse_bhavcopy_frame(
    raw_frame: pd.DataFrame,
    trade_date: str,
    symbol_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    columns = [
        "symbol_id", "security_id", "exchange", "timestamp", "open", "high", "low", "close",
        "volume", "isin", "series", "trading_segment",
    ]
    if raw_frame is None or raw_frame.empty:
        return pd.DataFrame(columns=columns)
    frame = _clean_columns(raw_frame)
    rename = {
        "SC_CODE": "source_security_id", "SC_GROUP": "series", "OPEN": "open", "HIGH": "high",
        "LOW": "low", "CLOSE": "close", "NO_OF_SHRS": "volume", "FinInstrmId": "source_security_id",
        "ISIN": "source_isin", "SctySrs": "series", "OpnPric": "open", "HghPric": "high",
        "LwPric": "low", "ClsPric": "close", "TtlTradgVol": "volume", "TradDt": "source_trade_date",
    }
    frame = frame.rename(columns=rename)
    required = {"source_security_id", "open", "high", "low", "close", "volume"}
    if not required.issubset(frame.columns):
        raise ValueError(f"Unsupported BSE bhavcopy schema; missing {sorted(required - set(frame.columns))}")
    if "source_trade_date" in frame.columns:
        source_dates = set(pd.to_datetime(frame["source_trade_date"], errors="coerce").dropna().dt.date.astype(str))
        if source_dates and source_dates != {trade_date}:
            raise ValueError(f"BSE bhavcopy date mismatch for {trade_date}: {sorted(source_dates)}")

    by_security = {str(row["security_id"]).removesuffix(".0"): row for row in symbol_rows}
    normalized_security_ids = (
        pd.to_numeric(frame["source_security_id"], errors="coerce")
        .astype("Int64")
        .astype(str)
        .str.replace("<NA>", "", regex=False)
    )
    frame = frame.assign(source_security_id=normalized_security_ids)
    frame = frame[frame["source_security_id"].isin(by_security)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame.loc[:, "symbol_id"] = frame["source_security_id"].map(lambda value: str(by_security[value]["symbol_id"]))
    frame.loc[:, "security_id"] = frame["source_security_id"]
    frame.loc[:, "isin"] = frame["source_security_id"].map(lambda value: by_security[value].get("isin"))
    frame.loc[:, "exchange"] = "BSE"
    frame.loc[:, "timestamp"] = pd.to_datetime(trade_date)
    frame.loc[:, "series"] = frame.get("series", pd.Series(index=frame.index, dtype=object)).fillna("").astype(str).str.strip().str.upper()
    frame.loc[:, "trading_segment"] = "bse_cash"
    for field in ["open", "high", "low", "close", "volume"]:
        frame.loc[:, field] = pd.to_numeric(frame[field], errors="coerce")
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    frame.loc[:, "volume"] = frame["volume"].fillna(0).astype("int64")
    return frame.loc[:, columns].drop_duplicates(["symbol_id", "exchange", "timestamp"], keep="last")


def _fetch_bse_rows(
    *,
    raw_dir: Path,
    trade_dates: list[str],
    symbol_rows: list[dict[str, Any]],
    max_workers: int = 4,
) -> tuple[pd.DataFrame, list[str], list[str], list[str]]:
    parts: list[pd.DataFrame] = []
    source_sessions: list[str] = []
    missing_weekdays: list[str] = []
    sessions_without_target_trades: list[str] = []
    thread_state = threading.local()

    def fetch_one(trade_date: str) -> tuple[str, pd.DataFrame]:
        collector = getattr(thread_state, "collector", None)
        if collector is None:
            collector = BSECollector(data_dir=str(raw_dir))
            thread_state.collector = collector
        raw = collector.get_bhavcopy(trade_date)
        return trade_date, raw

    workers = max(1, min(int(max_workers), 8))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for trade_date, raw in executor.map(fetch_one, trade_dates):
            if raw.empty:
                missing_weekdays.append(trade_date)
                continue
            source_sessions.append(trade_date)
            normalized = _normalize_bse_bhavcopy_frame(raw, trade_date, symbol_rows)
            if normalized.empty:
                sessions_without_target_trades.append(trade_date)
                continue
            parts.append(normalized)
    rows = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    return rows, source_sessions, missing_weekdays, sessions_without_target_trades


def _partition_source_anomalies(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if rows is None or rows.empty:
        return pd.DataFrame(), pd.DataFrame()
    invalid = (
        rows[["open", "high", "low", "close"]].isna().any(axis=1)
        | (rows["volume"] < 0)
        | (rows["high"] < rows["low"])
        | (rows["high"] < rows["open"])
        | (rows["high"] < rows["close"])
        | (rows["low"] > rows["open"])
        | (rows["low"] > rows["close"])
    )
    return rows.loc[~invalid].copy(), rows.loc[invalid].copy()


def _load_adjacent_rows(db_path: Path, symbol_ids: list[str], from_date: str, to_date: str) -> pd.DataFrame:
    if not symbol_ids:
        return pd.DataFrame(columns=["symbol_id", "timestamp", "close"])
    placeholders = ", ".join(["?"] * len(symbol_ids))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(
            f"""
            WITH ranked AS (
                SELECT symbol_id, timestamp, close,
                       ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp DESC) AS before_rank,
                       NULL::BIGINT AS after_rank
                FROM _catalog
                WHERE exchange = ? AND CAST(timestamp AS DATE) < CAST(? AS DATE)
                  AND symbol_id IN ({placeholders})
                UNION ALL
                SELECT symbol_id, timestamp, close, NULL::BIGINT,
                       ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY timestamp ASC)
                FROM _catalog
                WHERE exchange = ? AND CAST(timestamp AS DATE) > CAST(? AS DATE)
                  AND symbol_id IN ({placeholders})
            )
            SELECT symbol_id, timestamp, close FROM ranked
            WHERE before_rank = 1 OR after_rank = 1
            """,
            ["BSE", from_date, *symbol_ids, "BSE", to_date, *symbol_ids],
        ).fetchdf()
    finally:
        conn.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _backup_store(db_path: Path, backup_dir: Path, rows: pd.DataFrame) -> dict[str, str]:
    backup_dir.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    db_backup = backup_dir / db_path.name
    shutil.copy2(db_path, db_backup)
    row_backup = backup_dir / "target_catalog_rows.parquet"
    rows.to_parquet(row_backup, index=False)
    return {"database": str(db_backup), "database_sha256": _sha256(db_backup), "rows": str(row_backup)}


def _load_existing_window(db_path: Path, symbols: list[str], from_date: str, to_date: str) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(symbols))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(
            f"""SELECT * FROM _catalog WHERE exchange = ?
                AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                AND symbol_id IN ({placeholders}) ORDER BY symbol_id, timestamp""",
            ["BSE", from_date, to_date, *symbols],
        ).fetchdf()
    finally:
        conn.close()


def _load_latest_bse_dates(db_path: Path, symbol_ids: list[str]) -> dict[str, date]:
    if not symbol_ids:
        return {}
    placeholders = ", ".join(["?"] * len(symbol_ids))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT symbol_id, MAX(CAST(timestamp AS DATE))
            FROM _catalog
            WHERE exchange = ? AND symbol_id IN ({placeholders})
            GROUP BY symbol_id
            """,
            ["BSE", *symbol_ids],
        ).fetchall()
    finally:
        conn.close()
    return {str(symbol_id): latest for symbol_id, latest in rows if latest is not None}


def update_bse_bhavcopy_incremental(
    *,
    project_root: Path,
    target_end_date: str,
    run_id: str | None = None,
    symbol_limit: int | None = None,
    max_workers: int = 4,
) -> dict[str, Any]:
    """Incrementally refresh mastered BSE-only symbols for the daily ingest stage."""
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    require_data_root_available(paths)
    symbol_rows = _load_bse_symbols(paths.master_db_path)
    if symbol_limit is not None:
        symbol_rows = symbol_rows[: max(0, int(symbol_limit))]
    if not symbol_rows:
        return {
            "status": "skipped",
            "reason": "no_bse_symbols",
            "rows_written": 0,
            "updated_symbols": [],
            "source_sessions": [],
            "missing_weekdays": [],
            "source_anomalies": [],
        }

    target = date.fromisoformat(target_end_date)
    symbol_ids = [str(row["symbol_id"]) for row in symbol_rows]
    latest_dates = _load_latest_bse_dates(paths.ohlcv_db_path, symbol_ids)
    required_start = {
        symbol_id: (pd.Timestamp(latest_dates[symbol_id]) + pd.Timedelta(days=1)).date()
        if symbol_id in latest_dates
        else target
        for symbol_id in symbol_ids
    }
    pending = {symbol_id: start for symbol_id, start in required_start.items() if start <= target}
    if not pending:
        return {
            "status": "up_to_date",
            "rows_written": 0,
            "updated_symbols": [],
            "source_sessions": [],
            "missing_weekdays": [],
            "source_anomalies": [],
            "target_end_date": target_end_date,
        }

    earliest = min(pending.values()).isoformat()
    trade_dates = _weekday_dates(earliest, target_end_date)
    raw_dir = paths.raw_dir / "BSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rows, source_sessions, missing_weekdays, no_target_sessions = _fetch_bse_rows(
        raw_dir=raw_dir,
        trade_dates=trade_dates,
        symbol_rows=symbol_rows,
        max_workers=max_workers,
    )
    if not rows.empty:
        start_by_symbol = {symbol_id: pd.Timestamp(start) for symbol_id, start in pending.items()}
        rows = rows[
            rows.apply(
                lambda row: str(row["symbol_id"]) in start_by_symbol
                and pd.Timestamp(row["timestamp"]) >= start_by_symbol[str(row["symbol_id"])],
                axis=1,
            )
        ].copy()
    rows, source_anomalies = _partition_source_anomalies(rows)
    effective_run_id = run_id or f"bse-daily-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    if not rows.empty:
        rows.loc[:, "provider"] = "bse_bhavcopy"
        rows.loc[:, "provider_priority"] = 1
        rows.loc[:, "validation_status"] = "trusted_primary"
        rows.loc[:, "validated_against"] = None
        rows.loc[:, "ingest_run_id"] = effective_run_id
        rows.loc[:, "repair_batch_id"] = None
        rows = _with_default_trust_metadata(rows, run_id=effective_run_id)
        rows.loc[:, "instrument_type"] = "equity"
        rows.loc[:, "is_benchmark"] = False
        rows = annotate_provider_reconciliation(rows)
        rows = apply_adjustment_fields(rows)
        validate_ohlcv_frame(rows, source_label=f"{effective_run_id}:bse_daily_prewrite")

    rows_written = 0
    updated_symbols: list[str] = []
    if not rows.empty:
        ensure_data_trust_schema(paths.ohlcv_db_path)
        record_provenance_rows(paths.ohlcv_db_path, rows)
        collector = DhanCollector(
            db_path=str(paths.ohlcv_db_path),
            masterdb_path=str(paths.master_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            data_domain="operational",
        )
        rows_written = int(collector._upsert_ohlcv(_rows_to_symbol_frames(rows)) or 0)
        resolve_quarantine_for_rows(
            paths.ohlcv_db_path,
            rows,
            note=f"Resolved by daily BSE ingest {effective_run_id}",
        )
        updated_symbols = sorted(rows["symbol_id"].astype(str).unique().tolist())

    if not source_anomalies.empty:
        ensure_data_trust_schema(paths.ohlcv_db_path)
        symbol_lookup = {str(row["symbol_id"]): row for row in symbol_rows}
        for anomaly in source_anomalies.itertuples(index=False):
            symbol_row = symbol_lookup.get(str(anomaly.symbol_id))
            if symbol_row is None:
                continue
            quarantine_symbol_dates(
                paths.ohlcv_db_path,
                symbol_rows=[symbol_row],
                trade_dates=[pd.Timestamp(anomaly.timestamp).date().isoformat()],
                reason="invalid_official_source_ohlc",
                status="observed",
                source_run_id=effective_run_id,
                note="Official BSE daily bhavcopy row failed OHLC consistency validation.",
            )

    return {
        "status": "updated" if rows_written else "no_rows",
        "target_end_date": target_end_date,
        "rows_written": rows_written,
        "updated_symbols": updated_symbols,
        "source_sessions": source_sessions,
        "missing_weekdays": missing_weekdays,
        "sessions_without_target_trades": no_target_sessions,
        "source_anomaly_count": int(len(source_anomalies)),
        "source_anomalies": (
            source_anomalies[
                ["symbol_id", "security_id", "timestamp", "open", "high", "low", "close", "volume"]
            ].to_dict("records")
            if not source_anomalies.empty
            else []
        ),
    }


def backfill_bse_bhavcopy(
    *, project_root: Path, from_date: str, to_date: str, symbols: list[str] | None = None,
    apply: bool = False, recompute_features: bool = True, max_workers: int = 4,
) -> dict[str, Any]:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    require_data_root_available(paths)
    symbol_rows = _load_bse_symbols(paths.master_db_path, symbols)
    if not symbol_rows:
        raise RuntimeError("No BSE symbols are available in masterdata.db.")
    trade_dates = _weekday_dates(from_date, to_date)
    raw_dir = paths.raw_dir / "BSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rows, source_sessions, missing_weekdays, no_target_sessions = _fetch_bse_rows(
        raw_dir=raw_dir,
        trade_dates=trade_dates,
        symbol_rows=symbol_rows,
        max_workers=max_workers,
    )
    raw_candidate_count = int(len(rows))
    rows, source_anomalies = _partition_source_anomalies(rows)
    run_id = f"bse-bhavcopy-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    if not rows.empty:
        rows.loc[:, "provider"] = "bse_bhavcopy"
        rows.loc[:, "provider_priority"] = 1
        rows.loc[:, "validation_status"] = "trusted_primary"
        rows.loc[:, "validated_against"] = None
        rows.loc[:, "ingest_run_id"] = run_id
        rows.loc[:, "repair_batch_id"] = run_id
        rows = _with_default_trust_metadata(rows, run_id=run_id)
        rows.loc[:, "instrument_type"] = "equity"
        rows.loc[:, "is_benchmark"] = False
        rows = annotate_provider_reconciliation(rows)
        rows = apply_adjustment_fields(rows)

    symbol_ids = [str(row["symbol_id"]) for row in symbol_rows]
    per_symbol = {}
    if not rows.empty:
        for symbol_id, part in rows.groupby("symbol_id"):
            per_symbol[str(symbol_id)] = {
                "rows": int(len(part)),
                "first_date": pd.to_datetime(part["timestamp"]).min().date().isoformat(),
                "last_date": pd.to_datetime(part["timestamp"]).max().date().isoformat(),
            }
    report: dict[str, Any] = {
        "run_id": run_id, "from_date": from_date, "to_date": to_date, "apply": bool(apply),
        "symbol_count": len(symbol_rows), "candidate_rows": int(len(rows)),
        "raw_candidate_rows": raw_candidate_count,
        "source_anomaly_count": int(len(source_anomalies)),
        "source_anomalies": (
            source_anomalies[
                ["symbol_id", "security_id", "timestamp", "open", "high", "low", "close", "volume"]
            ].to_dict("records")
            if not source_anomalies.empty
            else []
        ),
        "source_session_count": len(source_sessions), "source_sessions": source_sessions,
        "missing_weekday_count": len(missing_weekdays), "missing_weekdays": missing_weekdays,
        "sessions_without_target_trades": no_target_sessions, "symbols": per_symbol,
        "rows_written": 0, "backup": None, "feature_result": {},
    }
    if not apply:
        return report
    if rows.empty:
        raise RuntimeError("Official BSE bhavcopies returned no rows for the requested symbols/window.")

    candidate_continuity = pd.concat(
        [_load_adjacent_rows(paths.ohlcv_db_path, symbol_ids, from_date, to_date), rows[["symbol_id", "timestamp", "close"]]],
        ignore_index=True,
    )
    require_no_bulk_raw_price_basis_shifts(candidate_continuity, operation=f"BSE bhavcopy backfill {from_date}..{to_date}")
    existing = _load_existing_window(paths.ohlcv_db_path, symbol_ids, from_date, to_date)
    backup_dir = paths.root_dir / "backups" / run_id
    report["backup"] = _backup_store(paths.ohlcv_db_path, backup_dir, existing)

    ensure_data_trust_schema(paths.ohlcv_db_path)
    validate_ohlcv_frame(rows, source_label=f"{run_id}:prewrite")
    record_provenance_rows(paths.ohlcv_db_path, rows)
    collector = DhanCollector(
        db_path=str(paths.ohlcv_db_path), masterdb_path=str(paths.master_db_path),
        feature_store_dir=str(paths.feature_store_dir), data_domain="operational",
    )
    report["rows_written"] = int(collector._upsert_ohlcv(_rows_to_symbol_frames(rows)) or 0)
    resolve_quarantine_for_rows(paths.ohlcv_db_path, rows, note=f"Resolved by {run_id}")
    if not source_anomalies.empty:
        symbol_lookup = {str(row["symbol_id"]): row for row in symbol_rows}
        for anomaly in source_anomalies.itertuples(index=False):
            symbol_row = symbol_lookup.get(str(anomaly.symbol_id))
            if symbol_row is None:
                continue
            quarantine_symbol_dates(
                paths.ohlcv_db_path,
                symbol_rows=[symbol_row],
                trade_dates=[pd.Timestamp(anomaly.timestamp).date().isoformat()],
                reason="invalid_official_source_ohlc",
                status="observed",
                repair_batch_id=run_id,
                note="Official BSE bhavcopy OHLC failed the shared write-boundary consistency check.",
            )
    if recompute_features:
        feature_store = FeatureStore(
            ohlcv_db_path=str(paths.ohlcv_db_path), feature_store_dir=str(paths.feature_store_dir),
            data_domain="operational",
        )
        report["feature_result"] = feature_store.compute_and_store_features(
            symbols=sorted(per_symbol), exchanges=["BSE"], feature_types=FEATURE_TYPES,
            incremental=True, tail_bars=252, full_rebuild=False,
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BSE equities from official daily bhavcopies.")
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-features", action="store_true")
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parents[4]
    load_project_env(project_root)
    report = backfill_bse_bhavcopy(
        project_root=project_root, from_date=args.from_date, to_date=args.to_date,
        symbols=args.symbols, apply=bool(args.apply), recompute_features=not bool(args.skip_features),
        max_workers=max(1, min(int(args.max_workers), 8)),
    )
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
