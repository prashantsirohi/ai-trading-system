"""Repair a corrupted OHLCV window using NSE bhavcopy and yfinance fallback.

This tool is designed for operational repair runs after corruption has already
been detected in `_catalog`. It can:

1. compare stored OHLCV against fresh NSE/yfinance daily candles
2. back up the currently stored rows for the target window
3. overwrite only mismatched symbol/date rows
4. recompute technical features for the repaired symbols
5. recompute sector RS artifacts
6. emit a repair report under `reports/data_repairs/`
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb
import pandas as pd

from analytics.data_trust import (
    ensure_data_trust_schema,
    quarantine_symbol_dates,
    record_provenance_rows,
    resolve_quarantine_for_rows,
)
from analytics.registry import RegistryStore
from collectors.daily_update_runner import _fetch_nse_bhavcopy_rows, _fetch_yfinance_rows, _rows_to_symbol_frames
from collectors.dhan_collector import DhanCollector
from core.env import load_project_env
from ai_trading_system.domains.features import FeatureStore, compute_all_symbols_rs
from core.paths import ensure_domain_layout
from core.logging import logger


FIELDS = ["open", "high", "low", "close", "volume"]
FEATURE_TYPES = ["rsi", "adx", "sma", "ema", "macd", "atr", "bb", "roc", "supertrend"]


@dataclass
class ComparisonResult:
    symbol_id: str
    security_id: str
    db_rows: int
    api_rows: int
    mismatch_dates: list[str]
    mismatches: list[dict[str, Any]]
    api_frame: pd.DataFrame
    db_frame: pd.DataFrame

    @property
    def has_mismatch(self) -> bool:
        return bool(self.mismatch_dates)


def _normalize_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["trade_date", *FIELDS])
    df = frame.copy()
    if "timestamp" in df.columns:
        df["trade_date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)
    elif "trade_date" not in df.columns:
        raise ValueError("frame must include timestamp or trade_date")
    keep = ["trade_date", *FIELDS]
    missing = [column for column in keep if column not in df.columns]
    for column in missing:
        df[column] = pd.NA
    df = df[keep].copy()
    df = df.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    return df.reset_index(drop=True)


def _compare_trade_frames(
    symbol_id: str,
    security_id: str,
    db_frame: pd.DataFrame,
    api_frame: pd.DataFrame,
) -> ComparisonResult:
    db_norm = _normalize_trade_frame(db_frame)
    api_norm = _normalize_trade_frame(api_frame)
    merged = db_norm.merge(api_norm, on="trade_date", how="outer", suffixes=("_db", "_api")).sort_values("trade_date")

    mismatches: list[dict[str, Any]] = []
    mismatch_dates: list[str] = []
    for row in merged.to_dict(orient="records"):
        per_field: dict[str, dict[str, float | None]] = {}
        for field in FIELDS:
            db_value = row.get(f"{field}_db")
            api_value = row.get(f"{field}_api")
            if pd.isna(db_value) and pd.isna(api_value):
                continue
            if pd.isna(db_value) != pd.isna(api_value):
                per_field[field] = {
                    "db": None if pd.isna(db_value) else float(db_value),
                    "api": None if pd.isna(api_value) else float(api_value),
                }
                continue
            if float(db_value) != float(api_value):
                per_field[field] = {"db": float(db_value), "api": float(api_value)}
        if per_field:
            mismatch_dates.append(str(row["trade_date"]))
            mismatches.append({"trade_date": str(row["trade_date"]), "fields": per_field})

    return ComparisonResult(
        symbol_id=symbol_id,
        security_id=security_id,
        db_rows=len(db_norm),
        api_rows=len(api_norm),
        mismatch_dates=mismatch_dates,
        mismatches=mismatches,
        api_frame=api_norm,
        db_frame=db_norm,
    )


def _build_comparison_results(
    *,
    db_path: Path,
    available_symbols: list[dict[str, Any]],
    api_frame_map: dict[str, pd.DataFrame],
    exchange: str,
    from_date: str,
    to_date: str,
) -> list[ComparisonResult]:
    comparison_results: list[ComparisonResult] = []
    for info in available_symbols:
        symbol_id = str(info["symbol_id"])
        security_id = str(info["security_id"])
        db_frame = _load_db_window(
            db_path=db_path,
            symbol_id=symbol_id,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
        api_frame = api_frame_map.get(symbol_id)
        if api_frame is None or api_frame.empty:
            comparison_results.append(
                ComparisonResult(
                    symbol_id=symbol_id,
                    security_id=security_id,
                    db_rows=len(_normalize_trade_frame(db_frame)),
                    api_rows=0,
                    mismatch_dates=[],
                    mismatches=[],
                    api_frame=pd.DataFrame(),
                    db_frame=db_frame,
                )
            )
            continue
        comparison_results.append(_compare_trade_frames(symbol_id, security_id, db_frame, api_frame.reset_index()))
    return comparison_results


def _unresolved_dates_by_symbol(comparison_results: list[ComparisonResult]) -> dict[str, list[str]]:
    unresolved: dict[str, list[str]] = {}
    for result in comparison_results:
        if result.api_rows != 0 or result.db_rows <= 0:
            continue
        db_dates = (
            _normalize_trade_frame(result.db_frame)["trade_date"].astype(str).dropna().drop_duplicates().tolist()
        )
        if db_dates:
            unresolved[result.symbol_id] = sorted(db_dates)
    return unresolved


def _unresolved_dates(comparison_results: list[ComparisonResult]) -> list[str]:
    by_symbol = _unresolved_dates_by_symbol(comparison_results)
    return sorted({trade_date for dates in by_symbol.values() for trade_date in dates})


def _load_db_window(
    db_path: Path,
    symbol_id: str,
    exchange: str,
    from_date: str,
    to_date: str,
) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(
            """
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM _catalog
            WHERE symbol_id = ?
              AND exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
            ORDER BY timestamp
            """,
            [symbol_id, exchange, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()


def _backup_current_rows(
    db_path: Path,
    report_dir: Path,
    symbol_ids: Iterable[str],
    exchange: str,
    from_date: str,
    to_date: str,
) -> Path:
    symbols = sorted({str(symbol_id) for symbol_id in symbol_ids if symbol_id})
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        window_df = conn.execute(
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
                volume
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
            ORDER BY symbol_id, timestamp
            """,
            [exchange, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()

    backed_up = window_df[window_df["symbol_id"].isin(symbols)].copy()
    backup_path = report_dir / "catalog_backup.parquet"
    backed_up.to_parquet(backup_path, index=False)
    return backup_path


def _delete_window_rows(
    *,
    db_path: Path,
    symbol_ids: Iterable[str],
    exchange: str,
    from_date: str,
    to_date: str,
) -> int:
    symbols = sorted({str(symbol_id) for symbol_id in symbol_ids if symbol_id})
    if not symbols:
        return 0

    conn = duckdb.connect(str(db_path))
    try:
        placeholders = ", ".join(["?"] * len(symbols))
        params = [exchange, from_date, to_date, *symbols]
        deleted = conn.execute(
            f"""
            DELETE FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN ? AND ?
              AND symbol_id IN ({placeholders})
            RETURNING 1
            """,
            params,
        ).fetchall()
        return len(deleted)
    finally:
        conn.close()


def _fetch_symbol_frames(
    *,
    project_root: Path,
    symbols: list[dict[str, Any]],
    from_date: str,
    to_date: str,
    ingest_run_id: str | None = None,
    repair_batch_id: str | None = None,
) -> list[pd.DataFrame]:
    security_map = {str(row["symbol_id"]): row for row in symbols}
    raw_dir = project_root / "data" / "raw" / "NSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    trade_dates = [ts.date().isoformat() for ts in pd.bdate_range(from_date, to_date)]
    nse_rows, _, missing_dates = _fetch_nse_bhavcopy_rows(
        raw_dir=raw_dir,
        trade_dates=trade_dates,
        security_map=security_map,
    )
    if not nse_rows.empty:
        nse_rows["provider"] = "nse_bhavcopy"
        nse_rows["provider_priority"] = 1
        nse_rows["validation_status"] = "trusted_repaired"
        nse_rows["validated_against"] = "nse_bhavcopy"
        nse_rows["ingest_run_id"] = ingest_run_id
        nse_rows["repair_batch_id"] = repair_batch_id

    all_rows = nse_rows.copy() if not nse_rows.empty else pd.DataFrame()
    if missing_dates:
        yfinance_rows = _fetch_yfinance_rows(symbol_rows=symbols, trade_dates=missing_dates, batch_size=100)
        if not yfinance_rows.empty:
            yfinance_rows["provider"] = "yfinance"
            yfinance_rows["provider_priority"] = 2
            yfinance_rows["validation_status"] = "trusted_repaired"
            yfinance_rows["validated_against"] = "yfinance_fallback"
            yfinance_rows["ingest_run_id"] = ingest_run_id
            yfinance_rows["repair_batch_id"] = repair_batch_id
            all_rows = yfinance_rows if all_rows.empty else pd.concat([all_rows, yfinance_rows], ignore_index=True)

    return _rows_to_symbol_frames(all_rows)


def _write_api_archive(report_dir: Path, fetched_frames: list[pd.DataFrame]) -> Path:
    rows: list[pd.DataFrame] = []
    for frame in fetched_frames:
        if frame is None or frame.empty:
            continue
        info = frame.attrs.get("symbol_info", {})
        symbol_id = str(info.get("symbol_id", "UNKNOWN"))
        security_id = str(info.get("security_id", ""))
        exchange = str(info.get("exchange", "NSE"))
        normalized = frame.reset_index().copy()
        normalized["symbol_id"] = symbol_id
        normalized["security_id"] = security_id
        normalized["exchange"] = exchange
        rows.append(normalized)
    archive = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    archive_path = report_dir / "source_refetch.parquet"
    archive.to_parquet(archive_path, index=False)
    return archive_path


def _report_payload(
    *,
    from_date: str,
    to_date: str,
    exchange: str,
    sample_count: int,
    repaired_symbols: list[str],
    comparison_results: list[ComparisonResult],
    backup_path: Path | None,
    api_archive_path: Path | None,
    feature_result: dict[str, int] | None,
    report_dir: Path,
    apply_changes: bool,
    unresolved_symbol_count: int = 0,
    unresolved_date_count: int = 0,
    repair_status: str = "completed",
) -> dict[str, Any]:
    mismatch_by_date: dict[str, int] = {}
    for result in comparison_results:
        for trade_date in result.mismatch_dates:
            mismatch_by_date[trade_date] = mismatch_by_date.get(trade_date, 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "from_date": from_date,
        "to_date": to_date,
        "exchange": exchange,
        "sample_count": sample_count,
        "comparison_count": len(comparison_results),
        "mismatch_symbol_count": len(repaired_symbols),
        "mismatch_by_date": mismatch_by_date,
        "repaired_symbols": repaired_symbols,
        "apply_changes": apply_changes,
        "backup_path": str(backup_path) if backup_path else None,
        "source_archive_path": str(api_archive_path) if api_archive_path else None,
        "feature_result": feature_result or {},
        "report_dir": str(report_dir),
        "unresolved_symbol_count": int(unresolved_symbol_count),
        "unresolved_date_count": int(unresolved_date_count),
        "repair_status": repair_status,
        "symbols": [
            {
                "symbol_id": result.symbol_id,
                "security_id": result.security_id,
                "db_rows": result.db_rows,
                "api_rows": result.api_rows,
                "mismatch_dates": result.mismatch_dates,
                "mismatches": result.mismatches[:10],
            }
            for result in comparison_results
        ],
    }


def repair_window(
    *,
    project_root: Path,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    symbol_limit: int | None = None,
    symbols: list[str] | None = None,
    apply_changes: bool = False,
    max_concurrent: int = 5,
    recompute_features: bool = True,
    feature_tail_bars: int = 252,
) -> dict[str, Any]:
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    collector = DhanCollector(
        db_path=str(paths.ohlcv_db_path),
        masterdb_path=str(paths.master_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        data_domain="operational",
    )
    ensure_data_trust_schema(paths.ohlcv_db_path)
    available_symbols = collector.get_symbols_from_masterdb(exchanges=[exchange])
    if symbols:
        symbol_filter = {str(symbol).upper() for symbol in symbols}
        available_symbols = [row for row in available_symbols if str(row["symbol_id"]).upper() in symbol_filter]
    if symbol_limit is not None:
        available_symbols = available_symbols[:symbol_limit]
    if not available_symbols:
        raise RuntimeError("No symbols available for repair window.")

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_dir = project_root / "reports" / "data_repairs" / f"{from_date}_to_{to_date}_{run_stamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    fetched_frames = _fetch_symbol_frames(
        project_root=project_root,
        symbols=available_symbols,
        from_date=from_date,
        to_date=to_date,
        ingest_run_id=None,
        repair_batch_id=run_stamp,
    )

    api_frame_map: dict[str, pd.DataFrame] = {}
    for frame in fetched_frames:
        info = frame.attrs.get("symbol_info", {})
        symbol_id = str(info.get("symbol_id", ""))
        if symbol_id:
            api_frame_map[symbol_id] = frame

    comparison_results = _build_comparison_results(
        db_path=paths.ohlcv_db_path,
        available_symbols=available_symbols,
        api_frame_map=api_frame_map,
        exchange=exchange,
        from_date=from_date,
        to_date=to_date,
    )

    mismatched_symbols = [result.symbol_id for result in comparison_results if result.has_mismatch]
    backup_path: Path | None = None
    api_archive_path: Path | None = _write_api_archive(report_dir, fetched_frames)
    feature_result: dict[str, int] | None = None
    rows_written = 0

    unresolved_by_symbol = _unresolved_dates_by_symbol(comparison_results)
    unresolved_symbols = sorted(unresolved_by_symbol.keys())
    unresolved_dates = _unresolved_dates(comparison_results)

    if apply_changes and (mismatched_symbols or unresolved_symbols):
        rewrite_symbols = [symbol for symbol in mismatched_symbols if symbol in api_frame_map]
        backup_path = _backup_current_rows(
            db_path=paths.ohlcv_db_path,
            report_dir=report_dir,
            symbol_ids=rewrite_symbols,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
        _delete_window_rows(
            db_path=paths.ohlcv_db_path,
            symbol_ids=rewrite_symbols,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
        rewrite_frames = [api_frame_map[symbol] for symbol in rewrite_symbols]
        source_rows = pd.concat([frame.reset_index() for frame in rewrite_frames], ignore_index=True) if rewrite_frames else pd.DataFrame()
        if not source_rows.empty:
            record_provenance_rows(paths.ohlcv_db_path, source_rows)
        rows_written = int(collector._upsert_ohlcv(rewrite_frames) or 0)
        if not source_rows.empty:
            resolve_quarantine_for_rows(
                paths.ohlcv_db_path,
                source_rows,
                note=f"Resolved by repair batch {run_stamp}",
            )
        logger.info(
            "Repaired %s symbols across %s to %s; wrote %s OHLCV rows.",
            len(rewrite_frames),
            from_date,
            to_date,
            rows_written,
        )

        if recompute_features and rewrite_frames:
            fs = FeatureStore(
                ohlcv_db_path=str(paths.ohlcv_db_path),
                feature_store_dir=str(paths.feature_store_dir),
                data_domain="operational",
            )
            feature_result = fs.compute_and_store_features(
                symbols=mismatched_symbols,
                exchanges=[exchange],
                feature_types=FEATURE_TYPES,
                incremental=True,
                tail_bars=feature_tail_bars,
                full_rebuild=False,
            )
            compute_all_symbols_rs(
                db_path=str(paths.ohlcv_db_path),
                feature_store_dir=str(paths.feature_store_dir),
                masterdb_path=str(paths.master_db_path),
            )

        if unresolved_symbols:
            symbol_lookup = {str(row["symbol_id"]): row for row in available_symbols}
            for symbol_id, trade_dates in sorted(unresolved_by_symbol.items()):
                symbol_row = symbol_lookup.get(str(symbol_id))
                if not symbol_row or not trade_dates:
                    continue
                quarantine_symbol_dates(
                    paths.ohlcv_db_path,
                    symbol_rows=[symbol_row],
                    trade_dates=trade_dates,
                    reason="repair_source_unavailable",
                    status="observed",
                    repair_batch_id=run_stamp,
                    note="Repair sources returned no validated OHLC rows for the target symbol/date window.",
                )

        comparison_results = _build_comparison_results(
            db_path=paths.ohlcv_db_path,
            available_symbols=available_symbols,
            api_frame_map=api_frame_map,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
        mismatched_symbols = [result.symbol_id for result in comparison_results if result.has_mismatch]
        unresolved_by_symbol = _unresolved_dates_by_symbol(comparison_results)
        unresolved_symbols = sorted(unresolved_by_symbol.keys())
        unresolved_dates = _unresolved_dates(comparison_results)

    repair_status = "completed"
    if unresolved_symbols or mismatched_symbols:
        repair_status = "partial"

    report = _report_payload(
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        sample_count=len(available_symbols),
        repaired_symbols=mismatched_symbols,
        comparison_results=comparison_results,
        backup_path=backup_path,
        api_archive_path=api_archive_path,
        feature_result=feature_result,
        report_dir=report_dir,
        apply_changes=apply_changes,
        unresolved_symbol_count=len(set(unresolved_symbols)),
        unresolved_date_count=len(set(unresolved_dates)),
        repair_status=repair_status,
    )
    report_path = report_dir / "repair_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    RegistryStore(project_root).record_data_repair_run(
        repair_run_id=run_stamp,
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        status=repair_status,
        repaired_row_count=rows_written,
        unresolved_symbol_count=len(set(unresolved_symbols)),
        unresolved_date_count=len(set(unresolved_dates)),
        report_uri=str(report_path),
        metadata={
            "mismatch_symbol_count": len(mismatched_symbols),
            "sample_count": len(available_symbols),
        },
    )

    mismatch_frame = pd.DataFrame(
        [
            {
                "symbol_id": result.symbol_id,
                "security_id": result.security_id,
                "db_rows": result.db_rows,
                "api_rows": result.api_rows,
                "mismatch_count": len(result.mismatch_dates),
                "mismatch_dates": ",".join(result.mismatch_dates),
            }
            for result in comparison_results
        ]
    )
    mismatch_frame.to_csv(report_dir / "mismatch_summary.csv", index=False)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair a corrupted OHLCV window from NSE bhavcopy and yfinance fallback.")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--symbol-limit", type=int, default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--feature-tail-bars", type=int, default=252)
    parser.add_argument("--apply", action="store_true", help="Rewrite mismatched OHLCV rows and recompute features.")
    parser.add_argument("--skip-features", action="store_true", help="Skip feature/sector recomputation after repair.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[4]
    load_project_env(project_root)
    report = repair_window(
        project_root=project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        exchange=args.exchange,
        symbol_limit=args.symbol_limit,
        symbols=args.symbols,
        apply_changes=bool(args.apply),
        max_concurrent=max(1, int(args.max_concurrent)),
        recompute_features=not bool(args.skip_features),
        feature_tail_bars=max(30, int(args.feature_tail_bars)),
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
