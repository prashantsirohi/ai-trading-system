"""Standalone diagnostics for Dhan OHLC corruption investigation.

This module intentionally does not modify the production pipeline. It is a
separate investigation tool to isolate Dhan data issues and produce a
structured fix strategy report.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import duckdb
import pandas as pd
import requests

from ai_trading_system.domains.ingest.daily_update_runner import _fetch_nse_bhavcopy_rows, _fetch_yfinance_rows
from ai_trading_system.domains.ingest.providers.dhan import DhanCollector, normalize_dhan_timestamps_ist
from core.env import load_project_env
from ai_trading_system.platform.db.paths import ensure_domain_layout

FIELDS = ["open", "high", "low", "close", "volume"]


@dataclass
class SymbolDiagnostic:
    symbol_id: str
    security_id: str
    exchange: str
    dhan_rows: int
    db_rows: int
    reference_rows: int
    issue_tags: list[str]
    mismatch_dates_db: list[str]
    mismatch_dates_reference: list[str]
    shift_score_db: float
    shift_score_reference: float
    scale_ratio_db: float | None
    scale_ratio_reference: float | None


def _normalize_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["trade_date", *FIELDS])
    df = frame.copy()
    if "timestamp" in df.columns:
        normalized_ts = normalize_dhan_timestamps_ist(df["timestamp"])
        df = df.assign(trade_date=pd.to_datetime(normalized_ts).dt.strftime("%Y-%m-%d"))
    elif "trade_date" not in df.columns:
        raise ValueError("frame must include timestamp or trade_date")
    keep = ["trade_date", *FIELDS]
    for column in keep:
        if column not in df.columns:
            df[column] = pd.NA
    df = df[keep].copy()
    df = df.assign(**{field: pd.to_numeric(df[field], errors="coerce") for field in FIELDS})
    df = df[df["trade_date"].notna()].copy()
    df = df[df["trade_date"] != "NaT"].copy()
    return df.sort_values("trade_date").drop_duplicates("trade_date", keep="last").reset_index(drop=True)


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
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY timestamp
            """,
            [symbol_id, exchange, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()


def _fetch_dhan_window(
    collector: DhanCollector,
    symbol_info: dict[str, Any],
    from_date: str,
    to_date: str,
) -> pd.DataFrame:
    session = requests.Session()
    try:
        frame = collector._fetch_sync(
            security_id=str(symbol_info["security_id"]),
            exchange=str(symbol_info.get("exchange", "NSE")),
            from_date=from_date,
            to_date=to_date,
            session=session,
        )
    finally:
        session.close()
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["timestamp", *FIELDS])
    return frame.reset_index()[["timestamp", *FIELDS]]


def _date_set(frame: pd.DataFrame) -> set[date]:
    if frame.empty:
        return set()
    out: set[date] = set()
    for item in frame["trade_date"].astype(str):
        if not item or item == "NaT":
            continue
        try:
            out.add(date.fromisoformat(item))
        except ValueError:
            continue
    return out


def _shift_score(base_dates: set[date], target_dates: set[date], offset_days: int = 1) -> float:
    if not base_dates or not target_dates:
        return 0.0
    shifted = {day + timedelta(days=offset_days) for day in base_dates}
    overlap = len(shifted.intersection(target_dates))
    return overlap / float(min(len(base_dates), len(target_dates)))


def _scale_ratio(left: pd.DataFrame, right: pd.DataFrame, field: str = "close") -> float | None:
    merged = left.merge(right, on="trade_date", suffixes=("_left", "_right"))
    if merged.empty:
        return None
    ratios: list[float] = []
    for _, row in merged.iterrows():
        lv = row.get(f"{field}_left")
        rv = row.get(f"{field}_right")
        if pd.isna(lv) or pd.isna(rv):
            continue
        lv = float(lv)
        rv = float(rv)
        if lv == 0.0 or rv == 0.0:
            continue
        ratios.append(abs(lv / rv))
    if not ratios:
        return None
    return float(pd.Series(ratios).median())


def _mismatch_dates(left: pd.DataFrame, right: pd.DataFrame) -> list[str]:
    mismatches: list[str] = []
    left_rows = left.set_index("trade_date")[FIELDS].to_dict("index") if not left.empty else {}
    right_rows = right.set_index("trade_date")[FIELDS].to_dict("index") if not right.empty else {}
    for trade_date in sorted(set(left_rows) | set(right_rows)):
        left_row = left_rows.get(trade_date, {})
        right_row = right_rows.get(trade_date, {})
        for field in FIELDS:
            lv = left_row.get(field)
            rv = right_row.get(field)
            if pd.isna(lv) and pd.isna(rv):
                continue
            if pd.isna(lv) != pd.isna(rv):
                mismatches.append(str(trade_date))
                break
            if float(lv) != float(rv):
                mismatches.append(str(trade_date))
                break
    return sorted(set(mismatches))


def _build_reference_map(
    project_root: Path,
    symbols: list[dict[str, Any]],
    from_date: str,
    to_date: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    security_map = {str(row["symbol_id"]): row for row in symbols}
    trade_dates = [ts.date().isoformat() for ts in pd.bdate_range(from_date, to_date)]
    raw_dir = project_root / "data" / "raw" / "NSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    nse_rows, nse_dates, missing_dates = _fetch_nse_bhavcopy_rows(
        raw_dir=raw_dir,
        trade_dates=trade_dates,
        security_map=security_map,
    )
    yf_rows = pd.DataFrame()
    if missing_dates:
        yf_rows = _fetch_yfinance_rows(symbol_rows=symbols, trade_dates=missing_dates, batch_size=100)

    all_rows = []
    if not nse_rows.empty:
        nse_rows = nse_rows.copy()
        nse_rows["provider"] = "nse_bhavcopy"
        all_rows.append(nse_rows)
    if not yf_rows.empty:
        yf_rows = yf_rows.copy()
        yf_rows["provider"] = "yfinance"
        all_rows.append(yf_rows)
    joined = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

    reference_map: dict[str, pd.DataFrame] = {}
    if not joined.empty:
        for symbol_id, part in joined.groupby("symbol_id"):
            reference_map[str(symbol_id)] = _normalize_trade_frame(part)

    summary = {
        "nse_dates": sorted(set(nse_dates)),
        "yfinance_dates": sorted(set(yf_rows["timestamp"].dt.date.astype(str).tolist())) if not yf_rows.empty else [],
        "missing_dates_after_fallback": sorted(set(missing_dates) - (set(yf_rows["timestamp"].dt.date.astype(str).tolist()) if not yf_rows.empty else set())),
    }
    return reference_map, summary


def build_fix_strategy(issue_counts: dict[str, int]) -> list[str]:
    steps: list[str] = []
    if issue_counts.get("db_vs_dhan_mismatch", 0) > 0:
        steps.append("Rebuild suspect window from trusted source and do not use current DB rows as truth for that window.")
    if issue_counts.get("dhan_vs_reference_mismatch", 0) > 0:
        steps.append("Demote Dhan to validation-only for daily OHLC and keep NSE/yfinance as ranking source of record.")
    if issue_counts.get("possible_one_day_shift", 0) > 0:
        steps.append("Audit Dhan timestamp parsing path for day-boundary shift and verify timestamp unit detection with raw payload snapshots.")
    if issue_counts.get("possible_scale_issue", 0) > 0:
        steps.append("Add scale-ratio guardrails before writes and quarantine symbols when close ratio exceeds safe bounds.")
    if issue_counts.get("missing_in_reference", 0) > 0:
        steps.append("For symbols missing in reference sources, quarantine and skip trading/ranking until a validated source is available.")
    if not steps:
        steps.append("No critical corruption signatures detected in sampled symbols.")
    return steps


def run_diagnostics(
    *,
    project_root: Path,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    symbol_limit: int = 100,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    collector = DhanCollector(
        db_path=str(paths.ohlcv_db_path),
        masterdb_path=str(paths.master_db_path),
        feature_store_dir=str(paths.feature_store_dir),
        data_domain="operational",
    )
    universe = collector.get_symbols_from_masterdb(exchanges=[exchange])
    if symbols:
        wanted = {item.upper() for item in symbols}
        universe = [row for row in universe if str(row["symbol_id"]).upper() in wanted]
    if symbol_limit:
        universe = universe[:symbol_limit]
    if not universe:
        raise RuntimeError("No symbols available for Dhan diagnostics.")

    reference_map, reference_summary = _build_reference_map(project_root, universe, from_date, to_date)

    diagnostics: list[SymbolDiagnostic] = []
    issue_counts: dict[str, int] = {
        "db_vs_dhan_mismatch": 0,
        "dhan_vs_reference_mismatch": 0,
        "possible_one_day_shift": 0,
        "possible_scale_issue": 0,
        "missing_in_reference": 0,
        "missing_in_dhan": 0,
    }

    for info in universe:
        symbol_id = str(info["symbol_id"])
        security_id = str(info["security_id"])
        dhan_frame = _normalize_trade_frame(_fetch_dhan_window(collector, info, from_date, to_date))
        db_frame = _normalize_trade_frame(_load_db_window(paths.ohlcv_db_path, symbol_id, exchange, from_date, to_date))
        reference_frame = _normalize_trade_frame(reference_map.get(symbol_id, pd.DataFrame()))

        mismatch_db = _mismatch_dates(dhan_frame, db_frame) if not dhan_frame.empty or not db_frame.empty else []
        mismatch_ref = _mismatch_dates(dhan_frame, reference_frame) if not dhan_frame.empty or not reference_frame.empty else []
        shift_db = _shift_score(_date_set(dhan_frame), _date_set(db_frame), offset_days=1)
        shift_ref = _shift_score(_date_set(dhan_frame), _date_set(reference_frame), offset_days=1)
        ratio_db = _scale_ratio(dhan_frame, db_frame)
        ratio_ref = _scale_ratio(dhan_frame, reference_frame)

        tags: list[str] = []
        if mismatch_db:
            tags.append("db_vs_dhan_mismatch")
            issue_counts["db_vs_dhan_mismatch"] += 1
        if mismatch_ref:
            tags.append("dhan_vs_reference_mismatch")
            issue_counts["dhan_vs_reference_mismatch"] += 1
        if shift_db >= 0.6 or shift_ref >= 0.6:
            tags.append("possible_one_day_shift")
            issue_counts["possible_one_day_shift"] += 1
        for ratio in (ratio_db, ratio_ref):
            if ratio is not None and (ratio >= 5.0 or ratio <= 0.2):
                tags.append("possible_scale_issue")
                issue_counts["possible_scale_issue"] += 1
                break
        if reference_frame.empty:
            tags.append("missing_in_reference")
            issue_counts["missing_in_reference"] += 1
        if dhan_frame.empty:
            tags.append("missing_in_dhan")
            issue_counts["missing_in_dhan"] += 1

        diagnostics.append(
            SymbolDiagnostic(
                symbol_id=symbol_id,
                security_id=security_id,
                exchange=exchange,
                dhan_rows=int(len(dhan_frame)),
                db_rows=int(len(db_frame)),
                reference_rows=int(len(reference_frame)),
                issue_tags=sorted(set(tags)),
                mismatch_dates_db=mismatch_db,
                mismatch_dates_reference=mismatch_ref,
                shift_score_db=float(shift_db),
                shift_score_reference=float(shift_ref),
                scale_ratio_db=ratio_db,
                scale_ratio_reference=ratio_ref,
            )
        )

    strategy = build_fix_strategy(issue_counts)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_dir = project_root / "reports" / "dhan_diagnostics" / f"{from_date}_to_{to_date}_{run_stamp}"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "from_date": from_date,
        "to_date": to_date,
        "exchange": exchange,
        "sample_count": len(universe),
        "issue_counts": issue_counts,
        "reference_summary": reference_summary,
        "strategy": strategy,
        "symbols": [
            {
                "symbol_id": item.symbol_id,
                "security_id": item.security_id,
                "exchange": item.exchange,
                "dhan_rows": item.dhan_rows,
                "db_rows": item.db_rows,
                "reference_rows": item.reference_rows,
                "issue_tags": item.issue_tags,
                "mismatch_dates_db": item.mismatch_dates_db,
                "mismatch_dates_reference": item.mismatch_dates_reference,
                "shift_score_db": item.shift_score_db,
                "shift_score_reference": item.shift_score_reference,
                "scale_ratio_db": item.scale_ratio_db,
                "scale_ratio_reference": item.scale_ratio_reference,
            }
            for item in diagnostics
        ],
    }
    report_path = report_dir / "diagnostic_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    summary_frame = pd.DataFrame(
        [
            {
                "symbol_id": item.symbol_id,
                "security_id": item.security_id,
                "dhan_rows": item.dhan_rows,
                "db_rows": item.db_rows,
                "reference_rows": item.reference_rows,
                "issue_tags": ",".join(item.issue_tags),
                "shift_score_db": item.shift_score_db,
                "shift_score_reference": item.shift_score_reference,
                "scale_ratio_db": item.scale_ratio_db,
                "scale_ratio_reference": item.scale_ratio_reference,
            }
            for item in diagnostics
        ]
    )
    summary_frame.to_csv(report_dir / "symbol_diagnostics.csv", index=False)
    report["report_dir"] = str(report_dir)
    report["report_path"] = str(report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Isolated diagnostics for Dhan OHLC corruption analysis.")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--symbol-limit", type=int, default=100)
    parser.add_argument("--symbols", nargs="*", default=None)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[4]
    load_project_env(project_root)
    report = run_diagnostics(
        project_root=project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        exchange=args.exchange,
        symbol_limit=max(1, int(args.symbol_limit)),
        symbols=args.symbols,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
