"""Canonical reset/re-ingest/validate runner for OHLCV repair windows.

This utility is intentionally explicit and safe:
1. dry-run by default
2. optional backup of the deleted window
3. re-ingestion via NSE bhavcopy with yfinance fallback (repair flow)
4. final validation gate against bhavcopy/yfinance reference
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.ingest.repair import repair_window
from ai_trading_system.domains.ingest.trust import resolve_quarantine_for_rows
from core.env import load_project_env
from run.stages import IngestStage
from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.platform.db.paths import ensure_domain_layout


def _window_summary(db_path: Path, exchange: str, from_date: str, to_date: str) -> dict[str, Any]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows, symbols, min_ts, max_ts = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol_id) AS symbols,
                MIN(timestamp) AS min_ts,
                MAX(timestamp) AS max_ts
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            [exchange, from_date, to_date],
        ).fetchone()
    finally:
        conn.close()
    return {
        "rows": int(rows or 0),
        "symbols": int(symbols or 0),
        "min_timestamp": str(min_ts) if min_ts is not None else None,
        "max_timestamp": str(max_ts) if max_ts is not None else None,
    }


def _backup_window(
    *,
    db_path: Path,
    report_dir: Path,
    exchange: str,
    from_date: str,
    to_date: str,
) -> Path:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        frame = conn.execute(
            """
            SELECT *
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY symbol_id, timestamp
            """,
            [exchange, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()
    backup_path = report_dir / "catalog_window_backup.parquet"
    frame.to_parquet(backup_path, index=False)
    return backup_path


def _delete_window(*, db_path: Path, exchange: str, from_date: str, to_date: str) -> int:
    conn = duckdb.connect(str(db_path))
    try:
        deleted = conn.execute(
            """
            DELETE FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            RETURNING 1
            """,
            [exchange, from_date, to_date],
        ).fetchall()
        return len(deleted)
    finally:
        conn.close()


def _load_window_rows(*, db_path: Path, exchange: str, from_date: str, to_date: str):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(
            """
            SELECT symbol_id, exchange, timestamp
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            [exchange, from_date, to_date],
        ).fetchdf()
    finally:
        conn.close()


def run_reset_reingest_validate(
    *,
    project_root: Path,
    from_date: str,
    to_date: str,
    exchange: str = "NSE",
    apply: bool = False,
    skip_backup: bool = False,
    skip_features: bool = False,
    data_domain: str = "operational",
    validation_source: str = "auto",
    validation_date: str | None = None,
    min_coverage: float = 0.9,
    max_mismatch_ratio: float = 0.05,
    close_tolerance_pct: float = 0.01,
) -> dict[str, Any]:
    paths = ensure_domain_layout(project_root=project_root, data_domain=data_domain)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_dir = project_root / "reports" / "data_repairs" / f"reset_reingest_{from_date}_to_{to_date}_{run_stamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    before = _window_summary(paths.ohlcv_db_path, exchange, from_date, to_date)
    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "from_date": from_date,
        "to_date": to_date,
        "exchange": exchange,
        "data_domain": data_domain,
        "apply": bool(apply),
        "window_before": before,
        "report_dir": str(report_dir),
    }

    if not apply:
        payload["status"] = "dry_run"
        payload["note"] = "No data deleted. Re-run with --apply after reviewing window_before."
        return payload

    backup_path = None
    if not skip_backup:
        backup_path = _backup_window(
            db_path=paths.ohlcv_db_path,
            report_dir=report_dir,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
        payload["backup_path"] = str(backup_path)

    deleted_rows = _delete_window(
        db_path=paths.ohlcv_db_path,
        exchange=exchange,
        from_date=from_date,
        to_date=to_date,
    )
    payload["deleted_rows"] = int(deleted_rows)

    repair_report = repair_window(
        project_root=project_root,
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        apply_changes=True,
        recompute_features=not skip_features,
    )
    payload["repair_report"] = repair_report

    effective_validation_date = validation_date or to_date
    validation_context = StageContext(
        project_root=project_root,
        db_path=paths.ohlcv_db_path,
        run_id=f"reingest-validation-{run_stamp}",
        run_date=to_date,
        stage_name="ingest",
        attempt_number=1,
        params={
            "data_domain": data_domain,
            "include_delivery": False,
            "validate_bhavcopy_after_ingest": True,
            "bhavcopy_validation_required": True,
            "bhavcopy_validation_date": effective_validation_date,
            "bhavcopy_validation_source": validation_source,
            "bhavcopy_min_coverage": float(min_coverage),
            "bhavcopy_max_mismatch_ratio": float(max_mismatch_ratio),
            "bhavcopy_close_tolerance_pct": float(close_tolerance_pct),
        },
    )
    validation_result = IngestStage(operation=lambda _context: {}).run(validation_context).metadata
    payload["final_validation"] = validation_result
    payload["window_after"] = _window_summary(paths.ohlcv_db_path, exchange, from_date, to_date)
    resolved_quarantine_rows = 0
    if (
        str(validation_result.get("bhavcopy_validation_status") or "").lower() == "passed"
        and int(payload["window_after"]["rows"] or 0) > 0
    ):
        repaired_rows = _load_window_rows(
            db_path=paths.ohlcv_db_path,
            exchange=exchange,
            from_date=from_date,
            to_date=to_date,
        )
        resolved_quarantine_rows = resolve_quarantine_for_rows(
            paths.ohlcv_db_path,
            repaired_rows,
            note=f"Resolved by validated reset/reingest window {from_date} to {to_date}",
        )
    payload["resolved_quarantine_rows"] = int(resolved_quarantine_rows)
    payload["status"] = "completed"
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete and re-ingest an OHLCV window, then run final reference validation."
    )
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--exchange", default="NSE", help="Exchange code, defaults to NSE")
    parser.add_argument("--apply", action="store_true", help="Actually delete and re-ingest the window.")
    parser.add_argument("--skip-backup", action="store_true", help="Skip writing pre-delete window backup parquet.")
    parser.add_argument("--skip-features", action="store_true", help="Skip feature/sector recompute during re-ingestion.")
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Storage domain for db paths and artifacts.",
    )
    parser.add_argument(
        "--validation-source",
        choices=["auto", "bhavcopy", "yfinance"],
        default="auto",
        help="Final validation reference source (auto=bhavcopy->yfinance fallback).",
    )
    parser.add_argument(
        "--validation-date",
        default=None,
        help="Final validation date (YYYY-MM-DD). Defaults to --to-date.",
    )
    parser.add_argument("--min-coverage", type=float, default=0.9, help="Final validation minimum coverage ratio.")
    parser.add_argument("--max-mismatch-ratio", type=float, default=0.05, help="Final validation maximum mismatch ratio.")
    parser.add_argument("--close-tolerance-pct", type=float, default=0.01, help="Final validation close tolerance.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[4]
    load_project_env(project_root)
    result = run_reset_reingest_validate(
        project_root=project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        exchange=args.exchange,
        apply=bool(args.apply),
        skip_backup=bool(args.skip_backup),
        skip_features=bool(args.skip_features),
        data_domain=args.data_domain,
        validation_source=args.validation_source,
        validation_date=args.validation_date,
        min_coverage=float(args.min_coverage),
        max_mismatch_ratio=float(args.max_mismatch_ratio),
        close_tolerance_pct=float(args.close_tolerance_pct),
    )
    report_dir = Path(result["report_dir"])
    report_path = report_dir / "reset_reingest_report.json"
    report_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
