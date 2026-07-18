"""CLI for the immutable, read-only ADR-0007 R0 calibration harness."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import tempfile
from time import perf_counter

import duckdb
import pandas as pd
from dotenv import load_dotenv

from ai_trading_system.domains.features.benchmark_index import load_benchmark_as_market_rows
from ai_trading_system.platform.db.paths import ensure_domain_layout, require_data_root_available

from .harness import run_calibration, write_calibration_result
from .policy import default_r0_policy
from .stage_source import load_weekly_stage_observations


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def _load_market(
    db_path: Path,
    *,
    exchange: str,
    through_date: str,
    symbols: list[str],
) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "_catalog"):
            raise RuntimeError(f"_catalog is missing from {db_path}")
        symbol_clause = ""
        params: list[object] = [exchange, through_date]
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            symbol_clause = f" AND symbol_id IN ({placeholders})"
            params.extend(symbols)
        return conn.execute(
            f"""
            SELECT symbol_id, exchange, timestamp, open, high, low, close, volume
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
              {symbol_clause}
            ORDER BY symbol_id, exchange, timestamp
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()


def _load_benchmark_index(
    db_path: Path,
    *,
    exchange: str,
    through_date: str,
    benchmark_symbol: str,
    benchmark_source: str,
) -> pd.DataFrame:
    return load_benchmark_as_market_rows(
        db_path,
        exchange=exchange,
        through_date=through_date,
        symbol=benchmark_symbol,
        source=benchmark_source,
    )


def _load_weekly_stage(
    db_path: Path,
    *,
    through_date: str,
    control_plane_db: Path | None = None,
    mode: str = "governed_current",
    stage_policy_version: str | None = None,
) -> pd.DataFrame:
    control_plane = control_plane_db or db_path.parent / "control_plane.duckdb"
    if not control_plane.exists():
        raise RuntimeError(f"control plane store missing: {control_plane}")
    return load_weekly_stage_observations(
        control_plane_db=control_plane,
        ohlcv_db=db_path,
        through_date=through_date,
        mode=mode,
        require_stage_policy_version=stage_policy_version,
    )


def _resolve_dates(args: argparse.Namespace, db_path: Path) -> list[str]:
    if args.as_of_date:
        return sorted({pd.Timestamp(value).date().isoformat() for value in args.as_of_date})
    if not args.from_date or not args.to_date:
        raise ValueError("use --as-of-date, or provide both --from-date and --to-date")
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        sessions = conn.execute(
            """
            SELECT DISTINCT CAST(timestamp AS DATE) AS session_date
            FROM _catalog
            WHERE exchange = ?
              AND CAST(timestamp AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            ORDER BY session_date
            """,
            [args.exchange, args.from_date, args.to_date],
        ).fetchdf()
    finally:
        conn.close()
    dates = [pd.Timestamp(value).date().isoformat() for value in sessions.get("session_date", [])]
    if args.cadence == "weekly":
        dated = pd.DataFrame({"date": pd.to_datetime(dates)})
        if not dated.empty:
            dates = (
                dated.assign(week=dated["date"].dt.to_period("W-FRI"))
                .groupby("week", sort=True)["date"].max()
                .dt.date.astype(str).tolist()
            )
    return dates


def _read_symbols(path: str | None) -> list[str]:
    if not path:
        return []
    return sorted({line.strip().upper() for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()})


def _verify_replay(result, manifest_path: Path) -> None:
    expected = json.loads(manifest_path.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="pattern-r0-verify-") as temp_dir:
        paths = write_calibration_result(result, Path(temp_dir) / "bundle")
        actual_path = next(path for path in paths if path.name == "r0_pattern_manifest.json")
        actual = json.loads(actual_path.read_text(encoding="utf-8"))
    fields = ("policy_hash", "source_hashes", "dataset_hashes", "row_counts")
    mismatches = [field for field in fields if actual.get(field) != expected.get(field)]
    if mismatches:
        raise RuntimeError(f"R0 replay does not match manifest fields: {mismatches}")


def _duration(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _progress_printer(*, quiet: bool):
    run_started = perf_counter()

    def emit(event: dict) -> None:
        if quiet:
            return
        kind = str(event.get("event", "progress"))
        stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        if kind == "scan_progress":
            message = (
                f"date={event['as_of_date']} scan={event['completed_symbols']}/{event['total_symbols']} "
                f"rate={event['symbols_per_second']:.2f} symbols/s "
                f"eta={_duration(event.get('eta_seconds'))} signals={event['signal_rows']}"
            )
        elif kind == "date_start":
            message = f"date={event['as_of_date']} start {event['date_index']}/{event['date_count']}"
        elif kind == "context_complete":
            message = (
                f"date={event['as_of_date']} context complete universe={event['universe_symbols']} "
                f"eligible={event['eligible_symbols']} lanes={json.dumps(event['lane_counts'], sort_keys=True)}"
            )
        elif kind == "date_complete":
            completed = int(event["date_index"])
            total = int(event["date_count"])
            elapsed = perf_counter() - run_started
            overall_eta = elapsed / completed * (total - completed) if completed else None
            message = (
                f"date={event['as_of_date']} complete {completed}/{total} "
                f"date_time={_duration(event['elapsed_seconds'])} overall_eta={_duration(overall_eta)} "
                f"signals={event['signal_rows']} invocations={event['detector_invocations']}"
            )
        elif kind == "checkpoint_loaded":
            message = f"date={event['as_of_date']} resumed from completed checkpoint"
        elif kind == "checkpoint_written":
            message = f"date={event['as_of_date']} checkpoint committed"
        elif kind == "source_hash_start":
            message = f"hashing replay sources market_rows={event['market_rows']}"
        elif kind == "source_hash_complete":
            message = f"source hash complete signature={str(event['signature'])[:12]}"
        elif kind == "aggregation_start":
            message = f"all {event['date_count']} dates complete; building outcomes, controls, and metrics"
        elif kind == "input_load_start":
            message = f"loading read-only inputs database={event['database']} dates={event['dates']}"
        elif kind == "input_load_complete":
            message = f"inputs loaded market_rows={event['market_rows']} weekly_rows={event['weekly_rows']}"
        elif kind == "run_configuration":
            message = (
                f"run configuration workers={event['workers']} resume={event['resume']} "
                f"checkpoint_dir={event['checkpoint_dir']}"
            )
        elif kind == "parallel_fallback":
            message = (
                f"date={event['as_of_date']} parallel workers unavailable; falling back to serial: "
                f"{event['reason']}"
            )
        elif kind == "interrupted":
            message = f"interrupted; completed-date checkpoints remain at {event['checkpoint_dir']}"
        else:
            message = json.dumps(event, sort_keys=True, default=str)
        print(f"[{stamp}] {message}", file=sys.stderr, flush=True)

    return emit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run read-only, point-in-time multi-lane pattern R0 calibration.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--ohlcv-db")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--as-of-date", action="append")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--cadence", choices=("daily", "weekly"), default="weekly")
    parser.add_argument("--symbols-file", help="Optional newline-delimited symbol restriction for canaries.")
    parser.add_argument("--exclusions-csv", help="Dated DQ/corporate-action exclusions with symbol_id and effective_from.")
    parser.add_argument("--winner-windows", help="Optional CSV used only for recall-before-first-guard analysis.")
    parser.add_argument(
        "--lane", action="append", dest="lanes",
        help="Restrict detector execution to this lane (repeatable). Context and lane assignment still cover the full universe.",
    )
    parser.add_argument(
        "--weekly-stage-source-mode", choices=("governed_current", "frozen_backfill"),
        default="governed_current",
        help="governed_current: live>backfill>snapshot precedence. frozen_backfill: backfill rows only (calibration).",
    )
    parser.add_argument(
        "--stage-policy-version",
        help="Require this stage policy version; mismatches fail in frozen_backfill mode.",
    )
    parser.add_argument("--control-plane-db", help="Override the control plane DuckDB path.")
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1), help="Parallel symbol workers; default: min(4, CPU count).")
    parser.add_argument("--progress-every", type=int, default=25, help="Emit symbol progress after this many completions.")
    parser.add_argument("--checkpoint-dir", help="Completed-date checkpoint directory; defaults beside output.")
    parser.add_argument("--no-resume", action="store_true", help="Ignore compatible completed-date checkpoints.")
    parser.add_argument("--quiet", action="store_true", help="Suppress live progress; final JSON is still printed.")
    parser.add_argument("--output-dir")
    parser.add_argument("--verify-against", help="Re-run and compare hashes with an existing R0 manifest.")
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    if args.progress_every < 1:
        raise ValueError("--progress-every must be at least 1")
    progress = _progress_printer(quiet=args.quiet)
    project_root = Path(args.project_root).resolve()
    paths = ensure_domain_layout(project_root=project_root, data_domain="operational")
    require_data_root_available(paths)
    db_path = Path(args.ohlcv_db).resolve() if args.ohlcv_db else paths.ohlcv_db_path
    dates = _resolve_dates(args, db_path)
    if not dates:
        raise RuntimeError("no exchange sessions resolved for replay")
    progress({"event": "input_load_start", "dates": len(dates), "database": str(db_path)})
    policy = default_r0_policy()
    symbols = _read_symbols(args.symbols_file)
    through_date = (pd.Timestamp(max(dates)) + pd.Timedelta(days=90)).date().isoformat()
    market = _load_market(db_path, exchange=args.exchange.upper(), through_date=through_date, symbols=symbols)
    benchmark = _load_benchmark_index(
        db_path,
        exchange=args.exchange.upper(),
        through_date=through_date,
        benchmark_symbol=policy.outcomes.benchmark_symbol,
        benchmark_source=policy.outcomes.benchmark_source,
    )
    market = pd.concat([market, benchmark], ignore_index=True)
    weekly = _load_weekly_stage(
        db_path,
        through_date=max(dates),
        control_plane_db=Path(args.control_plane_db).resolve() if args.control_plane_db else None,
        mode=args.weekly_stage_source_mode,
        stage_policy_version=args.stage_policy_version,
    )
    progress({"event": "input_load_complete", "market_rows": len(market), "weekly_rows": len(weekly)})
    checkpoint_dir = args.checkpoint_dir
    if checkpoint_dir is None and args.output_dir:
        checkpoint_dir = f"{Path(args.output_dir).resolve()}.checkpoints"
    if checkpoint_dir is None and args.verify_against:
        manifest_parent = Path(args.verify_against).resolve().parent
        checkpoint_dir = str(manifest_parent.with_name(f"{manifest_parent.name}.verify-checkpoints"))
    progress({
        "event": "run_configuration", "workers": args.workers, "checkpoint_dir": checkpoint_dir,
        "resume": not args.no_resume, "weekly_stage_source_mode": args.weekly_stage_source_mode,
        "stage_policy_version": args.stage_policy_version, "lanes": args.lanes,
    })
    try:
        result = run_calibration(
            market,
            as_of_dates=dates,
            weekly_stage_frame=weekly,
            policy=policy,
            exclusion_frame=pd.read_csv(args.exclusions_csv) if args.exclusions_csv else None,
            winner_windows=pd.read_csv(args.winner_windows) if args.winner_windows else None,
            workers=args.workers,
            progress_callback=progress,
            progress_every=args.progress_every,
            checkpoint_dir=checkpoint_dir,
            resume=not args.no_resume,
            lane_filter=tuple(args.lanes) if args.lanes else None,
        )
    except KeyboardInterrupt:
        progress({"event": "interrupted", "checkpoint_dir": checkpoint_dir})
        return 130
    if args.verify_against:
        _verify_replay(result, Path(args.verify_against).resolve())
        print(json.dumps({"status": "verified", "manifest": str(Path(args.verify_against).resolve())}, sort_keys=True), flush=True)
        return 0
    if not args.output_dir:
        raise ValueError("--output-dir is required unless --verify-against is used")
    written = write_calibration_result(result, Path(args.output_dir))
    print(json.dumps({"status": "written", "files": [str(path) for path in written], **result.summary}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
