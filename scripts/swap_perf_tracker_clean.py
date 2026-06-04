"""Atomically promote a validated tracker candidate and regenerate outputs."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from ai_trading_system.research.perf_tracker.backfill import run_backfill
from ai_trading_system.research.perf_tracker.digest import build_digest
from ai_trading_system.research.perf_tracker.health import build_tracker_health


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-db", type=Path, required=True)
    parser.add_argument("--live-db", type=Path, required=True)
    parser.add_argument("--validation-report", type=Path, required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    candidate = args.candidate_db.resolve()
    live = args.live_db.resolve()
    validation = json.loads(args.validation_report.read_text(encoding="utf-8"))
    if not validation.get("accepted"):
        raise SystemExit("Candidate validation report is not accepted")
    if str(candidate) != validation.get("candidate_db"):
        raise SystemExit("Candidate path does not match validation report")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    args.backup_dir.mkdir(parents=True, exist_ok=True)
    args.report_dir.mkdir(parents=True, exist_ok=True)
    polluted_backup = args.backup_dir / f"research_polluted_before_swap_{stamp}.duckdb"
    if polluted_backup.exists():
        raise SystemExit(f"Backup already exists: {polluted_backup}")

    os.replace(live, polluted_backup)
    os.replace(candidate, live)

    operational = run_backfill()
    health = build_tracker_health()
    digest = build_digest(as_of=date.today())
    payload = {
        "swapped_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "live_db": str(live),
        "polluted_backup": str(polluted_backup),
        "operational_top_up": operational,
        "health": health,
        "digest": str(digest.output_path),
    }
    (args.report_dir / "swap_result.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0 if health["status"] != "critical" else 1


if __name__ == "__main__":
    raise SystemExit(main())
