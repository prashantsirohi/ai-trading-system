"""Operational publish target test command."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from core.env import load_project_env
from run.preflight import PreflightChecker
from core.logging import logger
from publishers.google_sheets import GoogleSheetsManager


def run_publish_test(project_root: Path) -> dict:
    """Send a small healthcheck to configured publish targets."""
    preflight = PreflightChecker(project_root).run(["publish"], {"publish_test": True})
    if preflight["status"] != "passed":
        raise RuntimeError(f"Publish preflight failed: {preflight['blocking_failures']}")

    results = []
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        df = pd.DataFrame([{"timestamp": timestamp, "status": "ok", "source": "publish_test"}])
        sheets = GoogleSheetsManager()
        if sheets.write_dataframe(df, sheet_name="Pipeline Healthcheck", clear_sheet=False):
            results.append({"channel": "google_sheets", "status": "passed"})
        else:
            results.append({"channel": "google_sheets", "status": "failed"})
    except Exception as exc:
        results.append({"channel": "google_sheets", "status": "failed", "error": str(exc)})

    try:
        from publishers.telegram import TelegramReporter

        reporter = TelegramReporter(report_dir=project_root / "reports")
        status = reporter.send_message(f"Pipeline healthcheck {timestamp}")
        results.append({"channel": "telegram", "status": "passed" if status else "failed"})
    except Exception as exc:
        results.append({"channel": "telegram", "status": "failed", "error": str(exc)})

    logger.info("Publish test results: %s", results)
    return {"timestamp": timestamp, "results": results}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run live publish target healthchecks")
    parser.parse_args()
    load_project_env(project_root)
    run_publish_test(Path(project_root))


if __name__ == "__main__":
    main()
