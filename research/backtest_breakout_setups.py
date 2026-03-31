"""Backtest breakout setup families across bull and bear study windows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from analytics.breakout_research import build_breakout_dataset, summarize_breakout_period


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest breakout setup families on research history")
    parser.add_argument("--from-date", default="2020-01-01", help="Dataset start date to allow lookback warmup")
    parser.add_argument("--to-date", default="2024-12-31", help="Dataset end date")
    parser.add_argument("--exchange", default="NSE")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    reports_dir = project_root / "reports" / "research"
    reports_dir.mkdir(parents=True, exist_ok=True)

    dataset = build_breakout_dataset(
        project_root,
        from_date=args.from_date,
        to_date=args.to_date,
        exchange=args.exchange,
    )

    periods = [
        ("bear_2021_2022", "2021-01-01", "2022-12-31"),
        ("bull_2023_2024", "2023-01-01", "2024-12-31"),
    ]

    summaries = []
    best_by_period = {}
    for label, start_date, end_date in periods:
        summary, best = summarize_breakout_period(
            dataset,
            start_date=start_date,
            end_date=end_date,
            label=label,
        )
        summaries.append(summary)
        best_by_period[label] = best

    summary_df = pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()
    csv_path = reports_dir / "breakout_setup_backtest_summary.csv"
    json_path = reports_dir / "breakout_setup_backtest_summary.json"
    summary_df.to_csv(csv_path, index=False)
    json_path.write_text(
        json.dumps(
            {
                "dataset_from": args.from_date,
                "dataset_to": args.to_date,
                "period_best": best_by_period,
                "rows": summary_df.to_dict(orient="records"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(summary_df.to_string(index=False))
    print(f"\nCSV: {csv_path}")
    print(f"JSON: {json_path}")


if __name__ == "__main__":
    main()
