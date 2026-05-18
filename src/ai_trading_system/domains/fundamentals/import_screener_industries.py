"""Import manually downloaded Screener Industries Overview CSV."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.import_screener import (
    _replace_snapshot_table,
    _table_exists,
)
from ai_trading_system.domains.fundamentals.industry_schema import normalize_industry_columns
from ai_trading_system.domains.fundamentals.industry_scoring import compute_industry_fundamental_scores
from ai_trading_system.domains.fundamentals.industry_trends import compute_industry_fundamental_trends
from ai_trading_system.platform.db.paths import get_domain_paths


_PATHS = get_domain_paths()
DEFAULT_INDUSTRY_DB_PATH = _PATHS.root_dir / "fundamentals.duckdb"
DEFAULT_INDUSTRY_LATEST_OUTPUT = _PATHS.fundamentals_dir / "industry_fundamental_scores_latest.csv"
DEFAULT_INDUSTRY_TRENDS_OUTPUT = _PATHS.fundamentals_dir / "industry_fundamental_trends_latest.csv"


def _previous_industry_snapshot_date(conn: duckdb.DuckDBPyConnection, snapshot_date: str) -> str | None:
    if not _table_exists(conn, "industry_fundamental_scores"):
        return None
    row = conn.execute(
        "SELECT MAX(snapshot_date) FROM industry_fundamental_scores WHERE snapshot_date < ?",
        [snapshot_date],
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])[:10]


def _read_industry_snapshot(
    conn: duckdb.DuckDBPyConnection, table_name: str, snapshot_date: str | None
) -> pd.DataFrame:
    if snapshot_date is None or not _table_exists(conn, table_name):
        return pd.DataFrame()
    return conn.execute(
        f"SELECT * FROM {table_name} WHERE snapshot_date = ?", [snapshot_date]
    ).fetchdf()


def import_screener_industries_file(
    *,
    csv_path: str | Path,
    snapshot_date: str,
    db_path: str | Path = DEFAULT_INDUSTRY_DB_PATH,
    latest_output: str | Path = DEFAULT_INDUSTRY_LATEST_OUTPUT,
    trends_output: str | Path = DEFAULT_INDUSTRY_TRENDS_OUTPUT,
) -> pd.DataFrame:
    """Import one Screener Industries Overview CSV and return industry scores."""

    csv_path = Path(csv_path)
    db_path = Path(db_path)
    latest_output = Path(latest_output)
    trends_output = Path(trends_output)

    raw = pd.read_csv(csv_path)
    normalized = normalize_industry_columns(raw)
    normalized.loc[:, "snapshot_date"] = snapshot_date
    scores = compute_industry_fundamental_scores(normalized, snapshot_date=snapshot_date)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    latest_output.parent.mkdir(parents=True, exist_ok=True)
    trends_output.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        prev_snapshot_date = _previous_industry_snapshot_date(conn, snapshot_date)
        previous_scores = _read_industry_snapshot(conn, "industry_fundamental_scores", prev_snapshot_date)
        trends = compute_industry_fundamental_trends(
            current_scores=scores, previous_scores=previous_scores
        )
        _replace_snapshot_table(conn, "industry_fundamental_snapshot", normalized, snapshot_date)
        _replace_snapshot_table(conn, "industry_fundamental_scores", scores, snapshot_date)
        _replace_snapshot_table(conn, "industry_fundamental_trends", trends, snapshot_date)
    finally:
        conn.close()

    scores.to_csv(latest_output, index=False)
    trends.to_csv(trends_output, index=False)
    return scores


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import a manually exported Screener Industries Overview CSV.")
    parser.add_argument("--file", required=True, help="Path to Screener Industries Overview CSV export")
    parser.add_argument("--snapshot-date", required=True, help="Screener export date in YYYY-MM-DD format")
    parser.add_argument("--db-path", default=str(DEFAULT_INDUSTRY_DB_PATH), help="DuckDB path")
    parser.add_argument(
        "--latest-output",
        default=str(DEFAULT_INDUSTRY_LATEST_OUTPUT),
        help="Latest industry scores CSV output",
    )
    parser.add_argument(
        "--trends-output",
        default=str(DEFAULT_INDUSTRY_TRENDS_OUTPUT),
        help="Latest industry trends CSV output",
    )
    return parser


def _print_summary(scores: pd.DataFrame) -> None:
    print(f"rows imported: {len(scores)}")
    if scores.empty:
        return
    label_counts = scores["industry_fundamental_label"].value_counts()
    print("label counts:")
    for label, count in label_counts.items():
        print(f"  {label}: {int(count)}")
    top = scores.sort_values("industry_fundamental_score", ascending=False).head(20)
    print("top 20 by industry_fundamental_score:")
    columns = [
        "industry",
        "industry_fundamental_score",
        "industry_fundamental_label",
        "industry_warning",
    ]
    print(top[columns].to_string(index=False))


def main() -> None:
    args = build_parser().parse_args()
    scores = import_screener_industries_file(
        csv_path=args.file,
        snapshot_date=args.snapshot_date,
        db_path=args.db_path,
        latest_output=args.latest_output,
        trends_output=args.trends_output,
    )
    _print_summary(scores)


if __name__ == "__main__":
    main()
