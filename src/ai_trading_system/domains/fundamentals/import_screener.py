"""Import manually downloaded Screener CSV fundamentals."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.schema import normalize_columns
from ai_trading_system.domains.fundamentals.scoring import compute_fundamental_scores
from ai_trading_system.domains.fundamentals.trends import compute_fundamental_trends


DEFAULT_DB_PATH = Path("data/fundamentals.duckdb")
DEFAULT_LATEST_OUTPUT = Path("data/fundamentals/fundamental_scores_latest.csv")
DEFAULT_TRENDS_OUTPUT = Path("data/fundamentals/fundamental_trends_latest.csv")


def _replace_snapshot_table(conn: duckdb.DuckDBPyConnection, table_name: str, frame: pd.DataFrame, snapshot_date: str) -> None:
    conn.register("_fundamental_frame", frame)
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM _fundamental_frame WHERE 1 = 0")
        conn.execute(f"DELETE FROM {table_name} WHERE snapshot_date = ?", [snapshot_date])
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM _fundamental_frame")
    finally:
        conn.unregister("_fundamental_frame")


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
    )


def _previous_snapshot_date(conn: duckdb.DuckDBPyConnection, snapshot_date: str) -> str | None:
    if not _table_exists(conn, "fundamental_scores"):
        return None
    row = conn.execute(
        "SELECT MAX(snapshot_date) FROM fundamental_scores WHERE snapshot_date < ?",
        [snapshot_date],
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])[:10]


def _read_snapshot(conn: duckdb.DuckDBPyConnection, table_name: str, snapshot_date: str | None) -> pd.DataFrame:
    if snapshot_date is None or not _table_exists(conn, table_name):
        return pd.DataFrame()
    return conn.execute(f"SELECT * FROM {table_name} WHERE snapshot_date = ?", [snapshot_date]).fetchdf()


def import_screener_file(
    *,
    csv_path: str | Path,
    snapshot_date: str,
    db_path: str | Path = DEFAULT_DB_PATH,
    latest_output: str | Path = DEFAULT_LATEST_OUTPUT,
    trends_output: str | Path = DEFAULT_TRENDS_OUTPUT,
) -> pd.DataFrame:
    """Import one Screener export and return the computed score frame."""

    csv_path = Path(csv_path)
    db_path = Path(db_path)
    latest_output = Path(latest_output)
    trends_output = Path(trends_output)
    raw = pd.read_csv(csv_path)
    normalized = normalize_columns(raw)
    normalized.loc[:, "snapshot_date"] = snapshot_date
    scores = compute_fundamental_scores(normalized, snapshot_date=snapshot_date)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    latest_output.parent.mkdir(parents=True, exist_ok=True)
    trends_output.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        prev_snapshot_date = _previous_snapshot_date(conn, snapshot_date)
        previous_scores = _read_snapshot(conn, "fundamental_scores", prev_snapshot_date)
        previous_raw = _read_snapshot(conn, "fundamental_snapshot", prev_snapshot_date)
        trends = compute_fundamental_trends(
            current_scores=scores,
            previous_scores=previous_scores,
            current_raw=normalized,
            previous_raw=previous_raw,
        )
        _replace_snapshot_table(conn, "fundamental_snapshot", normalized, snapshot_date)
        _replace_snapshot_table(conn, "fundamental_scores", scores, snapshot_date)
        _replace_snapshot_table(conn, "fundamental_trends", trends, snapshot_date)
    finally:
        conn.close()
    scores.to_csv(latest_output, index=False)
    trends.to_csv(trends_output, index=False)
    return scores


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import a manually exported Screener fundamentals CSV.")
    parser.add_argument("--file", required=True, help="Path to Screener CSV export")
    parser.add_argument("--snapshot-date", required=True, help="Screener export date in YYYY-MM-DD format")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="DuckDB path")
    parser.add_argument("--latest-output", default=str(DEFAULT_LATEST_OUTPUT), help="Latest scores CSV output")
    parser.add_argument("--trends-output", default=str(DEFAULT_TRENDS_OUTPUT), help="Latest fundamental trends CSV output")
    return parser


def _print_summary(scores: pd.DataFrame) -> None:
    tier_counts = scores["fundamental_tier"].value_counts().reindex(["A", "B", "C", "Reject"], fill_value=0)
    print(f"rows scored: {len(scores)}")
    print("tier counts:")
    for tier, count in tier_counts.items():
        print(f"  {tier}: {int(count)}")
    hard_count = int(scores["hard_red_flag"].astype(bool).sum()) if "hard_red_flag" in scores.columns else 0
    print(f"hard red flag count: {hard_count}")
    top = scores.sort_values("fundamental_score", ascending=False).head(20)
    if not top.empty:
        print("top 20 by fundamental_score:")
        print(top[["symbol", "name", "fundamental_score", "fundamental_tier", "red_flags"]].to_string(index=False))


def main() -> None:
    args = build_parser().parse_args()
    scores = import_screener_file(
        csv_path=args.file,
        snapshot_date=args.snapshot_date,
        db_path=args.db_path,
        latest_output=args.latest_output,
        trends_output=args.trends_output,
    )
    print(f"rows imported: {len(scores)}")
    _print_summary(scores)


if __name__ == "__main__":
    main()
