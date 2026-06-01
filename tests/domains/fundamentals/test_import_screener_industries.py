from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.import_screener_industries import (
    import_screener_industries_file,
)


INDUSTRY_COLUMNS = [
    "S.No.",
    "Industry",
    "No. of Companies",
    "Total Market Cap.",
    "Median Market Cap.",
    "Median P/E",
    "Wtd. Avg Sales Growth",
    "Wtd. Avg OPM",
    "Wtd. Avg ROCE",
    "Median 1Y Return",
]


def _row(idx: int, name: str) -> dict[str, object]:
    return {
        "S.No.": idx,
        "Industry": name,
        "No. of Companies": 10 + idx,
        "Total Market Cap.": 100000 + idx,
        "Median Market Cap.": 5000 + idx,
        "Median P/E": 18 + idx,
        "Wtd. Avg Sales Growth": 12 + idx,
        "Wtd. Avg OPM": 25 + idx,
        "Wtd. Avg ROCE": 18 + idx,
        "Median 1Y Return": 20 + idx,
    }


def test_import_writes_latest_csv_and_duckdb_tables(tmp_path: Path) -> None:
    csv_path = tmp_path / "industries.csv"
    pd.DataFrame([_row(1, "Banks"), _row(2, "Pharma"), _row(3, "Cement")]).to_csv(csv_path, index=False)
    db_path = tmp_path / "fundamentals.duckdb"
    latest = tmp_path / "industry_scores_latest.csv"
    trends = tmp_path / "industry_trends_latest.csv"

    scores = import_screener_industries_file(
        csv_path=csv_path,
        snapshot_date="2026-05-08",
        db_path=db_path,
        latest_output=latest,
        trends_output=trends,
    )

    assert latest.exists()
    assert len(scores) == 3
    assert {"industry", "industry_fundamental_score", "industry_fundamental_label"}.issubset(scores.columns)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        snapshot_rows = conn.execute("SELECT COUNT(*) FROM industry_fundamental_snapshot").fetchone()[0]
        score_rows = conn.execute("SELECT COUNT(*) FROM industry_fundamental_scores").fetchone()[0]
    finally:
        conn.close()
    assert snapshot_rows == 3
    assert score_rows == 3


def test_reimport_same_snapshot_replaces_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "industries.csv"
    pd.DataFrame([_row(1, "Banks"), _row(2, "Pharma")]).to_csv(csv_path, index=False)
    db_path = tmp_path / "fundamentals.duckdb"
    latest = tmp_path / "industry_scores_latest.csv"
    trends = tmp_path / "industry_trends_latest.csv"

    import_screener_industries_file(
        csv_path=csv_path, snapshot_date="2026-05-08", db_path=db_path, latest_output=latest,
        trends_output=trends,
    )
    import_screener_industries_file(
        csv_path=csv_path, snapshot_date="2026-05-08", db_path=db_path, latest_output=latest,
        trends_output=trends,
    )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        score_rows = conn.execute(
            "SELECT COUNT(*) FROM industry_fundamental_scores WHERE snapshot_date = ?",
            ["2026-05-08"],
        ).fetchone()[0]
    finally:
        conn.close()
    assert score_rows == 2
