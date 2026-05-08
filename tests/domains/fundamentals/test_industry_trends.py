from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.import_screener_industries import (
    import_screener_industries_file,
)
from ai_trading_system.domains.fundamentals.industry_trends import (
    INDUSTRY_TREND_OUTPUT_COLUMNS,
    compute_industry_fundamental_trends,
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


def _row(idx: int, name: str, *, sales: float, opm: float, roce: float, momentum: float, pe: float) -> dict[str, object]:
    return {
        "S.No.": idx,
        "Industry": name,
        "No. of Companies": 20,
        "Total Market Cap.": 100000,
        "Median Market Cap.": 5000,
        "Median P/E": pe,
        "Wtd. Avg Sales Growth": sales,
        "Wtd. Avg OPM": opm,
        "Wtd. Avg ROCE": roce,
        "Median 1Y Return": momentum,
    }


def test_first_snapshot_yields_insufficient_history() -> None:
    current = pd.DataFrame(
        [
            {
                "industry": "Banks",
                "industry_key": "BANKS",
                "industry_fundamental_score": 76,
                "industry_growth_score": 80,
                "industry_quality_score": 80,
                "industry_valuation_score": 70,
                "industry_momentum_score": 60,
                "snapshot_date": "2026-05-08",
            }
        ]
    )
    trends = compute_industry_fundamental_trends(current_scores=current, previous_scores=None)
    assert list(trends.columns) == INDUSTRY_TREND_OUTPUT_COLUMNS
    assert (trends["industry_trend_label"] == "INSUFFICIENT_HISTORY").all()


def test_score_rise_yields_improving_label() -> None:
    prev = pd.DataFrame(
        [
            {
                "industry": "Banks",
                "industry_key": "BANKS",
                "industry_fundamental_score": 60,
                "industry_quality_score": 60,
                "industry_growth_score": 60,
                "industry_valuation_score": 60,
                "industry_momentum_score": 60,
                "roce_wavg": 18,
                "opm_wavg": 22,
                "snapshot_date": "2026-02-01",
            }
        ]
    )
    curr = pd.DataFrame(
        [
            {
                "industry": "Banks",
                "industry_key": "BANKS",
                "industry_fundamental_score": 75,
                "industry_quality_score": 70,
                "industry_growth_score": 70,
                "industry_valuation_score": 60,
                "industry_momentum_score": 65,
                "roce_wavg": 22,
                "opm_wavg": 25,
                "snapshot_date": "2026-05-08",
            }
        ]
    )
    trends = compute_industry_fundamental_trends(current_scores=curr, previous_scores=prev)
    by_key = trends.set_index("industry_key").loc["BANKS"]
    assert by_key["industry_trend_label"] == "IMPROVING"
    assert float(by_key["industry_fundamental_score_delta"]) == 15.0


def test_quality_drop_yields_deteriorating_label() -> None:
    prev = pd.DataFrame(
        [
            {
                "industry": "Pharma",
                "industry_key": "PHARMA",
                "industry_fundamental_score": 65,
                "industry_quality_score": 70,
                "industry_growth_score": 60,
                "industry_valuation_score": 55,
                "industry_momentum_score": 60,
                "roce_wavg": 22,
                "opm_wavg": 25,
                "snapshot_date": "2026-02-01",
            }
        ]
    )
    curr = pd.DataFrame(
        [
            {
                "industry": "Pharma",
                "industry_key": "PHARMA",
                "industry_fundamental_score": 60,
                "industry_quality_score": 55,
                "industry_growth_score": 60,
                "industry_valuation_score": 55,
                "industry_momentum_score": 60,
                "roce_wavg": 18,
                "opm_wavg": 18,
                "snapshot_date": "2026-05-08",
            }
        ]
    )
    trends = compute_industry_fundamental_trends(current_scores=curr, previous_scores=prev)
    by_key = trends.set_index("industry_key").loc["PHARMA"]
    assert by_key["industry_trend_label"] == "DETERIORATING"


def test_importer_persists_trends_table_and_csv(tmp_path: Path) -> None:
    csv1 = tmp_path / "industries_q1.csv"
    csv2 = tmp_path / "industries_q2.csv"
    # filler rows make percentile space wider so Banks's improvement shifts its rank.
    fillers_q1 = [
        _row(10 + i, f"Filler{i}", sales=20 + i, opm=30 + i, roce=24 + i, momentum=18 + i, pe=24)
        for i in range(8)
    ]
    fillers_q2 = [
        _row(10 + i, f"Filler{i}", sales=20 + i, opm=30 + i, roce=24 + i, momentum=18 + i, pe=24)
        for i in range(8)
    ]
    pd.DataFrame(
        [
            _row(1, "Banks",  sales=10, opm=20, roce=15, momentum=10, pe=18),
            _row(2, "Pharma", sales=15, opm=25, roce=20, momentum=15, pe=22),
            _row(3, "Cement", sales=8,  opm=18, roce=14, momentum=8,  pe=20),
            *fillers_q1,
        ]
    ).to_csv(csv1, index=False)
    pd.DataFrame(
        [
            _row(1, "Banks",  sales=40, opm=45, roce=35, momentum=45, pe=18),  # jumped above all fillers
            _row(2, "Pharma", sales=2,  opm=4,  roce=3,  momentum=2,  pe=22),  # collapsed below all fillers
            _row(3, "Cement", sales=8,  opm=18, roce=14, momentum=8,  pe=20),  # unchanged
            *fillers_q2,
        ]
    ).to_csv(csv2, index=False)

    db = tmp_path / "fundamentals.duckdb"
    latest = tmp_path / "industry_scores_latest.csv"
    trends_csv = tmp_path / "industry_trends_latest.csv"

    import_screener_industries_file(
        csv_path=csv1, snapshot_date="2026-02-01", db_path=db, latest_output=latest, trends_output=trends_csv
    )
    import_screener_industries_file(
        csv_path=csv2, snapshot_date="2026-05-08", db_path=db, latest_output=latest, trends_output=trends_csv
    )

    assert trends_csv.exists()
    trends = pd.read_csv(trends_csv)
    assert set(["industry_key", "industry_trend_label", "industry_fundamental_score_delta"]).issubset(trends.columns)
    by_key = trends.set_index("industry_key")
    assert by_key.loc["BANKS", "industry_trend_label"] == "IMPROVING"
    assert by_key.loc["PHARMA", "industry_trend_label"] == "DETERIORATING"

    conn = duckdb.connect(str(db), read_only=True)
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM industry_fundamental_trends WHERE snapshot_date = ?",
            ["2026-05-08"],
        ).fetchone()[0]
    finally:
        conn.close()
    assert rows == 11
