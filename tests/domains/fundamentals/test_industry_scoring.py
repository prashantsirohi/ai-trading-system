from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import INDUSTRY_FUNDAMENTAL_OUTPUT_COLUMNS
from ai_trading_system.domains.fundamentals.industry_schema import normalize_industry_columns
from ai_trading_system.domains.fundamentals.industry_scoring import compute_industry_fundamental_scores


def _industry_row(
    name: str,
    *,
    no_of_companies: float = 20,
    median_pe: float = 25,
    sales: float = 15,
    opm: float = 25,
    roce: float = 20,
    momentum: float = 25,
) -> dict[str, object]:
    return {
        "Industry": name,
        "No. of Companies": no_of_companies,
        "Total Market Cap.": 100000,
        "Median Market Cap.": 5000,
        "Median P/E": median_pe,
        "Wtd. Avg Sales Growth": sales,
        "Wtd. Avg OPM": opm,
        "Wtd. Avg ROCE": roce,
        "Median 1Y Return": momentum,
    }


def _scored(rows: list[dict[str, object]]) -> pd.DataFrame:
    raw = pd.DataFrame(rows)
    normalized = normalize_industry_columns(raw)
    return compute_industry_fundamental_scores(normalized, snapshot_date="2026-05-08")


def test_compute_returns_expected_columns() -> None:
    scored = _scored([_industry_row("Banks"), _industry_row("Pharma", median_pe=40)])
    assert list(scored.columns) == INDUSTRY_FUNDAMENTAL_OUTPUT_COLUMNS


def test_scores_are_within_bounds() -> None:
    scored = _scored(
        [
            _industry_row("A", median_pe=10, sales=10, opm=20, roce=15, momentum=15),
            _industry_row("B", median_pe=80, sales=40, opm=35, roce=25, momentum=80),
            _industry_row("C", median_pe=15, sales=-10, opm=5, roce=6, momentum=-20),
        ]
    )
    for column in [
        "industry_growth_score",
        "industry_quality_score",
        "industry_valuation_score",
        "industry_momentum_score",
        "industry_fundamental_score",
    ]:
        assert (scored[column] >= 0).all()
        assert (scored[column] <= 100).all()


def test_quality_growth_leader_label() -> None:
    rows = [
        _industry_row("Leader", median_pe=18, sales=40, opm=35, roce=30, momentum=40),
        _industry_row("Mid", median_pe=25, sales=10, opm=15, roce=12, momentum=10),
        _industry_row("Weak", median_pe=80, sales=-5, opm=2, roce=4, momentum=-5),
        _industry_row("Lower1", median_pe=30, sales=5, opm=10, roce=8, momentum=5),
        _industry_row("Lower2", median_pe=22, sales=8, opm=12, roce=10, momentum=8),
    ]
    scored = _scored(rows)
    leader = scored.loc[scored["industry"] == "Leader"].iloc[0]
    assert leader["industry_fundamental_label"] == "QUALITY_GROWTH_LEADER"


def test_expensive_momentum_label() -> None:
    # Hot must NOT qualify for QUALITY_GROWTH_LEADER (so quality/growth percentiles must be lower
    # than peers), but momentum must be highest and PE must be most expensive.
    rows = [
        _industry_row("Hot", median_pe=120, sales=2, opm=8, roce=8, momentum=80),
        _industry_row("Solid1", median_pe=10, sales=20, opm=30, roce=25, momentum=10),
        _industry_row("Solid2", median_pe=12, sales=22, opm=32, roce=27, momentum=15),
        _industry_row("Solid3", median_pe=14, sales=24, opm=34, roce=29, momentum=20),
    ]
    scored = _scored(rows)
    hot = scored.loc[scored["industry"] == "Hot"].iloc[0]
    assert hot["industry_fundamental_label"] == "EXPENSIVE_MOMENTUM"


def test_value_rotation_candidate_label() -> None:
    # Cheap row has highest quality (top roce/opm), cheapest PE, weak momentum.
    rows = [
        _industry_row("Cheap", median_pe=6, sales=12, opm=35, roce=30, momentum=-10),
        _industry_row("Hot1", median_pe=60, sales=15, opm=18, roce=12, momentum=60),
        _industry_row("Hot2", median_pe=70, sales=18, opm=20, roce=14, momentum=70),
        _industry_row("Hot3", median_pe=80, sales=20, opm=22, roce=16, momentum=80),
    ]
    scored = _scored(rows)
    cheap = scored.loc[scored["industry"] == "Cheap"].iloc[0]
    assert cheap["industry_fundamental_label"] == "VALUE_ROTATION_CANDIDATE"


def test_low_company_count_distorts_or_warns() -> None:
    rows = [
        _industry_row("Tiny", no_of_companies=2, median_pe=20, sales=10, opm=15, roce=12, momentum=5),
        _industry_row("Big", no_of_companies=30, median_pe=22, sales=12, opm=18, roce=14, momentum=8),
    ]
    scored = _scored(rows)
    tiny = scored.loc[scored["industry"] == "Tiny"].iloc[0]
    assert tiny["industry_fundamental_label"] == "DISTORTED_DATA"
    assert "low_company_count" in tiny["industry_warning"]


def test_non_positive_pe_does_not_get_high_valuation_score() -> None:
    rows = [
        _industry_row("Loss", median_pe=0, sales=10, opm=15, roce=12, momentum=10),
        _industry_row("OK", median_pe=20, sales=10, opm=15, roce=12, momentum=10),
    ]
    scored = _scored(rows)
    loss = scored.loc[scored["industry"] == "Loss"].iloc[0]
    assert loss["industry_valuation_score"] == 50.0
