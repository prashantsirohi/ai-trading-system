from __future__ import annotations

import pandas as pd
import pytest

from ai_trading_system.domains.fundamentals.industry_schema import (
    normalize_industry_columns,
    normalize_industry_key,
)


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_normalize_handles_exact_screener_columns() -> None:
    df = _frame(
        [
            {
                "S.No.": 1,
                "Industry": " Banks ",
                "No. of Companies": "12",
                "Total Market Cap.": "1,234,567",
                "Median Market Cap.": "12,345",
                "Median P/E": "18.5",
                "Wtd. Avg Sales Growth": "12.3%",
                "Wtd. Avg OPM": "30%",
                "Wtd. Avg ROCE": "15%",
                "Median 1Y Return": "22.5%",
            }
        ]
    )
    out = normalize_industry_columns(df)
    assert out.loc[0, "industry"] == "Banks"
    assert out.loc[0, "no_of_companies"] == 12.0
    assert out.loc[0, "total_market_cap"] == 1234567.0
    assert out.loc[0, "median_pe"] == 18.5
    assert out.loc[0, "sales_growth_wavg"] == 12.3
    assert out.loc[0, "industry_key"] == "BANKS"


def test_normalize_handles_blanks_dashes_and_na() -> None:
    df = _frame(
        [
            {"Industry": "Cement", "Median P/E": "-", "Wtd. Avg OPM": "NA", "Median 1Y Return": ""},
            {"Industry": "Pharma", "Median P/E": "N/A", "Wtd. Avg OPM": "nan", "Median 1Y Return": "10%"},
        ]
    )
    out = normalize_industry_columns(df)
    assert pd.isna(out.loc[0, "median_pe"])
    assert pd.isna(out.loc[0, "opm_wavg"])
    assert pd.isna(out.loc[0, "median_1y_return"])
    assert out.loc[1, "median_1y_return"] == 10.0


def test_normalize_creates_industry_key() -> None:
    df = _frame([{"Industry": "Oil & Gas - Exploration"}])
    out = normalize_industry_columns(df)
    assert out.loc[0, "industry_key"] == "OIL AND GAS EXPLORATION"


def test_normalize_raises_when_industry_missing() -> None:
    df = _frame([{"Foo": 1}])
    with pytest.raises(ValueError):
        normalize_industry_columns(df)


def test_normalize_tolerates_missing_optional_numeric_columns() -> None:
    df = _frame([{"Industry": "Banks"}])
    out = normalize_industry_columns(df)
    assert "median_pe" in out.columns
    assert pd.isna(out.loc[0, "median_pe"])
    assert pd.isna(out.loc[0, "no_of_companies"])


def test_normalize_handles_newline_header_variants() -> None:
    df = pd.DataFrame(
        [
            {
                "Industry": "Banks",
                "No. of\nCompanies": 5,
                "Total\nMarket\nCap.": 100,
                "Wtd. Avg\nOPM": 25,
                "Median\n1Y\nReturn": 15,
            }
        ]
    )
    out = normalize_industry_columns(df)
    assert out.loc[0, "no_of_companies"] == 5
    assert out.loc[0, "total_market_cap"] == 100
    assert out.loc[0, "opm_wavg"] == 25
    assert out.loc[0, "median_1y_return"] == 15


def test_normalize_industry_key_handles_none_and_nan() -> None:
    assert normalize_industry_key(None) == ""
    assert normalize_industry_key(float("nan")) == ""
    assert normalize_industry_key("Banks") == "BANKS"
    assert normalize_industry_key("  Banks  ") == "BANKS"
