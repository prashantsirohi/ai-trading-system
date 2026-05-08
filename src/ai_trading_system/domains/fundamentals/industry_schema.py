"""Screener Industries Overview CSV normalization."""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from ai_trading_system.domains.fundamentals.schema import _coerce_numeric


INDUSTRY_COLUMN_MAP = {
    "Industry": "industry",
    "No. of Companies": "no_of_companies",
    "Total Market Cap.": "total_market_cap",
    "Median Market Cap.": "median_market_cap",
    "Median P/E": "median_pe",
    "Wtd. Avg Sales Growth": "sales_growth_wavg",
    "Wtd. Avg OPM": "opm_wavg",
    "Wtd. Avg ROCE": "roce_wavg",
    "Median 1Y Return": "median_1y_return",
}


_HEADER_ALIASES = {
    "industry": "Industry",
    "no of companies": "No. of Companies",
    "no. of companies": "No. of Companies",
    "no. companies": "No. of Companies",
    "total market cap": "Total Market Cap.",
    "total market cap.": "Total Market Cap.",
    "median market cap": "Median Market Cap.",
    "median market cap.": "Median Market Cap.",
    "median p/e": "Median P/E",
    "median pe": "Median P/E",
    "wtd. avg sales growth": "Wtd. Avg Sales Growth",
    "wtd avg sales growth": "Wtd. Avg Sales Growth",
    "wtd. avg opm": "Wtd. Avg OPM",
    "wtd avg opm": "Wtd. Avg OPM",
    "wtd. avg roce": "Wtd. Avg ROCE",
    "wtd avg roce": "Wtd. Avg ROCE",
    "median 1y return": "Median 1Y Return",
    "s.no.": "S.No.",
    "s. no.": "S.No.",
    "s no": "S.No.",
    "sno": "S.No.",
}


INDUSTRY_NUMERIC_COLUMNS = [
    "no_of_companies",
    "total_market_cap",
    "median_market_cap",
    "median_pe",
    "sales_growth_wavg",
    "opm_wavg",
    "roce_wavg",
    "median_1y_return",
]


def _canonicalize_header(raw: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(raw).replace("\n", " ")).strip()
    return _HEADER_ALIASES.get(cleaned.lower(), cleaned)


def normalize_industry_key(value: object) -> str:
    """Normalize an industry name into a stable join key."""

    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip().upper()
    if not text:
        return ""
    text = text.replace("&", " AND ")
    text = re.sub(r"[\.,/\\\-_:;'\"()\[\]]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_industry_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a manually exported Screener Industries Overview CSV."""

    if df is None:
        raise ValueError("Industry data frame is required")

    output = df.copy()
    output.columns = [_canonicalize_header(column) for column in output.columns]

    if "Industry" not in output.columns:
        raise ValueError("Missing required Screener industry column: Industry")

    output = output.rename(columns=INDUSTRY_COLUMN_MAP)

    for column in output.select_dtypes(include=["object", "string"]).columns:
        output.loc[:, column] = output[column].astype("string").str.strip()

    for column in INDUSTRY_NUMERIC_COLUMNS:
        if column in output.columns:
            output.loc[:, column] = _coerce_numeric(output[column])
        else:
            output.loc[:, column] = pd.Series(np.nan, index=output.index, dtype="float64")

    output = output.loc[output["industry"].notna() & output["industry"].astype(str).str.strip().ne("")].copy()
    output.loc[:, "industry"] = output["industry"].astype("string").str.strip()
    output.loc[:, "industry_key"] = output["industry"].map(normalize_industry_key)
    return output.reset_index(drop=True)
