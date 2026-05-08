"""Industry-level fundamental scoring from Screener Industries Overview."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import (
    INDUSTRY_FUNDAMENTAL_OUTPUT_COLUMNS,
)
from ai_trading_system.domains.fundamentals.scoring import percentile_score


WINSORIZE_BOUNDS: dict[str, tuple[float, float]] = {
    "sales_growth_wavg": (-50.0, 150.0),
    "opm_wavg": (-50.0, 80.0),
    "roce_wavg": (-30.0, 80.0),
    "median_1y_return": (-80.0, 200.0),
    "median_pe": (0.0, 150.0),
}


_KEY_METRIC_COLUMNS = (
    "median_pe",
    "sales_growth_wavg",
    "opm_wavg",
    "roce_wavg",
    "median_1y_return",
)


def _winsorize(series: pd.Series, low: float, high: float) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.clip(lower=low, upper=high)


def _round2(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").clip(0, 100).round(2)


def classify_industry_label(row: pd.Series) -> tuple[str, str]:
    """Return (label, warning) for a scored industry row."""

    no_of_companies = pd.to_numeric(row.get("no_of_companies"), errors="coerce")
    sales_growth = pd.to_numeric(row.get("sales_growth_wavg"), errors="coerce")
    opm = pd.to_numeric(row.get("opm_wavg"), errors="coerce")
    roce = pd.to_numeric(row.get("roce_wavg"), errors="coerce")
    median_pe = pd.to_numeric(row.get("median_pe"), errors="coerce")

    def _score(name: str) -> float:
        value = row.get(name)
        if value is None or pd.isna(value):
            return 50.0
        return float(value)

    fundamental = _score("industry_fundamental_score")
    growth = _score("industry_growth_score")
    quality = _score("industry_quality_score")
    valuation = _score("industry_valuation_score")
    momentum = _score("industry_momentum_score")

    warnings: list[str] = []
    if pd.notna(no_of_companies) and no_of_companies < 5:
        warnings.append("low_company_count")
    if pd.notna(median_pe) and median_pe > 60:
        warnings.append("expensive_sector")
    if pd.notna(opm) and opm < 0:
        warnings.append("negative_opm")
    if pd.notna(roce) and roce < 8:
        warnings.append("weak_roce")
    if pd.notna(sales_growth) and (sales_growth > 150 or sales_growth < -50):
        warnings.append("extreme_sales_growth")
    if pd.notna(opm) and (opm < -50 or opm > 80):
        warnings.append("distorted_operating_margin")
    missing_count = sum(
        1 for column in _KEY_METRIC_COLUMNS if pd.isna(pd.to_numeric(row.get(column), errors="coerce"))
    )
    if missing_count >= 3:
        warnings.append("missing_key_metrics")

    distorted = (
        (pd.notna(no_of_companies) and no_of_companies < 3)
        or (pd.notna(sales_growth) and (sales_growth > 200 or sales_growth < -75))
        or (pd.notna(opm) and opm < -50)
        or missing_count >= 3
    )
    if distorted:
        return "DISTORTED_DATA", ";".join(warnings)

    major_warning = "missing_key_metrics" in warnings or "distorted_operating_margin" in warnings

    if fundamental >= 70 and growth >= 65 and quality >= 65 and not major_warning:
        return "QUALITY_GROWTH_LEADER", ";".join(warnings)
    if momentum >= 70 and valuation <= 35:
        return "EXPENSIVE_MOMENTUM", ";".join(warnings)
    if valuation >= 65 and quality >= 55 and momentum <= 50:
        return "VALUE_ROTATION_CANDIDATE", ";".join(warnings)
    if growth >= 60 and momentum <= 55 and valuation >= 50:
        return "CYCLICAL_RECOVERY", ";".join(warnings)
    if fundamental < 40 or quality < 35:
        return "WEAK_FUNDAMENTALS", ";".join(warnings)
    return "BALANCED", ";".join(warnings)


def compute_industry_fundamental_scores(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """Compute industry-level fundamental scores and labels."""

    if df is None or df.empty:
        return pd.DataFrame(columns=INDUSTRY_FUNDAMENTAL_OUTPUT_COLUMNS)

    source = df.copy().reset_index(drop=True)
    numeric_columns = (
        "no_of_companies",
        "total_market_cap",
        "median_market_cap",
        "median_pe",
        "sales_growth_wavg",
        "opm_wavg",
        "roce_wavg",
        "median_1y_return",
    )
    for column in numeric_columns:
        if column not in source.columns:
            source.loc[:, column] = pd.Series(np.nan, index=source.index, dtype="float64")

    sales_w = _winsorize(source["sales_growth_wavg"], *WINSORIZE_BOUNDS["sales_growth_wavg"])
    opm_w = _winsorize(source["opm_wavg"], *WINSORIZE_BOUNDS["opm_wavg"])
    roce_w = _winsorize(source["roce_wavg"], *WINSORIZE_BOUNDS["roce_wavg"])
    momentum_w = _winsorize(source["median_1y_return"], *WINSORIZE_BOUNDS["median_1y_return"])
    pe_w = _winsorize(source["median_pe"], *WINSORIZE_BOUNDS["median_pe"])

    growth_score = percentile_score(sales_w)
    quality_score = (0.5 * percentile_score(roce_w)) + (0.5 * percentile_score(opm_w))
    momentum_score = percentile_score(momentum_w)

    pe_numeric = pd.to_numeric(source["median_pe"], errors="coerce")
    pe_for_rank = pe_w.where(pe_numeric.gt(0))
    valuation_raw = percentile_score(pe_for_rank, invert=True)
    valuation_score = valuation_raw.where(pe_numeric.gt(0), 50.0)

    fundamental = (
        0.30 * growth_score
        + 0.30 * quality_score
        + 0.20 * valuation_score
        + 0.20 * momentum_score
    )

    output = pd.DataFrame(
        {
            "snapshot_date": snapshot_date,
            "industry": source["industry"].astype("string").str.strip(),
            "industry_key": source.get(
                "industry_key",
                source["industry"].astype("string").str.strip().str.upper(),
            ),
            "no_of_companies": pd.to_numeric(source["no_of_companies"], errors="coerce"),
            "total_market_cap": pd.to_numeric(source["total_market_cap"], errors="coerce"),
            "median_market_cap": pd.to_numeric(source["median_market_cap"], errors="coerce"),
            "median_pe": pd.to_numeric(source["median_pe"], errors="coerce"),
            "sales_growth_wavg": pd.to_numeric(source["sales_growth_wavg"], errors="coerce"),
            "opm_wavg": pd.to_numeric(source["opm_wavg"], errors="coerce"),
            "roce_wavg": pd.to_numeric(source["roce_wavg"], errors="coerce"),
            "median_1y_return": pd.to_numeric(source["median_1y_return"], errors="coerce"),
            "industry_growth_score": _round2(growth_score),
            "industry_quality_score": _round2(quality_score),
            "industry_valuation_score": _round2(valuation_score),
            "industry_momentum_score": _round2(momentum_score),
            "industry_fundamental_score": _round2(fundamental),
        }
    )

    labels: list[str] = []
    warnings: list[str] = []
    for _, row in output.iterrows():
        label, warning = classify_industry_label(row)
        labels.append(label)
        warnings.append(warning)
    output.loc[:, "industry_fundamental_label"] = labels
    output.loc[:, "industry_warning"] = warnings
    output.loc[:, "screener_industry_snapshot_date"] = snapshot_date

    return output[INDUSTRY_FUNDAMENTAL_OUTPUT_COLUMNS].reset_index(drop=True)
