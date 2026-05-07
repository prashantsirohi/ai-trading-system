"""Fundamental scoring and red-flag generation."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import FUNDAMENTAL_OUTPUT_COLUMNS

FINANCIAL_MARKERS = (
    "BANK",
    "FINANCE",
    "FINANCIAL",
    "NBFC",
    "INSURANCE",
    "HOUSING FINANCE",
    "ASSET MANAGEMENT",
    "CAPITAL MARKETS",
)


def clip_score(value: float | int | None, low: float, high: float, invert: bool = False) -> float:
    """Map a scalar to a 0-100 score between low and high."""

    if value is None or pd.isna(value):
        return 50.0
    if high == low:
        return 50.0
    ratio = (float(value) - float(low)) / (float(high) - float(low))
    score = float(np.clip(ratio, 0.0, 1.0) * 100.0)
    return 100.0 - score if invert else score


def percentile_score(series: pd.Series, invert: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    scores = numeric.rank(pct=True, na_option="keep") * 100.0
    if invert:
        scores = 100.0 - scores
    return scores.fillna(50.0).clip(0, 100)


def sector_relative_score(
    df: pd.DataFrame,
    column: str,
    sector_col: str = "industry_group",
    invert: bool = False,
) -> pd.Series:
    if column not in df.columns:
        return pd.Series(50.0, index=df.index)
    if sector_col not in df.columns:
        return percentile_score(df[column], invert=invert)
    return df.groupby(sector_col, dropna=False)[column].transform(lambda values: percentile_score(values, invert=invert))


def _score_series(df: pd.DataFrame, column: str, low: float, high: float, *, invert: bool = False, missing: float = 50.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series(missing, index=df.index, dtype=float)
    values = pd.to_numeric(df[column], errors="coerce")
    scores = values.map(lambda value: clip_score(value, low, high, invert=invert))
    return scores.fillna(missing).astype(float)


def _positive_score(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(50.0, index=df.index, dtype=float)
    values = pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.where(values.isna(), 50.0, np.where(values > 0, 100.0, 0.0)), index=df.index, dtype=float)


def _peg_score(df: pd.DataFrame) -> pd.Series:
    if "peg_3y" not in df.columns:
        return pd.Series(50.0, index=df.index, dtype=float)
    peg = pd.to_numeric(df["peg_3y"], errors="coerce")
    score = pd.Series(50.0, index=df.index, dtype=float)
    strong = peg.gt(0) & peg.le(1.5)
    acceptable = peg.gt(1.5) & peg.le(3)
    weak = peg.gt(3)
    score.loc[strong] = 100.0 - ((peg.loc[strong] / 1.5) * 20.0)
    score.loc[acceptable] = 80.0 - (((peg.loc[acceptable] - 1.5) / 1.5) * 40.0)
    score.loc[weak] = (40.0 - ((peg.loc[weak] - 3.0) / 3.0) * 40.0).clip(lower=0.0)
    return score.clip(0, 100)


def _is_financial(df: pd.DataFrame) -> pd.Series:
    group = df.get("industry_group", pd.Series("", index=df.index)).astype("string").fillna("").str.upper()
    industry = df.get("industry", pd.Series("", index=df.index)).astype("string").fillna("").str.upper()
    text = group + " " + industry
    return text.map(lambda value: any(marker in value for marker in FINANCIAL_MARKERS))


def _flag(condition: pd.Series, name: str, flags: list[list[str]]) -> None:
    for idx in condition[condition.fillna(False)].index:
        flags[idx].append(name)


def _truthy_false(series: pd.Series, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(False, index=index)
    text = series.astype("string").str.strip().str.lower()
    numeric = pd.to_numeric(series, errors="coerce")
    return text.isin({"0", "false", "f", "no", "n"}) | numeric.eq(0)


def compute_fundamental_scores(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """Compute weighted fundamental scores, tiers, and red flags."""

    source = df.copy() if df is not None else pd.DataFrame()
    if source.empty:
        return pd.DataFrame(columns=FUNDAMENTAL_OUTPUT_COLUMNS)
    for column in ("symbol", "name", "industry_group", "industry"):
        if column not in source.columns:
            source.loc[:, column] = ""
    source.loc[:, "symbol"] = source["symbol"].astype("string").str.strip().str.upper()

    opm_stability = (
        pd.to_numeric(source.get("opm", pd.Series(np.nan, index=source.index)), errors="coerce")
        - pd.to_numeric(source.get("opm_last_year", pd.Series(np.nan, index=source.index)), errors="coerce")
    )
    quality = (
        0.35 * _score_series(source, "roce", 5, 30, missing=35)
        + 0.25 * _score_series(source, "roe", 5, 25, missing=35)
        + 0.20 * _score_series(source, "opm", 5, 30, missing=45)
        + 0.10 * opm_stability.map(lambda value: clip_score(value, -10, 10)).fillna(50)
        + 0.10 * _score_series(source, "piotroski_score", 0, 9, missing=45)
    )

    growth = (
        0.30 * _score_series(source, "sales_growth_3y", 0, 25, missing=40)
        + 0.20 * _score_series(source, "sales_growth_5y", 0, 25, missing=40)
        + 0.25 * _score_series(source, "profit_growth_3y", 0, 25, missing=35)
        + 0.15 * _score_series(source, "profit_growth_5y", 0, 25, missing=35)
        + 0.10 * _score_series(source, "yoy_quarterly_profit_growth", 0, 30, missing=40)
    )

    debt_score = _score_series(source, "debt_to_equity", 0.5, 2.0, invert=True, missing=50)
    balance = (
        0.40 * debt_score
        + 0.30 * _positive_score(source, "cash_from_operations_last_year")
        + 0.30 * _positive_score(source, "free_cash_flow_last_year")
    )

    valuation = (
        0.25 * sector_relative_score(source, "pe", invert=True)
        + 0.15 * sector_relative_score(source, "forward_pe", invert=True)
        + 0.25 * sector_relative_score(source, "ev_ebitda", invert=True)
        + 0.20 * _peg_score(source)
        + 0.10 * sector_relative_score(source, "price_to_sales", invert=True)
        + 0.05 * sector_relative_score(source, "price_to_book", invert=True)
    )

    pledge_score = _score_series(source, "pledged_pct", 0, 10, invert=True, missing=70)
    ownership = (
        0.40 * pledge_score
        + 0.30 * _score_series(source, "promoter_holding", 20, 50, missing=50)
        + 0.15 * _score_series(source, "dii_holding", 0, 15, missing=50)
        + 0.15 * _score_series(source, "fii_holding", 0, 20, missing=50)
    )

    fundamental = 0.35 * quality + 0.25 * growth + 0.20 * balance + 0.10 * valuation + 0.10 * ownership

    is_financial = _is_financial(source)
    debt = pd.to_numeric(source.get("debt_to_equity", pd.Series(np.nan, index=source.index)), errors="coerce")
    pledge = pd.to_numeric(source.get("pledged_pct", pd.Series(np.nan, index=source.index)), errors="coerce")
    roce = pd.to_numeric(source.get("roce", pd.Series(np.nan, index=source.index)), errors="coerce")
    roe = pd.to_numeric(source.get("roe", pd.Series(np.nan, index=source.index)), errors="coerce")
    profit_growth_3y = pd.to_numeric(source.get("profit_growth_3y", pd.Series(np.nan, index=source.index)), errors="coerce")
    sales_growth_3y = pd.to_numeric(source.get("sales_growth_3y", pd.Series(np.nan, index=source.index)), errors="coerce")
    cfo = pd.to_numeric(source.get("cash_from_operations_last_year", pd.Series(np.nan, index=source.index)), errors="coerce")
    fcf = pd.to_numeric(source.get("free_cash_flow_last_year", pd.Series(np.nan, index=source.index)), errors="coerce")
    piotroski = pd.to_numeric(source.get("piotroski_score", pd.Series(np.nan, index=source.index)), errors="coerce")
    yoy = pd.to_numeric(source.get("yoy_quarterly_profit_growth", pd.Series(np.nan, index=source.index)), errors="coerce")

    flags: list[list[str]] = [[] for _ in range(len(source))]
    hard = pd.Series(False, index=source.index)

    hard_conditions = [
        (_truthy_false(source.get("is_not_sme"), source.index), "is_not_sme false"),
        (pledge.gt(10), "pledged_pct > 10"),
        (debt.gt(2) & ~is_financial, "debt_to_equity > 2"),
        (roce.lt(8), "roce < 8"),
        (roe.lt(5), "roe < 5"),
        (profit_growth_3y.lt(0) & sales_growth_3y.lt(0), "profit_growth_3y < 0 and sales_growth_3y < 0"),
        (cfo.lt(0) & fcf.lt(0), "cash_from_operations_last_year < 0 and free_cash_flow_last_year < 0"),
        (piotroski.lt(4), "piotroski_score < 4"),
    ]
    for condition, label in hard_conditions:
        condition = condition.fillna(False)
        hard = hard | condition
        _flag(condition, label, flags)

    minor_conditions = [
        (pledge.ge(5) & pledge.le(10), "pledged_pct between 5 and 10"),
        (debt.ge(1) & debt.le(2) & ~is_financial, "debt_to_equity between 1 and 2"),
        (opm_stability.lt(-5), "opm declined more than 5"),
        (yoy.lt(0), "yoy_quarterly_profit_growth < 0"),
        (fcf.lt(0), "free_cash_flow_last_year < 0"),
        (cfo.lt(0), "cash_from_operations_last_year < 0"),
    ]
    minor = pd.Series(False, index=source.index)
    for condition, label in minor_conditions:
        condition = condition.fillna(False)
        minor = minor | condition
        _flag(condition, label, flags)

    tiers = []
    for idx, score in fundamental.items():
        if bool(hard.loc[idx]) or score < 40:
            tiers.append("Reject")
        elif score >= 70 and not bool(minor.loc[idx]):
            tiers.append("A")
        elif score >= 55 and not bool(minor.loc[idx]):
            tiers.append("B")
        else:
            tiers.append("C")

    output = pd.DataFrame(
        {
            "snapshot_date": snapshot_date,
            "symbol": source["symbol"],
            "name": source["name"],
            "industry_group": source["industry_group"],
            "industry": source["industry"],
            "quality_score": quality,
            "growth_score": growth,
            "balance_sheet_score": balance,
            "valuation_score": valuation,
            "ownership_score": ownership,
            "fundamental_score": fundamental,
            "fundamental_tier": tiers,
            "red_flags": ["; ".join(items) for items in flags],
            "hard_red_flag": hard.astype(bool),
            "screener_snapshot_date": snapshot_date,
        }
    )
    for column in ("quality_score", "growth_score", "balance_sheet_score", "valuation_score", "ownership_score", "fundamental_score"):
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce").round(2)
    return output[FUNDAMENTAL_OUTPUT_COLUMNS].reset_index(drop=True)

