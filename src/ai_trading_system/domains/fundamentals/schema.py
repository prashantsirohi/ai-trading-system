"""Screener CSV normalization for manual fundamental snapshots."""

from __future__ import annotations

import pandas as pd


SCREENER_COLUMN_MAP = {
    "Name": "name",
    "BSE Code": "bse_code",
    "NSE Code": "symbol",
    "ISIN Code": "isin_code",
    "Industry Group": "industry_group",
    "Industry": "industry",
    "Current Price": "current_price",
    "Market Capitalization": "market_cap",
    "Price to Earning": "pe",
    "Forward PE": "forward_pe",
    "PEG 3 Years Growth": "peg_3y",
    "YOY Quarterly profit growth": "yoy_quarterly_profit_growth",
    "Profit growth 3Years": "profit_growth_3y",
    "Sales growth 3Years": "sales_growth_3y",
    "Sales growth 5Years": "sales_growth_5y",
    "Piotroski score": "piotroski_score",
    "EVEBITDA": "ev_ebitda",
    "EPS QoQ Growth": "eps_qoq_growth",
    "Debt to equity": "debt_to_equity",
    "Profit growth 5Years": "profit_growth_5y",
    "Return on capital employed": "roce",
    "Return on equity": "roe",
    "Sales growth": "sales_growth",
    "Profit growth": "profit_growth",
    "Price to Sales": "price_to_sales",
    "Price to book value": "price_to_book",
    "Pledged percentage": "pledged_pct",
    "Promoter holding": "promoter_holding",
    "DII holding": "dii_holding",
    "FII holding": "fii_holding",
    "Public holding": "public_holding",
    "OPM": "opm",
    "OPM last year": "opm_last_year",
    "Sales 2quarters back": "sales_2q_back",
    "Sales 3quarters back": "sales_3q_back",
    "Net profit 2quarters back": "net_profit_2q_back",
    "Net profit 3quarters back": "net_profit_3q_back",
    "Cash from operations last year": "cash_from_operations_last_year",
    "Cash from investing last year": "cash_from_investing_last_year",
    "Cash from financing last year": "cash_from_financing_last_year",
    "Free cash flow last year": "free_cash_flow_last_year",
    "Is not SME": "is_not_sme",
}

REQUIRED_COLUMNS = ["Name", "NSE Code", "Industry Group", "Industry"]
ID_COLUMNS = ["name", "bse_code", "symbol", "isin_code", "industry_group", "industry"]
NUMERIC_COLUMNS = [
    "current_price",
    "market_cap",
    "pe",
    "forward_pe",
    "peg_3y",
    "yoy_quarterly_profit_growth",
    "profit_growth_3y",
    "sales_growth_3y",
    "sales_growth_5y",
    "piotroski_score",
    "ev_ebitda",
    "eps_qoq_growth",
    "debt_to_equity",
    "profit_growth_5y",
    "roce",
    "roe",
    "sales_growth",
    "profit_growth",
    "price_to_sales",
    "price_to_book",
    "pledged_pct",
    "promoter_holding",
    "dii_holding",
    "fii_holding",
    "public_holding",
    "opm",
    "opm_last_year",
    "sales_2q_back",
    "sales_3q_back",
    "net_profit_2q_back",
    "net_profit_3q_back",
    "cash_from_operations_last_year",
    "cash_from_investing_last_year",
    "cash_from_financing_last_year",
    "free_cash_flow_last_year",
]


def _coerce_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype("string")
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .replace({"": pd.NA, "-": pd.NA, "NA": pd.NA, "N/A": pd.NA, "nan": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce").astype(float)


def _coerce_boolish(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip().str.lower()
    mapped = text.map(
        {
            "1": 1,
            "true": 1,
            "t": 1,
            "yes": 1,
            "y": 1,
            "0": 0,
            "false": 0,
            "f": 0,
            "no": 0,
            "n": 0,
        }
    )
    numeric = pd.to_numeric(series, errors="coerce")
    return mapped.fillna(numeric.where(numeric.isna(), (numeric != 0).astype(int))).astype("Int64")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Screener export columns while preserving unknown extras."""

    if df is None:
        raise ValueError("Screener data frame is required")
    output = df.copy()
    output.columns = [str(column).strip() for column in output.columns]
    missing = [column for column in REQUIRED_COLUMNS if column not in output.columns]
    if missing:
        raise ValueError(f"Missing required Screener columns: {', '.join(missing)}")

    output = output.rename(columns=SCREENER_COLUMN_MAP)
    for column in output.select_dtypes(include=["object", "string"]).columns:
        output.loc[:, column] = output[column].astype("string").str.strip()
    if "symbol" in output.columns:
        output.loc[:, "symbol"] = output["symbol"].astype("string").str.strip().str.upper()

    for column in NUMERIC_COLUMNS:
        if column in output.columns:
            output.loc[:, column] = _coerce_numeric(output[column])
    if "is_not_sme" in output.columns:
        output.loc[:, "is_not_sme"] = _coerce_boolish(output["is_not_sme"])

    output = output.loc[output["symbol"].notna() & output["symbol"].astype(str).str.strip().ne("")].copy()
    return output.reset_index(drop=True)

