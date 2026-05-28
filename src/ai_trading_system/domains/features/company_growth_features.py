"""Company-level quarterly growth features for fundamental insights."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import (
    connect_fundamentals_duckdb,
    ensure_fundamentals_analytical_schema,
)


@dataclass(frozen=True)
class CompanyGrowthFeaturesResult:
    rows: int
    symbols: int
    start_date: str | None
    end_date: str | None


def refresh_company_growth_features(
    *,
    fundamentals_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> CompanyGrowthFeaturesResult:
    conn = connect_fundamentals_duckdb(fundamentals_db_path)
    try:
        ensure_fundamentals_analytical_schema(conn)
        facts = _load_quarterly_facts(conn, to_date=to_date)
        features_all = compute_company_growth_features(facts)
        features = _filter_dates(features_all, from_date, to_date)
        if not features.empty:
            start, end = str(features["report_date"].min())[:10], str(features["report_date"].max())[:10]
            conn.execute(
                "DELETE FROM company_growth_features WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
            conn.execute(
                "DELETE FROM fundamental_period_facts WHERE period_type = 'quarterly' AND report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
            _insert_frame(conn, "fundamental_period_facts", _filter_dates(facts, from_date, to_date))
            _insert_frame(conn, "company_growth_features", features)
        elif from_date or to_date:
            start = str(from_date or to_date)[:10]
            end = str(to_date or from_date)[:10]
            conn.execute(
                "DELETE FROM company_growth_features WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
        else:
            start = end = None
    finally:
        conn.close()
    return CompanyGrowthFeaturesResult(
        rows=int(len(features)),
        symbols=int(features["symbol"].nunique()) if not features.empty else 0,
        start_date=start,
        end_date=end,
    )


def compute_company_growth_features(facts: pd.DataFrame) -> pd.DataFrame:
    columns = _feature_columns()
    if facts.empty:
        return pd.DataFrame(columns=columns)
    q = facts.copy()
    q.loc[:, "report_date"] = pd.to_datetime(q["report_date"]).dt.date
    q.loc[:, "available_at"] = pd.to_datetime(q["available_at"], errors="coerce").dt.date
    q = q.sort_values(["symbol", "report_date"], kind="stable")
    for column in ("sales_cr", "net_profit_cr", "operating_profit_cr", "opm_pct", "npm_pct"):
        q.loc[:, column] = pd.to_numeric(q[column], errors="coerce")
    grouped = q.groupby("symbol", sort=False)
    for column, base_name in (
        ("sales_cr", "sales"),
        ("net_profit_cr", "profit"),
        ("operating_profit_cr", "operating_profit"),
        ("opm_pct", "opm"),
        ("npm_pct", "npm"),
    ):
        q.loc[:, f"{base_name}_prev_q"] = grouped[column].shift(1)
        q.loc[:, f"{base_name}_same_q_ly"] = grouped[column].shift(4)

    q.loc[:, "sales_qoq_growth"] = _growth(q["sales_cr"], q["sales_prev_q"])
    q.loc[:, "sales_yoy_growth"] = _growth(q["sales_cr"], q["sales_same_q_ly"])
    q.loc[:, "profit_qoq_growth"] = _growth(q["net_profit_cr"], q["profit_prev_q"])
    q.loc[:, "profit_yoy_growth"] = _growth(q["net_profit_cr"], q["profit_same_q_ly"])
    q.loc[:, "operating_profit_qoq_growth"] = _growth(q["operating_profit_cr"], q["operating_profit_prev_q"])
    q.loc[:, "operating_profit_yoy_growth"] = _growth(q["operating_profit_cr"], q["operating_profit_same_q_ly"])
    q.loc[:, "opm_qoq_change"] = q["opm_pct"] - q["opm_prev_q"]
    q.loc[:, "opm_yoy_change"] = q["opm_pct"] - q["opm_same_q_ly"]
    q.loc[:, "npm_qoq_change"] = q["npm_pct"] - q["npm_prev_q"]
    q.loc[:, "npm_yoy_change"] = q["npm_pct"] - q["npm_same_q_ly"]

    q.loc[:, "sales_4q_cagr"] = _rolling_cagr(grouped, "sales_cr", 4)
    q.loc[:, "profit_4q_cagr"] = _rolling_cagr(grouped, "net_profit_cr", 4)
    q.loc[:, "sales_8q_cagr"] = _rolling_cagr(grouped, "sales_cr", 8)
    q.loc[:, "profit_8q_cagr"] = _rolling_cagr(grouped, "net_profit_cr", 8)
    q.loc[:, "positive_profit_quarters_4q"] = grouped["net_profit_cr"].transform(lambda s: s.gt(0).rolling(4, min_periods=1).sum())
    q.loc[:, "sales_growth_positive_quarters_4q"] = grouped["sales_yoy_growth"].transform(lambda s: s.gt(0).rolling(4, min_periods=1).sum())
    q.loc[:, "profit_growth_positive_quarters_4q"] = grouped["profit_yoy_growth"].transform(lambda s: s.gt(0).rolling(4, min_periods=1).sum())
    q.loc[:, "margin_expansion_quarters_4q"] = grouped["opm_yoy_change"].transform(lambda s: s.gt(0).rolling(4, min_periods=1).sum())
    q.loc[:, "created_at"] = pd.Timestamp.now(tz='UTC').tz_localize(None)
    for column in (
        "positive_profit_quarters_4q",
        "sales_growth_positive_quarters_4q",
        "profit_growth_positive_quarters_4q",
        "margin_expansion_quarters_4q",
    ):
        q.loc[:, column] = pd.to_numeric(q[column], errors="coerce").fillna(0).astype(int)
    return q[columns].reset_index(drop=True)


def _load_quarterly_facts(conn: duckdb.DuckDBPyConnection, *, to_date: str | None) -> pd.DataFrame:
    params: list[str] = []
    filters = ["period_type = 'quarterly'"]
    if to_date:
        filters.append("report_date <= CAST(? AS DATE)")
        params.append(str(to_date)[:10])
    raw = conn.execute(
        f"""
        SELECT
            symbol,
            'quarterly' AS period_type,
            report_date,
            max(available_at) AS available_at,
            max(CASE WHEN metric_id = 'sales' THEN value END) AS sales_cr,
            max(CASE WHEN metric_id = 'net_profit' THEN value END) AS net_profit_cr,
            max(CASE WHEN metric_id = 'operating_profit' THEN value END) AS operating_profit_cr,
            max(CASE WHEN metric_id = 'expenses' THEN value END) AS expenses_cr
        FROM screener_financials
        WHERE {' AND '.join(filters)}
        GROUP BY symbol, report_date
        ORDER BY symbol, report_date
        """,
        params,
    ).df()
    if raw.empty:
        return pd.DataFrame(columns=_fact_columns())
    for column in ("sales_cr", "net_profit_cr", "operating_profit_cr", "expenses_cr"):
        raw.loc[:, column] = pd.to_numeric(raw[column], errors="coerce")
    sales = raw["sales_cr"].where(raw["sales_cr"].gt(0))
    raw.loc[:, "opm_pct"] = raw["operating_profit_cr"] / sales * 100.0
    raw.loc[:, "npm_pct"] = raw["net_profit_cr"] / sales * 100.0
    return raw[_fact_columns()].reset_index(drop=True)


def _growth(current: pd.Series, previous: pd.Series) -> pd.Series:
    return current / previous.where(previous.gt(0)) - 1.0


def _rolling_cagr(grouped: pd.core.groupby.DataFrameGroupBy, column: str, quarters: int) -> pd.Series:
    def _calc(values: pd.Series) -> pd.Series:
        prior = values.shift(quarters)
        years = quarters / 4.0
        return (values / prior.where(prior.gt(0))).pow(1.0 / years) - 1.0

    return grouped[column].transform(_calc)


def _filter_dates(frame: pd.DataFrame, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    dates = pd.to_datetime(out["report_date"]).dt.date
    if from_date:
        out = out.loc[dates >= pd.Timestamp(from_date).date()]
        dates = pd.to_datetime(out["report_date"]).dt.date
    if to_date:
        out = out.loc[dates <= pd.Timestamp(to_date).date()]
    return out.reset_index(drop=True)


def _insert_frame(conn: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    conn.register("_company_growth_frame", frame)
    try:
        conn.execute(f"INSERT INTO {table} SELECT * FROM _company_growth_frame")
    finally:
        conn.unregister("_company_growth_frame")


def _fact_columns() -> list[str]:
    return [
        "symbol",
        "period_type",
        "report_date",
        "available_at",
        "sales_cr",
        "net_profit_cr",
        "operating_profit_cr",
        "expenses_cr",
        "opm_pct",
        "npm_pct",
    ]


def _feature_columns() -> list[str]:
    return [
        "symbol",
        "report_date",
        "available_at",
        "sales_cr",
        "net_profit_cr",
        "operating_profit_cr",
        "opm_pct",
        "npm_pct",
        "sales_qoq_growth",
        "sales_yoy_growth",
        "profit_qoq_growth",
        "profit_yoy_growth",
        "operating_profit_qoq_growth",
        "operating_profit_yoy_growth",
        "opm_qoq_change",
        "opm_yoy_change",
        "npm_qoq_change",
        "npm_yoy_change",
        "sales_4q_cagr",
        "profit_4q_cagr",
        "sales_8q_cagr",
        "profit_8q_cagr",
        "positive_profit_quarters_4q",
        "sales_growth_positive_quarters_4q",
        "profit_growth_positive_quarters_4q",
        "margin_expansion_quarters_4q",
        "created_at",
    ]


__all__ = ["CompanyGrowthFeaturesResult", "compute_company_growth_features", "refresh_company_growth_features"]
