"""Sector earnings leadership scoring and refresh orchestration."""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import (
    connect_fundamentals_duckdb,
    ensure_fundamentals_analytical_schema,
)
from ai_trading_system.domains.features.fundamental_growth import refresh_fundamental_growth
from ai_trading_system.domains.features.fundamental_period_facts import (
    ensure_sector_earnings_schema,
    refresh_fundamental_period_facts,
)
from ai_trading_system.domains.fundamentals.screener_store import default_screener_db_path
from ai_trading_system.platform.db.paths import get_domain_paths


@dataclass(frozen=True)
class SectorEarningsLeadershipResult:
    status: str
    facts_rows: int
    company_rows: int
    sector_rows: int
    leadership_rows: int
    latest_rows: int
    latest_report_date: str | None
    output_csv: str | None


@dataclass(frozen=True)
class AnalyticalSectorLeadershipResult:
    rows: int
    latest_rows: int
    latest_report_date: str | None
    output_csv: str | None


def refresh_sector_earnings_leadership(
    *,
    ohlcv_db_path: str | Path | None = None,
    screener_db_path: str | Path | None = None,
    master_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    output_csv: str | Path | None = None,
) -> dict[str, Any]:
    paths = get_domain_paths()
    resolved_ohlcv = Path(ohlcv_db_path) if ohlcv_db_path is not None else paths.ohlcv_db_path
    resolved_screener = Path(screener_db_path) if screener_db_path is not None else default_screener_db_path()
    resolved_master = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    if not resolved_screener.exists():
        return asdict(SectorEarningsLeadershipResult("skipped_missing_screener_db", 0, 0, 0, 0, 0, None, None))

    facts = refresh_fundamental_period_facts(
        ohlcv_db_path=resolved_ohlcv,
        screener_db_path=resolved_screener,
        master_db_path=resolved_master,
        from_date=None,
        to_date=to_date,
    )
    growth = refresh_fundamental_growth(
        ohlcv_db_path=resolved_ohlcv,
        from_date=from_date,
        to_date=to_date,
    )
    leadership, latest = _refresh_leadership_table(
        resolved_ohlcv,
        from_date=from_date,
        to_date=to_date,
    )
    csv_path = None
    if output_csv is not None:
        csv_path = str(output_csv)
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        latest.to_csv(csv_path, index=False)
    return asdict(
        SectorEarningsLeadershipResult(
            status="completed",
            facts_rows=facts.facts_rows,
            company_rows=growth.company_rows,
            sector_rows=growth.sector_rows,
            leadership_rows=int(len(leadership)),
            latest_rows=int(len(latest)),
            latest_report_date=str(latest["report_date"].max())[:10] if not latest.empty else None,
            output_csv=csv_path,
        )
    )


def _refresh_leadership_table(
    ohlcv_db_path: str | Path,
    *,
    from_date: str | None,
    to_date: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = duckdb.connect(str(ohlcv_db_path))
    try:
        ensure_sector_earnings_schema(conn)
        sector = conn.execute(
            """
            SELECT *
            FROM sector_fundamental_growth
            WHERE report_date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            """,
            [str(to_date)[:10] if to_date else None],
        ).df()
        leadership_all = _score_leadership(sector)
        leadership = _filter_dates(leadership_all, from_date, to_date)
        if not leadership.empty:
            start, end = str(leadership["report_date"].min())[:10], str(leadership["report_date"].max())[:10]
            conn.execute(
                """
                DELETE FROM sector_earnings_leadership
                WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                """,
                [start, end],
            )
            conn.register("_sector_earnings_leadership_frame", leadership)
            try:
                conn.execute("INSERT INTO sector_earnings_leadership SELECT * FROM _sector_earnings_leadership_frame")
            finally:
                conn.unregister("_sector_earnings_leadership_frame")
        latest = _latest_frame(leadership)
        return leadership, latest
    finally:
        conn.close()


def _score_leadership(sector: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "sector_name",
        "report_date",
        "sector_sales_yoy_growth",
        "sector_sales_qoq_growth",
        "sector_profit_yoy_growth",
        "sector_profit_qoq_growth",
        "median_sales_yoy_growth",
        "median_profit_yoy_growth",
        "sales_yoy_positive_pct",
        "profit_yoy_positive_pct",
        "margin_expansion_pct",
        "sales_yoy_rank",
        "sales_qoq_rank",
        "profit_yoy_rank",
        "median_profit_rank",
        "sales_breadth_rank",
        "margin_rank",
        "sector_earnings_growth_score",
        "earnings_trend_label",
    ]
    if sector.empty:
        return pd.DataFrame(columns=columns)
    frame = sector.copy()
    frame.loc[:, "report_date"] = pd.to_datetime(frame["report_date"]).dt.date
    rank_inputs = {
        "sales_yoy_rank": "sector_sales_yoy_growth",
        "sales_qoq_rank": "sector_sales_qoq_growth",
        "profit_yoy_rank": "sector_profit_yoy_growth",
        "median_profit_rank": "median_profit_yoy_growth",
        "sales_breadth_rank": "sales_yoy_positive_pct",
        "margin_rank": "margin_expansion_pct",
    }
    for output_col, input_col in rank_inputs.items():
        frame.loc[:, output_col] = (
            frame.groupby("report_date", group_keys=False)[input_col]
            .transform(lambda values: values.rank(pct=True) * 100.0)
        )
    frame.loc[:, "sector_earnings_growth_score"] = (
        0.30 * frame["sales_yoy_rank"].fillna(50.0)
        + 0.20 * frame["sales_qoq_rank"].fillna(50.0)
        + 0.25 * frame["profit_yoy_rank"].fillna(50.0)
        + 0.10 * frame["median_profit_rank"].fillna(50.0)
        + 0.10 * frame["sales_breadth_rank"].fillna(50.0)
        + 0.05 * frame["margin_rank"].fillna(50.0)
    )
    frame.loc[:, "earnings_trend_label"] = frame.apply(_trend_label, axis=1)
    return frame[columns].sort_values(["report_date", "sector_earnings_growth_score"], ascending=[True, False]).reset_index(drop=True)


def _trend_label(row: pd.Series) -> str:
    sales_yoy = _num(row.get("sector_sales_yoy_growth"))
    profit_yoy = _num(row.get("sector_profit_yoy_growth"))
    sales_qoq = _num(row.get("sector_sales_qoq_growth"))
    profit_qoq = _num(row.get("sector_profit_qoq_growth"))
    sales_breadth = _num(row.get("sales_yoy_positive_pct"))
    margin = _num(row.get("margin_expansion_pct"))
    if sales_yoy > 0.15 and profit_yoy > 0.20 and sales_qoq > 0.03 and profit_qoq > 0.05 and sales_breadth > 60 and margin > 50:
        return "accelerating_leader"
    if sales_yoy > 0 and profit_yoy > 0 and margin > 50:
        return "earnings_recovery"
    if sales_yoy > 0 and profit_yoy > 0 and margin < 40:
        return "growth_but_margin_pressure"
    if sales_yoy < 0 or profit_yoy < 0 or sales_breadth < 40:
        return "weak_or_declining"
    return "neutral"


def _num(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return parsed


def _filter_dates(frame: pd.DataFrame, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    if frame.empty:
        return frame
    output = frame.copy()
    dates = pd.to_datetime(output["report_date"]).dt.date
    if from_date:
        output = output.loc[dates >= pd.Timestamp(from_date).date()]
        dates = pd.to_datetime(output["report_date"]).dt.date
    if to_date:
        output = output.loc[dates <= pd.Timestamp(to_date).date()]
    return output.reset_index(drop=True)


def _latest_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    latest_date = frame["report_date"].max()
    latest = frame.loc[frame["report_date"].eq(latest_date)].copy()
    return latest.sort_values("sector_earnings_growth_score", ascending=False).reset_index(drop=True)


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(description="Refresh sector earnings leadership features.")
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--full-rebuild", action="store_true")
    parser.add_argument("--screener-db-path", default=str(default_screener_db_path()))
    parser.add_argument("--ohlcv-db-path", default=str(paths.ohlcv_db_path))
    parser.add_argument("--master-db-path", default=str(paths.master_db_path))
    parser.add_argument("--output-csv", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = refresh_sector_earnings_leadership(
        ohlcv_db_path=args.ohlcv_db_path,
        screener_db_path=args.screener_db_path,
        master_db_path=args.master_db_path,
        from_date=None if args.full_rebuild else args.from_date,
        to_date=args.to_date,
        output_csv=args.output_csv,
    )
    print(result)


def refresh_sector_earnings_leadership_analytical(
    *,
    fundamentals_db_path: str | Path | None = None,
    master_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    output_csv: str | Path | None = None,
) -> dict[str, Any]:
    paths = get_domain_paths()
    resolved_master = Path(master_db_path) if master_db_path is not None else paths.master_db_path
    sector_map = _load_sector_map_for_analytical(resolved_master)
    conn = connect_fundamentals_duckdb(fundamentals_db_path)
    try:
        ensure_fundamentals_analytical_schema(conn)
        company = conn.execute(
            """
            SELECT *
            FROM company_growth_features
            WHERE report_date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            ORDER BY symbol, report_date
            """,
            [str(to_date)[:10] if to_date else None],
        ).df()
        tags = conn.execute(
            """
            SELECT symbol, report_date, insight_type
            FROM company_insight_tags
            WHERE report_date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            """,
            [str(to_date)[:10] if to_date else None],
        ).df()
        leadership_all = compute_sector_earnings_leadership_analytical(company, tags, sector_map)
        leadership = _filter_dates(leadership_all, from_date, to_date)
        if not leadership.empty:
            start, end = str(leadership["report_date"].min())[:10], str(leadership["report_date"].max())[:10]
            conn.execute(
                "DELETE FROM sector_earnings_leadership WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
            conn.register("_sector_leadership_analytical_frame", leadership)
            try:
                conn.execute("INSERT INTO sector_earnings_leadership SELECT * FROM _sector_leadership_analytical_frame")
            finally:
                conn.unregister("_sector_leadership_analytical_frame")
        elif from_date or to_date:
            start = str(from_date or to_date)[:10]
            end = str(to_date or from_date)[:10]
            conn.execute(
                "DELETE FROM sector_earnings_leadership WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
    finally:
        conn.close()

    latest = _latest_frame(leadership.rename(columns={"sector_fundamental_score": "sector_earnings_growth_score"}))
    if "sector_earnings_growth_score" in latest.columns:
        latest = latest.rename(columns={"sector_earnings_growth_score": "sector_fundamental_score"})
    csv_path = None
    if output_csv is not None:
        csv_path = str(output_csv)
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        latest.to_csv(csv_path, index=False)
    return asdict(
        AnalyticalSectorLeadershipResult(
            rows=int(len(leadership)),
            latest_rows=int(len(latest)),
            latest_report_date=str(latest["report_date"].max())[:10] if not latest.empty else None,
            output_csv=csv_path,
        )
    )


def compute_sector_earnings_leadership_analytical(
    company: pd.DataFrame,
    tags: pd.DataFrame,
    sector_map: dict[str, dict[str, str]] | None = None,
) -> pd.DataFrame:
    columns = [
        "sector_name",
        "report_date",
        "sector_sales_yoy_growth",
        "sector_profit_yoy_growth",
        "sector_sales_qoq_growth",
        "sector_profit_qoq_growth",
        "median_company_sales_yoy",
        "median_company_profit_yoy",
        "sales_positive_pct",
        "profit_positive_pct",
        "margin_expansion_pct",
        "great_result_count",
        "turnaround_count",
        "compounder_count",
        "aggregate_sales_growth_rank",
        "aggregate_profit_growth_rank",
        "sales_breadth_rank",
        "profit_breadth_rank",
        "margin_expansion_rank",
        "great_result_count_rank",
        "turnaround_count_rank",
        "sector_fundamental_score",
        "earnings_trend_label",
        "created_at",
    ]
    if company.empty:
        return pd.DataFrame(columns=columns)
    frame = company.copy()
    frame.loc[:, "symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    sector_map = sector_map or {}
    frame.loc[:, "sector_name"] = frame["symbol"].map(lambda symbol: sector_map.get(symbol, {}).get("sector_name", "Other"))
    frame.loc[:, "report_date"] = pd.to_datetime(frame["report_date"]).dt.date
    for col in ("sales_cr", "net_profit_cr", "sales_yoy_growth", "profit_yoy_growth", "sales_qoq_growth", "profit_qoq_growth", "opm_yoy_change"):
        frame.loc[:, col] = pd.to_numeric(frame[col], errors="coerce")
    grouped = frame.groupby(["sector_name", "report_date"], sort=True)
    sector = grouped.agg(
        sector_sales_cr=("sales_cr", "sum"),
        sector_profit_cr=("net_profit_cr", "sum"),
        median_company_sales_yoy=("sales_yoy_growth", "median"),
        median_company_profit_yoy=("profit_yoy_growth", "median"),
        sector_sales_qoq_growth=("sales_qoq_growth", "median"),
        sector_profit_qoq_growth=("profit_qoq_growth", "median"),
        sales_positive_pct=("sales_yoy_growth", _positive_pct),
        profit_positive_pct=("profit_yoy_growth", _positive_pct),
        margin_expansion_pct=("opm_yoy_change", _positive_pct),
    ).reset_index()
    sector = sector.sort_values(["sector_name", "report_date"], kind="stable")
    sgroup = sector.groupby("sector_name", sort=False)
    sector.loc[:, "sector_sales_yoy_growth"] = sector["sector_sales_cr"] / sgroup["sector_sales_cr"].shift(4).where(sgroup["sector_sales_cr"].shift(4).gt(0)) - 1.0
    sector.loc[:, "sector_profit_yoy_growth"] = sector["sector_profit_cr"] / sgroup["sector_profit_cr"].shift(4).where(sgroup["sector_profit_cr"].shift(4).gt(0)) - 1.0

    counts = _sector_tag_counts(tags, frame[["symbol", "report_date", "sector_name"]])
    sector = sector.merge(counts, on=["sector_name", "report_date"], how="left")
    for column in ("great_result_count", "turnaround_count", "compounder_count"):
        sector.loc[:, column] = pd.to_numeric(sector[column], errors="coerce").fillna(0).astype(int)

    rank_inputs = {
        "aggregate_sales_growth_rank": "sector_sales_yoy_growth",
        "aggregate_profit_growth_rank": "sector_profit_yoy_growth",
        "sales_breadth_rank": "sales_positive_pct",
        "profit_breadth_rank": "profit_positive_pct",
        "margin_expansion_rank": "margin_expansion_pct",
        "great_result_count_rank": "great_result_count",
        "turnaround_count_rank": "turnaround_count",
    }
    for out, source in rank_inputs.items():
        sector.loc[:, out] = sector.groupby("report_date", group_keys=False)[source].transform(lambda values: values.rank(pct=True) * 100.0)
    sector.loc[:, "sector_fundamental_score"] = (
        0.25 * sector["aggregate_sales_growth_rank"].fillna(50.0)
        + 0.25 * sector["aggregate_profit_growth_rank"].fillna(50.0)
        + 0.15 * sector["sales_breadth_rank"].fillna(50.0)
        + 0.15 * sector["profit_breadth_rank"].fillna(50.0)
        + 0.10 * sector["margin_expansion_rank"].fillna(50.0)
        + 0.05 * sector["great_result_count_rank"].fillna(50.0)
        + 0.05 * sector["turnaround_count_rank"].fillna(50.0)
    )
    sector.loc[:, "earnings_trend_label"] = sector.apply(_analytical_trend_label, axis=1)
    sector.loc[:, "created_at"] = pd.Timestamp.now(tz='UTC').tz_localize(None)
    return sector[columns].sort_values(["report_date", "sector_fundamental_score"], ascending=[True, False]).reset_index(drop=True)


def _sector_tag_counts(tags: pd.DataFrame, company_sectors: pd.DataFrame) -> pd.DataFrame:
    columns = ["sector_name", "report_date", "great_result_count", "turnaround_count", "compounder_count"]
    if tags.empty:
        return pd.DataFrame(columns=columns)
    tag_frame = tags.copy()
    tag_frame.loc[:, "symbol"] = tag_frame["symbol"].astype(str).str.upper().str.strip()
    tag_frame.loc[:, "report_date"] = pd.to_datetime(tag_frame["report_date"]).dt.date
    mapping = company_sectors.drop_duplicates(["symbol", "report_date"])
    tag_frame = tag_frame.merge(mapping, on=["symbol", "report_date"], how="left")
    tag_frame = tag_frame.loc[tag_frame["sector_name"].notna()]
    if tag_frame.empty:
        return pd.DataFrame(columns=columns)
    tag_frame.loc[:, "great_result"] = tag_frame["insight_type"].isin({"great_result", "blowout_result", "margin_expansion_result", "revenue_acceleration_result", "profit_acceleration_result"})
    tag_frame.loc[:, "turnaround"] = tag_frame["insight_type"].isin({"turnaround_candidate", "turnaround_confirmed", "loss_to_profit", "margin_recovery", "sales_recovery"})
    tag_frame.loc[:, "compounder"] = tag_frame["insight_type"].isin({"consistent_compounder", "emerging_compounder", "high_growth_compounder", "quality_growth", "expensive_compounder"})
    return tag_frame.groupby(["sector_name", "report_date"], sort=True).agg(
        great_result_count=("great_result", "sum"),
        turnaround_count=("turnaround", "sum"),
        compounder_count=("compounder", "sum"),
    ).reset_index()[columns]


def _analytical_trend_label(row: pd.Series) -> str:
    if _num(row.get("sector_sales_yoy_growth")) > 0.15 and _num(row.get("sector_profit_yoy_growth")) > 0.20 and _num(row.get("sales_positive_pct")) > 60:
        return "accelerating_leader"
    if _num(row.get("turnaround_count")) >= 2 and _num(row.get("margin_expansion_pct")) > 50:
        return "turnaround_cluster"
    if _num(row.get("compounder_count")) >= 2:
        return "quality_compounding"
    if _num(row.get("sector_sales_yoy_growth")) < 0 or _num(row.get("sector_profit_yoy_growth")) < 0:
        return "weak_or_declining"
    return "neutral"


def _positive_pct(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float(clean.gt(0).mean() * 100.0)


def _load_sector_map_for_analytical(master_db_path: str | Path) -> dict[str, dict[str, str]]:
    if not Path(master_db_path).exists():
        return {}
    conn = sqlite3.connect(str(master_db_path))
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        frames = []
        if "stock_details" in tables:
            frames.append(
                pd.read_sql_query(
                    "SELECT Symbol AS symbol, Sector AS sector_name, [Industry Group] AS industry_group FROM stock_details WHERE exchange = 'NSE'",
                    conn,
                )
            )
        if "symbols" in tables:
            frames.append(
                pd.read_sql_query(
                    """
                    SELECT
                        s.symbol_id AS symbol,
                        COALESCE(sm.system_sector, s.sector, 'Other') AS sector_name,
                        COALESCE(s.industry, s.sector, '') AS industry_group
                    FROM symbols s
                    LEFT JOIN sector_mapping sm ON s.sector = sm.industry
                    WHERE s.exchange = 'NSE'
                    """,
                    conn,
                )
            )
    finally:
        conn.close()
    if not frames:
        return {}
    frame = pd.concat(frames, ignore_index=True).drop_duplicates("symbol", keep="first")
    return {
        str(row["symbol"]).upper().strip(): {
            "sector_name": str(row.get("sector_name") or "Other"),
            "industry_group": str(row.get("industry_group") or ""),
        }
        for _, row in frame.iterrows()
    }


if __name__ == "__main__":
    main()


__all__ = [
    "AnalyticalSectorLeadershipResult",
    "SectorEarningsLeadershipResult",
    "compute_sector_earnings_leadership_analytical",
    "refresh_sector_earnings_leadership",
    "refresh_sector_earnings_leadership_analytical",
]
