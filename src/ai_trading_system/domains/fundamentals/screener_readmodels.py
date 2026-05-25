"""Derived CSV readmodels from the canonical Screener SQLite database."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.fundamentals.scoring import compute_fundamental_scores
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore, default_screener_db_path
from ai_trading_system.domains.fundamentals.trends import compute_fundamental_trends
from ai_trading_system.platform.db.paths import get_domain_paths


def build_scores_from_screener_db(
    *,
    db_path: str | Path | None = None,
    snapshot_date: str | None = None,
) -> pd.DataFrame:
    store = ScreenerFinancialsStore(db_path)
    raw = build_raw_factor_frame(store)
    resolved_snapshot_date = snapshot_date or _snapshot_date(store)
    return compute_fundamental_scores(raw, snapshot_date=resolved_snapshot_date)


def refresh_fundamental_readmodels(
    *,
    db_path: str | Path | None = None,
    latest_output: str | Path | None = None,
    trends_output: str | Path | None = None,
    snapshot_date: str | None = None,
) -> pd.DataFrame:
    paths = get_domain_paths()
    latest_path = Path(latest_output) if latest_output is not None else paths.fundamentals_dir / "fundamental_scores_latest.csv"
    trends_path = Path(trends_output) if trends_output is not None else paths.fundamentals_dir / "fundamental_trends_latest.csv"
    store = ScreenerFinancialsStore(db_path)
    raw = build_raw_factor_frame(store)
    resolved_snapshot_date = snapshot_date or _snapshot_date(store)
    scores = compute_fundamental_scores(raw, snapshot_date=resolved_snapshot_date)
    previous_scores = _read_existing(latest_path)
    previous_raw = previous_scores.copy()
    trends = compute_fundamental_trends(
        current_scores=scores,
        previous_scores=previous_scores,
        current_raw=raw.assign(snapshot_date=resolved_snapshot_date),
        previous_raw=previous_raw,
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    trends_path.parent.mkdir(parents=True, exist_ok=True)
    scores.to_csv(latest_path, index=False)
    trends.to_csv(trends_path, index=False)
    return scores


def build_raw_factor_frame(store: ScreenerFinancialsStore) -> pd.DataFrame:
    financials = store.read_financials_frame()
    if financials.empty:
        return pd.DataFrame()
    valuations = store.read_valuations_frame()
    snapshots = store.read_company_snapshot_frame()
    factors = store.read_factor_snapshot_frame()
    annual = _pivot(financials.loc[financials["period_type"].eq("annual")])
    quarterly = _pivot(financials.loc[financials["period_type"].eq("quarterly")])
    rows: list[dict[str, Any]] = []
    for symbol, group in annual.groupby("symbol", sort=True):
        group = group.sort_values("report_date")
        latest = group.iloc[-1]
        prev = group.iloc[-2] if len(group) >= 2 else latest
        val = _latest_for_symbol(valuations, symbol)
        snap = _latest_for_symbol(snapshots, symbol, date_column="as_of_date")
        qgroup = quarterly.loc[quarterly["symbol"].eq(symbol)].sort_values("report_date") if not quarterly.empty else pd.DataFrame()
        q_latest = qgroup.iloc[-1] if len(qgroup) else pd.Series(dtype=object)
        q_yoy = qgroup.iloc[-5] if len(qgroup) >= 5 else pd.Series(dtype=object)
        equity = _zero(_num(latest, "equity_share_capital")) + _zero(_num(latest, "reserves"))
        borrowings = _zero(_num(latest, "borrowings"))
        cash = _zero(_num(latest, "cash_and_bank"))
        sales = _num(latest, "sales")
        net_profit = _num(latest, "net_profit")
        operating_profit = _num(latest, "operating_profit")
        cfo = _num(latest, "cash_from_operations")
        investing = _num(latest, "cash_from_investing")
        row = {
            "symbol": symbol,
            "name": symbol,
            "industry_group": "",
            "industry": "",
            "current_price": _num(val, "price"),
            "market_cap": _first_number([_num(val, "market_cap_cr"), _num(snap, "market_cap_cr")]),
            "pe": _num(val, "pe"),
            "forward_pe": _num(val, "pe"),
            "ev_ebitda": _num(val, "ev_ebitda"),
            "price_to_book": _num(val, "pb"),
            "price_to_sales": _safe_div(_num(val, "market_cap_cr"), sales),
            "peg_3y": _peg(_num(val, "pe"), _growth(group, "net_profit", 3)),
            "yoy_quarterly_profit_growth": _pct_change(_num(q_latest, "net_profit"), _num(q_yoy, "net_profit")),
            "profit_growth_3y": _growth(group, "net_profit", 3),
            "sales_growth_3y": _growth(group, "sales", 3),
            "sales_growth_5y": _growth(group, "sales", 5),
            "profit_growth_5y": _growth(group, "net_profit", 5),
            "roce": _pct_of(operating_profit, equity + borrowings - cash),
            "roe": _pct_of(net_profit, equity),
            "opm": _first_number([_num(latest, "opm_pct"), _pct_of(operating_profit, sales)]),
            "opm_last_year": _first_number([_num(prev, "opm_pct"), _pct_of(_num(prev, "operating_profit"), _num(prev, "sales"))]),
            "debt_to_equity": _safe_div(borrowings, equity),
            "cash_from_operations_last_year": cfo,
            "free_cash_flow_last_year": cfo + investing if cfo is not None and investing is not None else cfo,
            "piotroski_score": 6,
            "pledged_pct": 0,
            "promoter_holding": 50,
            "dii_holding": 0,
            "fii_holding": 0,
            "is_not_sme": 1,
        }
        rows.append(row)
    frame = pd.DataFrame(rows)
    if not factors.empty and not frame.empty:
        latest_factors = factors.sort_values("snapshot_date").drop_duplicates(["symbol", "factor_name"], keep="last")
        factor_frame = latest_factors.pivot(index="symbol", columns="factor_name", values="factor_value").reset_index()
        frame = frame.merge(factor_frame, on="symbol", how="left", suffixes=("", "_factor"))
        for column in factor_frame.columns:
            if column == "symbol":
                continue
            factor_column = f"{column}_factor"
            if factor_column in frame.columns:
                frame[column] = frame[factor_column].combine_first(frame.get(column))
                frame = frame.drop(columns=[factor_column])
    return frame


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(description="Refresh fundamentals CSV readmodels from Screener SQLite.")
    parser.add_argument("--db-path", default=str(default_screener_db_path()))
    parser.add_argument("--latest-output", default=str(paths.fundamentals_dir / "fundamental_scores_latest.csv"))
    parser.add_argument("--trends-output", default=str(paths.fundamentals_dir / "fundamental_trends_latest.csv"))
    parser.add_argument("--snapshot-date", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    scores = refresh_fundamental_readmodels(
        db_path=args.db_path,
        latest_output=args.latest_output,
        trends_output=args.trends_output,
        snapshot_date=args.snapshot_date,
    )
    print(f"rows scored: {len(scores)}")


def _pivot(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    frame = frame.loc[~frame["metric_id"].isin({"symbol", "report_date", "period_type"})].copy()
    deduped = (
        frame.sort_values(["symbol", "report_date", "metric_id", "synced_at"], kind="stable")
        .drop_duplicates(["symbol", "report_date", "metric_id"], keep="last")
    )
    rows: list[dict[str, Any]] = []
    for (symbol, report_date), group in deduped.groupby(["symbol", "report_date"], sort=True, dropna=False):
        row: dict[str, Any] = {"symbol": symbol, "report_date": report_date}
        row.update(dict(zip(group["metric_id"].astype(str), group["value"], strict=False)))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["symbol", "report_date"], kind="stable").reset_index(drop=True)


def _latest_for_symbol(frame: pd.DataFrame, symbol: str, *, date_column: str = "date") -> pd.Series:
    if frame.empty or "symbol" not in frame.columns:
        return pd.Series(dtype=object)
    symbol_frame = frame.loc[frame["symbol"].eq(symbol)].copy()
    if symbol_frame.empty:
        return pd.Series(dtype=object)
    return symbol_frame.sort_values(date_column).iloc[-1]


def _num(row: pd.Series, column: str) -> float | None:
    if row is None or column not in row:
        return None
    value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    return None if pd.isna(value) else float(value)


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return float(numerator) / float(denominator)


def _pct_of(numerator: float | None, denominator: float | None) -> float | None:
    ratio = _safe_div(numerator, denominator)
    return None if ratio is None else ratio * 100.0


def _pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return ((float(current) / float(previous)) - 1.0) * 100.0


def _growth(group: pd.DataFrame, column: str, years: int) -> float | None:
    if column not in group.columns or len(group) < 2:
        return None
    latest = _num(group.iloc[-1], column)
    prior_index = max(0, len(group) - 1 - years)
    prior = _num(group.iloc[prior_index], column)
    actual_years = len(group) - 1 - prior_index
    if latest is None or prior is None or latest <= 0 or prior <= 0 or actual_years <= 0:
        return None
    return ((latest / prior) ** (1.0 / actual_years) - 1.0) * 100.0


def _peg(pe: float | None, growth: float | None) -> float | None:
    if pe is None or growth is None or growth <= 0:
        return None
    return pe / growth


def _first_number(values: list[float | None]) -> float | None:
    for value in values:
        if value is not None and not pd.isna(value):
            return float(value)
    return None


def _zero(value: float | None) -> float:
    return 0.0 if value is None or pd.isna(value) else float(value)


def _snapshot_date(store: ScreenerFinancialsStore) -> str:
    snapshots = store.read_company_snapshot_frame()
    if snapshots.empty or "as_of_date" not in snapshots.columns:
        return pd.Timestamp.utcnow().date().isoformat()
    return str(snapshots["as_of_date"].max())[:10]


def _read_existing(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


__all__ = ["build_scores_from_screener_db", "refresh_fundamental_readmodels", "build_raw_factor_frame"]


if __name__ == "__main__":
    main()
