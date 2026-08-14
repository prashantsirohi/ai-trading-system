"""Per-symbol statement-basis resolution without cross-basis mixing."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import DEFAULT_STATEMENT_BASIS


RESOLUTION_COLUMNS = [
    "symbol",
    "statement_basis",
    "basis_resolution_reason",
    "consolidated_latest_quarter",
    "standalone_latest_quarter",
]


def resolve_statement_basis(financials: pd.DataFrame) -> pd.DataFrame:
    """Select one usable basis per symbol using latest quarterly coverage."""

    if financials.empty:
        return pd.DataFrame(columns=RESOLUTION_COLUMNS)
    frame = financials.copy()
    frame.loc[:, "symbol"] = frame["symbol"].astype(str)
    frame.loc[:, "statement_basis"] = (
        frame.get("statement_basis", pd.Series(DEFAULT_STATEMENT_BASIS, index=frame.index))
        .fillna(DEFAULT_STATEMENT_BASIS)
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": DEFAULT_STATEMENT_BASIS})
    )
    frame.loc[:, "report_date"] = pd.to_datetime(frame["report_date"], errors="coerce")
    quarterly = frame.loc[frame["period_type"].astype(str).str.strip().str.lower().eq("quarterly")]
    latest = (
        quarterly.groupby(["symbol", "statement_basis"], sort=True)["report_date"]
        .max()
        .unstack("statement_basis")
        .rename(
            columns={
                "consolidated": "consolidated_latest_quarter",
                "standalone": "standalone_latest_quarter",
            }
        )
    )
    counts = (
        frame.groupby(["symbol", "statement_basis", "period_type"], sort=True)["report_date"]
        .nunique()
        .unstack(["statement_basis", "period_type"], fill_value=0)
    )
    symbols = pd.Index(sorted(frame["symbol"].unique()), name="symbol")
    resolution = latest.reindex(symbols).reset_index()
    count_lookup = counts.reindex(symbols, fill_value=0)
    for column in ("consolidated_latest_quarter", "standalone_latest_quarter"):
        if column not in resolution.columns:
            resolution.loc[:, column] = pd.NaT
    has_consolidated = set(frame.loc[frame["statement_basis"].eq("consolidated"), "symbol"])
    has_standalone = set(frame.loc[frame["statement_basis"].eq("standalone"), "symbol"])
    selected: list[str] = []
    reasons: list[str] = []
    for row in resolution.itertuples(index=False):
        symbol = str(row.symbol)
        consolidated_latest = row.consolidated_latest_quarter
        standalone_latest = row.standalone_latest_quarter
        consolidated_quarters = int(count_lookup.loc[symbol].get(("consolidated", "quarterly"), 0))
        standalone_quarters = int(count_lookup.loc[symbol].get(("standalone", "quarterly"), 0))
        consolidated_annual = int(count_lookup.loc[symbol].get(("consolidated", "annual"), 0))
        standalone_annual = int(count_lookup.loc[symbol].get(("standalone", "annual"), 0))
        sufficient_history = (
            consolidated_quarters >= min(standalone_quarters, 5)
            and consolidated_annual >= min(standalone_annual, 2)
        )
        if pd.notna(consolidated_latest) and (
            pd.isna(standalone_latest) or consolidated_latest >= standalone_latest
        ) and sufficient_history:
            selected.append("consolidated")
            reasons.append("consolidated_only_quarterly" if pd.isna(standalone_latest) else "consolidated_current")
        elif symbol in has_standalone:
            selected.append("standalone")
            if pd.isna(consolidated_latest):
                reasons.append("standalone_fallback_no_consolidated_quarterly")
            elif not sufficient_history:
                reasons.append("standalone_fallback_insufficient_consolidated_history")
            else:
                reasons.append("standalone_fallback_newer_quarter")
        elif symbol in has_consolidated:
            selected.append("consolidated")
            reasons.append("consolidated_only_no_quarterly")
        else:
            selected.append(DEFAULT_STATEMENT_BASIS)
            reasons.append("standalone_fallback")
    resolution.loc[:, "statement_basis"] = selected
    resolution.loc[:, "basis_resolution_reason"] = reasons
    for column in ("consolidated_latest_quarter", "standalone_latest_quarter"):
        resolution.loc[:, column] = pd.to_datetime(resolution[column], errors="coerce").dt.date
    return resolution[RESOLUTION_COLUMNS]


__all__ = ["RESOLUTION_COLUMNS", "resolve_statement_basis"]
