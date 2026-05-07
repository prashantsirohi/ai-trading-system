"""Historical trend computation for fundamental snapshots."""

from __future__ import annotations

import pandas as pd


TREND_OUTPUT_COLUMNS = [
    "symbol",
    "snapshot_date",
    "prev_snapshot_date",
    "fundamental_score_delta",
    "quality_score_delta",
    "growth_score_delta",
    "balance_sheet_score_delta",
    "valuation_score_delta",
    "ownership_score_delta",
    "roce_delta",
    "roe_delta",
    "opm_delta",
    "debt_to_equity_delta",
    "pledged_pct_delta",
    "sales_growth_3y_delta",
    "profit_growth_3y_delta",
    "sales_growth_delta",
    "profit_growth_delta",
    "fundamental_trend_label",
    "trend_reason",
]

SCORE_DELTA_COLUMNS = [
    "fundamental_score",
    "quality_score",
    "growth_score",
    "balance_sheet_score",
    "valuation_score",
    "ownership_score",
]
RAW_DELTA_COLUMNS = [
    "roce",
    "roe",
    "opm",
    "debt_to_equity",
    "pledged_pct",
    "sales_growth_3y",
    "profit_growth_3y",
]


def compute_fundamental_trends(
    current_scores: pd.DataFrame,
    previous_scores: pd.DataFrame | None,
    current_raw: pd.DataFrame,
    previous_raw: pd.DataFrame | None,
) -> pd.DataFrame:
    """Compare current fundamentals with the previous available snapshot."""

    if current_scores is None or current_scores.empty:
        return pd.DataFrame(columns=TREND_OUTPUT_COLUMNS)

    current = _symbol_frame(current_scores)
    previous = _symbol_frame(previous_scores)
    current_raw_norm = _symbol_frame(current_raw)
    previous_raw_norm = _symbol_frame(previous_raw)

    snapshot_date = _first_date(current, "snapshot_date") or _first_date(current, "screener_snapshot_date")
    prev_snapshot_date = _first_date(previous, "snapshot_date") or _first_date(previous, "screener_snapshot_date")
    if previous.empty:
        output = pd.DataFrame(
            {
                "symbol": current["symbol"],
                "snapshot_date": snapshot_date or "",
                "prev_snapshot_date": "",
                "fundamental_trend_label": "INSUFFICIENT_HISTORY",
                "trend_reason": "No previous fundamental snapshot available",
            }
        )
        for column in TREND_OUTPUT_COLUMNS:
            if column not in output.columns:
                output.loc[:, column] = pd.NA
        for column in [column for column in TREND_OUTPUT_COLUMNS if column.endswith("_delta")]:
            output.loc[:, column] = pd.to_numeric(output[column], errors="coerce").astype("Float64")
        for column in ("symbol", "snapshot_date", "prev_snapshot_date", "fundamental_trend_label", "trend_reason"):
            output.loc[:, column] = output[column].astype("string")
        return output[TREND_OUTPUT_COLUMNS]

    current_cols = ["symbol", "fundamental_tier", *SCORE_DELTA_COLUMNS]
    previous_cols = ["symbol", "fundamental_tier", *SCORE_DELTA_COLUMNS]
    merged = current[[col for col in current_cols if col in current.columns]].merge(
        previous[[col for col in previous_cols if col in previous.columns]],
        on="symbol",
        how="left",
        suffixes=("", "_prev"),
    )
    merged = merged.merge(
        current_raw_norm[["symbol", *[col for col in RAW_DELTA_COLUMNS if col in current_raw_norm.columns]]],
        on="symbol",
        how="left",
    )
    previous_raw_subset = previous_raw_norm[["symbol", *[col for col in RAW_DELTA_COLUMNS if col in previous_raw_norm.columns]]]
    merged = merged.merge(previous_raw_subset, on="symbol", how="left", suffixes=("", "_prev"))

    output = pd.DataFrame({"symbol": merged["symbol"], "snapshot_date": snapshot_date, "prev_snapshot_date": prev_snapshot_date})
    for column in SCORE_DELTA_COLUMNS:
        output.loc[:, f"{column}_delta"] = _delta(merged, column, f"{column}_prev")
    for column in RAW_DELTA_COLUMNS:
        output.loc[:, f"{column}_delta"] = _delta(merged, column, f"{column}_prev")
    output.loc[:, "sales_growth_delta"] = output.get("sales_growth_3y_delta", pd.Series(pd.NA, index=output.index))
    output.loc[:, "profit_growth_delta"] = output.get("profit_growth_3y_delta", pd.Series(pd.NA, index=output.index))

    labels: list[str] = []
    reasons: list[str] = []
    for idx, row in merged.iterrows():
        label, reason = _classify(row, output.loc[idx])
        labels.append(label)
        reasons.append(reason)
    output.loc[:, "fundamental_trend_label"] = labels
    output.loc[:, "trend_reason"] = reasons

    for column in TREND_OUTPUT_COLUMNS:
        if column not in output.columns:
            output.loc[:, column] = pd.NA
    delta_columns = [column for column in TREND_OUTPUT_COLUMNS if column.endswith("_delta")]
    for column in delta_columns:
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce").round(2)
    for column in ("symbol", "snapshot_date", "prev_snapshot_date", "fundamental_trend_label", "trend_reason"):
        output.loc[:, column] = output[column].fillna("").astype("string")
    return output[TREND_OUTPUT_COLUMNS].reset_index(drop=True)


def _symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol"])
    output = frame.copy()
    if "symbol" not in output.columns and "NSE Code" in output.columns:
        output.loc[:, "symbol"] = output["NSE Code"]
    if "symbol" not in output.columns:
        return pd.DataFrame(columns=["symbol"])
    output.loc[:, "symbol"] = output["symbol"].astype("string").str.strip().str.upper()
    return output.loc[output["symbol"].notna() & output["symbol"].ne("")].copy()


def _first_date(frame: pd.DataFrame, column: str) -> str | None:
    if frame is None or frame.empty or column not in frame.columns:
        return None
    values = frame[column].dropna()
    if values.empty:
        return None
    return str(values.iloc[0])[:10]


def _delta(frame: pd.DataFrame, current_col: str, previous_col: str) -> pd.Series:
    if current_col not in frame.columns or previous_col not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    current = pd.to_numeric(frame[current_col], errors="coerce")
    previous = pd.to_numeric(frame[previous_col], errors="coerce")
    return current - previous


def _classify(row: pd.Series, deltas: pd.Series) -> tuple[str, str]:
    if pd.isna(row.get("fundamental_score_prev")):
        return "INSUFFICIENT_HISTORY", "No previous row for symbol in prior snapshot"

    score_delta = _num(deltas.get("fundamental_score_delta"))
    quality_delta = _num(deltas.get("quality_score_delta"))
    growth_delta = _num(deltas.get("growth_score_delta"))
    valuation_delta = _num(deltas.get("valuation_score_delta"))
    roce_delta = _num(deltas.get("roce_delta"))
    roe_delta = _num(deltas.get("roe_delta"))
    opm_delta = _num(deltas.get("opm_delta"))
    debt_delta = _num(deltas.get("debt_to_equity_delta"))
    pledge_delta = _num(deltas.get("pledged_pct_delta"))
    previous_tier = str(row.get("fundamental_tier_prev") or "").upper()
    current_tier = str(row.get("fundamental_tier") or "").upper()
    quality_deterioration = quality_delta < -3 or roce_delta < -2 or roe_delta < -2 or opm_delta < -3
    sharp_balance_risk = pledge_delta > 5 or debt_delta > 0.5

    if previous_tier in {"C", "REJECT"} and current_tier in {"A", "B"}:
        return "TURNAROUND", f"Tier improved from {previous_tier} to {current_tier}"
    if valuation_delta > 5 and (quality_delta < -3 or growth_delta < -3):
        return "VALUE_TRAP_RISK", "Valuation improved while growth or quality deteriorated"
    if score_delta < -5 or sharp_balance_risk:
        reason = "Fundamental score deteriorated" if score_delta < -5 else "Pledge or debt rose sharply"
        return "DETERIORATING", reason
    if score_delta > 5 and not quality_deterioration:
        return "IMPROVING", "Fundamental score improved without major quality deterioration"
    if current_tier in {"A", "B"} and -5 <= score_delta <= 5:
        return "STABLE_GOOD", f"Tier {current_tier} with stable score"
    return "STABLE_GOOD", "Fundamental score broadly stable"


def _num(value: object) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(numeric):
        return 0.0
    return numeric
