"""Historical trend computation for industry fundamental snapshots."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.industry_schema import normalize_industry_key


INDUSTRY_TREND_OUTPUT_COLUMNS = [
    "industry_key",
    "industry",
    "snapshot_date",
    "prev_snapshot_date",
    "industry_fundamental_score_delta",
    "industry_growth_score_delta",
    "industry_quality_score_delta",
    "industry_valuation_score_delta",
    "industry_momentum_score_delta",
    "median_pe_delta",
    "sales_growth_wavg_delta",
    "opm_wavg_delta",
    "roce_wavg_delta",
    "median_1y_return_delta",
    "industry_trend_label",
    "industry_trend_reason",
]


_SCORE_DELTA_COLUMNS = [
    "industry_fundamental_score",
    "industry_growth_score",
    "industry_quality_score",
    "industry_valuation_score",
    "industry_momentum_score",
]
_RAW_DELTA_COLUMNS = [
    "median_pe",
    "sales_growth_wavg",
    "opm_wavg",
    "roce_wavg",
    "median_1y_return",
]


def compute_industry_fundamental_trends(
    current_scores: pd.DataFrame,
    previous_scores: pd.DataFrame | None,
) -> pd.DataFrame:
    """Compare current industry scores with the previous available snapshot."""

    if current_scores is None or current_scores.empty:
        return pd.DataFrame(columns=INDUSTRY_TREND_OUTPUT_COLUMNS)

    current = _key_frame(current_scores)
    previous = _key_frame(previous_scores)

    snapshot_date = _first_date(current, "snapshot_date") or _first_date(
        current, "screener_industry_snapshot_date"
    )
    prev_snapshot_date = _first_date(previous, "snapshot_date") or _first_date(
        previous, "screener_industry_snapshot_date"
    )

    if previous.empty:
        output = pd.DataFrame(
            {
                "industry_key": current["industry_key"],
                "industry": current.get("industry", pd.Series("", index=current.index)),
                "snapshot_date": snapshot_date or "",
                "prev_snapshot_date": "",
                "industry_trend_label": "INSUFFICIENT_HISTORY",
                "industry_trend_reason": "No previous industry snapshot available",
            }
        )
        for column in INDUSTRY_TREND_OUTPUT_COLUMNS:
            if column not in output.columns:
                output.loc[:, column] = pd.NA
        for column in [c for c in INDUSTRY_TREND_OUTPUT_COLUMNS if c.endswith("_delta")]:
            output.loc[:, column] = pd.to_numeric(output[column], errors="coerce").astype("Float64")
        for column in (
            "industry_key",
            "industry",
            "snapshot_date",
            "prev_snapshot_date",
            "industry_trend_label",
            "industry_trend_reason",
        ):
            output.loc[:, column] = output[column].fillna("").astype("string")
        return output[INDUSTRY_TREND_OUTPUT_COLUMNS].reset_index(drop=True)

    keep_current = ["industry_key", "industry", *[c for c in (_SCORE_DELTA_COLUMNS + _RAW_DELTA_COLUMNS) if c in current.columns]]
    keep_previous = ["industry_key", *[c for c in (_SCORE_DELTA_COLUMNS + _RAW_DELTA_COLUMNS) if c in previous.columns]]
    merged = current[keep_current].merge(
        previous[keep_previous],
        on="industry_key",
        how="left",
        suffixes=("", "_prev"),
    )

    output = pd.DataFrame(
        {
            "industry_key": merged["industry_key"],
            "industry": merged.get("industry", pd.Series("", index=merged.index)),
            "snapshot_date": snapshot_date,
            "prev_snapshot_date": prev_snapshot_date,
        }
    )
    for column in _SCORE_DELTA_COLUMNS:
        output.loc[:, f"{column}_delta"] = _delta(merged, column, f"{column}_prev")
    for column in _RAW_DELTA_COLUMNS:
        output.loc[:, f"{column}_delta"] = _delta(merged, column, f"{column}_prev")

    labels: list[str] = []
    reasons: list[str] = []
    for idx, row in merged.iterrows():
        label, reason = _classify(row, output.loc[idx])
        labels.append(label)
        reasons.append(reason)
    output.loc[:, "industry_trend_label"] = labels
    output.loc[:, "industry_trend_reason"] = reasons

    for column in INDUSTRY_TREND_OUTPUT_COLUMNS:
        if column not in output.columns:
            output.loc[:, column] = pd.NA
    delta_columns = [c for c in INDUSTRY_TREND_OUTPUT_COLUMNS if c.endswith("_delta")]
    for column in delta_columns:
        output.loc[:, column] = pd.to_numeric(output[column], errors="coerce").round(2)
    for column in (
        "industry_key",
        "industry",
        "snapshot_date",
        "prev_snapshot_date",
        "industry_trend_label",
        "industry_trend_reason",
    ):
        output.loc[:, column] = output[column].fillna("").astype("string")
    return output[INDUSTRY_TREND_OUTPUT_COLUMNS].reset_index(drop=True)


def _key_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["industry_key"])
    output = frame.copy()
    if "industry_key" not in output.columns:
        if "industry" not in output.columns:
            return pd.DataFrame(columns=["industry_key"])
        output.loc[:, "industry_key"] = output["industry"].map(normalize_industry_key)
    output.loc[:, "industry_key"] = output["industry_key"].astype("string").fillna("")
    return output.loc[output["industry_key"].ne("")].copy()


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
    if pd.isna(row.get("industry_fundamental_score_prev")):
        return "INSUFFICIENT_HISTORY", "Industry not present in previous snapshot"

    score_delta = _num(deltas.get("industry_fundamental_score_delta"))
    quality_delta = _num(deltas.get("industry_quality_score_delta"))
    growth_delta = _num(deltas.get("industry_growth_score_delta"))
    momentum_delta = _num(deltas.get("industry_momentum_score_delta"))
    valuation_delta = _num(deltas.get("industry_valuation_score_delta"))
    roce_delta = _num(deltas.get("roce_wavg_delta"))
    opm_delta = _num(deltas.get("opm_wavg_delta"))

    quality_deterioration = quality_delta < -3 or roce_delta < -2 or opm_delta < -3

    if score_delta > 5 and not quality_deterioration:
        return "IMPROVING", f"Industry fundamental score rose {score_delta:+.1f}"
    if score_delta < -5 or quality_deterioration:
        if score_delta < -5:
            return "DETERIORATING", f"Industry fundamental score fell {score_delta:+.1f}"
        return "DETERIORATING", "Industry quality (ROCE/OPM) eroded"
    if momentum_delta > 10 and growth_delta >= 0:
        return "MOMENTUM_BUILDING", f"Industry momentum rose {momentum_delta:+.1f}"
    if momentum_delta < -10:
        return "MOMENTUM_FADING", f"Industry momentum dropped {momentum_delta:+.1f}"
    if valuation_delta > 8 and quality_delta < 0:
        return "VALUE_TRAP_RISK", "Industry got cheaper while quality slipped"
    return "STABLE", "Industry score broadly stable"


def _num(value: object) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(numeric):
        return 0.0
    return numeric
