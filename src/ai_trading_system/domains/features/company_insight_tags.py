"""Deterministic company-level fundamental insight tags."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import (
    connect_fundamentals_duckdb,
    ensure_fundamentals_analytical_schema,
)


@dataclass(frozen=True)
class CompanyInsightTagsResult:
    rows: int
    symbols: int
    start_date: str | None
    end_date: str | None
    tag_counts: dict[str, int]


def refresh_company_insight_tags(
    *,
    fundamentals_db_path: str | Path | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> CompanyInsightTagsResult:
    conn = connect_fundamentals_duckdb(fundamentals_db_path)
    try:
        ensure_fundamentals_analytical_schema(conn)
        features = conn.execute(
            """
            SELECT *
            FROM company_growth_features
            WHERE report_date <= COALESCE(CAST(? AS DATE), DATE '9999-12-31')
            ORDER BY symbol, report_date
            """,
            [str(to_date)[:10] if to_date else None],
        ).df()
        tags_all = compute_company_insight_tags(features)
        tags = _filter_dates(tags_all, from_date, to_date)
        if not tags.empty:
            start, end = str(tags["report_date"].min())[:10], str(tags["report_date"].max())[:10]
            conn.execute(
                "DELETE FROM company_insight_tags WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
            conn.register("_company_insight_tags_frame", tags)
            try:
                conn.execute("INSERT INTO company_insight_tags SELECT * FROM _company_insight_tags_frame")
            finally:
                conn.unregister("_company_insight_tags_frame")
        elif from_date or to_date:
            start = str(from_date or to_date)[:10]
            end = str(to_date or from_date)[:10]
            conn.execute(
                "DELETE FROM company_insight_tags WHERE report_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
                [start, end],
            )
        else:
            start = end = None
    finally:
        conn.close()
    tag_counts = tags["insight_type"].value_counts().astype(int).to_dict() if not tags.empty else {}
    return CompanyInsightTagsResult(
        rows=int(len(tags)),
        symbols=int(tags["symbol"].nunique()) if not tags.empty else 0,
        start_date=start,
        end_date=end,
        tag_counts={str(k): int(v) for k, v in tag_counts.items()},
    )


def compute_company_insight_tags(features: pd.DataFrame) -> pd.DataFrame:
    columns = ["symbol", "report_date", "insight_type", "insight_score", "evidence_json", "created_at"]
    if features.empty:
        return pd.DataFrame(columns=columns)
    frame = features.copy()
    frame.loc[:, "report_date"] = pd.to_datetime(frame["report_date"]).dt.date
    frame = frame.sort_values(["symbol", "report_date"], kind="stable")
    numeric_cols = [
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
        "sales_8q_cagr",
        "profit_8q_cagr",
    ]
    for column in numeric_cols:
        if column in frame.columns:
            frame.loc[:, column] = pd.to_numeric(frame[column], errors="coerce")
    grouped = frame.groupby("symbol", sort=False)
    frame.loc[:, "profit_prev_q"] = grouped["net_profit_cr"].shift(1)
    frame.loc[:, "profit_same_q_ly"] = grouped["net_profit_cr"].shift(4)
    frame.loc[:, "sales_yoy_positive_2q"] = grouped["sales_yoy_growth"].transform(lambda s: s.gt(0.10).rolling(2, min_periods=2).sum())
    frame.loc[:, "opm_improving_2q"] = grouped["opm_yoy_change"].transform(lambda s: s.gt(0).rolling(2, min_periods=2).sum())
    frame.loc[:, "positive_profit_2q"] = grouped["net_profit_cr"].transform(lambda s: s.gt(0).rolling(2, min_periods=2).sum())
    frame.loc[:, "sales_yoy_positive_8q"] = grouped["sales_yoy_growth"].transform(lambda s: s.gt(0).rolling(8, min_periods=1).sum())
    frame.loc[:, "profit_yoy_positive_8q"] = grouped["profit_yoy_growth"].transform(lambda s: s.gt(0).rolling(8, min_periods=1).sum())
    frame.loc[:, "opm_8q_std"] = grouped["opm_pct"].transform(lambda s: s.rolling(8, min_periods=4).std())

    rank_inputs = {
        "sales_yoy_rank": "sales_yoy_growth",
        "profit_yoy_rank": "profit_yoy_growth",
        "profit_qoq_rank": "profit_qoq_growth",
        "opm_yoy_change_rank": "opm_yoy_change",
        "sales_qoq_rank": "sales_qoq_growth",
        "sales_8q_cagr_rank": "sales_8q_cagr",
        "profit_8q_cagr_rank": "profit_8q_cagr",
    }
    for output, source in rank_inputs.items():
        frame.loc[:, output] = frame.groupby("report_date", group_keys=False)[source].transform(
            lambda values: values.rank(pct=True) * 100.0
        )
    frame.loc[:, "earnings_consistency_score"] = (
        0.5 * frame["positive_profit_quarters_4q"].fillna(0) / 4.0 * 100.0
        + 0.25 * frame["sales_growth_positive_quarters_4q"].fillna(0) / 4.0 * 100.0
        + 0.25 * frame["profit_growth_positive_quarters_4q"].fillna(0) / 4.0 * 100.0
    )
    frame.loc[:, "great_result_score"] = (
        0.25 * frame["sales_yoy_rank"].fillna(50.0)
        + 0.25 * frame["profit_yoy_rank"].fillna(50.0)
        + 0.15 * frame["profit_qoq_rank"].fillna(50.0)
        + 0.15 * frame["opm_yoy_change_rank"].fillna(50.0)
        + 0.10 * frame["sales_qoq_rank"].fillna(50.0)
        + 0.10 * frame["earnings_consistency_score"].fillna(50.0)
    )
    frame.loc[:, "compounder_score"] = (
        0.20 * (frame["sales_yoy_positive_8q"].fillna(0) / 8.0 * 100.0)
        + 0.20 * (frame["profit_yoy_positive_8q"].fillna(0) / 8.0 * 100.0)
        + 0.15 * frame["sales_8q_cagr_rank"].fillna(50.0)
        + 0.15 * frame["profit_8q_cagr_rank"].fillna(50.0)
        + 0.10 * _margin_stability_score(frame["opm_8q_std"])
        + 0.10 * 50.0
        + 0.10 * 50.0
    )

    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        rows.extend(_turnaround_tags(row))
        rows.extend(_great_result_tags(row))
        rows.extend(_compounder_tags(row))
    result = pd.DataFrame(rows, columns=columns)
    if result.empty:
        return result
    result.loc[:, "created_at"] = pd.Timestamp.utcnow()
    return result.sort_values(["report_date", "symbol", "insight_type"]).reset_index(drop=True)


def _turnaround_tags(row: pd.Series) -> list[dict[str, Any]]:
    tags = []
    current_profit = _num(row.get("net_profit_cr"))
    same_q_profit = _num(row.get("profit_same_q_ly"))
    profit_yoy = _num(row.get("profit_yoy_growth"))
    sales_yoy = _num(row.get("sales_yoy_growth"))
    opm_yoy = _num(row.get("opm_yoy_change"))
    profit_qoq = _num(row.get("profit_qoq_growth"))
    candidate = (
        (same_q_profit <= 0 or profit_yoy > 0.50)
        and current_profit > 0
        and sales_yoy > 0.10
        and opm_yoy > 3.0
        and profit_qoq > 0.20
    )
    confirmed = (
        current_profit > 0
        and _num(row.get("positive_profit_2q")) >= 2
        and _num(row.get("sales_yoy_positive_2q")) >= 2
        and _num(row.get("opm_improving_2q")) >= 2
    )
    if candidate:
        tags.append(_tag(row, "turnaround_candidate", min(95.0, 65.0 + _clip(profit_yoy, 0, 1) * 20 + _clip(opm_yoy / 10, 0, 1) * 10)))
    if confirmed:
        tags.append(_tag(row, "turnaround_confirmed", max(75.0, min(98.0, 80.0 + _clip(sales_yoy, 0, 0.5) * 20 + _clip(opm_yoy / 10, 0, 1) * 8))))
    if same_q_profit <= 0 and current_profit > 0:
        tags.append(_tag(row, "loss_to_profit", 78.0))
    if opm_yoy > 3.0:
        tags.append(_tag(row, "margin_recovery", min(95.0, 65.0 + opm_yoy * 3.0)))
    if sales_yoy > 0.10 and _num(row.get("sales_qoq_growth")) > 0:
        tags.append(_tag(row, "sales_recovery", min(90.0, 60.0 + sales_yoy * 100.0)))
    return tags


def _great_result_tags(row: pd.Series) -> list[dict[str, Any]]:
    tags = []
    sales_yoy = _num(row.get("sales_yoy_growth"))
    profit_yoy = _num(row.get("profit_yoy_growth"))
    profit_qoq = _num(row.get("profit_qoq_growth"))
    opm_yoy = _num(row.get("opm_yoy_change"))
    profit = _num(row.get("net_profit_cr"))
    same_q_profit = abs(_num(row.get("profit_same_q_ly")))
    not_low_base_only = same_q_profit >= 1.0 or sales_yoy > 0.25
    score = _num(row.get("great_result_score"))
    great = sales_yoy > 0.15 and profit_yoy > 0.25 and profit_qoq > 0.10 and opm_yoy > 1.0 and profit > 0 and not_low_base_only
    if great:
        tags.append(_tag(row, "great_result", score))
    if great and sales_yoy > 0.25 and profit_yoy > 0.50 and opm_yoy > 3.0:
        tags.append(_tag(row, "blowout_result", max(score, 85.0)))
    if opm_yoy > 1.0 and profit_yoy > 0.20:
        tags.append(_tag(row, "margin_expansion_result", max(65.0, min(95.0, 60.0 + opm_yoy * 4.0))))
    if sales_yoy > 0.20 and _num(row.get("sales_qoq_growth")) > 0.05:
        tags.append(_tag(row, "revenue_acceleration_result", max(65.0, min(95.0, 60.0 + sales_yoy * 100.0))))
    if profit_yoy > 0.25 and profit_qoq > 0.10:
        tags.append(_tag(row, "profit_acceleration_result", max(65.0, min(95.0, 60.0 + profit_yoy * 50.0))))
    return tags


def _compounder_tags(row: pd.Series) -> list[dict[str, Any]]:
    sales_positive = _num(row.get("sales_yoy_positive_8q"))
    profit_positive = _num(row.get("profit_yoy_positive_8q"))
    score = _num(row.get("compounder_score"))
    margin_stable = _num(row.get("opm_8q_std")) <= 5.0
    if sales_positive >= 6 and profit_positive >= 6 and margin_stable:
        label = "consistent_compounder" if sales_positive >= 7 and profit_positive >= 7 else "emerging_compounder"
        tags = [_tag(row, label, score)]
        if _num(row.get("sales_8q_cagr")) > 0.18 and _num(row.get("profit_8q_cagr")) > 0.18:
            tags.append(_tag(row, "high_growth_compounder", max(score, 78.0)))
        if _num(row.get("opm_8q_std")) <= 3.0:
            tags.append(_tag(row, "quality_growth", max(score, 75.0)))
        return tags
    return []


def _tag(row: pd.Series, insight_type: str, score: float) -> dict[str, Any]:
    evidence = {
        "sales_yoy_growth": _round(row.get("sales_yoy_growth")),
        "profit_yoy_growth": _round(row.get("profit_yoy_growth")),
        "profit_qoq_growth": _round(row.get("profit_qoq_growth")),
        "opm_yoy_change": _round(row.get("opm_yoy_change")),
        "net_profit_cr": _round(row.get("net_profit_cr")),
        "note": _evidence_text(row, insight_type),
    }
    return {
        "symbol": row["symbol"],
        "report_date": row["report_date"],
        "insight_type": insight_type,
        "insight_score": round(float(score), 2),
        "evidence_json": json.dumps(evidence, sort_keys=True),
        "created_at": pd.Timestamp.utcnow(),
    }


def _evidence_text(row: pd.Series, insight_type: str) -> str:
    parts = []
    if pd.notna(row.get("sales_yoy_growth")):
        parts.append(f"Sales {_pct(row.get('sales_yoy_growth'))} YoY")
    if pd.notna(row.get("profit_yoy_growth")):
        parts.append(f"PAT {_pct(row.get('profit_yoy_growth'))} YoY")
    if pd.notna(row.get("opm_yoy_change")):
        parts.append(f"OPM {_bps(row.get('opm_yoy_change'))} bps YoY")
    if insight_type == "loss_to_profit":
        parts.append("PAT loss to profit")
    return ", ".join(parts)


def _margin_stability_score(std: pd.Series) -> pd.Series:
    clean = pd.to_numeric(std, errors="coerce")
    return (100.0 - clean.fillna(5.0).clip(lower=0, upper=10) * 10.0).clip(lower=0, upper=100)


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


def _num(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(parsed) else parsed


def _round(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(parsed) else round(parsed, 4)


def _pct(value: Any) -> str:
    return f"{_num(value) * 100:.1f}%"


def _bps(value: Any) -> str:
    return f"{_num(value) * 100:.0f}"


def _clip(value: float, lower: float, upper: float) -> float:
    return min(max(float(value), lower), upper)


__all__ = ["CompanyInsightTagsResult", "compute_company_insight_tags", "refresh_company_insight_tags"]
