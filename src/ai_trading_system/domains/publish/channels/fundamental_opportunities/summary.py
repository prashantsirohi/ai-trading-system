"""Decision-report summary builders for fundamental opportunities."""

from __future__ import annotations

from typing import Any

import pandas as pd

from ai_trading_system.domains.publish.channels.fundamental_opportunities.classifier import (
    BUCKET_LABELS,
    BUCKET_ORDER,
    bucket_counts,
)
from ai_trading_system.domains.publish.channels.fundamental_opportunities.display import (
    clean_label,
    display_row,
    first_clean,
    is_missing,
)

MAIN_BUCKETS = ["HIGH_GROWTH", "TURNAROUND_CANDIDATE", "CYCLICAL_COMMODITY", "DEEP_VALUE"]
APPENDIX_BUCKETS = ["QUALITY_COMPOUNDER", "DIVIDEND_CASH_COW", "AVOID_WATCH"]


def build_report_summary(
    *,
    classified: pd.DataFrame,
    shortlist: pd.DataFrame,
    as_of: str,
    universe_id: str,
    warnings: list[str],
    limit_per_bucket: int,
) -> dict[str, Any]:
    frame = classified.copy() if classified is not None else pd.DataFrame()
    counts = bucket_counts(frame)
    main_tables = _main_bucket_tables(frame, limit_per_bucket=limit_per_bucket)
    manual_rows = _manual_review_rows(frame, limit_per_bucket=limit_per_bucket)
    if manual_rows:
        main_tables["MANUAL_REVIEW"] = manual_rows

    main_counts = {bucket: len(rows) for bucket, rows in main_tables.items()}
    no_candidates = [
        BUCKET_LABELS.get(bucket, bucket)
        for bucket in MAIN_BUCKETS
        if not main_tables.get(bucket)
    ]
    return {
        "executive_summary": {
            "as_of": as_of,
            "universe_id": universe_id,
            "data_sources": "fundamentals.duckdb, ohlcv.duckdb, candidate_tracker.duckdb",
            "classified_count": int(len(frame)),
            "shortlist_count": int(len(shortlist)) if shortlist is not None else 0,
            "bucket_counts": counts,
            "run_id": f"fundamental_opportunities-{as_of}",
        },
        "top_opportunities": _top_opportunities(frame, limit=10),
        "data_quality": _data_quality(frame, warnings),
        "sector_map": _sector_map(frame),
        "main_bucket_tables": main_tables,
        "manual_review_rows": manual_rows,
        "no_candidate_buckets": no_candidates,
        "main_report_bucket_counts": main_counts,
        "appendix_bucket_counts": {bucket: counts.get(bucket, 0) for bucket in APPENDIX_BUCKETS},
    }


def _top_opportunities(frame: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    eligible = frame.loc[~frame.get("business_bucket", pd.Series("", index=frame.index)).astype(str).eq("AVOID_WATCH")].copy()
    if eligible.empty:
        return []
    eligible.loc[:, "_score"] = pd.to_numeric(eligible.get("final_watchlist_score"), errors="coerce").fillna(0)
    rows = eligible.sort_values(["_score", "symbol"], ascending=[False, True], kind="stable").head(limit).to_dict("records")
    return [_ranked_display(row, rank=idx + 1) for idx, row in enumerate(rows)]


def _main_bucket_tables(frame: pd.DataFrame, *, limit_per_bucket: int) -> dict[str, list[dict[str, Any]]]:
    tables: dict[str, list[dict[str, Any]]] = {}
    if frame.empty:
        return {bucket: [] for bucket in MAIN_BUCKETS}
    for bucket in MAIN_BUCKETS:
        rows = (
            frame.loc[frame["business_bucket"].astype(str).eq(bucket)]
            .sort_values(["final_watchlist_score", "symbol"], ascending=[False, True], kind="stable")
            .head(limit_per_bucket)
            .to_dict("records")
        )
        tables[bucket] = [_ranked_display(row, rank=idx + 1) for idx, row in enumerate(rows)]
    return tables


def _manual_review_rows(frame: pd.DataFrame, *, limit_per_bucket: int) -> list[dict[str, Any]]:
    if frame.empty or "manual_review_flag" not in frame.columns:
        return []
    rows = (
        frame.loc[
            frame["manual_review_flag"].fillna(False).astype(bool)
            & ~frame["business_bucket"].astype(str).eq("AVOID_WATCH")
        ]
        .sort_values(["final_watchlist_score", "symbol"], ascending=[False, True], kind="stable")
        .head(limit_per_bucket)
        .to_dict("records")
    )
    return [_ranked_display(row, rank=idx + 1) for idx, row in enumerate(rows)]


def _data_quality(frame: pd.DataFrame, warnings: list[str]) -> dict[str, Any]:
    if frame.empty:
        return {
            "missing_industry": 0,
            "missing_valuation": 0,
            "result_failures": 0,
            "insufficient_history": 0,
            "manual_review": 0,
            "warnings": warnings,
        }
    industry_missing = frame.apply(
        lambda row: all(is_missing(row.get(key)) for key in ("industry_group", "industry", "sector_name")),
        axis=1,
    )
    valuation = frame.get("valuation_history_bucket", pd.Series(pd.NA, index=frame.index))
    result_bucket = frame.get("quarterly_result_bucket", pd.Series("", index=frame.index)).astype(str).str.upper()
    tracker = frame.get("tracker_status", pd.Series("", index=frame.index)).astype(str).str.upper()
    return {
        "missing_industry": int(industry_missing.sum()),
        "missing_valuation": int(valuation.map(is_missing).sum()),
        "result_failures": int(result_bucket.eq("DETERIORATING").sum() + tracker.eq("RESULT_FAILURE").sum()),
        "insufficient_history": int(valuation.astype(str).str.upper().eq("INSUFFICIENT_HISTORY").sum()),
        "manual_review": int(frame.get("manual_review_flag", pd.Series(False, index=frame.index)).fillna(False).astype(bool).sum()),
        "warnings": warnings,
    }


def _sector_map(frame: pd.DataFrame, *, limit: int = 20) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    work = frame.copy()
    work.loc[:, "_sector"] = work.apply(lambda row: first_clean(row.to_dict(), ["industry_group", "industry", "sector_name"], missing="Unclassified"), axis=1)
    work.loc[:, "_score"] = pd.to_numeric(work.get("final_watchlist_score"), errors="coerce").fillna(0)
    rows: list[dict[str, Any]] = []
    for sector, group in work.groupby("_sector", dropna=False):
        counts = group["business_bucket"].value_counts().to_dict()
        best = (
            group.loc[~group["business_bucket"].astype(str).eq("AVOID_WATCH")]
            .sort_values(["_score", "symbol"], ascending=[False, True], kind="stable")
            .head(3)
        )
        rows.append(
            {
                "sector": clean_label(sector),
                "high_growth": int(counts.get("HIGH_GROWTH", 0)),
                "turnaround": int(counts.get("TURNAROUND_CANDIDATE", 0)),
                "cyclical": int(counts.get("CYCLICAL_COMMODITY", 0)),
                "avoid": int(counts.get("AVOID_WATCH", 0)),
                "best_names": ", ".join(best.get("symbol", pd.Series(dtype=str)).astype(str).tolist()) or "-",
                "opportunity_count": int(len(group.loc[~group["business_bucket"].astype(str).eq("AVOID_WATCH")])),
            }
        )
    return sorted(rows, key=lambda row: (row["opportunity_count"], row["high_growth"], row["turnaround"]), reverse=True)[:limit]


def _ranked_display(row: dict[str, Any], *, rank: int) -> dict[str, Any]:
    display = display_row(row)
    display["rank"] = rank
    return display


__all__ = ["APPENDIX_BUCKETS", "MAIN_BUCKETS", "build_report_summary"]
