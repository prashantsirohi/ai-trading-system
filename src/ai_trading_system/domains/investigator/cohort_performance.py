"""Investigator cohort-performance insert scaffold."""

from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd


COHORT_COLUMNS = [
    "trade_date",
    "symbol_id",
    "exchange",
    "trigger_reason",
    "verdict",
    "final_score",
    "hard_trap_flag",
    "credible_trigger",
    "move_tag",
    "sector",
    "close",
    "fwd_3d_return",
    "fwd_5d_return",
    "fwd_10d_return",
    "fwd_20d_return",
    "fwd_3d_matured_at",
    "fwd_5d_matured_at",
    "fwd_10d_matured_at",
    "fwd_20d_matured_at",
    "data_quality_status",
]


def build_cohort_rows(
    final_gate: pd.DataFrame,
    investigator_scores: pd.DataFrame | None = None,
    *,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Build pending investigator cohort rows from clean final-gate output."""
    if final_gate is None or final_gate.empty:
        return pd.DataFrame(columns=COHORT_COLUMNS)
    required = {"trade_date", "symbol_id"}
    if not required.issubset(final_gate.columns):
        return pd.DataFrame(columns=COHORT_COLUMNS)

    source = final_gate.copy().assign(
        trade_date=pd.to_datetime(final_gate["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d").astype("object")
    )
    source.loc[:, "symbol_id"] = source["symbol_id"].fillna("").astype(str).str.strip().str.upper()
    source = source.loc[source["trade_date"].notna() & source["symbol_id"].ne("")].copy()
    if source.empty:
        return pd.DataFrame(columns=COHORT_COLUMNS)

    scores = investigator_scores.copy() if isinstance(investigator_scores, pd.DataFrame) else pd.DataFrame()
    if not scores.empty and "symbol_id" in scores.columns:
        scores.loc[:, "symbol_id"] = scores["symbol_id"].fillna("").astype(str).str.strip().str.upper()
        if "trade_date" in scores.columns:
            scores = scores.assign(
                trade_date=pd.to_datetime(scores["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d").astype("object")
            )
        score_columns = [
            col
            for col in ("symbol_id", "trade_date", "trigger_reason", "move_tag", "sector", "close")
            if col in scores.columns
        ]
        scores = scores[score_columns].drop_duplicates(
            ["symbol_id", "trade_date"] if "trade_date" in score_columns else ["symbol_id"],
            keep="first",
        )
        if "trade_date" in score_columns:
            source = source.merge(scores, on=["symbol_id", "trade_date"], how="left", suffixes=("", "_score"))
        else:
            source = source.merge(scores, on="symbol_id", how="left", suffixes=("", "_score"))

    out = pd.DataFrame(index=source.index)
    out.loc[:, "trade_date"] = source["trade_date"]
    out.loc[:, "symbol_id"] = source["symbol_id"]
    out.loc[:, "exchange"] = str(exchange or "NSE").strip().upper() or "NSE"
    for column in ("trigger_reason", "verdict", "move_tag", "sector"):
        out.loc[:, column] = _first_available(source, column)
    out.loc[:, "final_score"] = pd.to_numeric(_first_available(source, "final_score"), errors="coerce")
    out.loc[:, "hard_trap_flag"] = _nullable_bool(_first_available(source, "hard_trap_flag"))
    out.loc[:, "credible_trigger"] = _nullable_bool(_first_available(source, "credible_trigger"))
    out.loc[:, "close"] = pd.to_numeric(_first_available(source, "close"), errors="coerce")
    for column in (
        "fwd_3d_return",
        "fwd_5d_return",
        "fwd_10d_return",
        "fwd_20d_return",
        "fwd_3d_matured_at",
        "fwd_5d_matured_at",
        "fwd_10d_matured_at",
        "fwd_20d_matured_at",
    ):
        out.loc[:, column] = pd.NA
    out.loc[:, "data_quality_status"] = "PENDING"
    return out[COHORT_COLUMNS].drop_duplicates(["trade_date", "symbol_id", "exchange"], keep="first").reset_index(drop=True)


def upsert_investigator_cohorts(
    conn: duckdb.DuckDBPyConnection,
    final_gate: pd.DataFrame,
    investigator_scores: pd.DataFrame | None = None,
    *,
    exchange: str = "NSE",
) -> int:
    """Idempotently seed pending investigator cohort-performance rows."""
    rows = build_cohort_rows(final_gate, investigator_scores, exchange=exchange)
    if rows.empty:
        return 0

    conn.register("incoming_investigator_cohorts", rows)
    try:
        conn.execute(
            """
            DELETE FROM investigator_cohort_performance AS existing
            USING incoming_investigator_cohorts AS incoming
            WHERE existing.trade_date = incoming.trade_date
              AND existing.symbol_id = incoming.symbol_id
              AND existing.exchange = incoming.exchange
            """
        )
        # Forward-return maturation is intentionally deferred to a later sprint.
        conn.execute(
            """
            INSERT INTO investigator_cohort_performance BY NAME
            SELECT
                incoming_investigator_cohorts.*,
                CURRENT_TIMESTAMP AS inserted_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM incoming_investigator_cohorts
            """
        )
    finally:
        conn.unregister("incoming_investigator_cohorts")
    return int(len(rows))


def _first_available(frame: pd.DataFrame, column: str) -> Any:
    if column in frame.columns:
        return frame[column]
    score_column = f"{column}_score"
    if score_column in frame.columns:
        return frame[score_column]
    return pd.Series(pd.NA, index=frame.index)


def _nullable_bool(series: Any) -> pd.Series:
    values = pd.Series(series)
    if values.empty:
        return values
    lowered = values.fillna("").astype(str).str.strip().str.lower()
    mapped = lowered.map({"true": True, "1": True, "yes": True, "false": False, "0": False, "no": False})
    return mapped.where(lowered.ne(""), pd.NA)
