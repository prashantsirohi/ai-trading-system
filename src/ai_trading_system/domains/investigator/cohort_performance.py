"""Investigator cohort-performance persistence, maturation, and analytics."""

from __future__ import annotations

from pathlib import Path
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
HORIZONS = (3, 5, 10, 20)
PERFORMANCE_GROUP_COLUMNS = [
    "trigger_reason",
    "verdict",
    "move_tag",
    "sector",
    "final_score_bucket",
    "credible_trigger",
    "hard_trap_flag",
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
            CREATE TEMP TABLE incoming_investigator_cohorts_enriched AS
            SELECT
                incoming.trade_date,
                incoming.symbol_id,
                incoming.exchange,
                incoming.trigger_reason,
                incoming.verdict,
                incoming.final_score,
                incoming.hard_trap_flag,
                incoming.credible_trigger,
                incoming.move_tag,
                incoming.sector,
                incoming.close,
                COALESCE(existing.fwd_3d_return, incoming.fwd_3d_return) AS fwd_3d_return,
                COALESCE(existing.fwd_5d_return, incoming.fwd_5d_return) AS fwd_5d_return,
                COALESCE(existing.fwd_10d_return, incoming.fwd_10d_return) AS fwd_10d_return,
                COALESCE(existing.fwd_20d_return, incoming.fwd_20d_return) AS fwd_20d_return,
                COALESCE(existing.fwd_3d_matured_at, incoming.fwd_3d_matured_at) AS fwd_3d_matured_at,
                COALESCE(existing.fwd_5d_matured_at, incoming.fwd_5d_matured_at) AS fwd_5d_matured_at,
                COALESCE(existing.fwd_10d_matured_at, incoming.fwd_10d_matured_at) AS fwd_10d_matured_at,
                COALESCE(existing.fwd_20d_matured_at, incoming.fwd_20d_matured_at) AS fwd_20d_matured_at,
                COALESCE(existing.data_quality_status, incoming.data_quality_status) AS data_quality_status,
                COALESCE(existing.inserted_at, CURRENT_TIMESTAMP) AS inserted_at
            FROM incoming_investigator_cohorts AS incoming
            LEFT JOIN investigator_cohort_performance AS existing
              ON existing.trade_date = incoming.trade_date
             AND existing.symbol_id = incoming.symbol_id
             AND existing.exchange = incoming.exchange
            """
        )
        conn.execute(
            """
            DELETE FROM investigator_cohort_performance AS existing
            USING incoming_investigator_cohorts AS incoming
            WHERE existing.trade_date = incoming.trade_date
              AND existing.symbol_id = incoming.symbol_id
              AND existing.exchange = incoming.exchange
            """
        )
        conn.execute(
            """
            INSERT INTO investigator_cohort_performance BY NAME
            SELECT
                incoming_investigator_cohorts_enriched.*,
                CURRENT_TIMESTAMP AS updated_at
            FROM incoming_investigator_cohorts_enriched
            """
        )
    finally:
        conn.execute("DROP TABLE IF EXISTS incoming_investigator_cohorts_enriched")
        conn.unregister("incoming_investigator_cohorts")
    return int(len(rows))


def mature_investigator_cohorts(
    conn: duckdb.DuckDBPyConnection,
    *,
    ohlcv_db_path: str | Path,
    horizons: tuple[int, ...] = HORIZONS,
) -> int:
    """Backfill matured forward returns for pending investigator cohorts."""
    pending = _pending_cohorts(conn)
    if pending.empty:
        return 0
    prices = _load_catalog_prices(ohlcv_db_path, pending)
    updates = _mature_rows(pending, prices, horizons=horizons)
    if updates.empty:
        return 0

    conn.register("investigator_cohort_updates", updates)
    try:
        conn.execute(
            """
            CREATE TEMP TABLE investigator_cohort_matured_rows AS
            SELECT
                cohorts.trade_date,
                cohorts.symbol_id,
                cohorts.exchange,
                cohorts.trigger_reason,
                cohorts.verdict,
                cohorts.final_score,
                cohorts.hard_trap_flag,
                cohorts.credible_trigger,
                cohorts.move_tag,
                cohorts.sector,
                cohorts.close,
                updates.fwd_3d_return,
                updates.fwd_5d_return,
                updates.fwd_10d_return,
                updates.fwd_20d_return,
                updates.fwd_3d_matured_at,
                updates.fwd_5d_matured_at,
                updates.fwd_10d_matured_at,
                updates.fwd_20d_matured_at,
                updates.data_quality_status,
                cohorts.inserted_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM investigator_cohort_performance AS cohorts
            JOIN investigator_cohort_updates AS updates
              ON cohorts.trade_date = updates.trade_date
             AND cohorts.symbol_id = updates.symbol_id
             AND cohorts.exchange = updates.exchange
            """
        )
        conn.execute(
            """
            DELETE FROM investigator_cohort_performance AS cohorts
            USING investigator_cohort_updates AS updates
            WHERE cohorts.trade_date = updates.trade_date
              AND cohorts.symbol_id = updates.symbol_id
              AND cohorts.exchange = updates.exchange
            """
        )
        conn.execute(
            """
            INSERT INTO investigator_cohort_performance BY NAME
            SELECT * FROM investigator_cohort_matured_rows
            """
        )
    finally:
        conn.execute("DROP TABLE IF EXISTS investigator_cohort_matured_rows")
        conn.unregister("investigator_cohort_updates")
    return int(len(updates))


def build_performance_summary(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build grouped investigator cohort-performance metrics."""
    cohorts = _read_cohorts(conn)
    summary = _summary_payload(cohorts)
    if cohorts.empty:
        return pd.DataFrame(columns=_performance_columns()), summary
    working = cohorts.copy()
    working.loc[:, "final_score_bucket"] = _score_bucket(working.get("final_score", pd.Series(dtype=float)))
    rows: list[dict[str, Any]] = []
    for group_column in PERFORMANCE_GROUP_COLUMNS:
        if group_column not in working.columns:
            continue
        labels = working[group_column].fillna("UNKNOWN").astype(str)
        for label, group in working.assign(_group_label=labels).groupby("_group_label", dropna=False):
            for horizon in HORIZONS:
                metrics = _return_metrics(group, f"fwd_{horizon}d_return")
                if metrics["sample_count"] <= 0:
                    continue
                rows.append(
                    {
                        "group_type": group_column,
                        "group_value": str(label),
                        "horizon": f"{horizon}d",
                        **metrics,
                    }
                )
    frame = pd.DataFrame(rows, columns=_performance_columns())
    summary.update(_best_worst_payload(frame))
    summary["score_bucket_performance"] = _records(
        frame.loc[frame.get("group_type", pd.Series(dtype=str)).eq("final_score_bucket")]
        if not frame.empty
        else pd.DataFrame()
    )
    return frame, summary


def build_threshold_recommendations(
    performance_summary: pd.DataFrame,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return diagnostic-only tuning recommendations."""
    summary = summary or {}
    matured_5d = int(dict(summary.get("matured_by_horizon") or {}).get("5d", 0) or 0)
    base = {
        "insufficient_sample": matured_5d < 100,
        "minimum_overall_matured_5d": 100,
        "minimum_group_matured_5d": 30,
        "matured_5d_count": matured_5d,
        "recommendation": "Do not tune thresholds yet.",
        "recommendations": [],
    }
    if base["insufficient_sample"] or performance_summary is None or performance_summary.empty:
        return base
    five_day = performance_summary.loc[performance_summary["horizon"].astype(str).eq("5d")].copy()
    eligible = five_day.loc[pd.to_numeric(five_day["sample_count"], errors="coerce").fillna(0).ge(30)]
    if eligible.empty:
        return {**base, "insufficient_sample": True}
    recommendations: list[dict[str, Any]] = []
    weak = eligible.sort_values(["avg_return", "win_rate"], ascending=[True, True]).head(3)
    strong = eligible.sort_values(["avg_return", "win_rate"], ascending=[False, False]).head(3)
    for row in weak.to_dict(orient="records"):
        recommendations.append(
            {
                "action": "review_or_penalize_underperforming_group",
                "group_type": row.get("group_type"),
                "group_value": row.get("group_value"),
                "evidence": f"5D avg return {row.get('avg_return')} with sample {row.get('sample_count')}",
            }
        )
    for row in strong.to_dict(orient="records"):
        recommendations.append(
            {
                "action": "review_or_promote_outperforming_group",
                "group_type": row.get("group_type"),
                "group_value": row.get("group_value"),
                "evidence": f"5D avg return {row.get('avg_return')} with sample {row.get('sample_count')}",
            }
        )
    return {
        **base,
        "insufficient_sample": False,
        "recommendation": "Review diagnostic recommendations; do not apply automatically.",
        "recommendations": recommendations,
    }


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


def _pending_cohorts(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    try:
        return conn.execute(
            """
            SELECT *
            FROM investigator_cohort_performance
            WHERE data_quality_status IS NULL
               OR data_quality_status IN ('PENDING', 'PARTIAL_MATURED', 'INSUFFICIENT_PRICE_DATA')
               OR fwd_3d_return IS NULL
               OR fwd_5d_return IS NULL
               OR fwd_10d_return IS NULL
               OR fwd_20d_return IS NULL
            """
        ).fetchdf()
    except Exception:
        return pd.DataFrame()


def _read_cohorts(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    try:
        return conn.execute("SELECT * FROM investigator_cohort_performance").fetchdf()
    except Exception:
        return pd.DataFrame()


def _load_catalog_prices(ohlcv_db_path: str | Path, cohorts: pd.DataFrame) -> pd.DataFrame:
    path = Path(ohlcv_db_path)
    if cohorts.empty or not path.exists():
        return pd.DataFrame(columns=["symbol_id", "exchange", "trade_date", "close", "idx"])
    symbols = sorted(cohorts["symbol_id"].dropna().astype(str).str.upper().unique().tolist())
    if not symbols:
        return pd.DataFrame(columns=["symbol_id", "exchange", "trade_date", "close", "idx"])
    trade_dates = pd.to_datetime(cohorts.get("trade_date", pd.Series(dtype="object")), errors="coerce").dropna()
    min_trade_date = trade_dates.min().strftime("%Y-%m-%d") if not trade_dates.empty else None
    conn: duckdb.DuckDBPyConnection | None = None
    try:
        conn = duckdb.connect(str(path), read_only=True)
        placeholders = ", ".join(["?"] * len(symbols))
        params: list[Any] = [*symbols]
        date_filter = ""
        if min_trade_date:
            date_filter = "AND CAST(timestamp AS DATE) >= CAST(? AS DATE)"
            params.append(min_trade_date)
        prices = conn.execute(
            f"""
            WITH catalog_prices AS (
                SELECT
                    UPPER(symbol_id) AS symbol_id,
                    UPPER(COALESCE(exchange, 'NSE')) AS exchange,
                    CAST(timestamp AS DATE) AS trade_date,
                    close
                FROM _catalog
                WHERE UPPER(symbol_id) IN ({placeholders})
                  AND COALESCE(is_benchmark, false) = false
                  {date_filter}
            )
            SELECT
                symbol_id,
                exchange,
                trade_date,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol_id, exchange
                    ORDER BY trade_date
                ) - 1 AS idx
            FROM catalog_prices
            ORDER BY symbol_id, exchange, trade_date
            """,
            params,
        ).fetchdf()
    except Exception:
        return pd.DataFrame(columns=["symbol_id", "exchange", "trade_date", "close", "idx"])
    finally:
        if conn is not None:
            conn.close()
    if prices.empty:
        return prices
    prices = prices.assign(
        trade_date=pd.to_datetime(prices["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d"),
        close=pd.to_numeric(prices["close"], errors="coerce"),
        idx=pd.to_numeric(prices["idx"], errors="coerce"),
    )
    prices = prices.dropna(subset=["trade_date", "close", "idx"]).reset_index(drop=True)
    prices.loc[:, "idx"] = prices["idx"].astype(int)
    return prices


def _mature_rows(cohorts: pd.DataFrame, prices: pd.DataFrame, *, horizons: tuple[int, ...]) -> pd.DataFrame:
    updates: list[dict[str, Any]] = []
    grouped = {
        key: group.sort_values("trade_date").reset_index(drop=True)
        for key, group in prices.groupby(["symbol_id", "exchange"])
    } if not prices.empty else {}
    for record in cohorts.to_dict(orient="records"):
        symbol = str(record.get("symbol_id") or "").strip().upper()
        exchange = str(record.get("exchange") or "NSE").strip().upper() or "NSE"
        trade_date = _date_text(record.get("trade_date"))
        row = {
            "trade_date": trade_date,
            "symbol_id": symbol,
            "exchange": exchange,
        }
        matured_count = 0
        price_frame = grouped.get((symbol, exchange), pd.DataFrame())
        entry_close = _as_float(record.get("close"))
        entry_idx = None
        if not price_frame.empty and trade_date:
            matches = price_frame.index[price_frame["trade_date"].eq(trade_date)].tolist()
            if matches:
                entry_idx = int(matches[0])
                entry_close = _as_float(price_frame.iloc[entry_idx].get("close"))
        for horizon in horizons:
            existing_return = _as_float(record.get(f"fwd_{horizon}d_return"))
            existing_matured_at = _date_text(record.get(f"fwd_{horizon}d_matured_at"))
            value = existing_return
            matured_at = existing_matured_at or None
            if value is None and entry_idx is not None and entry_close not in {None, 0}:
                target_idx = entry_idx + horizon
                if target_idx < len(price_frame):
                    future = price_frame.iloc[target_idx]
                    future_close = _as_float(future.get("close"))
                    if future_close is not None:
                        value = (future_close - float(entry_close)) / float(entry_close) * 100.0
                        matured_at = _date_text(future.get("trade_date"))
            if value is not None:
                matured_count += 1
            row[f"fwd_{horizon}d_return"] = value
            row[f"fwd_{horizon}d_matured_at"] = matured_at
        if price_frame.empty or entry_idx is None or entry_close is None:
            status = "INSUFFICIENT_PRICE_DATA"
        elif matured_count == 0:
            status = "PENDING"
        elif matured_count < len(horizons):
            status = "PARTIAL_MATURED"
        else:
            status = "MATURED"
        row["data_quality_status"] = status
        updates.append(row)
    return pd.DataFrame(updates)


def _summary_payload(cohorts: pd.DataFrame) -> dict[str, Any]:
    if cohorts.empty:
        return {
            "total_cohorts": 0,
            "pending_cohorts": 0,
            "matured_cohorts": 0,
            "matured_by_horizon": {f"{horizon}d": 0 for horizon in HORIZONS},
        }
    matured_by_horizon = {
        f"{horizon}d": int(pd.to_numeric(cohorts.get(f"fwd_{horizon}d_return"), errors="coerce").notna().sum())
        for horizon in HORIZONS
    }
    status = cohorts.get("data_quality_status", pd.Series("", index=cohorts.index)).fillna("").astype(str)
    return {
        "total_cohorts": int(len(cohorts)),
        "pending_cohorts": int(status.isin(["", "PENDING", "PARTIAL_MATURED", "INSUFFICIENT_PRICE_DATA"]).sum()),
        "matured_cohorts": int(status.eq("MATURED").sum()),
        "matured_by_horizon": matured_by_horizon,
    }


def _best_worst_payload(frame: pd.DataFrame) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if frame.empty:
        return payload
    for group_type, key_prefix in (("trigger_reason", "trigger_reason"), ("move_tag", "move_tag")):
        for horizon in ("5d", "10d"):
            subset = frame.loc[(frame["group_type"].eq(group_type)) & (frame["horizon"].eq(horizon))].copy()
            if subset.empty:
                payload[f"best_{key_prefix}_{horizon}"] = None
                payload[f"worst_{key_prefix}_{horizon}"] = None
                continue
            ordered = subset.sort_values(["avg_return", "sample_count"], ascending=[False, False], kind="stable")
            payload[f"best_{key_prefix}_{horizon}"] = ordered.head(1).to_dict(orient="records")[0]
            payload[f"worst_{key_prefix}_{horizon}"] = ordered.tail(1).to_dict(orient="records")[0]
    return payload


def _return_metrics(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    values = pd.to_numeric(frame.get(column, pd.Series(dtype=float)), errors="coerce").dropna()
    positives = values.loc[values.gt(0)]
    negatives = values.loc[values.lt(0)]
    count = int(len(values))
    return {
        "sample_count": count,
        "win_rate": round(float(values.gt(0).mean() * 100.0), 2) if count else pd.NA,
        "avg_return": round(float(values.mean()), 4) if count else pd.NA,
        "median_return": round(float(values.median()), 4) if count else pd.NA,
        "hit_rate_above_2pct": round(float(values.gt(2.0).mean() * 100.0), 2) if count else pd.NA,
        "hit_rate_above_5pct": round(float(values.gt(5.0).mean() * 100.0), 2) if count else pd.NA,
        "avg_loss_when_negative": round(float(negatives.mean()), 4) if len(negatives) else pd.NA,
        "avg_gain_when_positive": round(float(positives.mean()), 4) if len(positives) else pd.NA,
    }


def _score_bucket(series: Any) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    out = pd.Series("UNKNOWN", index=values.index, dtype=object)
    out = out.mask(values.ge(55) & values.lt(65), "55-64")
    out = out.mask(values.ge(65) & values.lt(75), "65-74")
    out = out.mask(values.ge(75) & values.lt(85), "75-84")
    out = out.mask(values.ge(85), "85+")
    return out


def _performance_columns() -> list[str]:
    return [
        "group_type",
        "group_value",
        "horizon",
        "sample_count",
        "win_rate",
        "avg_return",
        "median_return",
        "hit_rate_above_2pct",
        "hit_rate_above_5pct",
        "avg_loss_when_negative",
        "avg_gain_when_positive",
    ]


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    safe = frame.where(frame.notna(), None)
    return safe.to_dict(orient="records")


def _date_text(value: object) -> str:
    date = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(date) else date.strftime("%Y-%m-%d")


def _as_float(value: object) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(out) else out
