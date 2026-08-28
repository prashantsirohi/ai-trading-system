"""Market-level stage classification from breadth of weekly_stage_snapshot.

Determines whether the broad market is in a bull (S2), transitional (S3),
bear (S4), or mixed/uncertain regime by counting the distribution of
Weinstein stages across all classified symbols.

Usage
-----
    from ai_trading_system.domains.ranking.market_stage import get_market_stage

    info = get_market_stage("data/ohlcv.duckdb")
    # -> {"market_stage": "S4", "method": "breadth", "s2_pct": 0.12,
    #     "s4_pct": 0.56, "s1_pct": 0.14, "s3_pct": 0.18,
    #     "classified_symbols": 1624, "asof": "2026-04-25"}
"""
from __future__ import annotations

import logging
from datetime import date as date_type
from typing import Optional

import duckdb
import pandas as pd

LOG = logging.getLogger(__name__)

_FALLBACK = {
    "market_stage": "MIXED",
    "method": "fallback_default",
    "s2_pct": None,
    "s4_pct": None,
    "s1_pct": None,
    "s3_pct": None,
    "classified_symbols": 0,
    "asof": None,
    "source": None,
    "source_as_of": None,
    "source_age_days": None,
    "freshness_status": "MISSING",
    "fallback_reason": None,
}

_CANONICAL_STAGE_FAMILY = {
    "stage_1_basing": "S1",
    "transition_1_to_2": "S1",
    "stage_2_advancing": "S2",
    "transition_2_to_3": "S2",
    "stage_3_topping": "S3",
    "transition_3_to_4": "S3",
    "stage_4_declining": "S4",
    "transition_4_to_1": "S4",
    "S1": "S1",
    "S2": "S2",
    "S3": "S3",
    "S4": "S4",
}


def _classify_breadth(
    label_counts: dict[str, int],
    *,
    breadth_s2_bull_threshold: float,
    breadth_s4_bear_threshold: float,
    breadth_s3_threshold: float,
) -> tuple[str, float, float, float, float, int]:
    total = sum(label_counts.values())
    s2_pct = label_counts.get("S2", 0) / total
    s4_pct = label_counts.get("S4", 0) / total
    s3_pct = label_counts.get("S3", 0) / total
    s1_pct = label_counts.get("S1", 0) / total
    if s4_pct > breadth_s4_bear_threshold:
        stage = "S4"
    elif s2_pct > breadth_s2_bull_threshold:
        stage = "S2"
    elif s3_pct > breadth_s3_threshold:
        stage = "S3"
    else:
        stage = "MIXED"
    return stage, s2_pct, s4_pct, s1_pct, s3_pct, total


def _cutoff_date(asof: str | None, fallback: date_type) -> date_type:
    return date_type.fromisoformat(str(asof)[:10]) if asof else fallback


def _fallback(
    *,
    asof: str | None,
    reason: str,
    source: str | None = None,
    source_as_of: date_type | None = None,
    source_age_days: int | None = None,
    freshness_status: str = "MISSING",
) -> dict:
    return {
        **_FALLBACK,
        "asof": asof,
        "source": source,
        "source_as_of": str(source_as_of) if source_as_of else None,
        "source_age_days": source_age_days,
        "freshness_status": freshness_status,
        "fallback_reason": reason,
    }


def get_market_stage(
    ohlcv_db_path: str,
    *,
    asof: Optional[str] = None,
    governed_stages: pd.DataFrame | None = None,
    breadth_s2_bull_threshold: float = 0.40,
    breadth_s4_bear_threshold: float = 0.40,
    breadth_s3_threshold: float = 0.30,
    min_classified_symbols: int = 200,
    max_source_age_days: int = 10,
) -> dict:
    """Classify the broad market stage from the weekly breadth snapshot.

    Parameters
    ----------
    ohlcv_db_path:
        Path to the DuckDB file that holds ``weekly_stage_snapshot``.
    asof:
        Cut-off date (YYYY-MM-DD).  Uses the most recent snapshot row per
        symbol on or before this date.  Defaults to the latest available date.
    governed_stages:
        Canonical point-in-time rows from ``weekly_stock_stage_history``. A
        fresh, sufficiently covered governed frame takes precedence over the
        legacy mutable snapshot.
    breadth_s2_bull_threshold:
        Fraction of classified symbols in S2 required to call market S2.
    breadth_s4_bear_threshold:
        Fraction of classified symbols in S4 required to call market S4.
    breadth_s3_threshold:
        Fraction of classified symbols in S3 required to call market S3
        (only evaluated when neither S4 nor S2 threshold is met).
    min_classified_symbols:
        Minimum number of classified rows required; fewer → fallback.
    max_source_age_days:
        Maximum calendar age for either source. Stale legacy rows cannot
        continue routing the market into S4 indefinitely.

    Returns
    -------
    dict with keys:
        market_stage, method, s2_pct, s4_pct, s1_pct, s3_pct,
        classified_symbols, asof
    """
    if governed_stages is not None and not governed_stages.empty:
        try:
            governed = governed_stages.copy()
            stage_column = (
                "effective_stage"
                if "effective_stage" in governed.columns
                else "locked_stage"
            )
            if stage_column not in governed.columns or "as_of" not in governed.columns:
                raise ValueError("governed stage rows lack effective_stage/locked_stage or as_of")
            governed.loc[:, "_source_as_of"] = pd.to_datetime(governed["as_of"], errors="coerce").dt.date
            governed = governed.loc[governed["_source_as_of"].notna()].copy()
            if governed.empty:
                raise ValueError("governed stage rows have no valid as_of values")
            source_as_of = max(governed["_source_as_of"])
            cutoff = _cutoff_date(asof, source_as_of)
            governed = governed.loc[governed["_source_as_of"] <= cutoff].copy()
            if governed.empty:
                raise ValueError("governed stage rows are future-dated for the requested cutoff")
            entity_columns = [
                column
                for column in ("exchange", "symbol_id")
                if column in governed.columns
            ]
            if entity_columns:
                governed = governed.sort_values("_source_as_of").drop_duplicates(
                    entity_columns,
                    keep="last",
                )
            governed.loc[:, "_source_age_days"] = governed["_source_as_of"].map(
                lambda value: (cutoff - value).days
            )
            fresh_governed = governed.loc[
                governed["_source_age_days"] <= max_source_age_days
            ].copy()
            stale_symbols_excluded = len(governed) - len(fresh_governed)
            if not fresh_governed.empty:
                source_as_of = max(fresh_governed["_source_as_of"])
                source_age_days = (cutoff - source_as_of).days
                labels = fresh_governed[stage_column].map(
                    lambda value: _CANONICAL_STAGE_FAMILY.get(str(value))
                )
                label_counts = labels.dropna().value_counts().astype(int).to_dict()
                stage, s2_pct, s4_pct, s1_pct, s3_pct, total = (
                    _classify_breadth(
                        label_counts,
                        breadth_s2_bull_threshold=breadth_s2_bull_threshold,
                        breadth_s4_bear_threshold=breadth_s4_bear_threshold,
                        breadth_s3_threshold=breadth_s3_threshold,
                    )
                    if label_counts
                    else ("MIXED", 0.0, 0.0, 0.0, 0.0, 0)
                )
                if total >= min_classified_symbols:
                    return {
                        "market_stage": stage,
                        "method": "governed_breadth",
                        "s2_pct": round(s2_pct, 4),
                        "s4_pct": round(s4_pct, 4),
                        "s1_pct": round(s1_pct, 4),
                        "s3_pct": round(s3_pct, 4),
                        "classified_symbols": total,
                        "asof": str(cutoff),
                        "source": "control_plane.duckdb:weekly_stock_stage_history",
                        "source_as_of": str(source_as_of),
                        "source_age_days": source_age_days,
                        "freshness_status": "FRESH",
                        "fallback_reason": None,
                        "stale_symbols_excluded": stale_symbols_excluded,
                    }
                LOG.warning(
                    "market_stage: only %d governed symbols (need %d) — checking legacy fallback",
                    total,
                    min_classified_symbols,
                )
            else:
                LOG.warning(
                    "market_stage: all governed rows exceed max age %d — checking legacy fallback",
                    max_source_age_days,
                )
        except Exception as exc:
            LOG.warning("market_stage: governed source unavailable (%s) — checking legacy fallback", exc)

    try:
        conn = duckdb.connect(ohlcv_db_path, read_only=True)
        try:
            # Verify table exists.
            tables = {r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'weekly_stage_snapshot'"
            ).fetchall()}
            if "weekly_stage_snapshot" not in tables:
                LOG.warning("market_stage: weekly_stage_snapshot table missing — using fallback")
                return _fallback(asof=asof, reason="legacy_table_missing")

            # Resolve asof.
            if asof:
                asof_val = asof
            else:
                asof_row = conn.execute(
                    "SELECT MAX(week_end_date) FROM weekly_stage_snapshot"
                ).fetchone()
                if not asof_row or asof_row[0] is None:
                    LOG.warning("market_stage: no rows in weekly_stage_snapshot — using fallback")
                    return _fallback(asof=asof, reason="legacy_source_empty")
                asof_val = str(asof_row[0])

            # Latest row per symbol, excluding UNDEFINED labels.
            counts_df = conn.execute("""
                WITH latest AS (
                    SELECT symbol, stage_label, week_end_date
                    FROM weekly_stage_snapshot
                    WHERE week_end_date <= CAST(? AS DATE)
                      AND stage_label != 'UNDEFINED'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol ORDER BY week_end_date DESC
                    ) = 1
                )
                SELECT stage_label, week_end_date AS source_as_of, COUNT(*) AS n
                FROM latest
                GROUP BY stage_label, week_end_date
            """, [asof_val]).fetchdf()
        finally:
            conn.close()

        if counts_df.empty:
            LOG.warning("market_stage: no classified symbols asof=%s — using fallback", asof_val)
            return _fallback(asof=asof_val, reason="legacy_source_empty")

        counts_df = counts_df.assign(
            source_as_of=pd.to_datetime(counts_df["source_as_of"]).dt.date
        )
        source_as_of = max(counts_df["source_as_of"])
        cutoff = _cutoff_date(asof, source_as_of)
        source_age_days = (cutoff - source_as_of).days
        counts_df.loc[:, "source_age_days"] = counts_df["source_as_of"].map(
            lambda value: (cutoff - value).days
        )
        fresh_counts = counts_df.loc[
            counts_df["source_age_days"] <= max_source_age_days
        ].copy()
        if fresh_counts.empty:
            LOG.warning(
                "market_stage: legacy source is %d days old (max %d) — using fallback",
                source_age_days, max_source_age_days,
            )
            return _fallback(
                asof=str(cutoff), reason="legacy_source_stale",
                source="ohlcv.duckdb:weekly_stage_snapshot", source_as_of=source_as_of,
                source_age_days=source_age_days, freshness_status="STALE",
            )

        label_counts: dict[str, int] = (
            fresh_counts.groupby("stage_label")["n"].sum().astype(int).to_dict()
        )
        total = sum(label_counts.values())
        if total < min_classified_symbols:
            LOG.warning(
                "market_stage: only %d classified symbols (need %d) — using fallback",
                total, min_classified_symbols,
            )
            return _fallback(
                asof=str(cutoff), reason="legacy_coverage_insufficient",
                source="ohlcv.duckdb:weekly_stage_snapshot", source_as_of=source_as_of,
                source_age_days=source_age_days, freshness_status="FRESH",
            )

        stage, s2_pct, s4_pct, s1_pct, s3_pct, total = _classify_breadth(
            label_counts,
            breadth_s2_bull_threshold=breadth_s2_bull_threshold,
            breadth_s4_bear_threshold=breadth_s4_bear_threshold,
            breadth_s3_threshold=breadth_s3_threshold,
        )

        result = {
            "market_stage": stage,
            "method": "breadth",
            "s2_pct": round(s2_pct, 4),
            "s4_pct": round(s4_pct, 4),
            "s1_pct": round(s1_pct, 4),
            "s3_pct": round(s3_pct, 4),
            "classified_symbols": total,
            "asof": str(cutoff),
            "source": "ohlcv.duckdb:weekly_stage_snapshot",
            "source_as_of": str(source_as_of),
            "source_age_days": source_age_days,
            "freshness_status": "FRESH",
            "fallback_reason": None,
        }
        LOG.info(
            "market_stage=%s  S2=%.1f%%  S3=%.1f%%  S4=%.1f%%  S1=%.1f%%  n=%d  asof=%s",
            stage,
            s2_pct * 100, s3_pct * 100, s4_pct * 100, s1_pct * 100,
            total, asof_val,
        )
        return result

    except Exception as exc:
        LOG.warning("market_stage: unexpected error (%s) — using fallback", exc)
        return _fallback(asof=asof, reason="market_stage_error")
