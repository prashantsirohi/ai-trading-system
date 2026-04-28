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
from typing import Optional

import duckdb

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
}


def get_market_stage(
    ohlcv_db_path: str,
    *,
    asof: Optional[str] = None,
    breadth_s2_bull_threshold: float = 0.40,
    breadth_s4_bear_threshold: float = 0.40,
    breadth_s3_threshold: float = 0.30,
    min_classified_symbols: int = 200,
) -> dict:
    """Classify the broad market stage from the weekly breadth snapshot.

    Parameters
    ----------
    ohlcv_db_path:
        Path to the DuckDB file that holds ``weekly_stage_snapshot``.
    asof:
        Cut-off date (YYYY-MM-DD).  Uses the most recent snapshot row per
        symbol on or before this date.  Defaults to the latest available date.
    breadth_s2_bull_threshold:
        Fraction of classified symbols in S2 required to call market S2.
    breadth_s4_bear_threshold:
        Fraction of classified symbols in S4 required to call market S4.
    breadth_s3_threshold:
        Fraction of classified symbols in S3 required to call market S3
        (only evaluated when neither S4 nor S2 threshold is met).
    min_classified_symbols:
        Minimum number of classified rows required; fewer → fallback.

    Returns
    -------
    dict with keys:
        market_stage, method, s2_pct, s4_pct, s1_pct, s3_pct,
        classified_symbols, asof
    """
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
                return dict(_FALLBACK)

            # Resolve asof.
            if asof:
                asof_clause = f"week_end_date <= CAST('{asof}' AS DATE)"
                asof_val = asof
            else:
                asof_row = conn.execute(
                    "SELECT MAX(week_end_date) FROM weekly_stage_snapshot"
                ).fetchone()
                if not asof_row or asof_row[0] is None:
                    LOG.warning("market_stage: no rows in weekly_stage_snapshot — using fallback")
                    return dict(_FALLBACK)
                asof_val = str(asof_row[0])
                asof_clause = f"week_end_date <= CAST('{asof_val}' AS DATE)"

            # Latest row per symbol, excluding UNDEFINED labels.
            counts_df = conn.execute(f"""
                WITH latest AS (
                    SELECT symbol, stage_label
                    FROM weekly_stage_snapshot
                    WHERE {asof_clause}
                      AND stage_label != 'UNDEFINED'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY symbol ORDER BY week_end_date DESC
                    ) = 1
                )
                SELECT stage_label, COUNT(*) AS n
                FROM latest
                GROUP BY stage_label
            """).fetchdf()
        finally:
            conn.close()

        if counts_df.empty:
            LOG.warning("market_stage: no classified symbols asof=%s — using fallback", asof_val)
            return dict(_FALLBACK)

        label_counts: dict[str, int] = dict(
            zip(counts_df["stage_label"], counts_df["n"])
        )
        total = sum(label_counts.values())
        if total < min_classified_symbols:
            LOG.warning(
                "market_stage: only %d classified symbols (need %d) — using fallback",
                total, min_classified_symbols,
            )
            return dict(_FALLBACK)

        s2_pct = label_counts.get("S2", 0) / total
        s4_pct = label_counts.get("S4", 0) / total
        s3_pct = label_counts.get("S3", 0) / total
        s1_pct = label_counts.get("S1", 0) / total

        # Priority: S4 bear → S2 bull → S3 transitional → MIXED
        if s4_pct > breadth_s4_bear_threshold:
            stage = "S4"
        elif s2_pct > breadth_s2_bull_threshold:
            stage = "S2"
        elif s3_pct > breadth_s3_threshold:
            stage = "S3"
        else:
            stage = "MIXED"

        result = {
            "market_stage": stage,
            "method": "breadth",
            "s2_pct": round(s2_pct, 4),
            "s4_pct": round(s4_pct, 4),
            "s1_pct": round(s1_pct, 4),
            "s3_pct": round(s3_pct, 4),
            "classified_symbols": total,
            "asof": asof_val,
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
        return dict(_FALLBACK)
