"""Top-1000 liquid equal-weight point-in-time universe index.

Pure compute layer for the ``UNIV_TOP1000`` benchmark series. Consumed by:
- :mod:`tools.build_universe_index` (offline backfill + incremental)
- :mod:`ai_trading_system.research.optimization.baselines` (per-fold return)
- :mod:`ai_trading_system.research.backtesting.research_loader` (RS blend)
- :mod:`ai_trading_system.domains.features.sector_rs` (benchmark-relative)

Anti-lookahead invariants
-------------------------
- ``compute_membership_for_rebalance(D)`` reads turnover strictly before ``D``.
- ``compute_index_bar(D)`` uses prices on ``D`` and ``D-1`` only.

Coverage / safety
-----------------
- Eligibility filter excludes pathological rows (see :data:`ELIGIBILITY_SQL`).
- ``compute_index_bar`` holds the prior level when ``n_used / n_members``
  drops below ``min_used_ratio`` (default 0.70) so a partial-coverage day
  does not corrupt the cumulative chain. Diagnostics record the event.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd


logger = logging.getLogger(__name__)


UNIVERSE_INDEX_CODE = "UNIV_TOP1000"
UNIVERSE_INDEX_BASE_LEVEL = 100.0
DEFAULT_TOP_N = 1000
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_MIN_RECENT_DAYS = 180
DEFAULT_MIN_USED_RATIO = 0.70


# Predicates that define "eligible for top-1000 selection". Listed here so
# future enrichments (SME / T2T / suspended) are grep-able. Composed into the
# WHERE clause of :func:`eligible_symbols_for_lookback`.
ELIGIBILITY_SQL = (
    "close > 0",       # no zero/negative
    "volume > 0",      # no untraded bars
    "exchange = ?",    # parameter-bound; cash equity, NSE only
    # TODO when masterdata has these flags:
    #   "instrument_segment NOT IN ('SME', 'T2T', 'BE', 'BZ')"
    #   "is_suspended = FALSE"
)


@dataclass(frozen=True)
class IndexBarDiagnostics:
    """One row of `_universe_index_diagnostics` for a given bar."""

    index_code: str
    date: date
    rebalance_date: date
    n_members: int
    n_used: int
    n_missing: int
    used_ratio: float
    daily_return: float
    index_level: float
    quality_flag: str  # 'ok' | 'low_coverage' | 'sparse_membership' | 'gap'


def trading_days_between(
    ohlcv_db_path: Path | str,
    start: date,
    end: date,
    *,
    exchange: str = "NSE",
) -> list[date]:
    """Distinct trading dates in ``_catalog`` within ``[start, end]`` for the
    given exchange. Ordered ascending."""
    con = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT CAST(timestamp AS DATE) AS d
              FROM _catalog
             WHERE exchange = ?
               AND timestamp IS NOT NULL
               AND CAST(timestamp AS DATE) BETWEEN ? AND ?
             ORDER BY d
            """,
            [exchange, start, end],
        ).fetchall()
    finally:
        con.close()
    return [row[0] for row in rows]


def first_trading_day_of_month(trading_days: list[date]) -> dict[tuple[int, int], date]:
    """Map (year, month) -> earliest trading date in that month."""
    out: dict[tuple[int, int], date] = {}
    for d in trading_days:
        key = (d.year, d.month)
        if key not in out:
            out[key] = d
    return out


def eligible_symbols_for_lookback(
    ohlcv_db_path: Path | str,
    *,
    rebalance_date: date,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_recent_days: int = DEFAULT_MIN_RECENT_DAYS,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """Symbols with sufficient clean OHLCV in [rebalance_date - lookback, rebalance_date - 1].

    Returns DataFrame: symbol_id, median_turnover, recent_days. Sorted by
    median_turnover DESC.
    """
    from datetime import timedelta

    lookback_start = rebalance_date - timedelta(days=lookback_days)
    lookback_end = rebalance_date - timedelta(days=1)  # strictly BEFORE rebalance

    where_predicates = " AND ".join(p for p in ELIGIBILITY_SQL)
    sql = f"""
        SELECT
            symbol_id,
            MEDIAN(close * volume) AS median_turnover,
            COUNT(*) AS recent_days
          FROM _catalog
         WHERE CAST(timestamp AS DATE) BETWEEN ? AND ?
           AND {where_predicates}
         GROUP BY symbol_id
        HAVING COUNT(*) >= ?
         ORDER BY median_turnover DESC
    """
    con = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        df = con.execute(
            sql,
            [lookback_start, lookback_end, exchange, min_recent_days],
        ).fetchdf()
    finally:
        con.close()
    return df


def compute_membership_for_rebalance(
    ohlcv_db_path: Path | str,
    *,
    rebalance_date: date,
    top_n: int = DEFAULT_TOP_N,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_recent_days: int = DEFAULT_MIN_RECENT_DAYS,
    exchange: str = "NSE",
) -> tuple[pd.DataFrame, bool]:
    """Top-N by trailing median turnover ending strictly before ``rebalance_date``.

    Returns ``(members_df, sparse_history)`` where ``members_df`` has columns
    ``symbol_id, rank_by_turnover, median_turnover, recent_days`` and
    ``sparse_history`` is True when fewer than ``top_n`` eligible symbols exist.
    """
    eligible = eligible_symbols_for_lookback(
        ohlcv_db_path,
        rebalance_date=rebalance_date,
        lookback_days=lookback_days,
        min_recent_days=min_recent_days,
        exchange=exchange,
    )
    if eligible.empty:
        return eligible.assign(rank_by_turnover=[]), True

    sparse_history = len(eligible) < top_n
    members = eligible.head(top_n).copy()
    members["rank_by_turnover"] = range(1, len(members) + 1)
    members = members[["symbol_id", "rank_by_turnover", "median_turnover", "recent_days"]]
    return members, sparse_history


def compute_index_bar(
    ohlcv_db_path: Path | str,
    *,
    bar_date: date,
    constituents: list[str],
    previous_index_level: float,
    rebalance_date: date,
    min_used_ratio: float = DEFAULT_MIN_USED_RATIO,
    exchange: str = "NSE",
) -> tuple[float, IndexBarDiagnostics]:
    """Equal-weight composite of constituents' daily returns at ``bar_date``.

    ``daily_return = mean( close[bar_date] / close[prev_bar] - 1 )`` over
    constituents that have both prices present.

    If ``n_used / len(constituents) < min_used_ratio``, the bar holds at the
    previous level (no chain corruption) and ``quality_flag='low_coverage'``.
    If ``len(constituents) == 0``, ``quality_flag='sparse_membership'`` and the
    level holds.
    """
    n_members = len(constituents)

    if n_members == 0:
        return previous_index_level, IndexBarDiagnostics(
            index_code=UNIVERSE_INDEX_CODE,
            date=bar_date,
            rebalance_date=rebalance_date,
            n_members=0,
            n_used=0,
            n_missing=0,
            used_ratio=0.0,
            daily_return=0.0,
            index_level=previous_index_level,
            quality_flag="sparse_membership",
        )

    # Pull today's close + the most recent prior close strictly before bar_date
    # for each constituent. Single round-trip query.
    placeholders = ",".join("?" for _ in constituents)
    sql = f"""
        WITH today AS (
            SELECT symbol_id, close AS close_today
              FROM _catalog
             WHERE exchange = ?
               AND CAST(timestamp AS DATE) = ?
               AND symbol_id IN ({placeholders})
               AND close > 0
        ),
        prior AS (
            SELECT symbol_id,
                   FIRST(close ORDER BY timestamp DESC) AS close_prev
              FROM _catalog
             WHERE exchange = ?
               AND CAST(timestamp AS DATE) < ?
               AND symbol_id IN ({placeholders})
               AND close > 0
             GROUP BY symbol_id
        )
        SELECT today.symbol_id, today.close_today, prior.close_prev
          FROM today
          JOIN prior USING (symbol_id)
         WHERE prior.close_prev > 0
    """
    params: list[object] = [exchange, bar_date, *constituents, exchange, bar_date, *constituents]
    con = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        df = con.execute(sql, params).fetchdf()
    finally:
        con.close()

    n_used = len(df)
    n_missing = n_members - n_used
    used_ratio = n_used / n_members if n_members > 0 else 0.0

    if used_ratio < min_used_ratio:
        logger.warning(
            "universe_index low_coverage bar_date=%s used=%d/%d ratio=%.3f",
            bar_date, n_used, n_members, used_ratio,
        )
        return previous_index_level, IndexBarDiagnostics(
            index_code=UNIVERSE_INDEX_CODE,
            date=bar_date,
            rebalance_date=rebalance_date,
            n_members=n_members,
            n_used=n_used,
            n_missing=n_missing,
            used_ratio=used_ratio,
            daily_return=0.0,
            index_level=previous_index_level,
            quality_flag="low_coverage",
        )

    returns = df["close_today"] / df["close_prev"] - 1.0
    daily_return = float(returns.mean())
    new_level = previous_index_level * (1.0 + daily_return)

    return new_level, IndexBarDiagnostics(
        index_code=UNIVERSE_INDEX_CODE,
        date=bar_date,
        rebalance_date=rebalance_date,
        n_members=n_members,
        n_used=n_used,
        n_missing=n_missing,
        used_ratio=used_ratio,
        daily_return=daily_return,
        index_level=new_level,
        quality_flag="ok",
    )


def ensure_index_catalog_tables(ohlcv_db_path: Path | str) -> None:
    """Idempotently create the four tables the universe-index build needs:
    ``_index_catalog``, ``_index_metadata``, ``_universe_membership``, and
    ``_universe_index_diagnostics``. Also registers the UNIV_TOP1000 row in
    ``_index_metadata``.

    The first two mirror :func:`ai_trading_system.domains.ingest.trust.ensure_data_trust_schema`
    so the universe-index build does not depend on the ingest path having run.
    The latter two duplicate ``pipeline/migrations/016_universe_index.sql`` so
    the tool works when the research OHLCV DB is separate from the
    control-plane DB (the typical setup).
    """
    con = duckdb.connect(str(ohlcv_db_path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS _index_catalog (
                index_code VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE NOT NULL,
                volume BIGINT,
                value DOUBLE,
                provider VARCHAR DEFAULT 'nseindia',
                ingest_run_id VARCHAR,
                validated_at TIMESTAMP,
                PRIMARY KEY (index_code, date)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS _universe_membership (
                rebalance_date DATE NOT NULL,
                symbol_id TEXT NOT NULL,
                rank_by_turnover INTEGER NOT NULL,
                median_turnover DOUBLE,
                recent_days INTEGER,
                sparse_history BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                PRIMARY KEY (rebalance_date, symbol_id)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS _universe_index_diagnostics (
                index_code TEXT NOT NULL,
                date DATE NOT NULL,
                rebalance_date DATE NOT NULL,
                n_members INTEGER NOT NULL,
                n_used INTEGER NOT NULL,
                n_missing INTEGER NOT NULL,
                used_ratio DOUBLE,
                daily_return DOUBLE,
                index_level DOUBLE NOT NULL,
                quality_flag TEXT NOT NULL DEFAULT 'ok',
                created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                PRIMARY KEY (index_code, date)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS _index_metadata (
                index_code VARCHAR PRIMARY KEY,
                display_name VARCHAR NOT NULL,
                family VARCHAR,
                is_sectoral BOOLEAN DEFAULT FALSE,
                benchmark_for VARCHAR,
                source VARCHAR,
                is_benchmark BOOLEAN DEFAULT FALSE,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            """
            INSERT INTO _index_metadata
                (index_code, display_name, family, is_sectoral, benchmark_for,
                 source, is_benchmark, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (index_code) DO NOTHING
            """,
            [
                UNIVERSE_INDEX_CODE,
                "Top 1000 Liquid Equal-Weight (PIT)",
                "UNIVERSE",
                False,
                "optimizer,sector_rs,research_loader",
                "derived",
                True,
                True,
            ],
        )
    finally:
        con.close()


def upsert_membership(
    ohlcv_db_path: Path | str,
    *,
    rebalance_date: date,
    members_df: pd.DataFrame,
    sparse_history: bool,
) -> int:
    """Replace + insert membership for one rebalance_date. Returns rows written."""
    con = duckdb.connect(str(ohlcv_db_path))
    try:
        con.execute(
            "DELETE FROM _universe_membership WHERE rebalance_date = ?",
            [rebalance_date],
        )
        if members_df.empty:
            return 0
        frame = members_df.copy()
        frame["rebalance_date"] = rebalance_date
        frame["sparse_history"] = sparse_history
        frame = frame[
            [
                "rebalance_date",
                "symbol_id",
                "rank_by_turnover",
                "median_turnover",
                "recent_days",
                "sparse_history",
            ]
        ]
        con.register("incoming_membership", frame)
        con.execute(
            """
            INSERT INTO _universe_membership
                (rebalance_date, symbol_id, rank_by_turnover, median_turnover,
                 recent_days, sparse_history)
            SELECT rebalance_date, symbol_id, rank_by_turnover, median_turnover,
                   recent_days, sparse_history
              FROM incoming_membership
            """
        )
        con.unregister("incoming_membership")
        return int(len(frame))
    finally:
        con.close()


def upsert_index_bar(
    ohlcv_db_path: Path | str,
    *,
    diagnostics: IndexBarDiagnostics,
) -> None:
    """Write one bar to ``_index_catalog`` and one diagnostics row."""
    con = duckdb.connect(str(ohlcv_db_path))
    try:
        # _index_catalog row: open=high=low=close=index_level, volume=0.
        con.execute(
            "DELETE FROM _index_catalog WHERE index_code = ? AND date = ?",
            [diagnostics.index_code, diagnostics.date],
        )
        con.execute(
            """
            INSERT INTO _index_catalog
                (index_code, date, open, high, low, close, volume, value,
                 provider, ingest_run_id, validated_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, NULL, 'derived', NULL, NULL)
            """,
            [
                diagnostics.index_code,
                diagnostics.date,
                diagnostics.index_level,
                diagnostics.index_level,
                diagnostics.index_level,
                diagnostics.index_level,
            ],
        )
        # Diagnostics: one row per bar.
        con.execute(
            "DELETE FROM _universe_index_diagnostics WHERE index_code = ? AND date = ?",
            [diagnostics.index_code, diagnostics.date],
        )
        con.execute(
            """
            INSERT INTO _universe_index_diagnostics
                (index_code, date, rebalance_date, n_members, n_used, n_missing,
                 used_ratio, daily_return, index_level, quality_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                diagnostics.index_code,
                diagnostics.date,
                diagnostics.rebalance_date,
                diagnostics.n_members,
                diagnostics.n_used,
                diagnostics.n_missing,
                diagnostics.used_ratio,
                diagnostics.daily_return,
                diagnostics.index_level,
                diagnostics.quality_flag,
            ],
        )
    finally:
        con.close()


def latest_index_level(
    ohlcv_db_path: Path | str,
    *,
    index_code: str = UNIVERSE_INDEX_CODE,
) -> tuple[date, float] | None:
    """Return ``(latest_date, level)`` from ``_index_catalog`` or None."""
    con = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        row = con.execute(
            """
            SELECT date, close FROM _index_catalog
             WHERE index_code = ?
             ORDER BY date DESC
             LIMIT 1
            """,
            [index_code],
        ).fetchone()
    finally:
        con.close()
    return (row[0], float(row[1])) if row else None
