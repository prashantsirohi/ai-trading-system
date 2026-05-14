"""Baseline comparisons.

The *initial-pack baseline* is computed by simply running ``run_backtest`` with
the v1 YAML pack — no separate function needed here.

NIFTY buy-hold and equal-weight are reference returns over the same window,
used in reports (and in the worst-fold acceptance check vs NIFTY). The
top-winners oracle wraps the existing ``winner_capture`` module and is *report
only* — it must never enter fitness.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout


@dataclass(frozen=True)
class BenchmarkReturn:
    name: str
    start_date: date
    end_date: date
    start_price: float
    end_price: float
    total_return_pct: float


def benchmark_buyhold_return(
    project_root: Path | str,
    *,
    symbol: str,
    from_date: date,
    to_date: date,
    exchange: str = "NSE",
) -> BenchmarkReturn | None:
    """Close-to-close return for one symbol over the window. ``None`` if the
    symbol has no data in range.
    """
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    if not paths.ohlcv_db_path.exists():
        return None
    conn = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT
                MIN(CAST(timestamp AS DATE)) AS first_d,
                MAX(CAST(timestamp AS DATE)) AS last_d,
                FIRST(close ORDER BY timestamp ASC) AS first_close,
                LAST(close ORDER BY timestamp ASC) AS last_close
            FROM _catalog
            WHERE symbol_id = ? AND exchange = ?
              AND CAST(timestamp AS DATE) >= ?
              AND CAST(timestamp AS DATE) <= ?
              AND close IS NOT NULL
            """,
            [symbol, exchange, from_date, to_date],
        ).fetchone()
    finally:
        conn.close()
    if not row or row[0] is None or not row[2] or not row[3]:
        return None
    first_close = float(row[2])
    last_close = float(row[3])
    return BenchmarkReturn(
        name=symbol,
        start_date=row[0],
        end_date=row[1],
        start_price=first_close,
        end_price=last_close,
        total_return_pct=(last_close / first_close - 1.0) * 100.0,
    )
