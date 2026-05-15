"""Baseline comparisons.

The *initial-pack baseline* is computed by simply running ``run_backtest`` with
the v1 YAML pack — no separate function needed here.

``benchmark_buyhold_return`` returns a passive buy-and-hold reference for the
optimizer's worst-fold acceptance check. It unifies two source tables:

- ``Benchmark(source="index_catalog")`` → ``_index_catalog`` (UNIV_TOP1000,
  NIFTY_50, sector indices, ...).
- ``Benchmark(source="catalog")`` → ``_catalog`` (legacy stock-as-benchmark).

If the symbol is not present, returns ``None`` — the acceptance gate then
silently skips the benchmark check rather than blocking on a missing series.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Union

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.optimization.recipe import Benchmark


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
    benchmark: Benchmark | None = None,
    symbol: str | None = None,
    from_date: date,
    to_date: date,
    exchange: str = "NSE",
) -> BenchmarkReturn | None:
    """Close-to-close return of the benchmark over the window.

    Accepts either a ``Benchmark`` config object (preferred) or a legacy
    ``symbol`` string (treated as ``Benchmark(symbol=symbol, source='catalog')``
    for backward compatibility with one prior release).
    """
    if benchmark is None:
        if symbol is None:
            raise TypeError("benchmark_buyhold_return requires benchmark= or symbol=")
        benchmark = Benchmark(symbol=symbol, source="catalog")

    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    if not paths.ohlcv_db_path.exists():
        return None

    conn = duckdb.connect(str(paths.ohlcv_db_path), read_only=True)
    try:
        if benchmark.source == "index_catalog":
            try:
                row = conn.execute(
                    """
                    SELECT
                        MIN(date) AS first_d,
                        MAX(date) AS last_d,
                        FIRST(close ORDER BY date ASC) AS first_close,
                        LAST(close ORDER BY date ASC) AS last_close
                      FROM _index_catalog
                     WHERE index_code = ?
                       AND date BETWEEN ? AND ?
                       AND close IS NOT NULL
                    """,
                    [benchmark.symbol, from_date, to_date],
                ).fetchone()
            except duckdb.CatalogException:
                # _index_catalog hasn't been created yet (no index ingest run).
                row = None
        else:
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
                [benchmark.symbol, exchange, from_date, to_date],
            ).fetchone()
    finally:
        conn.close()

    if not row or row[0] is None or not row[2] or not row[3]:
        return None

    first_close = float(row[2])
    last_close = float(row[3])
    return BenchmarkReturn(
        name=benchmark.symbol,
        start_date=row[0],
        end_date=row[1],
        start_price=first_close,
        end_price=last_close,
        total_return_pct=(last_close / first_close - 1.0) * 100.0,
    )
