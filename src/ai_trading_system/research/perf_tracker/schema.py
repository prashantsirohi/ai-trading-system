"""DuckDB schema + connection helpers for the rank performance tracker."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from ai_trading_system.platform.db.paths import get_domain_paths


# Single source of truth for table DDL. Idempotent (CREATE IF NOT EXISTS) so
# safe to call on every connect — saves us a separate migration system at this
# scale.
RANK_COHORT_DDL = """
CREATE TABLE IF NOT EXISTS rank_cohort_performance (
    run_date                  DATE,
    symbol_id                 VARCHAR,
    exchange                  VARCHAR,
    rank_position             INTEGER,
    composite_score           DOUBLE,
    composite_score_adjusted  DOUBLE,
    rank_mode                 VARCHAR,
    watchlist_bucket          VARCHAR,
    config_id                 VARCHAR,           -- nullable until Phase 1 ships
    fwd_5d_return             DOUBLE,
    fwd_10d_return            DOUBLE,
    fwd_20d_return            DOUBLE,
    fwd_60d_return            DOUBLE,
    fwd_5d_matured_at         DATE,
    fwd_10d_matured_at        DATE,
    fwd_20d_matured_at        DATE,
    fwd_60d_matured_at        DATE,
    factor_rs                 DOUBLE,            -- factor scores for IC tracking
    factor_vol                DOUBLE,
    factor_trend              DOUBLE,
    factor_prox               DOUBLE,
    factor_deliv              DOUBLE,
    factor_sector             DOUBLE,
    factor_momentum_accel     DOUBLE,
    factor_above_200dma       DOUBLE,
    factor_liquidity          DOUBLE,
    factor_delivery_trend     DOUBLE,
    sector_name               VARCHAR,
    inserted_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_date, symbol_id, exchange)
);
"""

# Index on date alone speeds up the digest queries (most filters scan by date).
RANK_COHORT_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_rank_cohort_date ON rank_cohort_performance(run_date);
"""

# Additive migrations applied after table creation. Each entry is an idempotent
# DDL statement (ADD COLUMN IF NOT EXISTS). Append-only: existing rows get NULL
# for new columns, then get populated by the next ingest.
RANK_COHORT_ALTER_DDLS: tuple[str, ...] = (
    "ALTER TABLE rank_cohort_performance ADD COLUMN IF NOT EXISTS factor_above_200dma DOUBLE",
    "ALTER TABLE rank_cohort_performance ADD COLUMN IF NOT EXISTS factor_liquidity DOUBLE",
    "ALTER TABLE rank_cohort_performance ADD COLUMN IF NOT EXISTS factor_delivery_trend DOUBLE",
)


def research_db_path(project_root: str | Path | None = None) -> Path:
    """Return the path to data/research.duckdb (creating parent dir if needed)."""
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    db_path = paths.root_dir / "research.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create rank_cohort_performance + index if missing. Idempotent."""
    con.execute(RANK_COHORT_DDL)
    con.execute(RANK_COHORT_INDEX_DDL)
    for stmt in RANK_COHORT_ALTER_DDLS:
        con.execute(stmt)


@contextmanager
def open_research_db(
    *,
    project_root: str | Path | None = None,
    read_only: bool = False,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context-managed connection to the research DB with schema ensured.

    Use ``read_only=True`` when the caller only needs to query — it allows
    concurrent readers while a writer is active in another process.
    """
    path = research_db_path(project_root=project_root)
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        if not read_only:
            ensure_schema(con)
        yield con
    finally:
        con.close()
