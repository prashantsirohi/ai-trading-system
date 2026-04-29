"""Persistence for weekly stage snapshots.

Two sinks, kept in sync:
  1. DuckDB table `weekly_stage_snapshot` in `ohlcv.duckdb` (read by the ranker).
  2. Parquet artifact under `data/stage_store/weekly_stage_snapshots/`,
     partitioned by `week_end_date` (versioned, audit-friendly).

Idempotent on `(symbol, week_end_date)` — re-running a date overwrites
that week's rows for the listed symbols.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb
import pandas as pd

from .stage_classifier import StageResult


SCHEMA_COLUMNS: tuple[str, ...] = (
    "symbol",
    "week_end_date",
    "stage_label",
    "stage_confidence",
    "stage_transition",
    "bars_in_stage",
    "stage_entry_date",
    "ma10w",
    "ma30w",
    "ma40w",
    "ma30w_slope_4w",
    "weekly_rs_score",
    "weekly_volume_ratio",
    "support_level",
    "resistance_level",
    "created_at",
    "run_id",
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weekly_stage_snapshot (
    symbol               VARCHAR,
    week_end_date        DATE,
    stage_label          VARCHAR,
    stage_confidence     DOUBLE,
    stage_transition     VARCHAR,
    bars_in_stage        INTEGER,
    stage_entry_date     DATE,
    ma10w                DOUBLE,
    ma30w                DOUBLE,
    ma40w                DOUBLE,
    ma30w_slope_4w       DOUBLE,
    weekly_rs_score      DOUBLE,
    weekly_volume_ratio  DOUBLE,
    support_level        DOUBLE,
    resistance_level     DOUBLE,
    created_at           TIMESTAMP,
    run_id               VARCHAR,
    PRIMARY KEY (symbol, week_end_date)
)
"""


def results_to_frame(
    results: Iterable[StageResult],
    *,
    run_id: str,
    created_at: Optional[datetime] = None,
) -> pd.DataFrame:
    """Convert StageResult rows into a DataFrame matching the table schema."""
    ts = created_at or datetime.now(timezone.utc)
    rows = []
    for r in results:
        d = asdict(r)
        # asdict gives us a Timestamp; coerce to date for the schema.
        d["week_end_date"] = pd.Timestamp(d["week_end_date"]).date()
        if d.get("stage_entry_date") is not None and not pd.isna(d.get("stage_entry_date")):
            d["stage_entry_date"] = pd.Timestamp(d["stage_entry_date"]).date()
        else:
            d["stage_entry_date"] = None
        d["created_at"] = ts
        d["run_id"] = run_id
        rows.append(d)
    df = pd.DataFrame(rows, columns=list(SCHEMA_COLUMNS))
    return df


def write_snapshots(
    results: Sequence[StageResult],
    *,
    ohlcv_db_path: Path | str,
    parquet_root: Path | str,
    run_id: str,
    created_at: Optional[datetime] = None,
) -> dict:
    """Upsert snapshots into DuckDB and append a parquet partition.

    Returns a small summary dict with row counts and paths.
    """
    if not results:
        return {"rows": 0, "parquet_path": None, "duckdb_path": str(ohlcv_db_path)}

    frame = results_to_frame(results, run_id=run_id, created_at=created_at)

    # Drop rows we cannot key on (e.g. symbols with no history at all).
    frame = frame.dropna(subset=["symbol", "week_end_date"])
    if frame.empty:
        return {"rows": 0, "parquet_path": None, "duckdb_path": str(ohlcv_db_path)}

    parquet_path = _write_parquet(frame, parquet_root)
    _upsert_duckdb(frame, ohlcv_db_path)

    return {
        "rows": int(len(frame)),
        "parquet_path": str(parquet_path),
        "duckdb_path": str(ohlcv_db_path),
    }


def read_latest_snapshot(
    ohlcv_db_path: Path | str,
    *,
    symbols: Sequence[str] | None = None,
    asof: Optional[str] = None,
) -> pd.DataFrame:
    """Return the most recent stage row per symbol from DuckDB.

    If `asof` is provided, returns the last snapshot at or before that date.
    """
    db_path = Path(ohlcv_db_path)
    if not db_path.exists():
        return pd.DataFrame(columns=list(SCHEMA_COLUMNS))

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "weekly_stage_snapshot"):
            return pd.DataFrame(columns=list(SCHEMA_COLUMNS))

        clauses = []
        params: list[object] = []
        if asof:
            clauses.append("week_end_date <= CAST(? AS DATE)")
            params.append(asof)
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            clauses.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        sql = f"""
            SELECT * FROM weekly_stage_snapshot
            {where}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol ORDER BY week_end_date DESC
            ) = 1
        """
        return _ensure_snapshot_columns(conn.execute(sql, params).fetchdf())
    finally:
        conn.close()


def get_prior_stage(
    ohlcv_db_path: Path | str,
    *,
    symbol: str,
    before_date: str,
) -> Optional[str]:
    """Look up the previous stage label for a symbol before `before_date`."""
    db_path = Path(ohlcv_db_path)
    if not db_path.exists():
        return None
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "weekly_stage_snapshot"):
            return None
        row = conn.execute(
            """
            SELECT stage_label FROM weekly_stage_snapshot
            WHERE symbol = ? AND week_end_date < CAST(? AS DATE)
            ORDER BY week_end_date DESC LIMIT 1
            """,
            [symbol, before_date],
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def get_prior_stage_state(
    ohlcv_db_path: Path | str,
    *,
    symbol: str,
    before_date: str,
) -> dict[str, object] | None:
    """Look up the previous stage state used for stage age continuity."""
    db_path = Path(ohlcv_db_path)
    if not db_path.exists():
        return None
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "weekly_stage_snapshot"):
            return None
        columns = _table_columns(conn, "weekly_stage_snapshot")
        bars_expr = "bars_in_stage" if "bars_in_stage" in columns else "NULL AS bars_in_stage"
        entry_expr = "stage_entry_date" if "stage_entry_date" in columns else "NULL AS stage_entry_date"
        row = conn.execute(
            f"""
            SELECT stage_label, {bars_expr}, {entry_expr}
            FROM weekly_stage_snapshot
            WHERE symbol = ? AND week_end_date < CAST(? AS DATE)
            ORDER BY week_end_date DESC LIMIT 1
            """,
            [symbol, before_date],
        ).fetchone()
        if not row:
            return None
        return {
            "stage_label": row[0],
            "bars_in_stage": row[1],
            "stage_entry_date": row[2],
        }
    finally:
        conn.close()


# ---- internals ----

def _write_parquet(frame: pd.DataFrame, parquet_root: Path | str) -> Path:
    root = Path(parquet_root)
    root.mkdir(parents=True, exist_ok=True)
    # One file per (run_id, week_end_date) partition for easy versioning.
    week_dates = sorted(frame["week_end_date"].unique())
    written: list[Path] = []
    for w in week_dates:
        sub = frame[frame["week_end_date"] == w]
        run_id = str(sub["run_id"].iloc[0])
        part_dir = root / f"week_end_date={w.isoformat()}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out = part_dir / f"{run_id}.parquet"
        sub.to_parquet(out, index=False)
        written.append(out)
    return written[-1] if written else root


def _upsert_duckdb(frame: pd.DataFrame, db_path: Path | str) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(CREATE_TABLE_SQL)
        _migrate_weekly_stage_snapshot(conn)
        conn.register("staging_df", frame)
        # Replace rows on conflict by deleting existing keys then inserting.
        conn.execute(
            """
            DELETE FROM weekly_stage_snapshot AS t
            USING staging_df AS s
            WHERE t.symbol = s.symbol AND t.week_end_date = s.week_end_date
            """
        )
        conn.execute(
            f"""
            INSERT INTO weekly_stage_snapshot ({", ".join(SCHEMA_COLUMNS)})
            SELECT {", ".join(SCHEMA_COLUMNS)} FROM staging_df
            """
        )
    finally:
        conn.unregister("staging_df")
        conn.close()


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [name]
    ).fetchone() is not None


def _table_columns(conn: duckdb.DuckDBPyConnection, name: str) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [name],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _migrate_weekly_stage_snapshot(conn: duckdb.DuckDBPyConnection) -> None:
    columns = _table_columns(conn, "weekly_stage_snapshot")
    if "bars_in_stage" not in columns:
        conn.execute("ALTER TABLE weekly_stage_snapshot ADD COLUMN bars_in_stage INTEGER")
    if "stage_entry_date" not in columns:
        conn.execute("ALTER TABLE weekly_stage_snapshot ADD COLUMN stage_entry_date DATE")


def _ensure_snapshot_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in SCHEMA_COLUMNS:
        if column not in output.columns:
            output.loc[:, column] = pd.NA
    return output[list(SCHEMA_COLUMNS)] if not output.empty else pd.DataFrame(columns=list(SCHEMA_COLUMNS))
