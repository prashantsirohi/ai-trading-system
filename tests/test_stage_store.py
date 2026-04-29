"""Tests for stage_store: DuckDB upsert + parquet sink + readers."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.ranking.stage_classifier import StageResult
from ai_trading_system.domains.ranking.stage_store import (
    SCHEMA_COLUMNS,
    get_prior_stage,
    get_prior_stage_state,
    read_latest_snapshot,
    results_to_frame,
    write_snapshots,
)


def _mk(symbol: str, week: str, label: str = "S2", confidence: float = 0.8,
        transition: str = "NONE", bars_in_stage: int = 1,
        stage_entry_date: str | None = None) -> StageResult:
    return StageResult(
        symbol=symbol,
        week_end_date=pd.Timestamp(week),
        stage_label=label,
        stage_confidence=confidence,
        stage_transition=transition,
        ma10w=100.0, ma30w=95.0, ma40w=92.0, ma30w_slope_4w=0.01,
        weekly_rs_score=70.0, weekly_volume_ratio=1.2,
        support_level=88.0, resistance_level=110.0,
        bars_in_stage=bars_in_stage,
        stage_entry_date=pd.Timestamp(stage_entry_date or week),
    )


def test_results_to_frame_matches_schema():
    df = results_to_frame([_mk("A", "2024-04-26")], run_id="r1")
    assert list(df.columns) == list(SCHEMA_COLUMNS)
    assert df.iloc[0]["run_id"] == "r1"
    assert df.iloc[0]["bars_in_stage"] == 1
    assert df.iloc[0]["stage_entry_date"] == pd.Timestamp("2024-04-26").date()
    assert isinstance(df.iloc[0]["created_at"], datetime)


def test_write_and_read_roundtrip(tmp_path: Path):
    db = tmp_path / "ohlcv.duckdb"
    parquet_root = tmp_path / "stage_store" / "weekly"

    summary = write_snapshots(
        [_mk("A", "2024-04-26", "S2"), _mk("B", "2024-04-26", "S4")],
        ohlcv_db_path=db,
        parquet_root=parquet_root,
        run_id="run-1",
        created_at=datetime(2024, 4, 27, tzinfo=timezone.utc),
    )
    assert summary["rows"] == 2
    assert (parquet_root / "week_end_date=2024-04-26" / "run-1.parquet").exists()

    snap = read_latest_snapshot(db)
    assert set(snap["symbol"]) == {"A", "B"}
    assert snap.set_index("symbol").loc["A", "stage_label"] == "S2"
    assert "bars_in_stage" in snap.columns
    assert "stage_entry_date" in snap.columns


def test_upsert_overwrites_same_key(tmp_path: Path):
    db = tmp_path / "ohlcv.duckdb"
    parquet_root = tmp_path / "stage_store"

    write_snapshots([_mk("A", "2024-04-26", "S1", 0.5)],
                    ohlcv_db_path=db, parquet_root=parquet_root, run_id="r1")
    write_snapshots([_mk("A", "2024-04-26", "S2", 0.9)],
                    ohlcv_db_path=db, parquet_root=parquet_root, run_id="r2")

    snap = read_latest_snapshot(db)
    assert len(snap) == 1
    assert snap.iloc[0]["stage_label"] == "S2"
    assert snap.iloc[0]["run_id"] == "r2"


def test_get_prior_stage_returns_previous_week(tmp_path: Path):
    db = tmp_path / "ohlcv.duckdb"
    parquet_root = tmp_path / "stage_store"

    write_snapshots(
        [_mk("A", "2024-04-19", "S1"), _mk("A", "2024-04-26", "S2", transition="S1_TO_S2")],
        ohlcv_db_path=db, parquet_root=parquet_root, run_id="r1",
    )
    assert get_prior_stage(db, symbol="A", before_date="2024-04-26") == "S1"
    state = get_prior_stage_state(db, symbol="A", before_date="2024-04-26")
    assert state is not None
    assert state["stage_label"] == "S1"
    assert state["bars_in_stage"] == 1
    # No earlier rows for B
    assert get_prior_stage(db, symbol="B", before_date="2024-04-26") is None
    assert get_prior_stage_state(db, symbol="B", before_date="2024-04-26") is None


def test_read_latest_snapshot_handles_missing_db(tmp_path: Path):
    snap = read_latest_snapshot(tmp_path / "missing.duckdb")
    assert snap.empty
    assert list(snap.columns) == list(SCHEMA_COLUMNS)


def test_write_snapshots_empty_input(tmp_path: Path):
    summary = write_snapshots(
        [], ohlcv_db_path=tmp_path / "x.duckdb",
        parquet_root=tmp_path / "p", run_id="r",
    )
    assert summary["rows"] == 0


def test_read_latest_filters_by_asof(tmp_path: Path):
    db = tmp_path / "ohlcv.duckdb"
    pr = tmp_path / "p"
    write_snapshots(
        [_mk("A", "2024-04-19", "S1"), _mk("A", "2024-04-26", "S2")],
        ohlcv_db_path=db, parquet_root=pr, run_id="r1",
    )
    snap = read_latest_snapshot(db, asof="2024-04-19")
    assert len(snap) == 1
    assert snap.iloc[0]["stage_label"] == "S1"


def test_old_weekly_stage_table_is_migrated_on_write(tmp_path: Path):
    db = tmp_path / "ohlcv.duckdb"
    pr = tmp_path / "p"
    conn = duckdb.connect(str(db))
    try:
        conn.execute(
            """
            CREATE TABLE weekly_stage_snapshot (
                symbol VARCHAR,
                week_end_date DATE,
                stage_label VARCHAR,
                stage_confidence DOUBLE,
                stage_transition VARCHAR,
                ma10w DOUBLE,
                ma30w DOUBLE,
                ma40w DOUBLE,
                ma30w_slope_4w DOUBLE,
                weekly_rs_score DOUBLE,
                weekly_volume_ratio DOUBLE,
                support_level DOUBLE,
                resistance_level DOUBLE,
                created_at TIMESTAMP,
                run_id VARCHAR,
                PRIMARY KEY (symbol, week_end_date)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO weekly_stage_snapshot VALUES
            ('OLD', DATE '2024-04-19', 'S1', 0.7, 'NONE', 100, 95, 90, 0.01, 70, 1.2, 88, 110, TIMESTAMP '2024-04-20', 'old')
            """
        )
    finally:
        conn.close()

    old_snap = read_latest_snapshot(db)
    assert "bars_in_stage" in old_snap.columns
    assert pd.isna(old_snap.iloc[0]["bars_in_stage"])

    write_snapshots([_mk("NEW", "2024-04-26", "S2", bars_in_stage=3)], ohlcv_db_path=db, parquet_root=pr, run_id="new")
    snap = read_latest_snapshot(db)

    assert {"OLD", "NEW"} == set(snap["symbol"])
    assert int(snap.set_index("symbol").loc["NEW", "bars_in_stage"]) == 3
