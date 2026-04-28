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
    read_latest_snapshot,
    results_to_frame,
    write_snapshots,
)


def _mk(symbol: str, week: str, label: str = "S2", confidence: float = 0.8,
        transition: str = "NONE") -> StageResult:
    return StageResult(
        symbol=symbol,
        week_end_date=pd.Timestamp(week),
        stage_label=label,
        stage_confidence=confidence,
        stage_transition=transition,
        ma10w=100.0, ma30w=95.0, ma40w=92.0, ma30w_slope_4w=0.01,
        weekly_rs_score=70.0, weekly_volume_ratio=1.2,
        support_level=88.0, resistance_level=110.0,
    )


def test_results_to_frame_matches_schema():
    df = results_to_frame([_mk("A", "2024-04-26")], run_id="r1")
    assert list(df.columns) == list(SCHEMA_COLUMNS)
    assert df.iloc[0]["run_id"] == "r1"
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
    # No earlier rows for B
    assert get_prior_stage(db, symbol="B", before_date="2024-04-26") is None


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
