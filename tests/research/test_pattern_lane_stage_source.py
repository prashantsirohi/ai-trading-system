"""Tests for the governed weekly-stage source contract and backfill protocol."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.research.pattern_lane_calibration.stage_source import (
    BACKFILL_TABLE,
    annotate_stage_age,
    load_weekly_stage_observations,
    normalize_stage_label,
    normalize_stage_transition,
    reconcile_backfill_vs_live,
)
from ai_trading_system.research.pattern_lane_calibration.stage_backfill import (
    canonical_content_hash,
    stage_policy_hash,
    write_backfill,
)


def _make_control_plane(path: Path, *, live_rows: list[tuple], backfill_rows: list[tuple]) -> None:
    conn = duckdb.connect(str(path))
    # Mirrors the real governed schema: transition and confidence live inside
    # observation_json, not as top-level columns.
    conn.execute("""
        CREATE TABLE weekly_stock_stage_history (
            observation_id VARCHAR, symbol_id VARCHAR, exchange VARCHAR,
            as_of DATE, source_week_end DATE, stage_status VARCHAR,
            effective_stage VARCHAR, classifier_version VARCHAR,
            observation_json VARCHAR
        )
    """)
    import json as _json
    for row in live_rows:
        obs_json = _json.dumps({"stage_transition": row[7], "stage_confidence_score": row[8]})
        values = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[9], obs_json]
        conn.execute("INSERT INTO weekly_stock_stage_history VALUES (?,?,?,?,?,?,?,?,?)", values)
    conn.execute(f"""
        CREATE TABLE {BACKFILL_TABLE} (
            observation_id VARCHAR, symbol_id VARCHAR, exchange VARCHAR,
            week_end DATE, stage_policy_version VARCHAR, stage_label VARCHAR,
            stage_transition VARCHAR, stage_score DOUBLE, reason_codes VARCHAR,
            backfill_run_id VARCHAR, source_bar_max_date DATE, input_hash VARCHAR,
            classifier_version VARCHAR, created_at TIMESTAMP
        )
    """)
    for row in backfill_rows:
        conn.execute(
            f"INSERT INTO {BACKFILL_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", list(row)
        )
    conn.close()


def _make_snapshot(path: Path, rows: list[tuple]) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("""
        CREATE TABLE weekly_stage_snapshot (
            symbol VARCHAR, week_end_date DATE, stage_label VARCHAR,
            stage_transition VARCHAR, stage_confidence DOUBLE
        )
    """)
    for row in rows:
        conn.execute("INSERT INTO weekly_stage_snapshot VALUES (?,?,?,?,?)", list(row))
    conn.close()


def _bf_row(symbol: str, week: str, label: str, *, transition: str = "none",
            run: str = "run-1", score: float = 60.0, input_hash: str = "hash",
            policy: str = "weekly-stage-v2") -> tuple:
    return (f"bf-{symbol}-{week}", symbol, "NSE", week, policy, label, transition,
            score, "{}", run, week, input_hash, policy, "2026-07-18 00:00:00")


def test_label_and_transition_stay_separate() -> None:
    assert normalize_stage_label("stage_1_basing") == "S1"
    assert normalize_stage_label("stage_2_advancing") == "S2"
    # provisional transition label: the current stage is the target stage
    assert normalize_stage_label("transition_1_to_2") == "S2"
    assert normalize_stage_label("unknown") == "UNDEFINED"
    assert normalize_stage_transition("stage_1_basing_to_stage_2_advancing") == "S1_TO_S2"
    assert normalize_stage_transition("none") == "NONE"
    assert normalize_stage_transition(None, raw_label="transition_1_to_2") == "S1_TO_S2"
    # A current S2 with a recorded 1->2 transition keeps stage_label S2.
    assert normalize_stage_label("stage_2_advancing") == "S2"


def test_precedence_and_fallback(tmp_path: Path) -> None:
    control = tmp_path / "control_plane.duckdb"
    ohlcv = tmp_path / "ohlcv.duckdb"
    _make_control_plane(
        control,
        live_rows=[
            ("obs-p", "AAA", "NSE", "2026-07-14", "2026-07-10", "provisional", "stage_1_basing", "none", 80.0, "weekly-stage-v2"),
            ("obs-l", "AAA", "NSE", "2026-07-12", "2026-07-10", "locked", "stage_2_advancing", "stage_1_basing_to_stage_2_advancing", 82.0, "weekly-stage-v2"),
        ],
        backfill_rows=[
            _bf_row("AAA", "2026-07-03", "stage_1_basing"),
            _bf_row("AAA", "2026-07-10", "stage_4_declining"),
        ],
    )
    _make_snapshot(ohlcv, [
        ("AAA", "2026-07-03", "S3", "NONE", 0.5),
        ("BBB", "2026-07-03", "S1", "NONE", 0.7),
    ])

    frame = load_weekly_stage_observations(
        control_plane_db=control, ohlcv_db=ohlcv, through_date="2026-07-16"
    )
    aaa_live = frame.loc[(frame["symbol"] == "AAA") & (frame["week_end_date"] == "2026-07-10")]
    assert len(aaa_live) == 1
    row = aaa_live.iloc[0]
    assert row["stage_source"] == "governed_live"
    assert row["stage_label"] == "S2"                # current stage preserved
    assert row["stage_transition"] == "S1_TO_S2"     # transition separate
    assert pd.Timestamp(row["stage_transition_as_of"]) == pd.Timestamp("2026-07-10")
    assert not row["stage_source_fallback_used"]

    aaa_backfill = frame.loc[(frame["symbol"] == "AAA") & (frame["week_end_date"] == "2026-07-03")]
    assert aaa_backfill.iloc[0]["stage_source"] == "governed_backfill"
    assert aaa_backfill.iloc[0]["stage_label"] == "S1"

    bbb = frame.loc[frame["symbol"] == "BBB"]
    assert len(bbb) == 1
    assert bbb.iloc[0]["stage_source"] == "snapshot_fallback"
    assert bool(bbb.iloc[0]["stage_source_fallback_used"])

    no_fallback = load_weekly_stage_observations(
        control_plane_db=control, ohlcv_db=ohlcv, through_date="2026-07-16",
        allow_snapshot_fallback=False,
    )
    assert no_fallback.loc[no_fallback["symbol"] == "BBB"].empty


def test_frozen_backfill_mode_excludes_live_and_checks_policy(tmp_path: Path) -> None:
    control = tmp_path / "control_plane.duckdb"
    ohlcv = tmp_path / "ohlcv.duckdb"
    _make_control_plane(
        control,
        live_rows=[
            ("obs-l", "AAA", "NSE", "2026-07-12", "2026-07-10", "locked", "stage_2_advancing", "none", 82.0, "weekly-stage-v2"),
        ],
        backfill_rows=[
            _bf_row("AAA", "2026-07-10", "stage_4_declining"),
            _bf_row("BBB", "2026-07-10", "stage_1_basing", policy="weekly-stage-v1"),
        ],
    )
    _make_snapshot(ohlcv, [("CCC", "2026-07-10", "S1", "NONE", 0.7)])

    frozen = load_weekly_stage_observations(
        control_plane_db=control, ohlcv_db=ohlcv, through_date="2026-07-16",
        mode="frozen_backfill",
    )
    # backfill row wins over live in the analytical dataset; no snapshot rows
    assert frozen.loc[frozen["symbol"] == "AAA"].iloc[0]["stage_label"] == "S4"
    assert frozen.loc[frozen["symbol"] == "CCC"].empty
    assert set(frozen["stage_source"]) == {"governed_backfill"}

    with pytest.raises(RuntimeError, match="stage policy mismatch"):
        load_weekly_stage_observations(
            control_plane_db=control, ohlcv_db=ohlcv, through_date="2026-07-16",
            mode="frozen_backfill", require_stage_policy_version="weekly-stage-v2",
        )

    conflicts = reconcile_backfill_vs_live(control_plane_db=control, through_date="2026-07-16")
    assert len(conflicts) == 1
    assert conflicts.iloc[0]["backfill_stage"] == "S4"
    assert conflicts.iloc[0]["live_stage"] == "S2"


def test_write_backfill_noop_and_conflict(tmp_path: Path) -> None:
    control = tmp_path / "control_plane.duckdb"
    _make_control_plane(control, live_rows=[], backfill_rows=[
        _bf_row("AAA", "2026-07-10", "stage_2_advancing", input_hash="h1", score=70.0),
    ])
    report = tmp_path / "report"
    report.mkdir()
    observations = pd.DataFrame([
        {"observation_id": "o1", "symbol_id": "AAA", "exchange": "NSE",
         "week_end": "2026-07-10", "stage_policy_version": "weekly-stage-v2",
         "stage_label": "stage_2_advancing", "stage_transition": "none",
         "stage_score": 70.0, "reason_codes": "{}", "backfill_run_id": "run-2",
         "source_bar_max_date": "2026-07-10", "input_hash": "h1",
         "classifier_version": "weekly-stage-v2", "created_at": "2026-07-18 01:00:00"},
        {"observation_id": "o2", "symbol_id": "BBB", "exchange": "NSE",
         "week_end": "2026-07-10", "stage_policy_version": "weekly-stage-v2",
         "stage_label": "stage_1_basing", "stage_transition": "none",
         "stage_score": 60.0, "reason_codes": "{}", "backfill_run_id": "run-2",
         "source_bar_max_date": "2026-07-10", "input_hash": "h2",
         "classifier_version": "weekly-stage-v2", "created_at": "2026-07-18 01:00:00"},
    ])
    observations.to_csv(report / "backfill_observations.csv", index=False)
    for name in ("backfill_coverage_report.csv", "backfill_conflicts.csv"):
        (report / name).write_text("empty\n")
    obs_typed = observations.copy()
    obs_typed["week_end"] = pd.to_datetime(obs_typed["week_end"]).dt.date
    obs_typed["source_bar_max_date"] = pd.to_datetime(obs_typed["source_bar_max_date"]).dt.date
    import hashlib, json
    snapshot = tmp_path / "snapshot.duckdb"
    snapshot.write_bytes(b"frozen")
    manifest = {
        "backfill_run_id": "run-2",
        "canonical_content_hash": canonical_content_hash(obs_typed),
        "policy_hash": stage_policy_hash(),
        "snapshot": {
            "path": str(snapshot),
            "sha256": hashlib.sha256(b"frozen").hexdigest(),
        },
        "dataset_hashes": {
            name: hashlib.sha256((report / name).read_bytes()).hexdigest()
            for name in ("backfill_observations.csv", "backfill_coverage_report.csv", "backfill_conflicts.csv")
        },
    }
    (report / "backfill_manifest.json").write_text(json.dumps(manifest))

    # identical existing row -> no-op; new row -> inserted
    result = write_backfill(report_dir=report, control_plane_db=control)
    assert result["inserted"] == 1
    assert result["noop_identical"] == 1
    assert result["table_rows_after"] == 2

    # rerun is fully idempotent
    result2 = write_backfill(report_dir=report, control_plane_db=control)
    assert result2["inserted"] == 0
    assert result2["noop_identical"] == 2

    # different content for an existing grain -> conflict, abort, nothing written
    observations.loc[observations["symbol_id"] == "AAA", "input_hash"] = "h1-changed"
    observations.to_csv(report / "backfill_observations.csv", index=False)
    obs_typed2 = observations.copy()
    obs_typed2["week_end"] = pd.to_datetime(obs_typed2["week_end"]).dt.date
    obs_typed2["source_bar_max_date"] = pd.to_datetime(obs_typed2["source_bar_max_date"]).dt.date
    manifest["canonical_content_hash"] = canonical_content_hash(obs_typed2)
    manifest["dataset_hashes"]["backfill_observations.csv"] = hashlib.sha256(
        (report / "backfill_observations.csv").read_bytes()
    ).hexdigest()
    (report / "backfill_manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="differ in content"):
        write_backfill(report_dir=report, control_plane_db=control)


def test_annotate_stage_age(tmp_path: Path) -> None:
    observations = pd.DataFrame({
        "symbol": ["AAA", "AAA"],
        "week_end_date": pd.to_datetime(["2026-07-03", "2026-07-10"]),
        "stage_label": ["S1", "S2"],
    })
    sessions = pd.DatetimeIndex(pd.bdate_range("2026-06-29", "2026-07-16"))
    latest = annotate_stage_age(observations, as_of_date="2026-07-16", exchange_sessions=sessions)
    assert len(latest) == 1
    row = latest.iloc[0]
    assert row["stage_label"] == "S2"
    assert row["stage_age_trading_days"] == 4
