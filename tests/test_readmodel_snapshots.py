from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.interfaces.api.services.readmodels.latest_operational_snapshot import (
    LatestOperationalSnapshot,
    load_latest_operational_snapshot,
)
from ai_trading_system.interfaces.api.services.readmodels.rank_snapshot import (
    _records,
    get_pipeline_workspace_snapshot_read_model,
    get_ranking_snapshot_read_model,
)


def _write_snapshot_artifacts(base: Path, run_id: str, *, score: float, smoke: bool) -> Path:
    attempt_dir = base / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    payload_path = attempt_dir / "dashboard_payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "summary": {"run_id": run_id, "smoke": smoke},
                "ranked_leaders": [{"symbol_id": "AAA", "rank": 1}],
                "pattern_discoveries": [{"symbol_id": "PATTERNX", "discovered_by_pattern_scan": True}],
                "breakout_candidates": [{"symbol_id": "BREAKOUTX", "breakout_positive": True}],
                "warnings": [],
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "composite_score": score,
                "stage2_score": 82.5,
                "is_stage2_uptrend": True,
                "stage2_label": "stage2_uptrend",
                "run_date": "2026-04-21",
            }
        ]
    ).to_csv(attempt_dir / "ranked_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "breakout_state": "qualified",
                "volume_zscore_20": 2.8,
                "volume_zscore_50": 1.7,
                "is_any_volume_confirmed": True,
            }
        ]
    ).to_csv(
        attempt_dir / "breakout_scan.csv", index=False
    )
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "pattern_state": "confirmed",
                "pattern_operational_tier": "tier_1",
                "pattern_priority_score": 92.0,
                "pattern_priority_rank": 1,
                "volume_zscore_20": 2.5,
                "volume_zscore_50": 1.8,
                "discovered_by_pattern_scan": False,
                "pattern_positive": True,
                "breakout_positive": True,
            },
            {
                "symbol_id": "PATTERNX",
                "pattern_state": "watchlist",
                "pattern_operational_tier": "tier_1",
                "pattern_priority_score": 89.0,
                "pattern_priority_rank": 2,
                "volume_zscore_20": 2.1,
                "volume_zscore_50": 1.4,
                "discovered_by_pattern_scan": True,
                "pattern_positive": True,
                "breakout_positive": False,
            }
        ]
    ).to_csv(
        attempt_dir / "pattern_scan.csv", index=False
    )
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "rank": 1, "composite_score": score, "pattern_positive": True, "breakout_positive": True, "discovered_by_pattern_scan": False},
            {"symbol_id": "PATTERNX", "rank": None, "composite_score": None, "pattern_positive": True, "breakout_positive": False, "discovered_by_pattern_scan": True},
            {"symbol_id": "BREAKOUTX", "rank": None, "composite_score": None, "pattern_positive": False, "breakout_positive": True, "discovered_by_pattern_scan": False},
        ]
    ).to_csv(
        attempt_dir / "stock_scan.csv", index=False
    )
    pd.DataFrame([{"Sector": "Tech", "RS": 0.8}]).to_csv(
        attempt_dir / "sector_dashboard.csv", index=False
    )
    return payload_path


def _seed_control_plane(base: Path, rows: list[tuple[str, dict]]) -> None:
    db_path = base / "data" / "control_plane.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE pipeline_run (
                run_id VARCHAR,
                pipeline_name VARCHAR,
                run_date DATE,
                status VARCHAR,
                current_stage VARCHAR,
                error_class VARCHAR,
                error_message VARCHAR,
                metadata_json VARCHAR
            )
            """
        )
        for run_id, metadata in rows:
            conn.execute(
                """
                INSERT INTO pipeline_run
                (run_id, pipeline_name, run_date, status, current_stage, error_class, error_message, metadata_json)
                VALUES (?, 'daily', DATE '2026-04-21', 'completed', 'rank', NULL, NULL, ?)
                """,
                [run_id, json.dumps(metadata)],
            )
    finally:
        conn.close()


def test_load_latest_operational_snapshot_prefers_live_payload(tmp_path: Path) -> None:
    smoke_run = "pipeline-2026-04-21-smoke1111"
    live_run = "pipeline-2026-04-21-live2222"
    _write_snapshot_artifacts(tmp_path, smoke_run, score=10.0, smoke=True)
    live_payload = _write_snapshot_artifacts(tmp_path, live_run, score=99.0, smoke=False)
    _seed_control_plane(
        tmp_path,
        [
            (smoke_run, {"params": {"smoke": True}}),
            (live_run, {"params": {}}),
        ],
    )

    snapshot = load_latest_operational_snapshot(tmp_path)

    assert isinstance(snapshot, LatestOperationalSnapshot)
    assert snapshot.payload_path == live_payload
    assert snapshot.payload["_artifact_path"] == str(live_payload)
    assert float(snapshot.frames["ranked_signals"].iloc[0]["composite_score"]) == 99.0
    assert snapshot.frames["breakout_scan"].iloc[0]["symbol_id"] == "AAA"


def test_records_serializes_datetime_columns() -> None:
    rows = _records(
        pd.DataFrame(
            {
                "symbol_id": ["AAA"],
                "prediction_date": [pd.Timestamp("2026-04-21")],
            }
        )
    )

    assert rows == [{"symbol_id": "AAA", "prediction_date": "2026-04-21"}]


def test_ranking_snapshot_readmodels_use_seeded_snapshot(tmp_path: Path, monkeypatch) -> None:
    live_run = "pipeline-2026-04-21-live3333"
    payload_path = _write_snapshot_artifacts(tmp_path, live_run, score=88.0, smoke=False)
    _seed_control_plane(tmp_path, [(live_run, {"params": {}})])
    monkeypatch.setattr(
        "ai_trading_system.interfaces.api.services.readmodels.rank_snapshot.get_execution_health",
        lambda *args, **kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        "ai_trading_system.interfaces.api.services.readmodels.rank_snapshot.get_execution_ops_health_snapshot",
        lambda *args, **kwargs: {"available": True},
    )
    monkeypatch.setattr(
        "ai_trading_system.interfaces.api.services.readmodels.rank_snapshot.get_execution_data_trust_snapshot",
        lambda *args, **kwargs: {"status": "ok"},
    )

    snapshot = load_latest_operational_snapshot(tmp_path)
    ranking = get_ranking_snapshot_read_model(tmp_path, snapshot=snapshot)
    workspace = get_pipeline_workspace_snapshot_read_model(tmp_path, snapshot=snapshot)
    ranking_stage2 = get_ranking_snapshot_read_model(tmp_path, snapshot=snapshot, stage2_only=True, stage2_min_score=80.0)

    assert ranking["artifact_count"] == 1
    assert ranking["top_ranked"][0]["symbol_id"] == "AAA"
    assert ranking["chart"][0]["composite_score"] == 88.0
    assert ranking["stage2_summary"]["available"] is True
    assert ranking["stage2_summary"]["counts_by_label"]["stage2_uptrend"] == 1
    assert ranking["stage2_filter"]["requested"] is False
    assert ranking_stage2["visible_count"] == 1
    assert ranking_stage2["stage2_filter"]["requested"] is True
    assert ranking_stage2["stage2_filter"]["gate_unavailable"] is False
    assert workspace["artifact_path"] == str(payload_path)
    assert workspace["counts"]["patterns"] == 2
    assert workspace["patterns"][0]["pattern_operational_tier"] == "tier_1"
    assert workspace["patterns"][0]["pattern_priority_rank"] == 1
    assert workspace["patterns"][0]["volume_zscore_20"] == 2.5
    assert workspace["ranked_leaders"][0]["symbol_id"] == "AAA"
    assert workspace["pattern_discoveries"][0]["symbol_id"] == "PATTERNX"
    assert workspace["breakout_candidates"][0]["symbol_id"] == "BREAKOUTX"
    assert workspace["visible_counts"]["ranked"] == 1
    assert workspace["stage2_summary"]["uptrend_count"] == 1
    assert workspace["breakouts"][0]["symbol_id"] == "AAA"
    assert workspace["breakouts"][0]["volume_zscore_20"] == 2.8
