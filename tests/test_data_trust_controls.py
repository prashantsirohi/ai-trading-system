from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from analytics.registry import RegistryStore
from core.contracts import StageArtifact, StageContext
from run.stages.execute import ExecuteStage
from run.stages.publish import PublishStage


def test_execute_stage_blocks_when_rank_payload_is_blocked(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-trust",
        run_date="2026-04-08",
        stage_name="execute",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )
    rank_dir = tmp_path / "data" / "operational" / "pipeline_runs" / context.run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)

    ranked_path = rank_dir / "ranked_signals.csv"
    pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "composite_score": 90.0}]).to_csv(ranked_path, index=False)
    dashboard_path = rank_dir / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps(
            {
                "summary": {
                    "data_trust_status": "blocked",
                    "run_date": context.run_date,
                }
            }
        ),
        encoding="utf-8",
    )
    context.artifacts = {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", ranked_path, row_count=1, attempt_number=1),
            "dashboard_payload": StageArtifact.from_file("dashboard_payload", dashboard_path, row_count=1, attempt_number=1),
        }
    }

    with pytest.raises(RuntimeError) as exc_info:
        ExecuteStage().run(context)

    assert "Execution blocked because rank data trust status is 'blocked'" in str(exc_info.value)


def test_execute_stage_blocks_degraded_trust_when_strict_mode_enabled(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-trust-strict",
        run_date="2026-04-08",
        stage_name="execute",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational", "block_degraded_execution": True},
    )
    rank_dir = tmp_path / "data" / "operational" / "pipeline_runs" / context.run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)

    ranked_path = rank_dir / "ranked_signals.csv"
    pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "composite_score": 90.0}]).to_csv(ranked_path, index=False)
    dashboard_path = rank_dir / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps(
            {
                "summary": {
                    "data_trust_status": "degraded",
                    "run_date": context.run_date,
                }
            }
        ),
        encoding="utf-8",
    )
    context.artifacts = {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", ranked_path, row_count=1, attempt_number=1),
            "dashboard_payload": StageArtifact.from_file("dashboard_payload", dashboard_path, row_count=1, attempt_number=1),
        }
    }

    with pytest.raises(RuntimeError) as exc_info:
        ExecuteStage().run(context)

    assert "Execution blocked because rank data trust status is 'degraded'" in str(exc_info.value)


def test_publish_tearsheet_includes_trust_banner(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-publish",
        run_date="2026-04-08",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )
    stage = PublishStage()
    message = stage._build_telegram_tearsheet(
        context,
        {
            "dashboard_payload": {
                "summary": {
                    "run_date": context.run_date,
                    "data_trust_status": "degraded",
                    "latest_trade_date": "2026-04-08",
                    "latest_validated_date": "2026-04-07",
                },
                "data_trust": {
                    "status": "degraded",
                    "latest_trade_date": "2026-04-08",
                    "latest_validated_date": "2026-04-07",
                    "active_quarantined_dates": ["2026-04-06"],
                    "fallback_ratio_latest": 0.12,
                },
            },
            "ranked_signals": pd.DataFrame([{"symbol_id": "AAA", "sector": "Tech", "composite_score": 90.0, "close": 100.0, "rel_strength_score": 88.0}]),
            "breakout_scan": pd.DataFrame(),
            "sector_dashboard": pd.DataFrame([{"Sector": "Tech", "RS": 0.9, "Momentum": 0.2, "Quadrant": "Leading", "RS_rank": 1}]),
        },
    )

    assert "Data trust: <b>degraded</b>" in message
    assert "Latest trade: <b>2026-04-08</b>" in message
    assert "Latest validated: <b>2026-04-07</b>" in message
    assert "Trust notes: Quarantined: 2026-04-06 | Fallback ratio: 12.0%" in message
