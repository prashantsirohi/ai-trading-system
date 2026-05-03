from __future__ import annotations

import json
from pathlib import Path
import shutil

import duckdb
import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageArtifact, StageResult
from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator
from ai_trading_system.pipeline.stages import FeaturesStage, IngestStage, PublishStage, RankStage


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "artifacts"


def _seed_catalog(db_path: Path, run_date: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('AAA', 'NSE', ?, 100.0, 105.0, 99.0, 104.0, 1000),
            ('BBB', 'NSE', ?, 95.0, 99.0, 94.0, 98.0, 1500)
            """,
            [f"{run_date} 15:30:00", f"{run_date} 15:30:00"],
        )
    finally:
        conn.close()


def _rank_operation(_context) -> dict[str, object]:
    fixture_dir = FIXTURE_ROOT / "rank"
    return {
        "ranked_signals": pd.read_csv(fixture_dir / "ranked_signals.csv"),
        "breakout_scan": pd.read_csv(fixture_dir / "breakout_scan.csv"),
        "pattern_scan": pd.read_csv(fixture_dir / "pattern_scan.csv"),
        "stock_scan": pd.read_csv(fixture_dir / "stock_scan.csv"),
        "sector_dashboard": pd.read_csv(fixture_dir / "sector_dashboard.csv"),
        "__dashboard_payload__": json.loads((fixture_dir / "dashboard_payload.json").read_text(encoding="utf-8")),
    }


class _FixtureExecuteStage:
    name = "execute"

    def run(self, context):
        fixture_dir = FIXTURE_ROOT / "execute"
        output_dir = context.output_dir()
        summary_payload = json.loads((fixture_dir / "execute_summary.json").read_text(encoding="utf-8"))
        artifacts = []

        for artifact_type, filename in (
            ("trade_actions", "trade_actions.csv"),
            ("executed_orders", "executed_orders.csv"),
            ("executed_fills", "executed_fills.csv"),
            ("positions", "positions.csv"),
        ):
            source = fixture_dir / filename
            destination = output_dir / filename
            shutil.copy2(source, destination)
            frame = pd.read_csv(destination)
            artifacts.append(
                StageArtifact.from_file(
                    artifact_type,
                    destination,
                    row_count=len(frame),
                    metadata={"columns": list(frame.columns)},
                    attempt_number=context.attempt_number,
                )
            )

        summary_path = output_dir / "execute_summary.json"
        shutil.copy2(fixture_dir / "execute_summary.json", summary_path)
        artifacts.append(
            StageArtifact.from_file(
                "execute_summary",
                summary_path,
                row_count=int(summary_payload["summary"]["actions_count"]),
                metadata=summary_payload["summary"],
                attempt_number=context.attempt_number,
            )
        )
        return StageResult(artifacts=artifacts, metadata=summary_payload["summary"])


def test_orchestrator_smoke_runs_all_stages_and_registers_artifacts(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    publish_fixture = json.loads((FIXTURE_ROOT / "publish" / "publish_summary.json").read_text(encoding="utf-8"))

    orchestrator = PipelineOrchestrator(
        project_root=tmp_path,
        registry=registry,
        stages={
            "ingest": IngestStage(operation=lambda context: (_seed_catalog(context.db_path, context.run_date) or {})),
            "features": FeaturesStage(
                operation=lambda _context: {
                    "snapshot_id": 11,
                    "feature_rows": 2,
                    "feature_registry_entries": 1,
                }
            ),
            "rank": RankStage(operation=_rank_operation),
            "execute": _FixtureExecuteStage(),
            "publish": PublishStage(operation=lambda _context: publish_fixture),
        },
    )

    result = orchestrator.run_pipeline(
        run_date="2026-04-10",
        params={
            "preflight": False,
            "include_delivery": False,
            "data_domain": "operational",
        },
    )

    assert result["status"] == "completed"
    assert [row["stage_name"] for row in result["stages"]] == ["ingest", "features", "rank", "events", "execute", "insight", "publish"]
    assert all(row["status"] == "completed" for row in result["stages"])

    run_id = result["run_id"]
    rank_dir = tmp_path / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    execute_dir = tmp_path / "data" / "pipeline_runs" / run_id / "execute" / "attempt_1"
    publish_dir = tmp_path / "data" / "pipeline_runs" / run_id / "publish" / "attempt_1"

    assert (rank_dir / "ranked_signals.csv").exists()
    assert (rank_dir / "dashboard_payload.json").exists()
    assert (execute_dir / "execute_summary.json").exists()
    assert (publish_dir / "publish_summary.json").exists()

    artifact_rows = registry.get_artifact_map(run_id)
    assert "ranked_signals" in artifact_rows["rank"]
    assert "execute_summary" in artifact_rows["execute"]
    assert "publish_summary" in artifact_rows["publish"]
