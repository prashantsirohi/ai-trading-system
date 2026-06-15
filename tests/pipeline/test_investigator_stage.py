from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import PIPELINE_ORDER
from ai_trading_system.pipeline.stages.investigator import InvestigatorStage


def _seed_ohlcv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                is_benchmark BOOLEAN,
                instrument_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE _delivery (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                delivery_pct DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('AAA', 'NSE', '2026-05-06', 98, 101, 97, 100, 1000, false, 'equity'),
            ('AAA', 'NSE', '2026-05-07', 101, 112, 100, 110, 3000, false, 'equity'),
            ('BBB', 'NSE', '2026-05-06', 100, 101, 99, 100, 1000, false, 'equity'),
            ('BBB', 'NSE', '2026-05-07', 100, 103, 99, 102, 3000, false, 'equity')
            """
        )
        conn.execute(
            """
            INSERT INTO _delivery VALUES
            ('AAA', 'NSE', '2026-05-07', 65),
            ('BBB', 'NSE', '2026-05-07', 30)
            """
        )
    finally:
        conn.close()


def _rank_artifacts(project_root: Path, run_id: str) -> dict[str, dict[str, StageArtifact]]:
    rank_dir = project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "symbol_id": "TOP",
                "composite_score": 90,
                "relative_strength": 70,
                "trend_persistence": 80,
                "volume_intensity": 90,
                "sector_strength": 65,
                "sector": "Finance",
                "market_cap_cr": 1000,
            },
            {"symbol_id": "BBB", "composite_score": 25, "sector": "IT", "market_cap_cr": 1000},
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "composite_score": 82,
                "rank": 42,
                "relative_strength": 70,
                "trend_persistence": 80,
                "volume_intensity": 90,
                "sector_strength": 65,
                "sector": "Finance",
                "market_cap_cr": 1000,
            },
            {"symbol_id": "BBB", "composite_score": 25, "rank": 1200, "sector": "IT", "market_cap_cr": 1000},
        ]
    ).to_csv(rank_dir / "stock_scan.csv", index=False)
    pd.DataFrame([{"symbol_id": "AAA", "breakout_positive": True, "qualified": True}]).to_csv(rank_dir / "breakout_scan.csv", index=False)
    pd.DataFrame([{"symbol_id": "AAA", "Sector": "Finance", "RS_rank_pct": 80, "Quadrant": "Leading"}]).to_csv(rank_dir / "sector_dashboard.csv", index=False)
    (rank_dir / "dashboard_payload.json").write_text(json.dumps({"summary": {"run_date": "2026-05-07"}}), encoding="utf-8")
    return {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", rank_dir / "ranked_signals.csv", row_count=2, attempt_number=1),
            "stock_scan": StageArtifact.from_file("stock_scan", rank_dir / "stock_scan.csv", row_count=2, attempt_number=1),
            "breakout_scan": StageArtifact.from_file("breakout_scan", rank_dir / "breakout_scan.csv", row_count=1, attempt_number=1),
            "sector_dashboard": StageArtifact.from_file("sector_dashboard", rank_dir / "sector_dashboard.csv", row_count=1, attempt_number=1),
            "dashboard_payload": StageArtifact.from_file("dashboard_payload", rank_dir / "dashboard_payload.json", row_count=1, attempt_number=1),
        }
    }


def test_investigator_stage_writes_artifacts_and_tables(tmp_path: Path) -> None:
    run_id = "pipeline-2026-05-07-test"
    _seed_ohlcv(tmp_path / "data" / "ohlcv.duckdb")
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=run_id,
        run_date="2026-05-07",
        stage_name="investigator",
        attempt_number=1,
        registry=registry,
        params={},
        artifacts=_rank_artifacts(tmp_path, run_id),
    )

    result = InvestigatorStage().run(context)

    output_dir = tmp_path / "data" / "pipeline_runs" / run_id / "investigator" / "attempt_1"
    assert (output_dir / "daily_gainer_log.csv").exists()
    assert (output_dir / "investigator_scores.csv").exists()
    assert (output_dir / "repeat_tracker.csv").exists()
    assert (output_dir / "active_watchlist.csv").exists()
    assert (output_dir / "trap_log.csv").exists()
    assert (output_dir / "archived_investigator.csv").exists()
    assert (output_dir / "final_3q_gate.csv").exists()
    assert (output_dir / "investigator_summary.json").exists()
    assert result.metadata["daily_gainer_count"] == 1
    assert {artifact.artifact_type for artifact in result.artifacts} >= {
        "daily_gainer_log",
        "investigator_scores",
        "investigator_summary",
    }
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM investigator_scores").fetchone()[0] == 1
        row = conn.execute("SELECT composite_score, rank_position FROM investigator_scores WHERE symbol_id = 'AAA'").fetchone()
        assert row == (82.0, 42.0)


def test_pipeline_order_places_investigator_after_rank() -> None:
    assert PIPELINE_ORDER.index("investigator") == PIPELINE_ORDER.index("rank") + 1
