from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import DEFAULT_CLI_STAGES, PIPELINE_ORDER, PipelineOrchestrator, build_parser
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.pipeline.stages.opportunities import OpportunityStage, OpportunityStageError


def _context(tmp_path: Path, *, mode: str, include_rank: bool = True) -> StageContext:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    artifacts: dict[str, dict[str, StageArtifact]] = {}
    if include_rank:
        path = tmp_path / "ranked_signals.csv"
        path.write_text("symbol_id,exchange,composite_score,sector_name\nABC,NSE,95,Capital Goods\n", encoding="utf-8")
        artifacts = {"rank": {"ranked_signals": StageArtifact.from_file("ranked_signals", path, row_count=1)}}
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="run-opportunities",
        run_date="2026-07-14",
        stage_name="opportunities",
        attempt_number=1,
        registry=registry,
        params={"opportunity_registry_mode": mode, "opportunity_registry_dry_run": True},
        artifacts=artifacts,
    )


def test_mode_off_is_a_clean_noop(tmp_path):
    result = OpportunityStage().run(_context(tmp_path, mode="off", include_rank=False))
    assert result.artifacts == []
    assert result.metadata["status"] == "skipped"


def test_shadow_missing_rank_raises_nonblocking_stage_error(tmp_path):
    with pytest.raises(OpportunityStageError):
        OpportunityStage().run(_context(tmp_path, mode="shadow", include_rank=False))


def test_shadow_dry_run_registerable_artifacts_and_no_registry_writes(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    result = OpportunityStage().run(context)
    assert {artifact.artifact_type for artifact in result.artifacts} >= {
        "opportunity_shadow_summary", "candidate_admissions", "candidate_reconciliation",
        "adapter_warnings", "registry_conflicts", "current_candidate_state",
    }
    assert result.metadata["no_database_writes_performed"] is True
    assert _opportunity_shadow_count(context.registry) == 0


def _opportunity_shadow_count(registry: RegistryStore) -> int:
    with registry._reader() as conn:  # noqa: SLF001
        return int(conn.execute("SELECT COUNT(*) FROM candidate_episode").fetchone()[0])


def test_pipeline_order_and_cli_defaults_are_feature_flagged(tmp_path):
    parser = build_parser()
    assert parser.parse_args([]).opportunity_registry_mode == "off"
    assert "opportunities" not in DEFAULT_CLI_STAGES.split(",")
    assert PIPELINE_ORDER.index("opportunities") == PIPELINE_ORDER.index("investigator") + 1
    orchestrator = PipelineOrchestrator(tmp_path)
    assert "opportunities" not in orchestrator._normalize_stage_names(None)
    enabled = orchestrator._normalize_stage_names(None, opportunity_registry_mode="shadow")
    assert enabled.index("opportunities") == enabled.index("investigator") + 1
