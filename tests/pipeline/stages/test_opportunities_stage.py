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
    assert parser.parse_args([]).opportunity_scan_routing_mode == "off"
    assert "opportunities" not in DEFAULT_CLI_STAGES.split(",")
    assert PIPELINE_ORDER.index("opportunities") == PIPELINE_ORDER.index("investigator") + 1
    orchestrator = PipelineOrchestrator(tmp_path, allow_control_plane_migrations=True)
    assert "opportunities" not in orchestrator._normalize_stage_names(None)
    enabled = orchestrator._normalize_stage_names(None, opportunity_registry_mode="shadow")
    assert enabled.index("opportunities") == enabled.index("investigator") + 1
    routed = orchestrator._normalize_stage_names(None, opportunity_scan_routing_mode="compare")
    assert routed.index("weekly_stage") == routed.index("rank") + 1
    assert routed.index("scan_router") == routed.index("weekly_stage") + 1


def test_phase3b_recovers_position_only_episode_without_transition_history(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    routing = tmp_path / "scan_routing.csv"
    routing.write_text(
        "symbol_id,exchange,scan_tier,scan_reasons,active_position,recently_exited,position_cycle_opened_at\n"
        "ABC,NSE,position_monitor,['active_position'],true,false,2026-07-01T10:00:00+00:00\n",
        encoding="utf-8",
    )
    context.params.update({
        "opportunity_registry_dry_run": False,
        "opportunity_scan_routing_mode": "shadow",
        "recover_position_only_episodes": True,
    })
    context.artifacts["scan_router"] = {
        "scan_routing": StageArtifact.from_file("scan_routing", routing, row_count=1)
    }
    OpportunityStage().run(context)
    with context.registry._reader() as conn:  # noqa: SLF001
        episode = conn.execute(
            "SELECT setup_family, episode_type, opening_reason FROM candidate_episode"
        ).fetchone()
        snapshot = conn.execute(
            "SELECT lifecycle_state, active_position FROM candidate_snapshot"
        ).fetchone()
        transitions = conn.execute("SELECT COUNT(*) FROM candidate_transition").fetchone()[0]
        proposal = conn.execute(
            "SELECT recovery_mode, proposal_status FROM position_recovery_proposal"
        ).fetchone()
        action = conn.execute(
            "SELECT recovery_mode, payload_json FROM position_recovery_action"
        ).fetchone()
    assert episode == ("position_state_recovery", "position_state_recovery", "position_state_recovery")
    assert snapshot is None
    assert transitions == 0
    assert proposal == ("automatic", "PROPOSED")
    assert action[0] == "automatic"
    assert '"pre_entry_history_available": false' in action[1]


def test_phase3c3_report_only_creates_proposal_without_episode(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    routing = tmp_path / "scan_routing.csv"
    routing.write_text(
        "symbol_id,exchange,scan_tier,scan_reasons,active_position,recently_exited,position_cycle_opened_at,market_data_complete\n"
        "ABC,NSE,position_monitor,['active_position'],true,false,2026-07-01T10:00:00+00:00,true\n",
        encoding="utf-8",
    )
    context.params.update({
        "opportunity_registry_dry_run": False,
        "opportunity_scan_routing_mode": "shadow",
        "position_recovery_mode": "report_only",
    })
    context.artifacts["scan_router"] = {
        "scan_routing": StageArtifact.from_file("scan_routing", routing, row_count=1)
    }
    result = OpportunityStage().run(context)
    with context.registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM candidate_episode").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM position_recovery_proposal").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM position_recovery_action").fetchone()[0] == 0
    assert result.metadata["recovery_proposals"] == 1


def test_phase3b_sector_membership_comes_from_full_universe_stock_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    context.params["opportunity_scan_routing_mode"] = "shadow"
    stock = tmp_path / "weekly_stock_stage_universe.csv"
    stock.write_text(
        "symbol_id,exchange,sector_name,effective_stage,stage_status,stage_confidence_score,source_week_start,source_week_end,as_of\n"
        "ABC,NSE,Capital Goods,stage_1_basing,provisional,80,2026-07-13,2026-07-14,2026-07-14T00:00:00+00:00\n",
        encoding="utf-8",
    )
    sector = tmp_path / "weekly_sector_stage_universe.csv"
    sector.write_text(
        "sector_name,effective_stage,stage_status,stage_confidence_score,source_week_start,source_week_end,as_of\n"
        "Capital Goods,stage_1_basing,provisional,80,2026-07-13,2026-07-14,2026-07-14T00:00:00+00:00\n",
        encoding="utf-8",
    )
    context.artifacts["weekly_stage"] = {
        "weekly_stock_stage_universe": StageArtifact.from_file("weekly_stock_stage_universe", stock),
        "weekly_sector_stage_universe": StageArtifact.from_file("weekly_sector_stage_universe", sector),
    }
    result = OpportunityStage().run(context)
    assert result.metadata["unmatched_sector_mappings"] == 0
