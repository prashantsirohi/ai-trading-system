from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import DEFAULT_CLI_STAGES, PIPELINE_ORDER, PipelineOrchestrator, build_parser
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.pipeline.stages.opportunities import OpportunityStage, OpportunityStageError


def _context(tmp_path: Path, *, mode: str, include_rank: bool = True) -> StageContext:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    db_path = tmp_path / "ohlcv.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE _catalog (exchange VARCHAR, timestamp TIMESTAMP)")
        conn.execute("INSERT INTO _catalog VALUES ('NSE', '2026-07-14 15:30:00')")
    artifacts: dict[str, dict[str, StageArtifact]] = {}
    if include_rank:
        path = tmp_path / "ranked_signals.csv"
        path.write_text("symbol_id,exchange,composite_score,sector_name\nABC,NSE,95,Capital Goods\n", encoding="utf-8")
        artifacts = {"rank": {"ranked_signals": StageArtifact.from_file("ranked_signals", path, row_count=1)}}
    return StageContext(
        project_root=tmp_path,
        db_path=db_path,
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
    assert parser.parse_args([]).fundamental_discovery_mode == "off"
    assert "opportunities" not in DEFAULT_CLI_STAGES.split(",")
    assert PIPELINE_ORDER.index("fundamental_discovery") == PIPELINE_ORDER.index("fundamentals") + 1
    assert PIPELINE_ORDER.index("opportunities") == PIPELINE_ORDER.index("fundamental_discovery") + 1
    orchestrator = PipelineOrchestrator(tmp_path, allow_control_plane_migrations=True)
    assert "opportunities" not in orchestrator._normalize_stage_names(None)
    enabled = orchestrator._normalize_stage_names(None, opportunity_registry_mode="shadow")
    assert enabled.index("opportunities") == enabled.index("investigator") + 1
    discovery = orchestrator._normalize_stage_names(None, fundamental_discovery_mode="compare")
    assert discovery.index("fundamental_discovery") == discovery.index("fundamentals") + 1
    both = orchestrator._normalize_stage_names(
        None,
        fundamental_discovery_mode="shadow",
        opportunity_registry_mode="shadow",
    )
    assert both.index("opportunities") == both.index("fundamental_discovery") + 1
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
            "SELECT setup_family, episode_type, opening_reason, "
            "satisfied_admission_rules_json, rule_evaluations_json "
            "FROM candidate_episode"
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
    assert episode == (
        "position_state_recovery",
        "position_state_recovery",
        "position_state_recovery",
        None,
        None,
    )
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


def test_shadow_routing_cannot_replace_authoritative_investigator_scores(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    context.params["opportunity_scan_routing_mode"] = "shadow"
    full = tmp_path / "investigator_scores.csv"
    full.write_text(
        "symbol_id,exchange,final_score,verdict,move_tag,trigger_reason,breakout_type,sector_name\n"
        "ABC,NSE,67,MEDIUM_CONVICTION,WEEKLY_MOMENTUM,WEEKLY_GAINER,NONE,Capital Goods\n",
        encoding="utf-8",
    )
    routed = tmp_path / "routed_investigator_scores.csv"
    routed.write_text(
        "symbol_id,exchange,final_score,verdict,move_tag,trigger_reason,sector_name\n"
        "ABC,NSE,42,WATCH_ONLY,RANDOM_NOISE,WEEKLY_GAINER,Capital Goods\n",
        encoding="utf-8",
    )
    context.artifacts["investigator"] = {
        "investigator_scores": StageArtifact.from_file(
            "investigator_scores", full, row_count=1
        ),
        "routed_investigator_scores": StageArtifact.from_file(
            "routed_investigator_scores", routed, row_count=1
        ),
    }

    result = OpportunityStage().run(context)

    assert result.metadata["primary_qualifying_observations"] == 1
    assert result.metadata["primary_observations_captured"] == 1
    assert result.metadata["investigator_source_fidelity_pct"] == 100
    fidelity_artifact = next(
        item
        for item in result.artifacts
        if item.artifact_type == "investigator_source_fidelity"
    )
    with Path(fidelity_artifact.uri).open(newline="", encoding="utf-8") as handle:
        fidelity = next(csv.DictReader(handle))
    assert fidelity["authoritative_artifact"] == "investigator_scores"
    assert fidelity["routed_sidecar_divergence_count"] == "1"


def test_primary_onset_appends_one_immutable_event_and_preserves_none(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    context.params["opportunity_registry_dry_run"] = False
    scores = tmp_path / "investigator_scores.csv"
    scores.write_text(
        "symbol_id,exchange,final_score,verdict,move_tag,trigger_reason,breakout_type,sector_name\n"
        "ABC,NSE,67,MEDIUM_CONVICTION,WEEKLY_MOMENTUM,WEEKLY_GAINER,NONE,Capital Goods\n",
        encoding="utf-8",
    )
    context.artifacts["investigator"] = {
        "investigator_scores": StageArtifact.from_file(
            "investigator_scores", scores, row_count=1
        )
    }
    with duckdb.connect(str(context.db_path)) as conn:
        conn.execute("ALTER TABLE _catalog ADD COLUMN symbol_id VARCHAR")
        conn.execute("ALTER TABLE _catalog ADD COLUMN open DOUBLE")
        conn.execute("ALTER TABLE _catalog ADD COLUMN high DOUBLE")
        conn.execute("ALTER TABLE _catalog ADD COLUMN low DOUBLE")
        conn.execute("ALTER TABLE _catalog ADD COLUMN close DOUBLE")
        conn.execute("ALTER TABLE _catalog ADD COLUMN is_benchmark BOOLEAN")
        conn.execute(
            "UPDATE _catalog SET symbol_id = 'ABC', open = 100, high = 102, low = 99, close = 101, is_benchmark = FALSE"
        )

    first = OpportunityStage().run(context)
    second = OpportunityStage().run(context)

    with context.registry._reader() as conn:  # noqa: SLF001
        episode = conn.execute(
            "SELECT setup_family, opening_reason FROM candidate_episode"
        ).fetchone()
        event = conn.execute(
            """
            SELECT COUNT(*), BOOL_AND(primary_eligible),
                   MIN(json_extract_string(context_json, '$.breakout_type'))
            FROM investigator_performance_event
            WHERE event_type = 'CANDIDATE_DISCOVERED'
            """
        ).fetchone()
    assert episode == ("investigator_primary", "investigator_primary_onset")
    assert event == (1, True, "NONE")
    assert first.metadata["primary_observations_captured"] == 1
    assert second.metadata["primary_observations_captured"] == 1


def test_primary_onset_uses_full_investigator_rank_context_when_rank_artifact_is_sparse(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path / "runtime"))
    context = _context(tmp_path, mode="shadow")
    scores = tmp_path / "investigator_scores.csv"
    scores.write_text(
        "symbol_id,exchange,composite_score,rank_position,final_score,verdict,move_tag,trigger_reason,breakout_type,sector_name\n"
        "DEF,NSE,72,30,66,MEDIUM_CONVICTION,WEEKLY_MOMENTUM,WEEKLY_GAINER,NONE,Technology\n",
        encoding="utf-8",
    )
    context.artifacts["investigator"] = {
        "investigator_scores": StageArtifact.from_file(
            "investigator_scores", scores, row_count=1
        )
    }

    result = OpportunityStage().run(context)

    assert result.metadata["primary_qualifying_observations"] == 1
    assert result.metadata["primary_observations_captured"] == 1
    admissions = next(
        item for item in result.artifacts if item.artifact_type == "candidate_admissions"
    )
    with Path(admissions.uri).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert any(
        row["symbol_id"] == "DEF"
        and row["reason"] == "investigator_primary_onset"
        for row in rows
    )
