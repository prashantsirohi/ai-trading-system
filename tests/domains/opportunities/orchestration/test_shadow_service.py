from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import pandas as pd

from ai_trading_system.domains.opportunities.contracts import CandidateState, StageStatus, WeinsteinStage
from ai_trading_system.domains.opportunities.coverage import build_sector_coverage, persist_stage_history
from ai_trading_system.domains.opportunities.orchestration.contracts import BreakoutEvidence, OpportunitySourceBundle
from ai_trading_system.domains.opportunities.orchestration.contracts import OpportunityRegistryMode, OpportunityShadowConfig
from ai_trading_system.domains.opportunities.orchestration.service import (
    OpportunityArtifactSet,
    OpportunityShadowOrchestrator,
    _attach_sector_gate_evidence,
)
from ai_trading_system.domains.opportunities.orchestration.transitions import evaluate_transition
from ai_trading_system.domains.opportunities.routing import StageCoverageConfig
from ai_trading_system.domains.opportunities.stage_governance import MembershipTrust
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.registry import RegistryStore


NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _artifact(tmp_path, name, content):
    path = tmp_path / f"{name}.csv"
    path.write_text(content, encoding="utf-8")
    return StageArtifact.from_file(name, path, attempt_number=1)


def _artifacts(tmp_path):
    return OpportunityArtifactSet(
        ranked_signals=_artifact(tmp_path, "ranked_signals", "symbol_id,exchange,composite_score,sector_name\nABC,NSE,95,Capital Goods\n"),
        investigator_scores=_artifact(tmp_path, "investigator_scores", "symbol_id,exchange,final_score,verdict,early_accumulation_score,pattern_score,extension_risk,failure_risk\nABC,NSE,90,HIGH_CONVICTION,85,90,low,low\n"),
        breakout_scan=_artifact(tmp_path, "breakout_scan", "symbol_id,exchange,breakout_state,candidate_tier,breakout_score,qualified\nABC,NSE,QUALIFIED,A,90,true\n"),
        pattern_scan=_artifact(tmp_path, "pattern_scan", "symbol_id,exchange,pattern_family,pattern_state,pattern_score,qualified\nABC,NSE,VCP,READY,90,true\n"),
        stock_scan=_artifact(tmp_path, "stock_scan", "symbol_id,exchange,weekly_stage_label,weekly_stage_confidence,week_end_date\nABC,NSE,S1_TO_S2,0.80,2026-07-14\n"),
        sector_dashboard=_artifact(tmp_path, "sector_dashboard", "Sector,sector_stage,sector_stage_confidence,sector_stage_status,week_end_date,created_at,RS_rank_pct,Quadrant\nCapital Goods,S2,0.85,locked,2026-07-10,2026-07-10T12:00:00+00:00,Improving,Leading\n"),
    )


def _momentum_artifacts(tmp_path):
    return OpportunityArtifactSet(
        ranked_signals=_artifact(
            tmp_path,
            "momentum_ranked_signals",
            "symbol_id,exchange,composite_score,sector_name\n"
            "ABC,NSE,95,Capital Goods\n",
        )
    )


def _breakout_artifacts(tmp_path):
    return replace(
        _momentum_artifacts(tmp_path),
        breakout_scan=_artifact(
            tmp_path,
            "superseding_breakout_scan",
            "symbol_id,exchange,breakout_state,candidate_tier,breakout_score,qualified\n"
            "ABC,NSE,QUALIFIED,A,90,true\n",
        ),
    )


def test_shadow_service_writes_and_replay_is_idempotent(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    first = service.run(run_id="run-1", stage_attempt=1, artifact_set=artifacts, as_of=NOW, mode=config.mode, config=config)
    second = service.run(run_id="run-1", stage_attempt=2, artifact_set=artifacts, as_of=NOW, mode=config.mode, config=config)
    assert first.summary["new_episodes_opened"] == 1
    assert first.summary["snapshots_created"] == 1
    admission = first.artifact_rows["candidate_admissions"][0]
    assert admission["primary_admission_reason"] == "qualified_breakout"
    assert admission["primary_setup_family"] == "breakout"
    assert "rank_threshold" in json.loads(admission["satisfied_admission_rules"])
    assert len(json.loads(admission["rule_evaluations"])) == 7
    episode = service.registry.list_open_episodes()[0]
    assert episode.satisfied_admission_rules_json == admission[
        "satisfied_admission_rules"
    ]
    assert episode.rule_evaluations_json == admission["rule_evaluations"]
    assert second.summary["registry_duplicates"] == 1
    assert len(service.registry.list_open_episodes()) == 1


def test_not_admitted_reconciliation_surfaces_rule_evaluations(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _momentum_artifacts(tmp_path)
    config = OpportunityShadowConfig(
        mode=OpportunityRegistryMode.SHADOW,
        rank_admission_percentile=101,
        rank_velocity_floor=-999,
    )
    result = service.run(
        run_id="blocked-run",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    row = result.artifact_rows["candidate_reconciliation"][0]
    assert row["outcome"] == "not_admitted"
    evaluations = json.loads(row["rule_evaluations"])
    assert len(evaluations) == 7
    assert not any(item["passed"] for item in evaluations)


def test_dry_run_writes_no_registry_records(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW, dry_run=True)
    result = service.run(run_id="run-dry", stage_attempt=1, artifact_set=_artifacts(tmp_path), as_of=NOW, mode=config.mode, config=config)
    assert result.summary["no_database_writes_performed"] is True
    assert service.registry.list_open_episodes() == ()


def test_momentum_breakout_supersession_is_atomic_and_replay_idempotent(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="momentum-run",
        stage_attempt=1,
        artifact_set=_momentum_artifacts(tmp_path),
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    momentum = service.registry.list_open_episodes()[0]
    breakout_at = NOW + timedelta(days=1)
    superseded = service.run(
        run_id="breakout-run",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=breakout_at,
        mode=config.mode,
        config=config,
    )
    assert superseded.summary["registry_conflicts"] == 0
    assert superseded.summary["episodes_superseded"] == 1
    assert len(superseded.artifact_rows["candidate_supersessions"]) == 1
    episodes = service.registry.list_candidate_episodes(exchange="NSE", symbol_id="ABC")
    assert len(episodes) == 2
    assert episodes[0].closing_reason == "superseded_by_new_episode"
    assert episodes[1].setup_family == "breakout"
    assert service.registry.list_episode_relations(momentum.candidate_id)[0].successor_candidate_id == episodes[1].candidate_id

    replay = service.run(
        run_id="breakout-run",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=breakout_at,
        mode=config.mode,
        config=config,
    )
    assert replay.summary["registry_duplicates"] == 1
    assert len(service.registry.list_episode_relations(momentum.candidate_id)) == 1
    assert len(service.registry.list_candidate_episodes(exchange="NSE", symbol_id="ABC")) == 2


def test_dry_run_reports_supersession_without_mutation(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    live = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="momentum-run",
        stage_attempt=1,
        artifact_set=_momentum_artifacts(tmp_path),
        as_of=NOW,
        mode=live.mode,
        config=live,
    )
    momentum = service.registry.list_open_episodes()[0]
    dry = replace(live, dry_run=True)
    result = service.run(
        run_id="breakout-dry-run",
        stage_attempt=1,
        artifact_set=_breakout_artifacts(tmp_path),
        as_of=NOW + timedelta(days=1),
        mode=dry.mode,
        config=dry,
    )
    assert result.summary["episodes_superseded"] == 1
    assert len(result.artifact_rows["candidate_supersessions"]) == 1
    assert service.registry.get_candidate_episode(momentum.candidate_id).episode_status.value == "OPEN"
    assert service.registry.list_episode_relations(momentum.candidate_id) == ()


def test_sector_gate_taxonomy_is_emitted_in_summary_and_update_artifact(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(
        run_id="gate-ready",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    blocked = service.run(
        run_id="gate-trigger",
        stage_attempt=1,
        artifact_set=artifacts,
        as_of=NOW,
        mode=config.mode,
        config=config,
    )
    assert blocked.summary["sector_gate_taxonomy_counts"] == {
        "latest_only_untrusted_membership": 1
    }
    update = blocked.artifact_rows["candidate_updates"][0]
    assert update["sector_gate_taxonomy"] == "latest_only_untrusted_membership"
    assert "latest_only_untrusted_membership" in update["transition_blockers"]


def test_changed_artifact_hash_in_same_run_is_not_misclassified_as_exact_replay(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    service.run(run_id="run-change", stage_attempt=1, artifact_set=artifacts, as_of=NOW, mode=config.mode, config=config)
    rank_path = tmp_path / "ranked_signals.csv"
    rank_path.write_text("symbol_id,exchange,composite_score,sector_name\nABC,NSE,96,Capital Goods\n", encoding="utf-8")
    changed = replace(
        artifacts,
        ranked_signals=StageArtifact.from_file("ranked_signals", rank_path, attempt_number=1),
    )
    result = service.run(run_id="run-change", stage_attempt=2, artifact_set=changed, as_of=NOW, mode=config.mode, config=config)
    assert result.summary["snapshots_created"] == 1
    assert result.summary["registry_duplicates"] == 0


def test_bulk_gate_evidence_makes_prior_locked_stage2_trigger_reachable(
    tmp_path, stage_factory, sector_factory
):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    prior_stock = pd.DataFrame([{
        "exchange": "NSE",
        "symbol_id": "ABC",
        "sector_id": "capital-goods",
        "sector_name": "Capital Goods",
        "sector_membership_trust": MembershipTrust.POINT_IN_TIME_VERIFIED.value,
        "sector_membership_observation_id": "membership-1",
        "as_of": "2026-07-10",
        "source_week_start": "2026-07-06",
        "source_week_end": "2026-07-10",
        "stage_status": "locked",
        "effective_stage": WeinsteinStage.STAGE_2.value,
        "classifier_version": "weekly-stage-v1",
        "source_artifact_hash": "prior-stock",
        "price_vs_weekly_ma_30_pct": 2.0,
        "weekly_ma_30_slope": 0.2,
        "weekly_ma_30_slope_acceleration": 0.1,
        "weekly_rs_slope": 1.0,
    }])
    prior_sector = build_sector_coverage(
        prior_stock, config=StageCoverageConfig(minimum_sector_constituents=1)
    )
    persist_stage_history(
        registry,
        prior_stock,
        prior_sector,
        run_id="prior-week",
        attempt=1,
        recorded_at=datetime(2026, 7, 10, 18, tzinfo=timezone.utc),
    )
    from tests.domains.opportunities.orchestration.test_policies import _bundle

    stock = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        confidence=90,
    )
    current_sector = sector_factory(
        stage=stage_factory(
            status=StageStatus.PROVISIONAL,
            provisional=WeinsteinStage.TRANSITION_1_TO_2,
        )
    )
    bundle: OpportunitySourceBundle = replace(
        _bundle(stage_factory, sector_factory, stock=stock),
        sector_stage=current_sector,
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
    )
    attached = _attach_sector_gate_evidence(
        registry,
        (bundle,),
        raw_stock=[{
            "exchange": "NSE",
            "symbol_id": "ABC",
            "sector_membership_trust": MembershipTrust.POINT_IN_TIME_VERIFIED.value,
        }],
        raw_sector=[{
            "sector_id": "capital-goods",
            "sector_name": "Capital Goods",
            "effective_stage": WeinsteinStage.TRANSITION_1_TO_2.value,
            "stage_breadth_velocity": 0.2,
        }],
        as_of=NOW,
    )[0]
    assert attached.sector_gate is not None
    assert attached.sector_gate.prior_locked_stage is WeinsteinStage.STAGE_2
    assert attached.sector_gate.current_provisional_stage is WeinsteinStage.TRANSITION_1_TO_2
    assert attached.sector_gate.taxonomy_cause is None
    assert evaluate_transition(CandidateState.READY, attached).allowed
