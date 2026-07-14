from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from ai_trading_system.domains.opportunities.orchestration.contracts import OpportunityRegistryMode, OpportunityShadowConfig
from ai_trading_system.domains.opportunities.orchestration.service import OpportunityArtifactSet, OpportunityShadowOrchestrator
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


def test_shadow_service_writes_and_replay_is_idempotent(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    artifacts = _artifacts(tmp_path)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW)
    first = service.run(run_id="run-1", stage_attempt=1, artifact_set=artifacts, as_of=NOW, mode=config.mode, config=config)
    second = service.run(run_id="run-1", stage_attempt=2, artifact_set=artifacts, as_of=NOW, mode=config.mode, config=config)
    assert first.summary["new_episodes_opened"] == 1
    assert first.summary["snapshots_created"] == 1
    assert second.summary["registry_duplicates"] == 1
    assert len(service.registry.list_open_episodes()) == 1


def test_dry_run_writes_no_registry_records(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    service = OpportunityShadowOrchestrator(registry)
    config = OpportunityShadowConfig(mode=OpportunityRegistryMode.SHADOW, dry_run=True)
    result = service.run(run_id="run-dry", stage_attempt=1, artifact_set=_artifacts(tmp_path), as_of=NOW, mode=config.mode, config=config)
    assert result.summary["no_database_writes_performed"] is True
    assert service.registry.list_open_episodes() == ()


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
