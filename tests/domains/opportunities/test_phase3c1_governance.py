from __future__ import annotations

import shutil
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateDecision,
    CandidateSnapshot,
    CandidateState,
    DecisionContextSnapshot,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    OutcomeAttribution,
    OutcomeAttributionRecord,
    ProgressStatus,
    RiskLevel,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.coverage import (
    build_sector_coverage,
    persist_stage_history,
    read_sector_stage_as_of,
    read_stock_stage_as_of,
)
from ai_trading_system.domains.opportunities.registry.models import (
    AttributionObservation,
    DecisionContextObservation,
    OpenEpisodeRequest,
    SnapshotObservation,
    SourceLineage,
    StageObservation,
    StageScope,
)
from ai_trading_system.domains.opportunities.registry.store import DuckDBOpportunityRegistryStore
from ai_trading_system.domains.opportunities.routing import StageCoverageConfig
from ai_trading_system.domains.opportunities.stage_governance import (
    MembershipTrust,
    SectorMembershipRecord,
    annotate_legacy_stage_history,
    append_sector_memberships,
    read_sector_membership_as_of,
    resolve_historical_sector_mapping,
)
from ai_trading_system.interfaces.cli.annotate_phase3c1_governance import annotate_copied_store
from ai_trading_system.pipeline.registry import RegistryStore


T1 = datetime(2026, 7, 17, 18, tzinfo=timezone.utc)
T2 = T1 + timedelta(days=2)
T3 = T2 + timedelta(days=2)


def _membership(
    *,
    sector: str = "tech",
    trust: MembershipTrust = MembershipTrust.POINT_IN_TIME_VERIFIED,
    start: date = date(2026, 7, 1),
    end: date = date(2026, 7, 31),
    recorded_at: datetime = T1,
    supersedes: str | None = None,
) -> SectorMembershipRecord:
    return SectorMembershipRecord(
        exchange="NSE", symbol_id="ABC", sector_id=sector,
        sector_name=sector.title(), valid_from=start, valid_to=end,
        membership_trust=trust, source_type="exchange_reference",
        source_hash=f"membership-{sector}-{recorded_at.isoformat()}", recorded_at=recorded_at,
        run_id="membership-run", stage_attempt=1,
        supersedes_membership_observation_id=supersedes,
    )


def _stock_row(
    *,
    stage: str = WeinsteinStage.STAGE_1.value,
    source_hash: str = "stock-v1",
    membership_id: str = "membership-v1",
    status: str = "locked",
) -> dict[str, object]:
    return {
        "exchange": "NSE", "symbol_id": "ABC", "sector_id": "tech", "sector_name": "Tech",
        "sector_membership_trust": MembershipTrust.POINT_IN_TIME_VERIFIED.value,
        "sector_membership_observation_id": membership_id,
        "as_of": "2026-07-17", "source_week_start": "2026-07-13",
        "source_week_end": "2026-07-17", "stage_status": status,
        "effective_stage": stage, "classifier_version": "weekly-stage-v1",
        "source_artifact_hash": source_hash, "price_vs_weekly_ma_30_pct": 2.0,
        "weekly_ma_30_slope": 0.2, "weekly_ma_30_slope_acceleration": 0.1,
        "weekly_rs_slope": 1.0,
    }


def test_effective_membership_boundaries_corrections_and_overlap_rejection(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    original = _membership()
    assert append_sector_memberships(registry, [original]) == {"created": 1, "duplicates": 0}
    assert append_sector_memberships(registry, [original]) == {"created": 0, "duplicates": 1}
    with pytest.raises(ValueError, match="overlapping sector membership"):
        append_sector_memberships(registry, [_membership(sector="banks", recorded_at=T2)])

    corrected = _membership(
        sector="technology", recorded_at=T2,
        supersedes=original.membership_observation_id,
    )
    append_sector_memberships(registry, [corrected])
    before = read_sector_membership_as_of(
        registry, effective_at="2026-07-01", available_at=T1, exchange="NSE"
    )
    after = read_sector_membership_as_of(
        registry, effective_at="2026-07-31", available_at=T3, exchange="NSE"
    )
    outside = read_sector_membership_as_of(
        registry, effective_at="2026-08-01", available_at=T3, exchange="NSE"
    )
    assert before.iloc[0]["sector_id"] == "tech"
    assert after.iloc[0]["sector_id"] == "technology"
    assert outside.empty


def test_latest_only_membership_is_quarantined_from_authoritative_reads_and_sector_aggregation(
    tmp_path: Path,
) -> None:
    rows = [
        _stock_row(),
        {**_stock_row(source_hash="stock-v2"), "symbol_id": "XYZ",
         "sector_membership_trust": MembershipTrust.LATEST_ONLY_BACKFILL.value},
    ]
    sector = build_sector_coverage(
        pd.DataFrame(rows), config=StageCoverageConfig(minimum_sector_constituents=1)
    )
    assert sector.iloc[0]["eligible_constituents"] == 1
    assert sector.iloc[0]["membership_trust"] == MembershipTrust.POINT_IN_TIME_VERIFIED.value
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    latest_only = pd.DataFrame([{
        **_stock_row(),
        "sector_membership_trust": MembershipTrust.LATEST_ONLY_BACKFILL.value,
    }])
    persist_stage_history(
        registry, latest_only, pd.DataFrame(), run_id="latest-only", attempt=1, recorded_at=T1
    )
    canonical_stock = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T2
    )
    assert canonical_stock.iloc[0]["symbol_id"] == "ABC"
    assert canonical_stock.iloc[0]["sector_membership_trust"] == MembershipTrust.LATEST_ONLY_BACKFILL.value


def test_historical_master_fallback_is_tagged_and_replay_safe(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    first = resolve_historical_sector_mapping(
        registry, {"ABC": ("tech", "Tech")}, exchange="NSE",
        effective_at="2026-07-10", available_at=T1,
        run_id="backfill", stage_attempt=1,
    )
    second = resolve_historical_sector_mapping(
        registry, {"ABC": ("tech", "Tech")}, exchange="NSE",
        effective_at="2026-07-10", available_at=T2,
        run_id="backfill-replay", stage_attempt=1,
    )
    assert first["ABC"][2] == MembershipTrust.LATEST_ONLY_BACKFILL.value
    assert second == first
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM sector_membership_history").fetchone()[0] == 1


def test_locked_correction_chain_and_late_availability_are_canonical(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    first = pd.DataFrame([_stock_row()])
    second = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_2.value, source_hash="stock-v2")])
    third = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-v3")])
    persist_stage_history(registry, first, pd.DataFrame(), run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, second, pd.DataFrame(), run_id="run-2", attempt=1, recorded_at=T2)
    persist_stage_history(registry, third, pd.DataFrame(), run_id="run-3", attempt=1, recorded_at=T3)

    at_t1 = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T1 + timedelta(seconds=1)
    )
    at_t2 = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T2 + timedelta(seconds=1)
    )
    at_t3 = read_stock_stage_as_of(
        registry, as_of="2026-07-17", available_at=T3 + timedelta(seconds=1)
    )
    assert at_t1.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_1.value
    assert at_t2.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_2.value
    assert at_t3.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_3.value
    with registry._reader() as conn:  # noqa: SLF001
        chain = conn.execute(
            """SELECT governance_action, COUNT(*) FROM stage_observation_governance
               WHERE observation_scope = 'STOCK' GROUP BY governance_action"""
        ).fetchall()
    assert dict(chain) == {"ORIGINAL": 1, "CORRECTION": 2}


def test_membership_change_recalculates_sector_and_records_dependencies(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    stock_v1 = pd.DataFrame([_stock_row(membership_id="membership-v1")])
    stock_v2 = pd.DataFrame([_stock_row(source_hash="stock-v2", membership_id="membership-v2")])
    sector_v1 = build_sector_coverage(stock_v1, config=StageCoverageConfig(minimum_sector_constituents=1))
    sector_v2 = build_sector_coverage(stock_v2, config=StageCoverageConfig(minimum_sector_constituents=1))
    assert sector_v1.iloc[0]["source_artifact_hash"] != sector_v2.iloc[0]["source_artifact_hash"]
    persist_stage_history(registry, stock_v1, sector_v1, run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, stock_v2, sector_v2, run_id="run-2", attempt=1, recorded_at=T2)
    resolved = read_sector_stage_as_of(
        registry, as_of="2026-07-17", available_at=T3
    )
    assert resolved.iloc[0]["constituent_membership_observation_ids"] == "membership-v2"
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute(
            "SELECT COUNT(*) FROM stage_observation_dependency WHERE dependency_type = 'SECTOR_MEMBERSHIP'"
        ).fetchone()[0] == 2


def test_correction_flags_candidate_snapshot_decision_and_attribution(
    tmp_path: Path,
    stage_factory,
    sector_factory,
) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    opportunity_store = DuckDBOpportunityRegistryStore(registry)
    lineage = SourceLineage("candidate-run", "opportunities", 1, "shadow", "/tmp/shadow.csv", "candidate-hash")
    episode = opportunity_store.open_episode(OpenEpisodeRequest(
        symbol_id="ABC", exchange="NSE", setup_family="base_building",
        admission_identity="candidate-run:ABC", episode_started_at=T1,
        episode_type="analytical_shadow", opening_reason="test", lineage=lineage,
        contract_version="opportunity-contract-v1",
    ))
    stock_stage = replace(
        stage_factory(), source_week_start=date(2026, 7, 13),
        source_week_end=date(2026, 7, 17), stage_as_of=T1, stage_locked_at=T1,
    )
    stock_result = opportunity_store.append_stage_observation(StageObservation(
        episode.candidate_id, episode.setup_id, StageScope.STOCK, episode.symbol_id,
        episode.symbol_id, stock_stage, T1, lineage,
    ))
    opportunity = OpportunitySnapshot(
        85, 2, 99, -3, ProgressStatus.IMPROVING,
        {"relative_strength": 90}, "rank-v1", T1,
    )
    evidence = EvidenceSnapshot(
        88, EvidenceVerdict.HIGH_CONVICTION, 80, 85, 90, 82, 76, 85, 80,
        RiskLevel.LOW, RiskLevel.LOW, ("volume expansion",), (), (),
        "investigator-v1", T1,
    )
    candidate_snapshot = CandidateSnapshot(
        candidate_id=episode.candidate_id, setup_id=episode.setup_id,
        symbol_id=episode.symbol_id, exchange=episode.exchange, as_of=T1,
        opportunity=opportunity, evidence=evidence, lifecycle_state=CandidateState.DISCOVERED,
        followthrough_status=FollowthroughStatus.NOT_APPLICABLE, stock_stage=stock_stage,
        sector_stage=sector_factory(stage=stock_stage), market_regime="bull", sector_regime="leading",
        days_in_state=1, days_without_progress=0, active_position=False,
        latest_action=CandidateAction.WATCH, eligibility=ActionEligibility.NOT_APPLICABLE,
    )
    opportunity_store.append_snapshot(SnapshotObservation(
        candidate_snapshot, T1, lineage,
        stock_stage_observation_id=stock_result.record_id,
    ))
    decision = CandidateDecision(
        episode.candidate_id, episode.setup_id, CandidateAction.WATCH,
        ActionEligibility.NOT_APPLICABLE, 80, 0, ("monitor",), (), (),
        "wait", "action-v1", T1,
    )
    context = DecisionContextSnapshot(
        decision_stage=WeinsteinStage.STAGE_2, decision_stage_status=StageStatus.LOCKED,
        decision_stage_as_of=T1, decision_locked_stage=WeinsteinStage.STAGE_2,
        decision_provisional_stage=WeinsteinStage.UNKNOWN, decision_stage_confidence=80,
        decision_sector_stage=WeinsteinStage.STAGE_2,
        decision_sector_stage_status=StageStatus.LOCKED, decision_sector_stage_confidence=80,
        opportunity_score=85, evidence_score=88, lifecycle_state=CandidateState.DISCOVERED,
        followthrough_status=FollowthroughStatus.NOT_APPLICABLE, market_regime="bull",
        sector_regime="leading", rank_model_version="rank-v1",
        evidence_model_version="investigator-v1", stage_classifier_version="weekly-stage-v1",
        action_policy_version="action-v1", execution_policy_version="execution-v1",
        portfolio_context_summary={"blocked": False},
    )
    opportunity_store.append_decision_context(DecisionContextObservation(decision, context, lineage))
    attribution = OutcomeAttributionRecord(
        episode.candidate_id, episode.setup_id,
        OutcomeAttribution.VALID_SIGNAL_NORMAL_FAILURE, None, 70,
        "attribution-v1", ("structure observed",), None, T2,
    )
    opportunity_store.append_attribution(AttributionObservation(attribution, lineage))

    first = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_2.value)])
    corrected = pd.DataFrame([_stock_row(stage=WeinsteinStage.STAGE_3.value, source_hash="stock-v2")])
    persist_stage_history(registry, first, pd.DataFrame(), run_id="run-1", attempt=1, recorded_at=T1)
    persist_stage_history(registry, corrected, pd.DataFrame(), run_id="run-2", attempt=1, recorded_at=T3)
    with registry._reader() as conn:  # noqa: SLF001
        types = {row[0] for row in conn.execute(
            "SELECT affected_record_type FROM stage_correction_impact"
        ).fetchall()}
    assert types == {
        "candidate_episode", "candidate_snapshot", "candidate_decision_context",
        "candidate_outcome_attribution",
    }


def test_copied_store_legacy_annotation_is_idempotent_and_payload_immutable(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    registry = RegistryStore(tmp_path, db_path=source)
    stock = pd.DataFrame([_stock_row()])
    persist_stage_history(registry, stock, pd.DataFrame(), run_id="legacy", attempt=1, recorded_at=T1)
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute("DELETE FROM stage_observation_governance")
        before = conn.execute(
            "SELECT observation_id, observation_json, source_artifact_hash FROM weekly_stock_stage_history"
        ).fetchone()
    copied = tmp_path / "copied-control-plane.duckdb"
    shutil.copy2(source, copied)

    preview = annotate_copied_store(
        copied, apply=False, confirmed_copied_store=False, run_id="phase3c1-test"
    )
    assert preview["annotations"]["total"] == 1
    applied = annotate_copied_store(
        copied, apply=True, confirmed_copied_store=True, run_id="phase3c1-test"
    )
    assert applied["applied"]["total"] == 1
    copied_registry = RegistryStore(tmp_path, db_path=copied)
    assert annotate_legacy_stage_history(
        copied_registry, run_id="phase3c1-test", recorded_at=T3, apply=True
    )["total"] == 0
    with copied_registry._reader() as conn:  # noqa: SLF001
        after = conn.execute(
            "SELECT observation_id, observation_json, source_artifact_hash FROM weekly_stock_stage_history"
        ).fetchone()
    assert after == before
