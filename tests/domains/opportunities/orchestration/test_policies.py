from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.opportunities.orchestration import (
    contracts as admission_contracts,
)
from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    ProgressStatus,
    RiskLevel,
    StageStatus,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.orchestration.admission import (
    evaluate_admission,
    evaluate_all_admission_rules,
    rule_evaluations_json,
)
from ai_trading_system.domains.opportunities.registry.identity import stable_digest
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    ADMISSION_IDENTITY_RULE_VERSION,
    ADMISSION_RULE_PRECEDENCE,
    AdmissionReason,
    OpportunityShadowConfig,
    OpportunitySourceBundle,
    SectorGateEvidence,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import BreakoutEvidence
from ai_trading_system.domains.opportunities.orchestration.contracts import PatternEvidence
from ai_trading_system.domains.opportunities.orchestration.retention import (
    advance_session_counters,
    evaluate_retention,
)
from ai_trading_system.domains.opportunities.orchestration.service import (
    _resolve_observed_session,
)
from ai_trading_system.domains.opportunities.orchestration.transitions import evaluate_transition


NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _bundle(stage_factory, sector_factory, *, stock=None, evidence=90, lifecycle=None, followthrough=FollowthroughStatus.UNKNOWN):
    stock = stock or stage_factory(status=StageStatus.LOCKED, locked=WeinsteinStage.STAGE_2)
    return OpportunitySourceBundle(
        "ABC", "NSE", NOW,
        OpportunitySnapshot(90, 1, 99, None, ProgressStatus.UNKNOWN, {}, "rank-v1", NOW),
        EvidenceSnapshot(evidence, EvidenceVerdict.HIGH_CONVICTION, 80, 90, 90, None, None, 90, None, RiskLevel.LOW, RiskLevel.LOW, (), (), (), "inv-v1", NOW),
        stock,
        sector_factory(),
        lifecycle,
        followthrough,
        sector_name="Capital Goods",
        market_regime="bull",
        sector_regime="leading",
    )


def test_rank_admission_and_stage3_block(stage_factory, sector_factory):
    bundle = _bundle(stage_factory, sector_factory)
    result = evaluate_admission(bundle, OpportunityShadowConfig())
    assert result.admitted
    stage3 = replace(bundle, stock_stage=stage_factory(locked=WeinsteinStage.STAGE_3))
    blocked = evaluate_admission(stage3, OpportunityShadowConfig())
    assert not blocked.admitted
    assert blocked.rule_evaluations == ()
    assert blocked.blockers == ("stage_3_or_4_blocks_new_long_admission",)


def test_evaluate_all_records_every_rule_and_preserves_primary_precedence(
    stage_factory, sector_factory
):
    bundle = _bundle(stage_factory, sector_factory)
    evaluations = evaluate_all_admission_rules(bundle, OpportunityShadowConfig())
    result = evaluate_admission(bundle, OpportunityShadowConfig())
    assert tuple(item.rule.value for item in evaluations) == ADMISSION_RULE_PRECEDENCE
    assert result.satisfied_rules == (
        AdmissionReason.EARLY_ACCUMULATION,
        AdmissionReason.INVESTIGATOR_PROMOTION,
        AdmissionReason.RANK_THRESHOLD,
    )
    assert result.reason is AdmissionReason.EARLY_ACCUMULATION
    assert len(result.rule_evaluations) == 7


def test_breakout_and_rank_velocity_keep_frozen_primary_order(
    stage_factory, sector_factory
):
    base = _bundle(stage_factory, sector_factory)
    breakout = replace(
        base,
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
    )
    assert evaluate_admission(
        breakout, OpportunityShadowConfig()
    ).reason is AdmissionReason.QUALIFIED_BREAKOUT
    velocity = replace(
        base,
        evidence=None,
        opportunity=replace(base.opportunity, rank_velocity=-6),
    )
    velocity_result = evaluate_admission(velocity, OpportunityShadowConfig())
    assert velocity_result.satisfied_rules == (
        AdmissionReason.RANK_VELOCITY,
        AdmissionReason.RANK_THRESHOLD,
    )
    assert velocity_result.reason is AdmissionReason.RANK_VELOCITY


def test_runtime_precedence_object_controls_executed_selection(
    stage_factory, sector_factory, monkeypatch
):
    bundle = replace(
        _bundle(stage_factory, sector_factory),
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
    )
    monkeypatch.setattr(
        admission_contracts,
        "ADMISSION_RULE_PRECEDENCE",
        tuple(reversed(ADMISSION_RULE_PRECEDENCE)),
    )
    assert evaluate_admission(
        bundle, OpportunityShadowConfig()
    ).reason is AdmissionReason.RANK_THRESHOLD


@pytest.mark.parametrize(
    ("case", "reason", "family"),
    [
        ("breakout", AdmissionReason.QUALIFIED_BREAKOUT, "breakout"),
        ("stage", AdmissionReason.STAGE_TRANSITION, "stage_1_to_2_transition"),
        ("accumulation", AdmissionReason.EARLY_ACCUMULATION, "early_accumulation"),
        ("investigator", AdmissionReason.INVESTIGATOR_PROMOTION, "base_building"),
        ("pattern", AdmissionReason.QUALIFIED_PATTERN, "base_building"),
        ("velocity", AdmissionReason.RANK_VELOCITY, "momentum_leader"),
        ("rank", AdmissionReason.RANK_THRESHOLD, "momentum_leader"),
    ],
)
def test_each_admission_rule_preserves_primary_reason_and_family(
    stage_factory, sector_factory, case, reason, family
):
    original = _bundle(stage_factory, sector_factory)
    bundle = replace(
        original,
        opportunity=None,
        evidence=None,
        stock_stage=None,
        lifecycle_hint=None,
        breakout_events=(),
        pattern_events=(),
    )
    if case == "breakout":
        bundle = replace(
            bundle,
            breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        )
    elif case == "stage":
        bundle = replace(
            bundle,
            stock_stage=stage_factory(
                status=StageStatus.PROVISIONAL,
                provisional=WeinsteinStage.TRANSITION_1_TO_2,
                confidence=90,
            ),
        )
    elif case == "accumulation":
        bundle = replace(bundle, lifecycle_hint=CandidateState.EARLY_ACCUMULATION)
    elif case == "investigator":
        bundle = replace(
            bundle, evidence=replace(original.evidence, accumulation_score=0)
        )
    elif case == "pattern":
        bundle = replace(
            bundle,
            pattern_events=(PatternEvidence("VCP", "ready", 90, 90, True, False),),
        )
    elif case == "velocity":
        bundle = replace(
            bundle,
            opportunity=replace(
                original.opportunity, rank_percentile=80, rank_velocity=-6
            ),
        )
    elif case == "rank":
        bundle = replace(
            bundle,
            opportunity=replace(
                original.opportunity, rank_percentile=99, rank_velocity=None
            ),
        )
    result = evaluate_admission(bundle, OpportunityShadowConfig())
    assert result.reason is reason
    assert result.setup_family.value == family
    assert result.satisfied_rules == (reason,)


@pytest.mark.parametrize(
    ("hint", "without_evidence"),
    [
        (CandidateState.EARLY_ACCUMULATION, True),
        (None, False),
        (CandidateState.READY, False),
    ],
)
def test_accumulation_rule_preserves_hint_or_score_semantics(
    stage_factory, sector_factory, hint, without_evidence
):
    base = _bundle(stage_factory, sector_factory, lifecycle=hint)
    bundle = replace(base, evidence=None) if without_evidence else base
    assert evaluate_admission(
        bundle, OpportunityShadowConfig()
    ).reason is AdmissionReason.EARLY_ACCUMULATION


def test_admission_identity_pin_and_structured_json_are_deterministic(
    stage_factory, sector_factory
):
    bundle = _bundle(stage_factory, sector_factory)
    result = evaluate_admission(
        bundle, OpportunityShadowConfig(), policy_snapshot_id="policy-1"
    )
    expected = stable_digest(
        {
            "exchange": bundle.exchange,
            "symbol_id": bundle.symbol_id,
            "reason": result.reason.value,
            "setup_family": result.setup_family.value,
            "as_of": bundle.as_of.isoformat(),
            "source_rows": [],
            "artifact_hashes": [],
            "rule_version": ADMISSION_IDENTITY_RULE_VERSION,
            "policy_snapshot_id": "policy-1",
        }
    )
    assert result.admission_identity == expected
    assert result.rule_version == "admission-rules-v1.1"
    assert rule_evaluations_json(result) == rule_evaluations_json(result)


def test_ready_trigger_requires_real_event(stage_factory, sector_factory):
    bundle = _bundle(stage_factory, sector_factory)
    result = evaluate_transition(CandidateState.READY, bundle)
    assert not result.allowed
    assert "missing legitimate breakout trigger" in result.blockers


def test_confirmed_followthrough_collapses_missing_pending_observation(stage_factory, sector_factory):
    bundle = _bundle(stage_factory, sector_factory, followthrough=FollowthroughStatus.CONFIRMED)
    result = evaluate_transition(CandidateState.TRIGGERED, bundle)
    assert result.proposed_state is CandidateState.CONFIRMED
    assert result.metadata["collapsed_pending_followthrough"] is True


@pytest.mark.parametrize(
    "cause",
    [
        "missing_sector_mapping",
        "latest_only_untrusted_membership",
        "insufficient_constituent_coverage",
        "sector_not_stage_2",
        "sector_snapshot_not_locked",
        "sector_locked_snapshot_missing",
    ],
)
def test_provisional_trigger_emits_exact_sector_gate_taxonomy(
    stage_factory, sector_factory, cause
):
    stock = stage_factory(
        status=StageStatus.PROVISIONAL,
        provisional=WeinsteinStage.TRANSITION_1_TO_2,
        confidence=90,
    )
    bundle = replace(
        _bundle(stage_factory, sector_factory, stock=stock),
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        sector_gate=SectorGateEvidence(taxonomy_cause=cause),
    )
    result = evaluate_transition(CandidateState.READY, bundle)
    assert not result.allowed
    assert cause in result.blockers


def test_provisional_trigger_uses_prior_locked_stage2_not_current_provisional(
    stage_factory, sector_factory
):
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
    bundle = replace(
        _bundle(stage_factory, sector_factory, stock=stock),
        sector_stage=current_sector,
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        sector_gate=SectorGateEvidence(
            prior_locked_stage=WeinsteinStage.STAGE_2,
            current_provisional_stage=WeinsteinStage.TRANSITION_1_TO_2,
        ),
    )
    result = evaluate_transition(CandidateState.READY, bundle)
    assert result.allowed
    assert result.proposed_state is CandidateState.TRIGGERED


def test_normal_locked_stage2_does_not_consult_sector_gate(stage_factory, sector_factory):
    bundle = replace(
        _bundle(stage_factory, sector_factory),
        breakout_events=(BreakoutEvidence(True, False, 90, "A", "triggered"),),
        sector_gate=SectorGateEvidence(taxonomy_cause="missing_sector_mapping"),
    )
    assert evaluate_transition(CandidateState.READY, bundle).allowed


def test_retention_uses_age_and_stagnation_independently():
    retained = evaluate_retention(
        state=CandidateState.SETUP_FORMING, days_in_state=21, days_without_progress=5,
        progress_status=ProgressStatus.STABLE, stock_stage=WeinsteinStage.STAGE_1,
    )
    closed = evaluate_retention(
        state=CandidateState.SETUP_FORMING, days_in_state=21, days_without_progress=11,
        progress_status=ProgressStatus.STALLED, stock_stage=WeinsteinStage.STAGE_1,
    )
    assert retained.retain
    assert closed.close_episode


def test_session_counters_advance_once_from_friday_to_monday():
    friday = date(2026, 7, 10)
    monday = date(2026, 7, 13)
    first = advance_session_counters(
        previous_counted_session=friday,
        previous_sessions_in_state=2,
        previous_sessions_without_progress=1,
        previous_last_progress_at=None,
        observed_session=monday,
        observed_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
        progress_improving=False,
        transition_occurred=False,
    )
    repeated = advance_session_counters(
        previous_counted_session=monday,
        previous_sessions_in_state=first.sessions_in_state,
        previous_sessions_without_progress=first.sessions_without_progress,
        previous_last_progress_at=first.last_progress_at,
        observed_session=monday,
        observed_at=datetime(2026, 7, 13, 12, tzinfo=timezone.utc),
        progress_improving=False,
        transition_occurred=False,
    )
    assert (first.sessions_in_state, first.sessions_without_progress) == (3, 2)
    assert repeated == first


def test_same_session_improvement_is_carried_into_next_session():
    monday = date(2026, 7, 13)
    improvement_at = datetime(2026, 7, 14, tzinfo=timezone.utc)
    same_session = advance_session_counters(
        previous_counted_session=monday,
        previous_sessions_in_state=4,
        previous_sessions_without_progress=3,
        previous_last_progress_at=None,
        observed_session=monday,
        observed_at=improvement_at,
        progress_improving=True,
        transition_occurred=False,
    )
    next_session = advance_session_counters(
        previous_counted_session=monday,
        previous_sessions_in_state=same_session.sessions_in_state,
        previous_sessions_without_progress=same_session.sessions_without_progress,
        previous_last_progress_at=same_session.last_progress_at,
        observed_session=date(2026, 7, 14),
        observed_at=datetime(2026, 7, 14, 12, tzinfo=timezone.utc),
        progress_improving=False,
        transition_occurred=False,
    )
    assert (same_session.sessions_in_state, same_session.sessions_without_progress) == (4, 3)
    assert (next_session.sessions_in_state, next_session.sessions_without_progress) == (5, 0)


def test_transition_and_legacy_bootstrap_guards():
    session = date(2026, 7, 14)
    legacy = advance_session_counters(
        previous_counted_session=None,
        previous_sessions_in_state=7,
        previous_sessions_without_progress=4,
        previous_last_progress_at=None,
        legacy_last_snapshot_session=session,
        observed_session=session,
        observed_at=NOW,
        progress_improving=False,
        transition_occurred=False,
    )
    transitioned = advance_session_counters(
        previous_counted_session=session,
        previous_sessions_in_state=7,
        previous_sessions_without_progress=4,
        previous_last_progress_at=None,
        observed_session=session,
        observed_at=NOW,
        progress_improving=False,
        transition_occurred=True,
    )
    assert (legacy.sessions_in_state, legacy.sessions_without_progress) == (7, 4)
    assert (transitioned.sessions_in_state, transitioned.sessions_without_progress) == (0, 0)
    assert transitioned.last_progress_at == NOW


def test_observed_session_resolves_from_market_rows_not_weekend_run_date(
    tmp_path: Path,
):
    db_path = tmp_path / "ohlcv.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE _catalog (exchange VARCHAR, timestamp TIMESTAMP)")
        conn.execute(
            "INSERT INTO _catalog VALUES (?, ?)",
            ["NSE", datetime(2026, 7, 10, 15, 30)],
        )
    assert _resolve_observed_session(
        db_path, cutoff=date(2026, 7, 12), exchanges={"NSE"}
    ) == date(2026, 7, 10)
