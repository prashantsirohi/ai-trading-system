"""Pure, versioned candidate lifecycle transition policy."""

from __future__ import annotations

from ai_trading_system.domains.opportunities.contracts import (
    CandidateState,
    FollowthroughStatus,
    ProgressStatus,
    RiskLevel,
    StageStatus,
    TransitionReason,
    WeinsteinStage,
)

from .contracts import (
    OpportunityShadowConfig,
    OpportunitySourceBundle,
    SECTOR_GATE_RULES,
    TransitionEvaluation,
)

_PRE_CONFIRMATION = {
    CandidateState.DISCOVERED,
    CandidateState.INVESTIGATING,
    CandidateState.EARLY_ACCUMULATION,
    CandidateState.SETUP_FORMING,
    CandidateState.READY,
    CandidateState.TRIGGERED,
    CandidateState.PENDING_FOLLOWTHROUGH,
}
_ACTIVE = _PRE_CONFIRMATION | {
    CandidateState.CONFIRMED,
    CandidateState.ADVANCING,
    CandidateState.EXTENDED,
}


def evaluate_transition(
    previous_state: CandidateState,
    bundle: OpportunitySourceBundle,
    *,
    progress_status: ProgressStatus = ProgressStatus.UNKNOWN,
    active_position: bool = False,
    exit_event: bool = False,
    config: OpportunityShadowConfig | None = None,
) -> TransitionEvaluation:
    cfg = config or OpportunityShadowConfig()
    stock = bundle.stock_stage
    stock_stage = stock.effective_stage if stock else WeinsteinStage.UNKNOWN
    evidence = bundle.evidence
    extension = evidence.extension_risk if evidence else RiskLevel.UNKNOWN
    failed_event = any(
        item.failed for item in (*bundle.breakout_events, *bundle.pattern_events)
    )
    trigger_event = any(
        item.qualified and not item.failed for item in bundle.breakout_events
    )

    if exit_event and previous_state is CandidateState.WEAKENING:
        return _allow(
            previous_state, CandidateState.EXITED, TransitionReason.POSITION_CLOSED
        )
    if previous_state in _PRE_CONFIRMATION and (
        failed_event
        or bundle.followthrough_status
        in {FollowthroughStatus.FAILED, FollowthroughStatus.EXPIRED}
    ):
        reason = (
            TransitionReason.FOLLOWTHROUGH_FAILED
            if bundle.followthrough_status
            in {FollowthroughStatus.FAILED, FollowthroughStatus.EXPIRED}
            else TransitionReason.BREAKOUT_FAILED
        )
        return _allow(previous_state, CandidateState.FAILED, reason)
    if stock_stage in {WeinsteinStage.TRANSITION_3_TO_4, WeinsteinStage.STAGE_4}:
        target = CandidateState.WEAKENING if active_position else CandidateState.FAILED
        return _allow(previous_state, target, TransitionReason.STAGE_CHANGED)
    if previous_state in _ACTIVE and stock_stage in {
        WeinsteinStage.TRANSITION_2_TO_3,
        WeinsteinStage.STAGE_3,
    }:
        return _allow(
            previous_state,
            CandidateState.WEAKENING,
            TransitionReason.STRUCTURE_WEAKENED,
        )
    if previous_state in _ACTIVE and progress_status is ProgressStatus.DETERIORATING:
        return _allow(
            previous_state, CandidateState.WEAKENING, TransitionReason.EVIDENCE_WEAKENED
        )

    if previous_state is CandidateState.UNSEEN:
        return _allow(
            previous_state, CandidateState.DISCOVERED, TransitionReason.RANK_ADMISSION
        )
    if previous_state is CandidateState.DISCOVERED:
        if _ready(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.READY,
                TransitionReason.SETUP_READY,
                metadata={"monitoring_states_collapsed": True},
            )
        if _accumulating(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.EARLY_ACCUMULATION,
                TransitionReason.ACCUMULATION_DETECTED,
                metadata={"investigating_collapsed": True},
            )
        if _forming(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.SETUP_FORMING,
                TransitionReason.EVIDENCE_IMPROVED,
                metadata={"investigating_collapsed": True},
            )
        if bundle.opportunity or bundle.evidence:
            return _allow(
                previous_state,
                CandidateState.INVESTIGATING,
                TransitionReason.EVIDENCE_IMPROVED,
            )
    if previous_state is CandidateState.INVESTIGATING:
        if _ready(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.READY,
                TransitionReason.SETUP_READY,
                metadata={"monitoring_states_collapsed": True},
            )
        if _accumulating(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.EARLY_ACCUMULATION,
                TransitionReason.ACCUMULATION_DETECTED,
            )
        if _forming(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.SETUP_FORMING,
                TransitionReason.EVIDENCE_IMPROVED,
            )
    if previous_state is CandidateState.EARLY_ACCUMULATION:
        if _ready(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.READY,
                TransitionReason.SETUP_READY,
                metadata={"setup_forming_collapsed": True},
            )
        if progress_status is ProgressStatus.IMPROVING or _forming(bundle, cfg):
            return _allow(
                previous_state,
                CandidateState.SETUP_FORMING,
                TransitionReason.EVIDENCE_IMPROVED,
            )
    if previous_state is CandidateState.SETUP_FORMING:
        if _ready(bundle, cfg):
            return _allow(
                previous_state, CandidateState.READY, TransitionReason.SETUP_READY
            )
    if previous_state is CandidateState.READY:
        if not trigger_event:
            return _block(previous_state, "missing legitimate breakout trigger")
        blockers = _trigger_blockers(bundle, cfg)
        if blockers:
            return _block(previous_state, *blockers)
        return _allow(
            previous_state,
            CandidateState.TRIGGERED,
            TransitionReason.BREAKOUT_TRIGGERED,
        )
    if previous_state is CandidateState.TRIGGERED:
        if bundle.followthrough_status is FollowthroughStatus.CONFIRMED:
            return _allow(
                previous_state,
                CandidateState.CONFIRMED,
                TransitionReason.FOLLOWTHROUGH_CONFIRMED,
                metadata={"collapsed_pending_followthrough": True},
            )
        if bundle.followthrough_status in {
            FollowthroughStatus.PENDING_1D,
            FollowthroughStatus.PENDING_3D,
            FollowthroughStatus.PENDING_5D,
        }:
            return _allow(
                previous_state,
                CandidateState.PENDING_FOLLOWTHROUGH,
                TransitionReason.BREAKOUT_TRIGGERED,
            )
    if (
        previous_state is CandidateState.PENDING_FOLLOWTHROUGH
        and bundle.followthrough_status is FollowthroughStatus.CONFIRMED
    ):
        return _allow(
            previous_state,
            CandidateState.CONFIRMED,
            TransitionReason.FOLLOWTHROUGH_CONFIRMED,
        )
    if (
        previous_state is CandidateState.CONFIRMED
        and progress_status is ProgressStatus.IMPROVING
    ):
        return _allow(
            previous_state, CandidateState.ADVANCING, TransitionReason.EVIDENCE_IMPROVED
        )
    if previous_state is CandidateState.ADVANCING and extension is RiskLevel.HIGH:
        return _allow(
            previous_state, CandidateState.EXTENDED, TransitionReason.EXTENSION_DETECTED
        )
    if (
        previous_state is CandidateState.EXTENDED
        and extension in {RiskLevel.LOW, RiskLevel.MEDIUM}
        and progress_status is not ProgressStatus.DETERIORATING
    ):
        return _allow(
            previous_state, CandidateState.ADVANCING, TransitionReason.EVIDENCE_IMPROVED
        )
    if previous_state is CandidateState.FAILED and cfg.archive_failed_after_days == 0:
        return _allow(previous_state, CandidateState.ARCHIVED, TransitionReason.TIMEOUT)
    return TransitionEvaluation(
        previous_state,
        previous_state,
        False,
        TransitionReason.UNKNOWN,
        ("no transition rule passed",),
    )


def _accumulating(
    bundle: OpportunitySourceBundle, cfg: OpportunityShadowConfig
) -> bool:
    if bundle.lifecycle_hint is CandidateState.EARLY_ACCUMULATION:
        return True
    return bool(
        bundle.evidence
        and bundle.evidence.accumulation_score is not None
        and bundle.evidence.accumulation_score >= cfg.accumulation_admission_score
        and bundle.stock_stage
        and bundle.stock_stage.effective_stage
        in {WeinsteinStage.STAGE_1, WeinsteinStage.TRANSITION_1_TO_2}
    )


def _forming(bundle: OpportunitySourceBundle, cfg: OpportunityShadowConfig) -> bool:
    pattern = any(
        not event.failed
        and (event.qualified or (event.score or 0) >= cfg.pattern_admission_score)
        for event in bundle.pattern_events
    )
    return bool(
        pattern
        and bundle.evidence
        and bundle.evidence.evidence_score >= cfg.setup_forming_evidence_threshold
    )


def _ready(bundle: OpportunitySourceBundle, cfg: OpportunityShadowConfig) -> bool:
    if (
        not bundle.stock_stage
        or not bundle.evidence
        or bundle.evidence.evidence_score < cfg.ready_evidence_threshold
    ):
        return False
    if (
        bundle.evidence.extension_risk is RiskLevel.HIGH
        or bundle.sector_regime.lower() == "risk_off"
    ):
        return False
    stage = bundle.stock_stage.effective_stage
    if (
        stage not in {WeinsteinStage.TRANSITION_1_TO_2, WeinsteinStage.STAGE_2}
        or bundle.stock_stage.confidence_score < cfg.ready_stage_confidence_threshold
    ):
        return False
    return (
        any(event.qualified and not event.failed for event in bundle.pattern_events)
        or bundle.lifecycle_hint is CandidateState.READY
    )


def _trigger_blockers(
    bundle: OpportunitySourceBundle, cfg: OpportunityShadowConfig
) -> tuple[str, ...]:
    if not bundle.stock_stage or not bundle.evidence:
        return ("stock stage and evidence are required",)
    stock = bundle.stock_stage
    if (
        stock.stage_status is StageStatus.LOCKED
        and stock.locked_stage is WeinsteinStage.STAGE_2
    ):
        return (
            ()
            if stock.confidence_score >= cfg.ready_stage_confidence_threshold
            else ("locked stock stage confidence too low",)
        )
    blockers: list[str] = []
    if (
        stock.stage_status is not StageStatus.PROVISIONAL
        or stock.provisional_stage is not WeinsteinStage.TRANSITION_1_TO_2
    ):
        blockers.append("trigger requires locked Stage 2 or provisional Stage 1 to 2")
    if stock.confidence_score < cfg.early_trigger_stage_confidence_threshold:
        blockers.append("provisional stock-stage confidence too low")
    gate = bundle.sector_gate
    if gate is None:
        blockers.append("sector_locked_snapshot_missing")
    elif gate.taxonomy_cause:
        blockers.append(gate.taxonomy_cause)
    elif gate.prior_locked_stage.value not in SECTOR_GATE_RULES["passing_prior_locked_stages"]:
        # Fail closed if a caller constructed incomplete evidence without using
        # the bulk resolver/classifier.
        blockers.append("sector_not_stage_2")
    if bundle.evidence.evidence_score < cfg.early_trigger_evidence_threshold:
        blockers.append("early-trigger evidence too weak")
    if bundle.evidence.extension_risk is not RiskLevel.LOW:
        blockers.append("early trigger requires low extension risk")
    if bundle.market_regime.lower() not in cfg.allowed_market_regimes:
        blockers.append("market regime blocks early trigger")
    return tuple(blockers)


def _allow(
    current: CandidateState,
    target: CandidateState,
    reason: TransitionReason,
    *,
    metadata: dict | None = None,
) -> TransitionEvaluation:
    return TransitionEvaluation(
        current, target, current is not target, reason, metadata=metadata or {}
    )


def _block(current: CandidateState, *blockers: str) -> TransitionEvaluation:
    return TransitionEvaluation(
        current, current, False, TransitionReason.UNKNOWN, tuple(blockers)
    )
