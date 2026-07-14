"""Pure state-aware retention and closure evaluation."""

from __future__ import annotations

from ai_trading_system.domains.opportunities.contracts import CandidateState, FollowthroughStatus, ProgressStatus, WeinsteinStage
from ai_trading_system.domains.opportunities.validation import default_candidate_retention_policy

from .contracts import ClosureReason, OpportunityShadowConfig, RetentionEvaluation


def evaluate_retention(
    *, state: CandidateState, days_in_state: int, days_without_progress: int,
    progress_status: ProgressStatus, stock_stage: WeinsteinStage,
    followthrough_status: FollowthroughStatus = FollowthroughStatus.UNKNOWN,
    active_position: bool = False, evidence_weakened: bool = False,
    config: OpportunityShadowConfig | None = None,
) -> RetentionEvaluation:
    cfg = config or OpportunityShadowConfig()
    policy = cfg.retention_policy or default_candidate_retention_policy()
    rule = next(item for item in policy.rules if item.state is state)
    if stock_stage in {WeinsteinStage.TRANSITION_3_TO_4, WeinsteinStage.STAGE_4} and not active_position and cfg.close_stage_4_without_position:
        return RetentionEvaluation(False, True, True, ClosureReason.STRUCTURAL_STAGE_FAILURE)
    if state is CandidateState.PENDING_FOLLOWTHROUGH and followthrough_status in {FollowthroughStatus.FAILED, FollowthroughStatus.EXPIRED}:
        return RetentionEvaluation(False, True, True, ClosureReason.FOLLOWTHROUGH_FAILED)
    if state in {CandidateState.FAILED, CandidateState.EXITED, CandidateState.ARCHIVED}:
        reason = ClosureReason.POSITION_EXITED if state is CandidateState.EXITED else ClosureReason.FAILED_SETUP
        return RetentionEvaluation(False, True, cfg.archive_failed_after_days == 0, reason)
    if state is CandidateState.EARLY_ACCUMULATION and progress_status in {ProgressStatus.IMPROVING, ProgressStatus.STABLE}:
        return RetentionEvaluation(True, False, False, None)
    age_exceeded = rule.max_days_in_state is not None and days_in_state > rule.max_days_in_state
    stagnation_exceeded = rule.max_days_without_progress is not None and days_without_progress > rule.max_days_without_progress
    if state is CandidateState.SETUP_FORMING and age_exceeded and stagnation_exceeded:
        return RetentionEvaluation(False, True, True, ClosureReason.STAGNATION_TIMEOUT)
    if state is CandidateState.READY and age_exceeded and evidence_weakened:
        return RetentionEvaluation(True, False, False, None, ("ready timeout should weaken before closure",))
    if age_exceeded and stagnation_exceeded and state not in {CandidateState.CONFIRMED, CandidateState.ADVANCING}:
        return RetentionEvaluation(False, True, True, ClosureReason.STAGNATION_TIMEOUT)
    return RetentionEvaluation(True, False, False, None)
