"""Pure state-aware retention and closure evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from ai_trading_system.domains.opportunities.contracts import CandidateState, FollowthroughStatus, ProgressStatus, WeinsteinStage
from ai_trading_system.domains.opportunities.validation import default_candidate_retention_policy

from .contracts import ClosureReason, OpportunityShadowConfig, RetentionEvaluation


RETENTION_COUNTING_UNIT = "observed_trading_session"
RETENTION_COUNTER_GUARD = "first_successfully_persisted_observation_per_session"


@dataclass(frozen=True, slots=True)
class SessionCounterAdvance:
    """Counter state for the observation that will be persisted."""

    sessions_in_state: int
    sessions_without_progress: int
    last_progress_at: datetime | None
    counted_session: date


def advance_session_counters(
    *,
    previous_counted_session: date | None,
    previous_sessions_in_state: int,
    previous_sessions_without_progress: int,
    previous_last_progress_at: datetime | None,
    observed_session: date,
    observed_at: datetime,
    progress_improving: bool,
    transition_occurred: bool,
    legacy_last_snapshot_session: date | None = None,
) -> SessionCounterAdvance:
    """Advance retention counters at most once per observed market session.

    A same-session improvement is recorded immediately and carried into the
    next session's stagnation decision. Legacy rows bootstrap from their most
    recent snapshot date so a migration-day rerun cannot manufacture a day.
    """
    if previous_sessions_in_state < 0 or previous_sessions_without_progress < 0:
        raise ValueError("retention counters must be non-negative")
    effective_previous = previous_counted_session or legacy_last_snapshot_session
    last_progress_at = (
        observed_at
        if progress_improving
        and (previous_last_progress_at is None or observed_at > previous_last_progress_at)
        else previous_last_progress_at
    )
    if transition_occurred:
        return SessionCounterAdvance(0, 0, observed_at, observed_session)
    if effective_previous is None:
        return SessionCounterAdvance(
            previous_sessions_in_state,
            previous_sessions_without_progress,
            last_progress_at,
            observed_session,
        )
    if observed_session <= effective_previous:
        return SessionCounterAdvance(
            previous_sessions_in_state,
            previous_sessions_without_progress,
            last_progress_at,
            effective_previous,
        )
    pending_improvement = (
        previous_last_progress_at is not None
        and previous_last_progress_at.date() > effective_previous
    )
    return SessionCounterAdvance(
        previous_sessions_in_state + 1,
        0
        if progress_improving or pending_improvement
        else previous_sessions_without_progress + 1,
        last_progress_at,
        observed_session,
    )


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
