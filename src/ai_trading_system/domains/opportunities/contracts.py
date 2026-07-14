"""Canonical, persistence-free contracts for opportunity lifecycle management."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, NewType


OPPORTUNITY_CONTRACT_VERSION = "opportunity-contract-v1"
STAGE_CONFIDENCE_FORMULA_VERSION = "stage-confidence-v1"

CandidateId = NewType("CandidateId", str)
SetupId = NewType("SetupId", str)
SymbolId = NewType("SymbolId", str)

__all__ = [
    "OPPORTUNITY_CONTRACT_VERSION",
    "STAGE_CONFIDENCE_FORMULA_VERSION",
    "CandidateId",
    "SetupId",
    "SymbolId",
    "WeinsteinStage",
    "StageStatus",
    "StageConfidenceBand",
    "CandidateState",
    "FollowthroughStatus",
    "CandidateAction",
    "ActionEligibility",
    "TransitionReason",
    "StageTransitionReason",
    "OutcomeAttribution",
    "ProgressStatus",
    "EvidenceVerdict",
    "RiskLevel",
    "SymbolIdentity",
    "CandidateEpisodeIdentity",
    "StageConfidenceComponents",
    "StageConfidenceResult",
    "StageSnapshot",
    "SectorStageSnapshot",
    "OpportunitySnapshot",
    "EvidenceSnapshot",
    "CandidateSnapshot",
    "CandidateTransition",
    "CandidateDecision",
    "DecisionContextSnapshot",
    "OutcomeAttributionRecord",
    "ProgressSnapshot",
    "CandidateRetentionRule",
    "CandidateRetentionPolicy",
    "StructuralGuardConfig",
    "StructuralGuardResult",
    "StageAttributionConfig",
    "StageForwardObservation",
    "RegimeShockEvidence",
]


class WeinsteinStage(str, Enum):
    STAGE_1 = "stage_1_basing"
    TRANSITION_1_TO_2 = "transition_1_to_2"
    STAGE_2 = "stage_2_advancing"
    TRANSITION_2_TO_3 = "transition_2_to_3"
    STAGE_3 = "stage_3_topping"
    TRANSITION_3_TO_4 = "transition_3_to_4"
    STAGE_4 = "stage_4_declining"
    TRANSITION_4_TO_1 = "transition_4_to_1"
    UNKNOWN = "unknown"


class StageStatus(str, Enum):
    PROVISIONAL = "provisional"
    LOCKED = "locked"
    UNKNOWN = "unknown"


class StageConfidenceBand(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"
    UNKNOWN = "unknown"


class CandidateState(str, Enum):
    UNSEEN = "unseen"
    DISCOVERED = "discovered"
    INVESTIGATING = "investigating"
    EARLY_ACCUMULATION = "early_accumulation"
    SETUP_FORMING = "setup_forming"
    READY = "ready"
    TRIGGERED = "triggered"
    PENDING_FOLLOWTHROUGH = "pending_followthrough"
    CONFIRMED = "confirmed"
    ADVANCING = "advancing"
    EXTENDED = "extended"
    WEAKENING = "weakening"
    FAILED = "failed"
    EXITED = "exited"
    ARCHIVED = "archived"


class FollowthroughStatus(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    PENDING_1D = "pending_1d"
    PENDING_3D = "pending_3d"
    PENDING_5D = "pending_5d"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class CandidateAction(str, Enum):
    IGNORE = "ignore"
    WATCH = "watch"
    INVESTIGATE = "investigate"
    PREPARE = "prepare"
    ENTER = "enter"
    ADD = "add"
    HOLD = "hold"
    TIGHTEN_STOP = "tighten_stop"
    REDUCE = "reduce"
    EXIT = "exit"
    ARCHIVE = "archive"


class ActionEligibility(str, Enum):
    ELIGIBLE = "eligible"
    CONDITIONALLY_ELIGIBLE = "conditionally_eligible"
    BLOCKED = "blocked"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"


class TransitionReason(str, Enum):
    RANK_ADMISSION = "rank_admission"
    RANK_RECOVERY = "rank_recovery"
    EVIDENCE_IMPROVED = "evidence_improved"
    EVIDENCE_WEAKENED = "evidence_weakened"
    ACCUMULATION_DETECTED = "accumulation_detected"
    SETUP_READY = "setup_ready"
    BREAKOUT_TRIGGERED = "breakout_triggered"
    FOLLOWTHROUGH_CONFIRMED = "followthrough_confirmed"
    FOLLOWTHROUGH_FAILED = "followthrough_failed"
    BREAKOUT_FAILED = "breakout_failed"
    EXTENSION_DETECTED = "extension_detected"
    STRUCTURE_WEAKENED = "structure_weakened"
    STAGE_CHANGED = "stage_changed"
    SECTOR_GATE_CHANGED = "sector_gate_changed"
    STOP_TRIGGERED = "stop_triggered"
    TIMEOUT = "timeout"
    STAGNATION = "stagnation"
    POSITION_CLOSED = "position_closed"
    MANUAL_OVERRIDE = "manual_override"
    UNKNOWN = "unknown"


class StageTransitionReason(str, Enum):
    WEEKLY_MA_SLOPE_POSITIVE = "weekly_ma_slope_positive"
    WEEKLY_MA_SLOPE_NEGATIVE = "weekly_ma_slope_negative"
    WEEKLY_CLOSE_ABOVE_30W_MA = "weekly_close_above_30w_ma"
    WEEKLY_CLOSE_BELOW_30W_MA = "weekly_close_below_30w_ma"
    RELATIVE_STRENGTH_BREAKOUT = "relative_strength_breakout"
    RELATIVE_STRENGTH_BREAKDOWN = "relative_strength_breakdown"
    BASE_BREAKOUT_CONFIRMED = "base_breakout_confirmed"
    TOPPING_STRUCTURE_DETECTED = "topping_structure_detected"
    DECLINE_STRUCTURE_DETECTED = "decline_structure_detected"
    TRANSITION_PERSISTED = "transition_persisted"
    PROVISIONAL_REVERSAL = "provisional_reversal"
    UNKNOWN = "unknown"


class OutcomeAttribution(str, Enum):
    VALID_SIGNAL_NORMAL_FAILURE = "valid_signal_normal_failure"
    PROVISIONAL_STAGE_NONCONFIRMATION = "provisional_stage_nonconfirmation"
    STAGE_CLASSIFICATION_ERROR = "stage_classification_error"
    STAGE_TRANSITION_AFTER_VALID_ENTRY = "stage_transition_after_valid_entry"
    EVIDENCE_MODEL_ERROR = "evidence_model_error"
    LIFECYCLE_TRANSITION_ERROR = "lifecycle_transition_error"
    POLICY_ERROR = "policy_error"
    SIZING_ERROR = "sizing_error"
    EXECUTION_ERROR = "execution_error"
    EXIT_ERROR = "exit_error"
    EXOGENOUS_REGIME_SHOCK = "exogenous_regime_shock"
    UNDETERMINED = "undetermined"


class ProgressStatus(str, Enum):
    IMPROVING = "improving"
    STABLE = "stable"
    STALLED = "stalled"
    DETERIORATING = "deteriorating"
    UNKNOWN = "unknown"


class EvidenceVerdict(str, Enum):
    HIGH_CONVICTION = "high_conviction"
    MEDIUM_CONVICTION = "medium_conviction"
    WATCH_ONLY = "watch_only"
    NOISE_TRAP = "noise_trap"
    UNKNOWN = "unknown"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    UNKNOWN = "unknown"


def _require_text(value: object, field_name: str) -> None:
    if not str(value or "").strip():
        raise ValueError(f"{field_name} must be non-empty")


def _require_range(value: float, field_name: str, low: float = 0.0, high: float = 100.0) -> None:
    if not low <= float(value) <= high:
        raise ValueError(f"{field_name} must be between {low:g} and {high:g}")


def _require_aware(value: datetime | None, field_name: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must be timezone-aware")


def _monitoring_stage(provisional: WeinsteinStage, locked: WeinsteinStage) -> WeinsteinStage:
    return provisional if provisional is not WeinsteinStage.UNKNOWN else locked


def _confidence_band(score: float) -> StageConfidenceBand:
    if score < 50.0:
        return StageConfidenceBand.LOW
    if score < 65.0:
        return StageConfidenceBand.MEDIUM
    if score < 80.0:
        return StageConfidenceBand.HIGH
    return StageConfidenceBand.VERY_HIGH


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class SymbolIdentity:
    exchange: str
    symbol_id: str

    def __post_init__(self) -> None:
        _require_text(self.exchange, "exchange")
        _require_text(self.symbol_id, "symbol_id")


@dataclass(frozen=True, slots=True)
class CandidateEpisodeIdentity:
    candidate_id: str
    setup_id: str
    symbol_id: str
    exchange: str
    episode_started_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("candidate_id", "setup_id", "symbol_id", "exchange"):
            _require_text(getattr(self, field_name), field_name)
        _require_aware(self.episode_started_at, "episode_started_at")


@dataclass(frozen=True, slots=True)
class StageConfidenceComponents:
    ma_slope_quality: float
    price_position_quality: float
    relative_strength_quality: float
    base_breakout_quality: float
    volume_confirmation: float
    transition_persistence: float
    failed_breakout_penalty: float = 0.0

    def __post_init__(self) -> None:
        for field_name in (
            "ma_slope_quality",
            "price_position_quality",
            "relative_strength_quality",
            "base_breakout_quality",
            "volume_confirmation",
            "transition_persistence",
        ):
            _require_range(getattr(self, field_name), field_name)
        if self.failed_breakout_penalty < 0:
            raise ValueError("failed_breakout_penalty must be non-negative")


@dataclass(frozen=True, slots=True)
class StageConfidenceResult:
    score: float
    band: StageConfidenceBand
    formula_version: str = STAGE_CONFIDENCE_FORMULA_VERSION

    def __post_init__(self) -> None:
        _require_range(self.score, "score")
        if self.band is not _confidence_band(self.score):
            raise ValueError("band must match the canonical score boundary")
        _require_text(self.formula_version, "formula_version")


@dataclass(frozen=True, slots=True)
class StageSnapshot:
    provisional_stage: WeinsteinStage
    locked_stage: WeinsteinStage
    effective_stage: WeinsteinStage
    stage_status: StageStatus
    confidence_score: float
    confidence_band: StageConfidenceBand
    confidence_components: StageConfidenceComponents
    stage_as_of: datetime
    stage_locked_at: datetime | None
    source_week_start: date
    source_week_end: date
    previous_locked_stage: WeinsteinStage | None
    weeks_in_locked_stage: int
    provisional_persistence_days: int
    transition_reason: StageTransitionReason
    classifier_version: str
    confidence_formula_version: str = STAGE_CONFIDENCE_FORMULA_VERSION
    contract_version: str = OPPORTUNITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_range(self.confidence_score, "confidence_score")
        unknown_confidence = (
            self.stage_status is StageStatus.UNKNOWN
            and self.confidence_score == 0.0
            and self.confidence_band is StageConfidenceBand.UNKNOWN
        )
        if not unknown_confidence and self.confidence_band is not _confidence_band(self.confidence_score):
            raise ValueError("confidence_band must match confidence_score")
        _require_aware(self.stage_as_of, "stage_as_of")
        _require_aware(self.stage_locked_at, "stage_locked_at")
        if self.stage_status is StageStatus.LOCKED and self.stage_locked_at is None:
            raise ValueError("stage_locked_at is required when stage_status is locked")
        if self.source_week_start > self.source_week_end:
            raise ValueError("source_week_start must be on or before source_week_end")
        if self.weeks_in_locked_stage < 0:
            raise ValueError("weeks_in_locked_stage must be non-negative")
        if self.provisional_persistence_days < 0:
            raise ValueError("provisional_persistence_days must be non-negative")
        expected = _monitoring_stage(self.provisional_stage, self.locked_stage)
        if self.effective_stage is not expected:
            raise ValueError("effective_stage must match the canonical monitoring-stage selection")
        _require_text(self.classifier_version, "classifier_version")
        _require_text(self.confidence_formula_version, "confidence_formula_version")
        _require_text(self.contract_version, "contract_version")


@dataclass(frozen=True, slots=True)
class SectorStageSnapshot:
    sector_id: str
    sector_name: str
    stage_snapshot: StageSnapshot
    sector_relative_strength_state: str
    sector_rotation_state: str
    contract_version: str = OPPORTUNITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_text(self.sector_id, "sector_id")
        _require_text(self.sector_name, "sector_name")
        _require_text(self.sector_relative_strength_state, "sector_relative_strength_state")
        _require_text(self.sector_rotation_state, "sector_rotation_state")


@dataclass(frozen=True, slots=True)
class OpportunitySnapshot:
    opportunity_score: float
    rank_position: int
    rank_percentile: float
    rank_velocity: float | None
    rank_velocity_state: ProgressStatus
    factor_scores: Mapping[str, float]
    rank_model_version: str
    ranked_at: datetime
    contract_version: str = OPPORTUNITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_range(self.opportunity_score, "opportunity_score")
        _require_range(self.rank_percentile, "rank_percentile")
        if self.rank_position < 1:
            raise ValueError("rank_position must be at least 1")
        for name, value in self.factor_scores.items():
            _require_text(name, "factor_scores key")
            _require_range(value, f"factor_scores[{name!r}]")
        object.__setattr__(self, "factor_scores", _freeze_value(self.factor_scores))
        _require_text(self.rank_model_version, "rank_model_version")
        _require_aware(self.ranked_at, "ranked_at")


@dataclass(frozen=True, slots=True)
class EvidenceSnapshot:
    evidence_score: float
    investigator_verdict: EvidenceVerdict
    accumulation_score: float
    pattern_score: float
    breakout_quality: float
    volume_quality: float
    delivery_quality: float
    sector_alignment: float
    market_alignment: float
    extension_risk: RiskLevel
    failure_risk: RiskLevel
    positive_evidence: tuple[str, ...]
    negative_evidence: tuple[str, ...]
    missing_evidence: tuple[str, ...]
    evidence_model_version: str
    evaluated_at: datetime
    contract_version: str = OPPORTUNITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "evidence_score",
            "accumulation_score",
            "pattern_score",
            "breakout_quality",
            "volume_quality",
            "delivery_quality",
            "sector_alignment",
            "market_alignment",
        ):
            _require_range(getattr(self, field_name), field_name)
        _require_text(self.evidence_model_version, "evidence_model_version")
        _require_aware(self.evaluated_at, "evaluated_at")


@dataclass(frozen=True, slots=True)
class CandidateSnapshot:
    candidate_id: str
    setup_id: str
    symbol_id: str
    exchange: str
    as_of: datetime
    opportunity: OpportunitySnapshot
    evidence: EvidenceSnapshot
    lifecycle_state: CandidateState
    followthrough_status: FollowthroughStatus
    stock_stage: StageSnapshot
    sector_stage: SectorStageSnapshot
    market_regime: str
    sector_regime: str
    days_in_state: int
    days_without_progress: int
    active_position: bool
    latest_action: CandidateAction
    eligibility: ActionEligibility
    contract_version: str = OPPORTUNITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for field_name in ("candidate_id", "setup_id", "symbol_id", "exchange"):
            _require_text(getattr(self, field_name), field_name)
        _require_aware(self.as_of, "as_of")
        if self.days_in_state < 0:
            raise ValueError("days_in_state must be non-negative")
        if self.days_without_progress < 0:
            raise ValueError("days_without_progress must be non-negative")
        pending = {
            FollowthroughStatus.PENDING_1D,
            FollowthroughStatus.PENDING_3D,
            FollowthroughStatus.PENDING_5D,
        }
        if self.lifecycle_state is CandidateState.PENDING_FOLLOWTHROUGH:
            if self.followthrough_status not in pending:
                raise ValueError("pending_followthrough state requires a pending follow-through status")
        elif self.lifecycle_state is CandidateState.CONFIRMED:
            if self.followthrough_status is not FollowthroughStatus.CONFIRMED:
                raise ValueError("confirmed state requires confirmed follow-through status")
        elif self.followthrough_status in pending:
            raise ValueError("pending follow-through status requires pending_followthrough lifecycle state")
        _require_text(self.market_regime, "market_regime")
        _require_text(self.sector_regime, "sector_regime")


@dataclass(frozen=True, slots=True)
class CandidateTransition:
    candidate_id: str
    setup_id: str
    from_state: CandidateState
    to_state: CandidateState
    transition_reason: TransitionReason
    transitioned_at: datetime
    triggering_snapshot_as_of: datetime
    rule_version: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text(self.candidate_id, "candidate_id")
        _require_text(self.setup_id, "setup_id")
        _require_aware(self.transitioned_at, "transitioned_at")
        _require_aware(self.triggering_snapshot_as_of, "triggering_snapshot_as_of")
        _require_text(self.rule_version, "rule_version")
        object.__setattr__(self, "metadata", _freeze_value(self.metadata))


@dataclass(frozen=True, slots=True)
class CandidateDecision:
    candidate_id: str
    setup_id: str
    action: CandidateAction
    eligibility: ActionEligibility
    confidence: float
    size_multiplier: float
    reasons: tuple[str, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_required_event: str | None
    policy_version: str
    decided_at: datetime

    def __post_init__(self) -> None:
        _require_text(self.candidate_id, "candidate_id")
        _require_text(self.setup_id, "setup_id")
        _require_range(self.confidence, "confidence")
        if self.size_multiplier < 0:
            raise ValueError("size_multiplier must be non-negative")
        if self.eligibility is ActionEligibility.BLOCKED and not self.blockers:
            raise ValueError("blocked decisions must contain at least one blocker")
        _require_text(self.policy_version, "policy_version")
        _require_aware(self.decided_at, "decided_at")


@dataclass(frozen=True, slots=True)
class DecisionContextSnapshot:
    decision_stage: WeinsteinStage
    decision_stage_status: StageStatus
    decision_stage_as_of: datetime
    decision_locked_stage: WeinsteinStage
    decision_provisional_stage: WeinsteinStage
    decision_stage_confidence: float
    decision_sector_stage: WeinsteinStage
    decision_sector_stage_status: StageStatus
    decision_sector_stage_confidence: float
    opportunity_score: float
    evidence_score: float
    lifecycle_state: CandidateState
    followthrough_status: FollowthroughStatus
    market_regime: str
    sector_regime: str
    rank_model_version: str
    evidence_model_version: str
    stage_classifier_version: str
    action_policy_version: str
    execution_policy_version: str
    portfolio_context_summary: Mapping[str, Any]
    contract_version: str = OPPORTUNITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_aware(self.decision_stage_as_of, "decision_stage_as_of")
        for field_name in (
            "decision_stage_confidence",
            "decision_sector_stage_confidence",
            "opportunity_score",
            "evidence_score",
        ):
            _require_range(getattr(self, field_name), field_name)
        for field_name in (
            "market_regime",
            "sector_regime",
            "rank_model_version",
            "evidence_model_version",
            "stage_classifier_version",
            "action_policy_version",
            "execution_policy_version",
        ):
            _require_text(getattr(self, field_name), field_name)
        object.__setattr__(self, "portfolio_context_summary", _freeze_value(self.portfolio_context_summary))


@dataclass(frozen=True, slots=True)
class OutcomeAttributionRecord:
    candidate_id: str
    setup_id: str
    attribution_category: OutcomeAttribution
    attribution_subcategory: str | None
    attribution_confidence: float
    attribution_rule_version: str
    supporting_evidence: tuple[str, ...]
    counterfactual_notes: str | None
    resolved_at: datetime

    def __post_init__(self) -> None:
        _require_text(self.candidate_id, "candidate_id")
        _require_text(self.setup_id, "setup_id")
        _require_range(self.attribution_confidence, "attribution_confidence")
        _require_text(self.attribution_rule_version, "attribution_rule_version")
        _require_aware(self.resolved_at, "resolved_at")
        if self.attribution_category is OutcomeAttribution.STAGE_CLASSIFICATION_ERROR and not self.supporting_evidence:
            raise ValueError("stage_classification_error requires explicit supporting evidence")


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    status: ProgressStatus
    observed_at: datetime
    rank_velocity_improved: bool | None = None
    evidence_score_improved: bool | None = None
    base_contraction_improved: bool | None = None
    volume_dry_up_improved: bool | None = None
    weekly_ma_slope_improved: bool | None = None
    distance_to_pivot_narrowed: bool | None = None
    relative_strength_improved: bool | None = None
    sector_alignment_improved: bool | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_aware(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class CandidateRetentionRule:
    state: CandidateState
    max_days_in_state: int | None
    max_days_without_progress: int | None
    controlled_by_followthrough_window: bool = False
    review_daily: bool = False
    actively_retained: bool = True

    def __post_init__(self) -> None:
        for field_name in ("max_days_in_state", "max_days_without_progress"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative or None")
        if self.controlled_by_followthrough_window and (
            self.max_days_in_state is not None or self.max_days_without_progress is not None
        ):
            raise ValueError("follow-through-controlled rules cannot define fixed day limits")


@dataclass(frozen=True, slots=True)
class CandidateRetentionPolicy:
    rules: tuple[CandidateRetentionRule, ...]
    policy_version: str = "candidate-retention-v1"

    def __post_init__(self) -> None:
        _require_text(self.policy_version, "policy_version")
        states = [rule.state for rule in self.rules]
        if len(states) != len(set(states)):
            raise ValueError("retention policy contains duplicate states")
        missing = set(CandidateState) - set(states)
        if missing:
            labels = ", ".join(sorted(state.value for state in missing))
            raise ValueError(f"retention policy is missing states: {labels}")


@dataclass(frozen=True, slots=True)
class StructuralGuardConfig:
    early_stock_confidence_min: float = 75.0
    normal_stock_confidence_min: float = 65.0
    evidence_score_min: float = 80.0
    pilot_size_multiplier: float = 0.35
    allowed_market_regimes: tuple[str, ...] = ("cautious_bull", "bull", "strong_bull")
    blocked_sector_regimes: tuple[str, ...] = ("risk_off",)
    rule_version: str = "structural-entry-guard-v1"

    def __post_init__(self) -> None:
        _require_range(self.early_stock_confidence_min, "early_stock_confidence_min")
        _require_range(self.normal_stock_confidence_min, "normal_stock_confidence_min")
        _require_range(self.evidence_score_min, "evidence_score_min")
        if not 0.0 <= self.pilot_size_multiplier <= 0.40:
            raise ValueError("pilot_size_multiplier must be between 0 and 0.40")
        _require_text(self.rule_version, "rule_version")


@dataclass(frozen=True, slots=True)
class StructuralGuardResult:
    passed: bool
    eligibility: ActionEligibility
    reasons: tuple[str, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    recommended_max_size_multiplier: float
    rule_version: str

    def __post_init__(self) -> None:
        if self.recommended_max_size_multiplier < 0:
            raise ValueError("recommended_max_size_multiplier must be non-negative")
        if self.passed and self.blockers:
            raise ValueError("a passed structural guard cannot contain blockers")
        if not self.passed and not self.blockers:
            raise ValueError("a failed structural guard must contain blockers")
        _require_text(self.rule_version, "rule_version")


@dataclass(frozen=True, slots=True)
class StageAttributionConfig:
    lookforward_min_weeks: int = 2
    lookforward_max_weeks: int = 4
    opposite_structure_persistence_weeks: int = 2
    minimum_valid_hold_weeks: int = 4
    rule_version: str = "stage-attribution-v1"

    def __post_init__(self) -> None:
        if self.lookforward_min_weeks < 1:
            raise ValueError("lookforward_min_weeks must be at least 1")
        if self.lookforward_max_weeks < self.lookforward_min_weeks:
            raise ValueError("lookforward_max_weeks must be >= lookforward_min_weeks")
        if self.opposite_structure_persistence_weeks < 1:
            raise ValueError("opposite_structure_persistence_weeks must be at least 1")
        if self.minimum_valid_hold_weeks < 1:
            raise ValueError("minimum_valid_hold_weeks must be at least 1")
        _require_text(self.rule_version, "rule_version")


@dataclass(frozen=True, slots=True)
class StageForwardObservation:
    week_number_after_decision: int
    week_end: date
    ma30w_slope_negative: bool
    close_below_30w_ma: bool
    relative_strength_slope_negative: bool
    opposite_structure_confirmed: bool
    complete: bool = True

    def __post_init__(self) -> None:
        if self.week_number_after_decision < 1:
            raise ValueError("week_number_after_decision must be at least 1")


@dataclass(frozen=True, slots=True)
class RegimeShockEvidence:
    shock_confirmed: bool
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.shock_confirmed and not self.reasons:
            raise ValueError("confirmed regime shock requires at least one reason")
