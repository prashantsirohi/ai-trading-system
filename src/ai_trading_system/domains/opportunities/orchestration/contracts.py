"""Immutable contracts for Phase 3 shadow opportunity orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Generic, Mapping, TypeVar

from ai_trading_system.domains.opportunities.contracts import (
    CandidateRetentionPolicy,
    CandidateState,
    EvidenceSnapshot,
    FollowthroughStatus,
    OpportunitySnapshot,
    ProgressSnapshot,
    SectorStageSnapshot,
    StageSnapshot,
    TransitionReason,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.position_monitoring import (
    PositionRecoveryMode,
)


ADMISSION_RULE_VERSION = "admission-rules-v1"
SETUP_FAMILY_RULE_VERSION = "setup-family-v1"
LIFECYCLE_RULE_VERSION = "lifecycle-policy-v1.1"
PROGRESS_RULE_VERSION = "opportunity-progress-v1"
RETENTION_RULE_VERSION = "opportunity-retention-v1"
LEGACY_STAGE_CONFIDENCE_VERSION = "weekly-stage-legacy-v1"


# ADR-0006 A2. These values are consumed by the gate and by the A3 policy
# fingerprint. ``lifecycle-policy-v2`` remains reserved for a future calibrated
# size-haircut policy; v1.1 is the fail-closed completed-week correction to v1.
SECTOR_GATE_RULES: dict[str, Any] = {
    "passing_prior_locked_stages": (WeinsteinStage.STAGE_2.value,),
    "trusted_membership_states": ("OBSERVED_AT_RUN", "POINT_IN_TIME_VERIFIED"),
    "calibration_prior_locked_stage": WeinsteinStage.STAGE_1.value,
    "calibration_current_provisional_stages": (
        WeinsteinStage.TRANSITION_1_TO_2.value,
    ),
    "calibration_improving_velocity_floor_exclusive": 0.0,
}


class OpportunityRegistryMode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"


class AdmissionReason(str, Enum):
    RANK_THRESHOLD = "rank_threshold"
    RANK_VELOCITY = "rank_velocity"
    INVESTIGATOR_PROMOTION = "investigator_promotion"
    EARLY_ACCUMULATION = "early_accumulation"
    QUALIFIED_PATTERN = "qualified_pattern"
    QUALIFIED_BREAKOUT = "qualified_breakout"
    STAGE_TRANSITION = "stage_transition"
    MANUAL_IMPORT = "manual_import"
    POSITION_STATE_RECOVERY = "position_state_recovery"


class SetupFamily(str, Enum):
    EARLY_ACCUMULATION = "early_accumulation"
    BASE_BUILDING = "base_building"
    STAGE_1_TO_2_TRANSITION = "stage_1_to_2_transition"
    BREAKOUT = "breakout"
    POST_BREAKOUT_FOLLOWTHROUGH = "post_breakout_followthrough"
    PULLBACK_REENTRY = "pullback_reentry"
    MOMENTUM_LEADER = "momentum_leader"
    MANUAL = "manual"
    POSITION_STATE_RECOVERY = "position_state_recovery"


class ClosureReason(str, Enum):
    FAILED_SETUP = "failed_setup"
    FOLLOWTHROUGH_FAILED = "followthrough_failed"
    STRUCTURAL_STAGE_FAILURE = "structural_stage_failure"
    STAGNATION_TIMEOUT = "stagnation_timeout"
    NO_LONGER_ELIGIBLE = "no_longer_eligible"
    POSITION_EXITED = "position_exited"
    MANUAL_CLOSE = "manual_close"
    SUPERSEDED_BY_NEW_EPISODE = "superseded_by_new_episode"


class ReconciliationSeverity(str, Enum):
    WARNING = "warning"
    ERROR = "error"


class SetupMatchOutcome(str, Enum):
    EXACT = "exact"
    PROGRESSION = "progression"
    NEW_EPISODE = "new_episode"
    CONFLICT = "conflict"
    NOT_ADMITTED = "not_admitted"


@dataclass(frozen=True, slots=True)
class SourceDescriptor:
    stage_name: str
    artifact_type: str
    artifact_path: str
    artifact_hash: str
    run_id: str
    stage_attempt: int
    row_count: int = 0


@dataclass(frozen=True, slots=True)
class AdapterWarning:
    source_artifact: str
    row_identity: str
    code: str
    message: str
    severity: ReconciliationSeverity = ReconciliationSeverity.WARNING


@dataclass(frozen=True, slots=True)
class RejectedSourceRow:
    source_artifact: str
    row_identity: str
    reason: str
    invalid_fields: tuple[str, ...] = ()


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class AdapterResult(Generic[T]):
    records: tuple[T, ...]
    warnings: tuple[AdapterWarning, ...]
    rejected_rows: tuple[RejectedSourceRow, ...]
    source: SourceDescriptor


@dataclass(frozen=True, slots=True)
class AdaptedRecord(Generic[T]):
    exchange: str
    symbol_id: str
    value: T
    row_identity: str
    source: SourceDescriptor


@dataclass(frozen=True, slots=True)
class BreakoutEvidence:
    qualified: bool
    failed: bool
    score: float | None
    tier: str | None
    state: str
    trigger_price: float | None = None
    pivot_price: float | None = None
    occurred_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class PatternEvidence:
    family: str
    state: str
    score: float | None
    setup_quality: float | None
    qualified: bool
    failed: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class OpportunitySourceBundle:
    symbol_id: str
    exchange: str
    as_of: datetime
    opportunity: OpportunitySnapshot | None = None
    evidence: EvidenceSnapshot | None = None
    stock_stage: StageSnapshot | None = None
    sector_stage: SectorStageSnapshot | None = None
    lifecycle_hint: CandidateState | None = None
    followthrough_status: FollowthroughStatus = FollowthroughStatus.UNKNOWN
    progress_hint: ProgressSnapshot | None = None
    breakout_events: tuple[BreakoutEvidence, ...] = ()
    pattern_events: tuple[PatternEvidence, ...] = ()
    source_lineage: tuple[SourceDescriptor, ...] = ()
    warnings: tuple[AdapterWarning, ...] = ()
    source_row_identities: tuple[str, ...] = ()
    sector_name: str = "unknown"
    market_regime: str = "unknown"
    sector_regime: str = "unknown"
    scan_tier: str = "stage_only"
    scan_reasons: tuple[str, ...] = ()
    active_position: bool = False
    recently_exited: bool = False
    position_cycle_opened_at: str | None = None
    position_cycle_id: str | None = None
    routing_decision_id: str | None = None
    market_data_complete: bool = True
    missing_data_fields: tuple[str, ...] = ()
    sector_gate: "SectorGateEvidence | None" = None


@dataclass(frozen=True, slots=True)
class SectorGateEvidence:
    """Point-in-time evidence for the provisional S1→S2 sector gate."""

    prior_locked_stage: WeinsteinStage = WeinsteinStage.UNKNOWN
    prior_locked_week_end: date | None = None
    prior_locked_confidence: float | None = None
    current_provisional_stage: WeinsteinStage = WeinsteinStage.UNKNOWN
    current_stage_velocity: float | None = None
    membership_trust: str = "UNKNOWN"
    coverage_status: str = "unknown"
    taxonomy_cause: str | None = None
    calibration_cohort: str | None = None


@dataclass(frozen=True, slots=True)
class AdmissionEvaluation:
    admitted: bool
    reason: AdmissionReason | None
    setup_family: SetupFamily | None
    supporting_evidence: tuple[str, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    admission_identity: str | None
    rule_version: str = ADMISSION_RULE_VERSION


@dataclass(frozen=True, slots=True)
class EpisodeMatch:
    outcome: SetupMatchOutcome
    candidate_id: str | None
    setup_id: str | None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TransitionEvaluation:
    current_state: CandidateState
    proposed_state: CandidateState
    allowed: bool
    transition_reason: TransitionReason
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    rule_version: str = LIFECYCLE_RULE_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class RetentionEvaluation:
    retain: bool
    close_episode: bool
    archive: bool
    reason: ClosureReason | None
    warnings: tuple[str, ...] = ()
    rule_version: str = RETENTION_RULE_VERSION


@dataclass(frozen=True, slots=True)
class OpportunityShadowConfig:
    mode: OpportunityRegistryMode = OpportunityRegistryMode.OFF
    dry_run: bool = False
    rank_admission_percentile: float = 90.0
    rank_velocity_floor: float = -5.0
    rank_velocity_percentile_floor: float = 75.0
    investigator_admission_score: float = 70.0
    accumulation_admission_score: float = 75.0
    pattern_admission_score: float = 80.0
    breakout_admission_score: float = 80.0
    breakout_admission_tiers: tuple[str, ...] = ("A",)
    setup_forming_evidence_threshold: float = 55.0
    ready_evidence_threshold: float = 80.0
    ready_stage_confidence_threshold: float = 65.0
    early_trigger_evidence_threshold: float = 80.0
    early_trigger_stage_confidence_threshold: float = 75.0
    setup_progression_max_days: int = 30
    close_stage_4_without_position: bool = True
    recover_position_only_episodes: bool = False
    position_recovery_mode: PositionRecoveryMode = PositionRecoveryMode.REPORT_ONLY
    position_episode_compatibility_policy_version: str = "position-episode-compatibility-v1"
    position_recovery_policy_version: str = "position-recovery-policy-v1"
    position_recovery_reviewed_by: str | None = None
    position_recovery_reviewed_at: datetime | None = None
    position_recovery_review_notes: str | None = None
    archive_failed_after_days: int = 0
    allowed_market_regimes: tuple[str, ...] = ("cautious_bull", "bull", "strong_bull")
    retention_policy: CandidateRetentionPolicy | None = None

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "OpportunityShadowConfig":
        mode = OpportunityRegistryMode(str(values.get("opportunity_registry_mode", "off")).lower())
        legacy_recovery = bool(values.get("recover_position_only_episodes", False))
        recovery_mode = PositionRecoveryMode(
            str(
                values.get(
                    "position_recovery_mode",
                    "automatic" if legacy_recovery else "report_only",
                )
            ).lower()
        )
        return cls(
            mode=mode,
            dry_run=bool(values.get("opportunity_registry_dry_run", False)),
            recover_position_only_episodes=legacy_recovery,
            position_recovery_mode=recovery_mode,
            position_recovery_reviewed_by=(
                str(values.get("position_recovery_reviewed_by") or "") or None
            ),
            position_recovery_reviewed_at=(
                datetime.fromisoformat(str(values["position_recovery_reviewed_at"]).replace("Z", "+00:00"))
                if values.get("position_recovery_reviewed_at") else None
            ),
            position_recovery_review_notes=(
                str(values.get("position_recovery_review_notes") or "") or None
            ),
            rank_admission_percentile=float(values.get("opportunity_rank_admission_percentile", 90.0)),
            rank_velocity_floor=float(values.get("opportunity_rank_velocity_floor", -5.0)),
            rank_velocity_percentile_floor=float(values.get("opportunity_rank_velocity_percentile_floor", 75.0)),
            investigator_admission_score=float(values.get("opportunity_investigator_admission_score", 70.0)),
            accumulation_admission_score=float(values.get("opportunity_accumulation_admission_score", 75.0)),
            pattern_admission_score=float(values.get("opportunity_pattern_admission_score", 80.0)),
            breakout_admission_score=float(values.get("opportunity_breakout_admission_score", 80.0)),
        )


@dataclass(frozen=True, slots=True)
class OpportunityShadowRunResult:
    status: str
    dry_run: bool
    summary: Mapping[str, Any]
    artifact_rows: Mapping[str, tuple[Mapping[str, Any], ...]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "summary", MappingProxyType(dict(self.summary)))
        object.__setattr__(
            self,
            "artifact_rows",
            MappingProxyType({key: tuple(value) for key, value in self.artifact_rows.items()}),
        )
