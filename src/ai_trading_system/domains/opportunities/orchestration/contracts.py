"""Immutable contracts for Phase 3 shadow opportunity orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
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
)


ADMISSION_RULE_VERSION = "admission-rules-v1"
SETUP_FAMILY_RULE_VERSION = "setup-family-v1"
LIFECYCLE_RULE_VERSION = "lifecycle-policy-v1"
PROGRESS_RULE_VERSION = "opportunity-progress-v1"
RETENTION_RULE_VERSION = "opportunity-retention-v1"
LEGACY_STAGE_CONFIDENCE_VERSION = "weekly-stage-legacy-v1"


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


class SetupFamily(str, Enum):
    EARLY_ACCUMULATION = "early_accumulation"
    BASE_BUILDING = "base_building"
    STAGE_1_TO_2_TRANSITION = "stage_1_to_2_transition"
    BREAKOUT = "breakout"
    POST_BREAKOUT_FOLLOWTHROUGH = "post_breakout_followthrough"
    PULLBACK_REENTRY = "pullback_reentry"
    MOMENTUM_LEADER = "momentum_leader"
    MANUAL = "manual"


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
    archive_failed_after_days: int = 0
    allowed_market_regimes: tuple[str, ...] = ("cautious_bull", "bull", "strong_bull")
    retention_policy: CandidateRetentionPolicy | None = None

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "OpportunityShadowConfig":
        mode = OpportunityRegistryMode(str(values.get("opportunity_registry_mode", "off")).lower())
        return cls(
            mode=mode,
            dry_run=bool(values.get("opportunity_registry_dry_run", False)),
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
