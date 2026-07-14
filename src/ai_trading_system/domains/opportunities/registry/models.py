"""Persistence-specific immutable models for the opportunity registry."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from ai_trading_system.domains.opportunities.contracts import (
    CandidateDecision,
    CandidateSnapshot,
    CandidateState,
    DecisionContextSnapshot,
    EvidenceSnapshot,
    OpportunitySnapshot,
    OutcomeAttributionRecord,
    ProgressSnapshot,
    SectorStageSnapshot,
    StageSnapshot,
)


REGISTRY_SCHEMA_VERSION = "opportunity-registry-schema-v1"
REGISTRY_SERIALIZATION_VERSION = "opportunity-serialization-v1"

__all__ = [
    "REGISTRY_SCHEMA_VERSION", "REGISTRY_SERIALIZATION_VERSION", "EpisodeStatus", "AppendStatus",
    "StageScope", "SourceLineage", "CandidateEpisodeRecord", "OpenEpisodeRequest",
    "SnapshotObservation", "StageObservation", "EvidenceObservation", "OpportunityObservation",
    "ProgressObservation", "TransitionObservation", "DecisionContextObservation",
    "AttributionObservation", "AppendResult", "BatchAppendResult", "CandidateCurrentState",
    "TimelineEntry", "CandidateTimeline", "OpportunityRegistryConflictError",
    "EpisodeClosure", "OrchestrationBundle", "OrchestrationBundleResult",
]


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({str(key): item for key, item in value.items()})


class EpisodeStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    FAILED = "FAILED"
    EXITED = "EXITED"
    ARCHIVED = "ARCHIVED"


class AppendStatus(str, Enum):
    CREATED = "CREATED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"


class StageScope(str, Enum):
    STOCK = "STOCK"
    SECTOR = "SECTOR"


@dataclass(frozen=True, slots=True)
class SourceLineage:
    run_id: str
    stage_name: str
    stage_attempt: int
    source_artifact_type: str
    source_artifact_path: str
    source_artifact_hash: str

    def __post_init__(self) -> None:
        for name in (
            "run_id", "stage_name", "source_artifact_type", "source_artifact_path", "source_artifact_hash"
        ):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"{name} must be non-empty")
        if self.stage_attempt < 1:
            raise ValueError("stage_attempt must be at least 1")


@dataclass(frozen=True, slots=True)
class CandidateEpisodeRecord:
    candidate_id: str
    setup_id: str
    symbol_id: str
    exchange: str
    episode_number: int
    episode_type: str
    setup_family: str
    admission_identity: str
    episode_started_at: datetime
    episode_closed_at: datetime | None
    episode_status: EpisodeStatus
    opening_reason: str
    closing_reason: str | None
    created_run_id: str
    created_stage: str
    created_artifact_hash: str
    closed_run_id: str | None
    closed_stage: str | None
    contract_version: str
    schema_version: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OpenEpisodeRequest:
    symbol_id: str
    exchange: str
    setup_family: str
    admission_identity: str
    episode_started_at: datetime
    episode_type: str
    opening_reason: str
    lineage: SourceLineage
    contract_version: str


@dataclass(frozen=True, slots=True)
class SnapshotObservation:
    snapshot: CandidateSnapshot
    observed_at: datetime
    lineage: SourceLineage
    stock_stage_observation_id: str | None = None
    sector_stage_observation_id: str | None = None


@dataclass(frozen=True, slots=True)
class StageObservation:
    candidate_id: str
    setup_id: str
    scope: StageScope
    entity_id: str
    entity_name: str
    snapshot: StageSnapshot | SectorStageSnapshot
    observed_at: datetime
    lineage: SourceLineage


@dataclass(frozen=True, slots=True)
class EvidenceObservation:
    candidate_id: str
    setup_id: str
    as_of: datetime
    observed_at: datetime
    evidence_type: str
    source_module: str
    source_component: str
    snapshot: EvidenceSnapshot
    details: Mapping[str, Any]
    lineage: SourceLineage

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", _freeze_mapping(self.details))


@dataclass(frozen=True, slots=True)
class OpportunityObservation:
    candidate_id: str
    setup_id: str
    as_of: datetime
    observed_at: datetime
    snapshot: OpportunitySnapshot
    lineage: SourceLineage


@dataclass(frozen=True, slots=True)
class ProgressObservation:
    candidate_id: str
    setup_id: str
    as_of: datetime
    snapshot: ProgressSnapshot
    days_without_progress: int
    rule_version: str
    details: Mapping[str, Any]
    lineage: SourceLineage

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", _freeze_mapping(self.details))


@dataclass(frozen=True, slots=True)
class TransitionObservation:
    candidate_id: str
    setup_id: str
    from_state: CandidateState
    to_state: CandidateState
    transition_reason: str
    transitioned_at: datetime
    triggering_snapshot_id: str
    rule_version: str
    metadata: Mapping[str, Any]
    lineage: SourceLineage

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class DecisionContextObservation:
    decision: CandidateDecision
    context: DecisionContextSnapshot
    lineage: SourceLineage


@dataclass(frozen=True, slots=True)
class AttributionObservation:
    attribution: OutcomeAttributionRecord
    lineage: SourceLineage


@dataclass(frozen=True, slots=True)
class AppendResult:
    record_id: str
    status: AppendStatus
    idempotency_key: str
    duplicate: bool
    created: bool


@dataclass(frozen=True, slots=True)
class BatchAppendResult:
    results: tuple[AppendResult, ...]
    created: int
    duplicates: int
    conflicts: int = 0
    rejected: int = 0


@dataclass(frozen=True, slots=True)
class EpisodeClosure:
    status: EpisodeStatus
    closed_at: datetime
    closing_reason: str
    lineage: SourceLineage


@dataclass(frozen=True, slots=True)
class OrchestrationBundle:
    candidate_id: str
    episode_request: OpenEpisodeRequest | None = None
    opportunity: OpportunityObservation | None = None
    evidence: EvidenceObservation | None = None
    stages: tuple[StageObservation, ...] = ()
    progress: ProgressObservation | None = None
    snapshot: SnapshotObservation | None = None
    transition: TransitionObservation | None = None
    closure: EpisodeClosure | None = None


@dataclass(frozen=True, slots=True)
class OrchestrationBundleResult:
    episode: CandidateEpisodeRecord
    append_results: tuple[AppendResult, ...]
    closed: bool


@dataclass(frozen=True, slots=True)
class CandidateCurrentState:
    candidate_id: str
    setup_id: str
    symbol_id: str
    exchange: str
    episode_status: EpisodeStatus
    episode_started_at: datetime
    episode_closed_at: datetime | None
    current_lifecycle_state: str | None = None
    current_followthrough_status: str | None = None
    latest_opportunity_score: float | None = None
    latest_rank_position: int | None = None
    latest_rank_percentile: float | None = None
    latest_rank_velocity: float | None = None
    latest_evidence_score: float | None = None
    latest_evidence_verdict: str | None = None
    current_stock_stage: str | None = None
    current_stock_stage_status: str | None = None
    current_stock_stage_confidence: float | None = None
    current_sector_stage: str | None = None
    current_sector_stage_status: str | None = None
    current_sector_stage_confidence: float | None = None
    current_progress_status: str | None = None
    days_in_state: int | None = None
    days_without_progress: int | None = None
    latest_action: str | None = None
    current_eligibility: str | None = None
    last_snapshot_at: datetime | None = None
    last_transition_at: datetime | None = None
    last_observed_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class TimelineEntry:
    record_type: str
    record_id: str
    event_at: datetime
    created_at: datetime
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _freeze_mapping(self.payload))


@dataclass(frozen=True, slots=True)
class CandidateTimeline:
    episode: CandidateEpisodeRecord
    entries: tuple[TimelineEntry, ...]


class OpportunityRegistryConflictError(RuntimeError):
    def __init__(
        self,
        *,
        record_type: str,
        candidate_id: str,
        idempotency_key: str,
        existing_payload_hash: str,
        incoming_payload_hash: str,
    ) -> None:
        self.record_type = record_type
        self.candidate_id = candidate_id
        self.idempotency_key = idempotency_key
        self.existing_payload_hash = existing_payload_hash
        self.incoming_payload_hash = incoming_payload_hash
        super().__init__(
            f"{record_type} idempotency conflict for {candidate_id}: {idempotency_key} "
            f"({existing_payload_hash} != {incoming_payload_hash})"
        )
