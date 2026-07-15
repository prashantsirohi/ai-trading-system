"""Stable external schemas for the Phase 4A read-only contract."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class FreshnessStatus(str, Enum):
    FRESH = "FRESH"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"
    UNAVAILABLE = "UNAVAILABLE"


class LineageRef(BaseModel):
    source_type: str
    source_id: str
    run_id: str | None = None
    content_hash: str | None = None
    schema_version: str | None = None
    available_at: datetime | None = None
    policy_version: str | None = None
    source_as_of: datetime | None = None


class LineageMeta(BaseModel):
    primary: LineageRef | None = None
    supporting: list[LineageRef] = Field(default_factory=list)
    source_consistent: bool = True
    source_version_mismatch: bool = False


class SourceFreshness(BaseModel):
    source_as_of: datetime | None = None
    last_successful_run_at: datetime | None = None
    latest_market_session: date | None = None
    expected_market_session: date | None = None
    staleness_sessions: int | None = None
    freshness_status: FreshnessStatus = FreshnessStatus.UNKNOWN
    freshness_reasons: list[str] = Field(default_factory=list)


class PaginationMeta(BaseModel):
    next_cursor: str | None = None
    has_more: bool = False
    limit: int


class ResponseMeta(BaseModel):
    request_id: str
    generated_at: datetime
    as_of: datetime | None = None
    source_freshness: SourceFreshness = Field(default_factory=SourceFreshness)
    partial: bool = False
    limitations: list[str] = Field(default_factory=list)
    lineage: list[LineageRef] = Field(default_factory=list)
    lineage_meta: LineageMeta = Field(default_factory=LineageMeta)
    freshness: SourceFreshness = Field(default_factory=SourceFreshness)
    pagination: PaginationMeta | None = None


T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    data: T
    meta: ResponseMeta


class ApiError(BaseModel):
    code: str
    message: str
    request_id: str
    details: dict[str, Any] | None = None


class SystemLimitation(BaseModel):
    limitation_id: str
    description: str
    severity: str = "warning"
    development_blocking: bool = False
    production_blocking: bool = True
    remediation: str | None = None


class SystemReadinessResponse(BaseModel):
    readiness_status: str
    phase4_development_ready: bool
    phase4_production_ready: bool
    limitations: list[SystemLimitation]


class VersionResponse(BaseModel):
    api_version: str
    application_version: str
    git_commit: str
    schema_version: str
    readiness_policy_version: str
    routing_policy_version: str
    performance_policy_version: str
    calibration_policy_version: str


class HealthResponse(BaseModel):
    api_ready: bool
    source_readable: bool
    primary_source_readable: bool
    optional_sources: dict[str, str]
    phase4_development_ready: bool
    phase4_production_ready: bool
    limitations: list[str]


class StageObservation(BaseModel):
    model_config = ConfigDict(extra="ignore")
    observation_id: str
    exchange: str | None = None
    symbol_id: str | None = None
    sector_id: str | None = None
    sector_name: str | None = None
    effective_stage: str
    stage_status: str
    stage_confidence: float | None = None
    as_of: datetime | None = None
    available_at: datetime | None = None
    source_week_start: date | None = None
    source_week_end: date | None = None
    membership_trust: str | None = None
    governance_status: str = "AUTHORITATIVE"
    supersedes_observation_id: str | None = None


class MarketStageResponse(BaseModel):
    observations: list[StageObservation]
    conflicts: list[dict[str, Any]] = Field(default_factory=list)


class SectorSummary(StageObservation):
    pass


class StockSummary(StageObservation):
    sector_stage: str | None = None


class RoutingDecisionSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")
    decision_id: str
    exchange: str
    symbol_id: str
    as_of: date
    effective_scan_tier: str
    winning_reason: str
    all_reasons: list[str]
    policy_version: str
    routing_input_hash: str | None = None
    new_long_structural_block: bool = False
    active_position_structural_risk: bool = False
    risk_severity: str | None = None


class RoutingDecisionDetail(RoutingDecisionSummary):
    selection_details: list[dict[str, Any]] = Field(default_factory=list)
    lineage: list[LineageRef] = Field(default_factory=list)


class CandidateSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")
    candidate_id: str
    symbol_id: str
    exchange: str
    setup_family: str
    candidate_state: str
    opened_at: datetime
    closed_at: datetime | None = None
    followthrough_status: str | None = None
    recovered_from_position_state: bool = False
    pre_entry_history_available: bool = True
    recovery_mode: str | None = None
    history_completeness: str = "COMPLETE"


class CandidateDetail(CandidateSummary):
    latest_snapshot: dict[str, Any] | None = None
    correction_impact_status: str | None = None


class CandidateSnapshotResponse(BaseModel):
    snapshot_id: str
    candidate_id: str
    as_of: datetime
    lifecycle_state: str
    followthrough_status: str


class DecisionContextResponse(BaseModel):
    decision_context_id: str
    candidate_id: str
    decided_at: datetime
    action: str
    eligibility: str
    reasons: list[str] = Field(default_factory=list)


class OutcomeAttributionResponse(BaseModel):
    attribution_id: str
    candidate_id: str
    category: str
    confidence: float
    resolved_at: datetime


class PositionCoverageSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")
    position_cycle_id: str
    symbol_id: str
    exchange: str
    coverage_status: str
    position_monitor_present: bool = False
    effective_scan_tier: str | None = None
    routing_decision_id: str | None = None
    market_data_available: bool | None = None
    market_data_complete: bool = False
    last_valid_market_timestamp: datetime | None = None
    expected_market_session: date | None = None
    evidence_complete: bool = False
    investigator_evidence_complete: bool | None = None
    missing_fields: list[str] = Field(default_factory=list)
    missing_data_fields: list[str] = Field(default_factory=list)
    episode_compatibility: str = "unknown"
    episode_match_status: str | None = None
    opportunity_episode_id: str | None = None
    recovery_status: str | None = None
    positive_action_suppressed: bool
    suppression_reasons: list[str] = Field(default_factory=list)
    coverage_reasons: list[str] = Field(default_factory=list)
    policy_version: str | None = None


class AlertSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")
    alert_id: str
    alert_code: str
    severity: str
    status: str
    opened_at: datetime
    resolved_at: datetime | None = None
    dedupe_key: str | None = None
    position_cycle_id: str | None = None
    symbol_id: str | None = None
    missing_field_signature: str | None = None
    recommended_operator_action: str | None = None


class AlertIncidentSummary(AlertSummary):
    occurrence_count: int = 1


class GovernanceCorrectionResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    governance_event_id: str
    observation_id: str
    authority: str
    policy_version: str
    superseded_observation_id: str | None = None
    replacement_observation_id: str | None = None
    recorded_at: datetime
    available_at: datetime


class CorrectionImpactResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    impact_id: str
    candidate_id: str | None = None
    impact_link_status: str
    review_required: bool
    authoritative_calibration_eligible: bool


class CalibrationSummaryResponse(BaseModel):
    manifest_id: str | None = None
    eligible_count: int = 0
    excluded_count: int = 0
    quarantined_count: int = 0
    pending_count: int = 0
    top_exclusion_reasons: list[dict[str, Any]] = Field(default_factory=list)
    total_samples: int = 0
    class_counts: dict[str, int] = Field(default_factory=dict)
    largest_class_share: float | None = None
    date_range: dict[str, Any] = Field(default_factory=dict)
    regime_coverage: dict[str, int] = Field(default_factory=dict)
    stage_coverage: dict[str, Any] = Field(default_factory=dict)
    scan_tier_coverage: dict[str, int] = Field(default_factory=dict)
    setup_family_coverage: dict[str, int] = Field(default_factory=dict)
    policy_version: str | None = None
    formal_verdict: str | None = None
    phase4_development_ready: bool | None = None
    phase4_production_ready: bool | None = None


class CalibrationManifestResponse(BaseModel):
    manifest_id: str | None = None
    policy_version: str
    source_hashes: dict[str, str] = Field(default_factory=dict)
    replay_equivalent: bool | None = None
    schema_versions: dict[str, str] = Field(default_factory=dict)
    migration_lineage: list[Any] = Field(default_factory=list)
    configuration_hash: str | None = None
    policy_hashes: dict[str, str] = Field(default_factory=dict)
    dataset_hashes: dict[str, str] = Field(default_factory=dict)
    row_counts: dict[str, int] = Field(default_factory=dict)
    date_bounds: dict[str, Any] = Field(default_factory=dict)
    reproducibility_status: str | None = None


class PerformanceSummaryResponse(BaseModel):
    run_id: str
    functional_status: str
    performance_status: str
    stage_runtimes: dict[str, float] = Field(default_factory=dict)
    throughput: dict[str, float] = Field(default_factory=dict)
    peak_rss_mb: float | None = None
    database_time_ms: float | None = None
    artifact_metrics: list[dict[str, Any]] = Field(default_factory=list)
    threshold_results: list[dict[str, Any]] = Field(default_factory=list)
    replay_equivalence: str | None = None
    cache_mode: str | None = None
    as_of: datetime | None = None
    policy_version: str | None = None
    replay_mode: str | None = None
    total_runtime_ms: float | None = None
    symbols_processed: int | None = None
    rows_processed: int | None = None
    operation_metrics: list[dict[str, Any]] = Field(default_factory=list)
    database_metrics: list[dict[str, Any]] = Field(default_factory=list)
    replay_comparison: dict[str, Any] | None = None


class ReadinessCheckResponse(BaseModel):
    check_id: str
    category: str
    status: str
    required_for_ready: bool = False
    limitation: str | None = None
    severity: str | None = None
    observed_value: Any | None = None
    expected_condition: str | None = None
    development_blocking: bool = False
    production_blocking: bool = False
    remediation: str | None = None
    policy_version: str | None = None
