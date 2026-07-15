"""Canonical opportunity lifecycle contracts and pure policy semantics."""

# ruff: noqa: F401, F403

from .attribution import AttributionEvaluation, evaluate_stage_attribution
from .compatibility import (
    CompatibilityResult,
    LegacyWeeklyStageResult,
    adapt_legacy_weekly_stage,
    map_candidate_tracker_progress,
    map_investigator_status,
    map_legacy_evidence_verdict,
    map_legacy_followthrough,
    map_legacy_stage,
    map_stage1_lifecycle,
)
from . import contracts as _contracts
from .contracts import *  # noqa: F403
from .policy import evaluate_early_entry_stage_guard, evaluate_normal_entry_stage_guard
from .coverage import read_sector_stage_as_of, read_stock_stage_as_of
from .routing import (
    OpportunityScanRoutingMode,
    PositionMonitoringConfig,
    ScanProfile,
    ScanReason,
    ScanRoutingConfig,
    ScanRoutingDecision,
    ScanTier,
    StageCoverageConfig,
    StageDiscoveryReason,
    decide_scan_route,
)
from .serialization import from_dict, to_dict, to_json
from .stage_governance import (
    CORRECTION_IMPACT_POLICY_VERSION,
    SECTOR_MEMBERSHIP_POLICY_VERSION,
    STAGE_GOVERNANCE_POLICY_VERSION,
    CorrectionImpactStatus,
    MembershipTrust,
    SectorMembershipRecord,
    StageGovernanceAction,
    StageGovernanceRecord,
    annotate_legacy_stage_history,
    append_sector_memberships,
    read_sector_membership_as_of,
    resolve_historical_sector_mapping,
)
from .validation import (
    calculate_stage_confidence,
    confidence_band_for_score,
    default_candidate_retention_policy,
    derive_monitoring_stage,
    select_stage_for_use,
    validate_candidate_decision,
)

__all__ = [
    *_contracts.__all__,
    "AttributionEvaluation",
    "evaluate_stage_attribution",
    "CompatibilityResult",
    "LegacyWeeklyStageResult",
    "adapt_legacy_weekly_stage",
    "map_candidate_tracker_progress",
    "map_investigator_status",
    "map_legacy_evidence_verdict",
    "map_legacy_followthrough",
    "map_legacy_stage",
    "map_stage1_lifecycle",
    "evaluate_early_entry_stage_guard",
    "evaluate_normal_entry_stage_guard",
    "read_sector_stage_as_of",
    "read_stock_stage_as_of",
    "from_dict",
    "to_dict",
    "to_json",
    "calculate_stage_confidence",
    "confidence_band_for_score",
    "default_candidate_retention_policy",
    "derive_monitoring_stage",
    "select_stage_for_use",
    "validate_candidate_decision",
    "OpportunityScanRoutingMode",
    "PositionMonitoringConfig",
    "ScanProfile",
    "ScanReason",
    "ScanRoutingConfig",
    "ScanRoutingDecision",
    "ScanTier",
    "StageCoverageConfig",
    "StageDiscoveryReason",
    "decide_scan_route",
    "CORRECTION_IMPACT_POLICY_VERSION",
    "SECTOR_MEMBERSHIP_POLICY_VERSION",
    "STAGE_GOVERNANCE_POLICY_VERSION",
    "CorrectionImpactStatus",
    "MembershipTrust",
    "SectorMembershipRecord",
    "StageGovernanceAction",
    "StageGovernanceRecord",
    "annotate_legacy_stage_history",
    "append_sector_memberships",
    "read_sector_membership_as_of",
    "resolve_historical_sector_mapping",
]
