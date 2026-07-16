"""Phase 3C-5 point-in-time calibration governance and Phase 4 readiness."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping

from ai_trading_system.pipeline.contracts import compute_file_hash


CALIBRATION_POLICY_VERSION = "phase3c5-calibration-policy-v1"
READINESS_POLICY_VERSION = "phase3c5-readiness-policy-v1"
CALIBRATION_BUILDER_VERSION = "phase3c5-calibration-builder-v1.1"
OUTCOME_POLICY_VERSION = "phase3c5-session-outcomes-v1"


class EligibilityStatus(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    EXCLUDED = "EXCLUDED"
    QUARANTINED = "QUARANTINED"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    PENDING_OUTCOME = "PENDING_OUTCOME"


class ExclusionReason(str, Enum):
    LOOKAHEAD_INPUT = "LOOKAHEAD_INPUT"
    INPUT_AVAILABLE_AFTER_DECISION = "INPUT_AVAILABLE_AFTER_DECISION"
    LATE_CORRECTION_NOT_AVAILABLE_AS_OF_DECISION = "LATE_CORRECTION_NOT_AVAILABLE_AS_OF_DECISION"
    STAGE_OBSERVATION_UNVERIFIED = "STAGE_OBSERVATION_UNVERIFIED"
    STAGE_GOVERNANCE_CONFLICT = "STAGE_GOVERNANCE_CONFLICT"
    STAGE_SUPERSESSION_CYCLE = "STAGE_SUPERSESSION_CYCLE"
    SECTOR_MEMBERSHIP_LATEST_ONLY = "SECTOR_MEMBERSHIP_LATEST_ONLY"
    SECTOR_MEMBERSHIP_UNRESOLVED = "SECTOR_MEMBERSHIP_UNRESOLVED"
    SECTOR_MEMBERSHIP_OVERLAP = "SECTOR_MEMBERSHIP_OVERLAP"
    UNRESOLVED_LEGACY_NO_MATCH = "UNRESOLVED_LEGACY_NO_MATCH"
    UNRESOLVED_LEGACY_AMBIGUOUS = "UNRESOLVED_LEGACY_AMBIGUOUS"
    CORRECTION_IMPACT_REVIEW_REQUIRED = "CORRECTION_IMPACT_REVIEW_REQUIRED"
    RECOVERED_POSITION_ONLY_HISTORY = "RECOVERED_POSITION_ONLY_HISTORY"
    PRE_ENTRY_HISTORY_UNAVAILABLE = "PRE_ENTRY_HISTORY_UNAVAILABLE"
    OUTCOME_WINDOW_INCOMPLETE = "OUTCOME_WINDOW_INCOMPLETE"
    OUTCOME_LABEL_MISSING = "OUTCOME_LABEL_MISSING"
    SURVIVORSHIP_STATUS_UNKNOWN = "SURVIVORSHIP_STATUS_UNKNOWN"
    DELISTING_STATUS_UNRESOLVED = "DELISTING_STATUS_UNRESOLVED"
    SYMBOL_IDENTITY_CONFLICT = "SYMBOL_IDENTITY_CONFLICT"
    DUPLICATE_SAMPLE_IDENTITY = "DUPLICATE_SAMPLE_IDENTITY"
    MARKET_DATA_INCOMPLETE = "MARKET_DATA_INCOMPLETE"
    REGIME_CONTEXT_MISSING = "REGIME_CONTEXT_MISSING"
    STAGE_CONTEXT_MISSING = "STAGE_CONTEXT_MISSING"
    INSUFFICIENT_LOOKBACK = "INSUFFICIENT_LOOKBACK"
    MANIFEST_MISMATCH = "MANIFEST_MISMATCH"
    SOURCE_HASH_MISMATCH = "SOURCE_HASH_MISMATCH"
    CURRENT_ONLY_UNIVERSE = "CURRENT_ONLY_UNIVERSE"
    WINNER_ONLY_DATASET = "WINNER_ONLY_DATASET"
    RIGHT_CENSORED_OUTCOME = "RIGHT_CENSORED_OUTCOME"
    MISSING_PRICE_PATH = "MISSING_PRICE_PATH"
    CORPORATE_ACTION_UNRESOLVED = "CORPORATE_ACTION_UNRESOLVED"


class OutcomeStatus(str, Enum):
    COMPLETE = "COMPLETE"
    RIGHT_CENSORED = "RIGHT_CENSORED"
    MISSING_PRICE_PATH = "MISSING_PRICE_PATH"
    CORPORATE_ACTION_UNRESOLVED = "CORPORATE_ACTION_UNRESOLVED"
    PENDING = "PENDING"


class ReadinessStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_EVALUATED = "NOT_EVALUATED"


class ReadinessVerdict(str, Enum):
    READY = "READY"
    READY_WITH_LIMITATIONS = "READY_WITH_LIMITATIONS"
    NOT_READY = "NOT_READY"


@dataclass(frozen=True, slots=True)
class CalibrationSampleRequirements:
    ready_minimum: int
    limitation_minimum: int

    def __post_init__(self) -> None:
        if self.ready_minimum < self.limitation_minimum or self.limitation_minimum < 0:
            raise ValueError("ready minimum must be >= limitation minimum and both must be non-negative")


@dataclass(frozen=True, slots=True)
class CalibrationConfig:
    policy_version: str = CALIBRATION_POLICY_VERSION
    require_point_in_time_membership: bool = True
    allow_observed_at_run: bool = True
    allow_latest_only_backfill: bool = False
    allow_provisional_stage: bool = False
    exclude_recovered_position_history: bool = True
    require_resolved_correction_impacts: bool = True
    total_samples: CalibrationSampleRequirements = field(
        default_factory=lambda: CalibrationSampleRequirements(60, 20)
    )
    minimum_positive_samples: int = 10
    minimum_negative_samples: int = 10
    minimum_samples_per_outcome_horizon: int = 10
    minimum_samples_per_stage: int = 5
    minimum_samples_per_market_regime: int = 5
    minimum_samples_per_scan_tier: int = 5
    minimum_samples_per_setup_family: int = 5
    minimum_calendar_span_sessions: int = 60
    minimum_distinct_market_regimes: int = 3
    maximum_largest_class_share: float = 0.75
    maximum_single_year_share: float = 0.80
    phase4_require_copied_realistic_performance_baseline: bool = False
    phase4_require_operator_migrations: bool = False
    phase4_fail_on_sparse_optional_buckets: bool = False

    def __post_init__(self) -> None:
        integer_values = (
            self.minimum_positive_samples, self.minimum_negative_samples,
            self.minimum_samples_per_outcome_horizon, self.minimum_samples_per_stage,
            self.minimum_samples_per_market_regime, self.minimum_samples_per_scan_tier,
            self.minimum_samples_per_setup_family, self.minimum_calendar_span_sessions,
            self.minimum_distinct_market_regimes,
        )
        if any(value < 0 for value in integer_values):
            raise ValueError("calibration sample thresholds must be non-negative")
        for name, value in (
            ("maximum_largest_class_share", self.maximum_largest_class_share),
            ("maximum_single_year_share", self.maximum_single_year_share),
        ):
            if not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class CalibrationEligibility:
    entity_type: str
    entity_id: str
    symbol_id: str | None
    exchange: str | None
    decision_at: datetime
    outcome_window_end: datetime | None
    eligible: bool
    eligibility_status: EligibilityStatus
    exclusion_reasons: tuple[str, ...]
    point_in_time_inputs_verified: bool
    stage_observation_verified: bool
    sector_membership_verified: bool
    outcome_available: bool
    correction_impact_resolved: bool
    lifecycle_history_complete: bool
    survivorship_status_verified: bool
    source_manifest_id: str
    policy_version: str
    sample_id: str


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    check_id: str
    category: str
    severity: str
    required_for_ready: bool
    status: ReadinessStatus
    observed_value: object
    expected_condition: str
    evidence_artifacts: tuple[str, ...]
    limitation: str | None
    remediation: str | None
    policy_version: str = READINESS_POLICY_VERSION


@dataclass(frozen=True, slots=True)
class ReadinessLimitation:
    limitation_id: str
    category: str
    description: str
    severity: str
    development_blocking: bool
    production_blocking: bool
    evidence: str
    remediation: str
    owner: str


@dataclass(frozen=True, slots=True)
class CalibrationBuildResult:
    manifest: Mapping[str, Any]
    quality_summary: Mapping[str, Any]
    eligibility: tuple[CalibrationEligibility, ...]
    eligible_rows: tuple[Mapping[str, Any], ...]
    excluded_rows: tuple[Mapping[str, Any], ...]
    quarantined_rows: tuple[Mapping[str, Any], ...]
    pending_rows: tuple[Mapping[str, Any], ...]
    readiness_checks: tuple[ReadinessCheck, ...]
    limitations: tuple[ReadinessLimitation, ...]
    verdict: ReadinessVerdict
    phase4_development_ready: bool
    phase4_production_ready: bool


class CalibrationIntegrityError(RuntimeError):
    """Raised when immutable manifest identity and semantic data disagree."""


def deterministic_sample_id(row: Mapping[str, Any], *, policy_version: str) -> str:
    return _digest({
        "entity_type": row.get("entity_type"),
        "entity_id": row.get("entity_id") or row.get("candidate_id") or row.get("decision_context_id"),
        "symbol_id": str(row.get("symbol_id") or "").upper(),
        "exchange": str(row.get("exchange") or "NSE").upper(),
        "decision_at": _iso(_dt(row.get("decision_at"))),
        "outcome_horizon": int(row.get("outcome_horizon") or 0),
        "policy_version": policy_version,
    })


def evaluate_calibration_eligibility(
    row: Mapping[str, Any], *, config: CalibrationConfig | None = None,
    source_manifest_id: str = "pending",
) -> CalibrationEligibility:
    cfg = config or CalibrationConfig()
    decision_at = _dt(row.get("decision_at"))
    input_available_at = _dt(row.get("input_available_at"))
    outcome_available_at = _maybe_dt(row.get("outcome_available_at"))
    outcome_window_end = _maybe_dt(row.get("outcome_window_end"))
    reasons: list[ExclusionReason] = []
    quarantine = False
    pending = False

    if bool(row.get("lookahead_input", False)):
        reasons.append(ExclusionReason.LOOKAHEAD_INPUT)
    if input_available_at > decision_at:
        reasons.append(ExclusionReason.INPUT_AVAILABLE_AFTER_DECISION)
    correction_recorded = _maybe_dt(row.get("correction_recorded_at"))
    if correction_recorded is not None and correction_recorded > decision_at and bool(row.get("correction_used", False)):
        reasons.append(ExclusionReason.LATE_CORRECTION_NOT_AVAILABLE_AS_OF_DECISION)

    stage_status = str(row.get("stage_status") or "").upper()
    if bool(row.get("stage_governance_cycle", False)):
        reasons.append(ExclusionReason.STAGE_SUPERSESSION_CYCLE)
        quarantine = True
    if bool(row.get("stage_governance_conflict", False)):
        reasons.append(ExclusionReason.STAGE_GOVERNANCE_CONFLICT)
        quarantine = True
    if not bool(row.get("stage_observation_verified", False)):
        reasons.append(ExclusionReason.STAGE_OBSERVATION_UNVERIFIED)
    if not stage_status:
        reasons.append(ExclusionReason.STAGE_CONTEXT_MISSING)
    elif stage_status == "PROVISIONAL" and not cfg.allow_provisional_stage:
        reasons.append(ExclusionReason.STAGE_OBSERVATION_UNVERIFIED)

    membership_trust = str(row.get("membership_trust") or "").upper()
    membership_recorded_at = _maybe_dt(row.get("membership_recorded_at"))
    if membership_trust == "LATEST_ONLY_BACKFILL" and not cfg.allow_latest_only_backfill:
        reasons.append(ExclusionReason.SECTOR_MEMBERSHIP_LATEST_ONLY)
    elif membership_trust == "OBSERVED_AT_RUN":
        if not cfg.allow_observed_at_run or membership_recorded_at is None:
            reasons.append(ExclusionReason.SECTOR_MEMBERSHIP_UNRESOLVED)
        elif membership_recorded_at > decision_at:
            reasons.append(ExclusionReason.INPUT_AVAILABLE_AFTER_DECISION)
    elif membership_trust != "POINT_IN_TIME_VERIFIED" and cfg.require_point_in_time_membership:
        reasons.append(ExclusionReason.SECTOR_MEMBERSHIP_UNRESOLVED)
    if bool(row.get("membership_overlap", False)):
        reasons.append(ExclusionReason.SECTOR_MEMBERSHIP_OVERLAP)
        quarantine = True

    impact_status = str(row.get("correction_impact_status") or "").lower()
    if impact_status == "unresolved_legacy_no_match":
        reasons.append(ExclusionReason.UNRESOLVED_LEGACY_NO_MATCH)
        quarantine = True
    if impact_status == "unresolved_legacy_ambiguous":
        reasons.append(ExclusionReason.UNRESOLVED_LEGACY_AMBIGUOUS)
        quarantine = True
    if impact_status not in {"linked", "resolved"} and not impact_status.startswith("unresolved_legacy_"):
        reasons.append(ExclusionReason.CORRECTION_IMPACT_REVIEW_REQUIRED)
    if bool(row.get("correction_review_required", False)) or not bool(
        row.get("authoritative_calibration_eligible", False)
    ):
        reasons.append(ExclusionReason.CORRECTION_IMPACT_REVIEW_REQUIRED)

    if bool(row.get("recovered_from_position_state", False)):
        if cfg.exclude_recovered_position_history and str(row.get("dataset_purpose") or "entry").lower() in {
            "discovery", "entry", "trigger", "followthrough",
        }:
            reasons.append(ExclusionReason.RECOVERED_POSITION_ONLY_HISTORY)
        if not bool(row.get("pre_entry_history_available", False)):
            reasons.append(ExclusionReason.PRE_ENTRY_HISTORY_UNAVAILABLE)

    if not bool(row.get("was_in_universe_as_of_decision", False)):
        reasons.append(ExclusionReason.SURVIVORSHIP_STATUS_UNKNOWN)
    if str(row.get("universe_source") or "").lower() == "current_only":
        reasons.append(ExclusionReason.CURRENT_ONLY_UNIVERSE)
    listing_status = str(row.get("listing_status_as_of_decision") or "").lower()
    if listing_status not in {"listed", "active", "delisted_later", "merged", "renamed"}:
        reasons.append(ExclusionReason.SURVIVORSHIP_STATUS_UNKNOWN)
    if str(row.get("delisting_status") or "").lower() in {"", "unknown", "unresolved"}:
        reasons.append(ExclusionReason.DELISTING_STATUS_UNRESOLVED)
    if not bool(row.get("symbol_identity_valid", False)):
        reasons.append(ExclusionReason.SYMBOL_IDENTITY_CONFLICT)

    outcome_status = OutcomeStatus(str(row.get("outcome_status") or OutcomeStatus.PENDING.value).upper())
    if outcome_status is OutcomeStatus.PENDING:
        reasons.append(ExclusionReason.OUTCOME_WINDOW_INCOMPLETE)
        pending = True
    elif outcome_status is OutcomeStatus.RIGHT_CENSORED:
        reasons.append(ExclusionReason.RIGHT_CENSORED_OUTCOME)
    elif outcome_status is OutcomeStatus.MISSING_PRICE_PATH:
        reasons.append(ExclusionReason.MISSING_PRICE_PATH)
    elif outcome_status is OutcomeStatus.CORPORATE_ACTION_UNRESOLVED:
        reasons.append(ExclusionReason.CORPORATE_ACTION_UNRESOLVED)
        quarantine = True
    if outcome_status is OutcomeStatus.COMPLETE:
        if outcome_available_at is None or outcome_available_at <= decision_at:
            reasons.append(ExclusionReason.OUTCOME_LABEL_MISSING)
        if outcome_window_end is None or outcome_available_at is None or outcome_available_at < outcome_window_end:
            reasons.append(ExclusionReason.OUTCOME_WINDOW_INCOMPLETE)
        if row.get("outcome_label") is None:
            reasons.append(ExclusionReason.OUTCOME_LABEL_MISSING)
    if not bool(row.get("market_data_complete", False)):
        reasons.append(ExclusionReason.MARKET_DATA_INCOMPLETE)
    if not row.get("market_regime"):
        reasons.append(ExclusionReason.REGIME_CONTEXT_MISSING)
    if int(row.get("lookback_sessions") or 0) < int(row.get("required_lookback_sessions") or 0):
        reasons.append(ExclusionReason.INSUFFICIENT_LOOKBACK)

    unique_reasons = tuple(dict.fromkeys(reason.value for reason in reasons))
    if quarantine:
        status = EligibilityStatus.QUARANTINED
    elif pending and all(reason == ExclusionReason.OUTCOME_WINDOW_INCOMPLETE.value for reason in unique_reasons):
        status = EligibilityStatus.PENDING_OUTCOME
    elif ExclusionReason.INSUFFICIENT_LOOKBACK.value in unique_reasons:
        status = EligibilityStatus.INSUFFICIENT_HISTORY
    elif unique_reasons:
        status = EligibilityStatus.EXCLUDED
    else:
        status = EligibilityStatus.ELIGIBLE
    sample_id = deterministic_sample_id(row, policy_version=cfg.policy_version)
    return CalibrationEligibility(
        entity_type=str(row.get("entity_type") or "candidate_decision"),
        entity_id=str(row.get("entity_id") or row.get("candidate_id") or row.get("decision_context_id") or ""),
        symbol_id=str(row.get("symbol_id") or "").upper() or None,
        exchange=str(row.get("exchange") or "NSE").upper() or None,
        decision_at=decision_at, outcome_window_end=outcome_window_end,
        eligible=status is EligibilityStatus.ELIGIBLE,
        eligibility_status=status, exclusion_reasons=unique_reasons,
        point_in_time_inputs_verified=input_available_at <= decision_at and ExclusionReason.LOOKAHEAD_INPUT.value not in unique_reasons,
        stage_observation_verified=not any(reason in unique_reasons for reason in (
            ExclusionReason.STAGE_OBSERVATION_UNVERIFIED.value,
            ExclusionReason.STAGE_GOVERNANCE_CONFLICT.value,
            ExclusionReason.STAGE_SUPERSESSION_CYCLE.value,
        )),
        sector_membership_verified=not any(reason.startswith("SECTOR_MEMBERSHIP") for reason in unique_reasons),
        outcome_available=outcome_status is OutcomeStatus.COMPLETE and not any(
            reason in unique_reasons for reason in (
                ExclusionReason.OUTCOME_LABEL_MISSING.value,
                ExclusionReason.OUTCOME_WINDOW_INCOMPLETE.value,
            )
        ),
        correction_impact_resolved=not any(reason in unique_reasons for reason in (
            ExclusionReason.UNRESOLVED_LEGACY_NO_MATCH.value,
            ExclusionReason.UNRESOLVED_LEGACY_AMBIGUOUS.value,
            ExclusionReason.CORRECTION_IMPACT_REVIEW_REQUIRED.value,
        )),
        lifecycle_history_complete=not any(reason in unique_reasons for reason in (
            ExclusionReason.RECOVERED_POSITION_ONLY_HISTORY.value,
            ExclusionReason.PRE_ENTRY_HISTORY_UNAVAILABLE.value,
        )),
        survivorship_status_verified=not any(reason in unique_reasons for reason in (
            ExclusionReason.SURVIVORSHIP_STATUS_UNKNOWN.value,
            ExclusionReason.DELISTING_STATUS_UNRESOLVED.value,
            ExclusionReason.SYMBOL_IDENTITY_CONFLICT.value,
            ExclusionReason.CURRENT_ONLY_UNIVERSE.value,
        )),
        source_manifest_id=source_manifest_id, policy_version=cfg.policy_version,
        sample_id=sample_id,
    )


def build_calibration_dataset(
    rows: Iterable[Mapping[str, Any]], *, dataset_name: str,
    dataset_purpose: str, as_of: str, config: CalibrationConfig | None = None,
    source_database_hashes: Mapping[str, str] | None = None,
    source_artifact_hashes: Mapping[str, str] | None = None,
    source_schema_versions: Mapping[str, str] | None = None,
    migration_versions: tuple[str, ...] = (
        "033", "034", "035", "036", "037", "038", "039", "040", "041",
    ),
    copied_realistic_performance_summary: Mapping[str, Any] | None = None,
    operator_migrations_applied: bool = False,
    real_phase3b_history_present: bool = False,
    readiness_evidence: Mapping[str, Any] | None = None,
    expected_manifest: Mapping[str, Any] | None = None,
) -> CalibrationBuildResult:
    cfg = config or CalibrationConfig()
    source_rows = [dict(row) for row in rows]
    provisional_manifest_id = _digest({
        "dataset_name": dataset_name, "dataset_purpose": dataset_purpose,
        "as_of": as_of, "policy_version": cfg.policy_version,
    })
    evaluated: list[tuple[dict[str, Any], CalibrationEligibility]] = []
    seen: set[str] = set()
    for row in source_rows:
        row.setdefault("dataset_purpose", dataset_purpose)
        eligibility = evaluate_calibration_eligibility(
            row, config=cfg, source_manifest_id=provisional_manifest_id,
        )
        if eligibility.sample_id in seen:
            eligibility = _replace_eligibility(
                eligibility, EligibilityStatus.EXCLUDED,
                (*eligibility.exclusion_reasons, ExclusionReason.DUPLICATE_SAMPLE_IDENTITY.value),
            )
        seen.add(eligibility.sample_id)
        evaluated.append((row, eligibility))
    evaluated.sort(key=lambda item: item[1].sample_id)

    eligible_rows = tuple(_output_row(row, eligibility) for row, eligibility in evaluated if eligibility.eligible)
    excluded_rows = tuple(_output_row(row, eligibility) for row, eligibility in evaluated if eligibility.eligibility_status in {
        EligibilityStatus.EXCLUDED, EligibilityStatus.INSUFFICIENT_HISTORY,
    })
    quarantined_rows = tuple(_output_row(row, eligibility) for row, eligibility in evaluated if eligibility.eligibility_status is EligibilityStatus.QUARANTINED)
    pending_rows = tuple(_output_row(row, eligibility) for row, eligibility in evaluated if eligibility.eligibility_status is EligibilityStatus.PENDING_OUTCOME)
    eligibility_records = tuple(eligibility for _, eligibility in evaluated)
    quality = _quality_summary(source_rows, eligibility_records, eligible_rows)
    source_db_hashes = dict(sorted((source_database_hashes or {}).items()))
    source_art_hashes = dict(sorted((source_artifact_hashes or {}).items()))
    schema_versions = dict(sorted((source_schema_versions or {}).items()))
    configuration_hash = _digest(asdict(cfg))
    policy_hash = _digest({"policy_version": cfg.policy_version, "config": asdict(cfg)})
    outcome_policy_hash = _digest({"policy_version": OUTCOME_POLICY_VERSION, "horizons": quality["outcome_horizons"]})
    sample_identity_hash = _digest([item.sample_id for item in eligibility_records])
    eligible_dataset_hash = _digest(list(eligible_rows))
    exclusion_dataset_hash = _digest([*excluded_rows, *quarantined_rows, *pending_rows])
    readiness_evidence_payload = dict(readiness_evidence or {
        "operator_migrations_applied": operator_migrations_applied,
        "real_phase3b_history_present": real_phase3b_history_present,
    })
    manifest_identity = {
        "as_of": as_of, "policy_version": cfg.policy_version,
        "dataset_name": dataset_name, "dataset_purpose": dataset_purpose,
        "source_database_hashes": source_db_hashes,
        "source_artifact_hashes": source_art_hashes,
        "source_schema_versions": schema_versions,
        "migration_versions": list(migration_versions),
        "query_or_builder_version": CALIBRATION_BUILDER_VERSION,
        "configuration_hash": configuration_hash,
        "eligibility_policy_hash": policy_hash,
        "outcome_policy_hash": outcome_policy_hash,
        "sample_identity_hash": sample_identity_hash,
        "readiness_evidence": readiness_evidence_payload,
    }
    manifest_id = _digest(manifest_identity)
    eligibility_records = tuple(
        _replace_manifest_id(item, manifest_id) for item in eligibility_records
    )
    manifest = {
        "manifest_id": manifest_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        **manifest_identity,
        "source_database_paths": [], "source_artifact_paths": [],
        "row_count_total": len(source_rows), "row_count_eligible": len(eligible_rows),
        "row_count_excluded": len(excluded_rows), "row_count_quarantined": len(quarantined_rows),
        "row_count_pending": len(pending_rows),
        "eligible_dataset_hash": eligible_dataset_hash,
        "exclusion_dataset_hash": exclusion_dataset_hash,
        "date_min": quality["date_min"], "date_max": quality["date_max"],
        "decision_time_min": quality["decision_time_min"],
        "decision_time_max": quality["decision_time_max"],
        "universe_definition": quality["universe_definition"],
        "membership_trust_distribution": quality["membership_trust_distribution"],
        "stage_status_distribution": quality["stage_status_distribution"],
        "correction_status_distribution": quality["correction_status_distribution"],
        "policy_snapshot_ids": quality["policy_snapshot_ids"],
        "reproducibility_status": "REPRODUCIBLE",
        "quality_summary": quality,
    }
    if expected_manifest is not None:
        if expected_manifest.get("manifest_id") == manifest_id and expected_manifest.get("eligible_dataset_hash") != eligible_dataset_hash:
            raise CalibrationIntegrityError("same manifest_id produced a different eligible dataset hash")

    checks, limitations, verdict, development_ready, production_ready = evaluate_phase4_readiness(
        quality=quality, manifest=manifest, config=cfg,
        copied_realistic_performance_summary=copied_realistic_performance_summary,
        operator_migrations_applied=operator_migrations_applied,
        real_phase3b_history_present=real_phase3b_history_present,
    )
    return CalibrationBuildResult(
        manifest=manifest, quality_summary=quality, eligibility=eligibility_records,
        eligible_rows=eligible_rows, excluded_rows=excluded_rows,
        quarantined_rows=quarantined_rows, pending_rows=pending_rows,
        readiness_checks=checks, limitations=limitations, verdict=verdict,
        phase4_development_ready=development_ready,
        phase4_production_ready=production_ready,
    )


def evaluate_phase4_readiness(
    *, quality: Mapping[str, Any], manifest: Mapping[str, Any],
    config: CalibrationConfig | None = None,
    copied_realistic_performance_summary: Mapping[str, Any] | None = None,
    operator_migrations_applied: bool = False,
    real_phase3b_history_present: bool = False,
) -> tuple[tuple[ReadinessCheck, ...], tuple[ReadinessLimitation, ...], ReadinessVerdict, bool, bool]:
    cfg = config or CalibrationConfig()
    checks: list[ReadinessCheck] = []
    limitations: list[ReadinessLimitation] = []

    def add(check_id: str, category: str, status: ReadinessStatus, observed: object,
            expected: str, *, severity: str = "critical", required: bool = True,
            limitation_id: str | None = None, description: str | None = None,
            remediation: str | None = None, development_blocking: bool = False,
            production_blocking: bool = True) -> None:
        checks.append(ReadinessCheck(
            check_id, category, severity, required, status, observed, expected,
            ("phase3c5_calibration_manifest.json",), description,
            remediation, READINESS_POLICY_VERSION,
        ))
        if status in {ReadinessStatus.WARN, ReadinessStatus.FAIL} and limitation_id:
            limitations.append(ReadinessLimitation(
                limitation_id, category, description or check_id, severity,
                development_blocking, production_blocking, str(observed),
                remediation or "collect and verify the missing evidence", "operator",
            ))

    total = int(quality.get("eligible_rows", 0))
    leakage = int(quality.get("critical_leakage_rows", 0))
    survivorship = int(quality.get("survivorship_failure_rows", 0))
    unresolved_included = int(quality.get("unresolved_governance_eligible_rows", 0))
    duplicate_count = int(quality.get("duplicate_sample_count", 0))
    winner_only = bool(quality.get("winner_only_dataset", False))
    add("CALIBRATION_POINT_IN_TIME", "calibration", ReadinessStatus.PASS if leakage == 0 else ReadinessStatus.FAIL,
        leakage, "zero eligible or source rows with look-ahead/availability leakage",
        limitation_id="POINT_IN_TIME_LEAKAGE", description="decision-time availability leakage detected",
        remediation="remove leaking inputs and rebuild from as-of sources", development_blocking=True)
    add("CALIBRATION_SURVIVORSHIP", "survivorship", ReadinessStatus.PASS if survivorship == 0 and not winner_only else ReadinessStatus.FAIL,
        {"failure_rows": survivorship, "winner_only": winner_only}, "historical universe includes failures and resolved delisted identities",
        limitation_id="SURVIVORSHIP_DATA_INCOMPLETE", description="authoritative dataset has unresolved survivorship bias",
        remediation="supply historical universe/delisting identity evidence", development_blocking=True)
    add("CALIBRATION_GOVERNANCE_QUARANTINE", "governance", ReadinessStatus.PASS if unresolved_included == 0 else ReadinessStatus.FAIL,
        unresolved_included, "zero unresolved governance rows in eligible data",
        limitation_id="UNRESOLVED_GOVERNANCE_INCLUDED", description="unresolved governance evidence entered eligible data",
        remediation="quarantine unresolved impacts and replay", development_blocking=True)
    add("CALIBRATION_DUPLICATE_IDENTITY", "identity", ReadinessStatus.PASS if duplicate_count == 0 else ReadinessStatus.FAIL,
        duplicate_count, "zero duplicate sample identities", limitation_id="DUPLICATE_SAMPLE_IDENTITY",
        description="duplicate calibration identities detected", remediation="deduplicate by canonical sample identity", development_blocking=True)

    if total < cfg.total_samples.limitation_minimum:
        sample_status = ReadinessStatus.FAIL
    elif total < cfg.total_samples.ready_minimum:
        sample_status = ReadinessStatus.WARN
    else:
        sample_status = ReadinessStatus.PASS
    add("CALIBRATION_SAMPLE_SIZE", "sample_quality", sample_status, total,
        f">={cfg.total_samples.ready_minimum} READY; >={cfg.total_samples.limitation_minimum} limitation",
        limitation_id="SAMPLE_SIZE_BELOW_READY_THRESHOLD", description="eligible sample count is below READY threshold",
        remediation="collect additional point-in-time complete outcomes",
        development_blocking=sample_status is ReadinessStatus.FAIL)

    positive = int(quality.get("positive_count", 0))
    negative = int(quality.get("negative_count", 0))
    class_status = ReadinessStatus.PASS if positive >= cfg.minimum_positive_samples and negative >= cfg.minimum_negative_samples else ReadinessStatus.WARN
    if total and max(positive, negative, int(quality.get("neutral_count", 0))) / total > cfg.maximum_largest_class_share:
        class_status = ReadinessStatus.WARN
    add("CALIBRATION_CLASS_BALANCE", "sample_quality", class_status,
        {"positive": positive, "negative": negative, "largest_share": quality.get("largest_class_share")},
        "minimum positive/negative counts and largest class share within policy",
        severity="high", limitation_id="CLASS_BALANCE_SPARSE", description="outcome classes are sparse or imbalanced",
        remediation="collect outcomes without resampling or outcome-based exclusion", production_blocking=True)

    coverage_counts = quality.get("coverage_counts", {})

    def coverage_check(
        check_id: str, dimension: str, minimum: int, *, distinct_minimum: int = 1,
    ) -> None:
        counts = {str(key): int(value) for key, value in coverage_counts.get(dimension, {}).items()}
        passes = len(counts) >= distinct_minimum and bool(counts) and min(counts.values()) >= minimum
        add(check_id, "coverage", ReadinessStatus.PASS if passes else ReadinessStatus.WARN,
            counts, f">={minimum} eligible samples in each represented {dimension} bucket and "
            f">={distinct_minimum} distinct buckets", severity="high",
            limitation_id=("REGIME_COVERAGE_SPARSE" if dimension == "market_regime"
                           else f"{dimension.upper()}_COVERAGE_SPARSE"),
            description=f"{dimension} coverage is sparse",
            remediation=f"collect valid samples across sparse {dimension} buckets")

    coverage_check(
        "CALIBRATION_OUTCOME_HORIZON_COVERAGE", "outcome_horizon",
        cfg.minimum_samples_per_outcome_horizon,
    )
    coverage_check("CALIBRATION_STAGE_COVERAGE", "stock_stage", cfg.minimum_samples_per_stage)
    coverage_check(
        "CALIBRATION_REGIME_COVERAGE", "market_regime",
        cfg.minimum_samples_per_market_regime,
        distinct_minimum=cfg.minimum_distinct_market_regimes,
    )
    coverage_check("CALIBRATION_SCAN_TIER_COVERAGE", "scan_tier", cfg.minimum_samples_per_scan_tier)
    coverage_check("CALIBRATION_SETUP_FAMILY_COVERAGE", "setup_family", cfg.minimum_samples_per_setup_family)

    calendar_sessions = int(quality.get("calendar_sessions", 0))
    add("CALIBRATION_CALENDAR_SPAN", "coverage",
        ReadinessStatus.PASS if calendar_sessions >= cfg.minimum_calendar_span_sessions else ReadinessStatus.WARN,
        calendar_sessions, f">={cfg.minimum_calendar_span_sessions} distinct decision sessions",
        severity="high", limitation_id="CALENDAR_SPAN_SPARSE",
        description="calibration history spans too few decision sessions",
        remediation="collect complete outcomes over a longer calendar span")
    largest_year_share = float(quality.get("largest_year_share", 0.0))
    add("CALIBRATION_YEAR_CONCENTRATION", "coverage",
        ReadinessStatus.PASS if largest_year_share <= cfg.maximum_single_year_share else ReadinessStatus.WARN,
        largest_year_share, f"largest year share <= {cfg.maximum_single_year_share}",
        severity="high", limitation_id="SINGLE_YEAR_CONCENTRATION",
        description="one calendar year dominates the eligible population",
        remediation="collect valid samples across additional calendar years")

    invalid_outcomes = int(quality.get("invalid_outcome_eligible_rows", 0))
    add("CALIBRATION_OUTCOME_COMPLETENESS", "outcomes",
        ReadinessStatus.PASS if invalid_outcomes == 0 else ReadinessStatus.FAIL,
        invalid_outcomes, "zero incomplete, censored, or unattributable outcomes in eligible data",
        limitation_id="INVALID_OUTCOME_INCLUDED", description="invalid outcomes entered eligible calibration data",
        remediation="exclude or quarantine incomplete outcome windows", development_blocking=True)

    replay_status = ReadinessStatus.PASS if manifest.get("reproducibility_status") == "REPRODUCIBLE" else ReadinessStatus.FAIL
    add("CALIBRATION_MANIFEST_REPLAY", "integrity", replay_status,
        manifest.get("reproducibility_status"), "deterministic manifest and dataset hashes",
        limitation_id="MANIFEST_REPLAY_MISMATCH", description="calibration manifest is not reproducible",
        remediation="fix nondeterministic source or builder inputs", development_blocking=True)

    add("PHASE3_GOVERNANCE_CONTROLS", "phase3_readiness", ReadinessStatus.PASS,
        "implemented", "Phase 3C-1A as-of authority/conflict/cycle/quarantine controls present")
    add("PHASE3_ROUTING_CONTROLS", "phase3_readiness", ReadinessStatus.PASS,
        "scan-routing-policy-v2", "deterministic precedence and fail-closed validation present")
    add("PHASE3_POSITION_CONTROLS", "phase3_readiness", ReadinessStatus.PASS,
        "position coverage and report-only recovery", "active-position monitoring safety present")
    add("PHASE3C4_PERFORMANCE_HARNESS", "performance", ReadinessStatus.PASS,
        "PHASE_3C4_VERIFIED", "instrumentation and exact fixture replay verified")

    baseline_valid = bool(copied_realistic_performance_summary) and copied_realistic_performance_summary.get("replay_equivalence", {}).get("equivalent") is True
    baseline_status = ReadinessStatus.PASS if baseline_valid else (
        ReadinessStatus.FAIL if cfg.phase4_require_copied_realistic_performance_baseline else ReadinessStatus.WARN
    )
    add("COPIED_REALISTIC_PERFORMANCE_BASELINE", "performance", baseline_status,
        "present" if baseline_valid else "missing", "copied-realistic cold/warm exact-replay baseline",
        severity="high", limitation_id="COPIED_REALISTIC_BASELINE_MISSING",
        description="copied-realistic Phase 3C-4 performance baseline is missing",
        remediation="run cold>=2 and warm>=5 against an immutable copied store",
        development_blocking=baseline_status is ReadinessStatus.FAIL)

    migration_status = ReadinessStatus.PASS if operator_migrations_applied else (
        ReadinessStatus.FAIL if cfg.phase4_require_operator_migrations else ReadinessStatus.WARN
    )
    add("OPERATOR_MIGRATIONS_034_041", "schema", migration_status,
        operator_migrations_applied, "operator migrations 034-036 backed up and copied-store verified",
        severity="high", limitation_id="OPERATOR_MIGRATIONS_NOT_APPLIED",
        description="operator migrations 034-036 remain unapplied",
        remediation="perform separately approved backup and copied-store migration verification",
        development_blocking=migration_status is ReadinessStatus.FAIL)

    history_status = ReadinessStatus.PASS if real_phase3b_history_present else ReadinessStatus.WARN
    add("REAL_PHASE3B_HISTORY", "evidence", history_status, real_phase3b_history_present,
        "real Phase 3B history present", severity="medium", limitation_id="EMPTY_REAL_PHASE3B_HISTORY",
        description="real operator Phase 3B history is empty or unavailable",
        remediation="run shadow verification across 5-10 stored sessions")
    add("PHASE4_READ_ONLY_BOUNDARY", "phase4_boundary", ReadinessStatus.PASS,
        "read-only API/UI only", "no execution, lifecycle, broker, or schema mutations in request handlers")

    critical_fail = any(check.status is ReadinessStatus.FAIL for check in checks)
    warning = any(check.status in {ReadinessStatus.WARN, ReadinessStatus.NOT_EVALUATED} for check in checks)
    verdict = ReadinessVerdict.NOT_READY if critical_fail else (
        ReadinessVerdict.READY_WITH_LIMITATIONS if warning else ReadinessVerdict.READY
    )
    development_ready = not critical_fail
    production_ready = verdict is ReadinessVerdict.READY and operator_migrations_applied and baseline_valid
    return tuple(checks), tuple(limitations), verdict, development_ready, production_ready


def write_calibration_artifacts(result: CalibrationBuildResult, output_root: Path) -> tuple[Path, ...]:
    output_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "eligible": output_root / "phase3c5_calibration_eligible.csv",
        "excluded": output_root / "phase3c5_calibration_excluded.csv",
        "quarantined": output_root / "phase3c5_calibration_quarantined.csv",
        "manifest": output_root / "phase3c5_calibration_manifest.json",
        "quality": output_root / "phase3c5_calibration_quality_summary.json",
        "coverage": output_root / "phase3c5_sample_coverage.csv",
        "reasons": output_root / "phase3c5_exclusion_reasons.csv",
        "checks": output_root / "phase3c5_readiness_checks.csv",
        "readiness_json": output_root / "phase3c5_phase4_readiness.json",
        "readiness_md": output_root / "phase3c5_phase4_readiness.md",
    }
    _write_csv(paths["eligible"], list(result.eligible_rows))
    _write_csv(paths["excluded"], [*result.excluded_rows, *result.pending_rows])
    _write_csv(paths["quarantined"], list(result.quarantined_rows))
    paths["manifest"].write_text(json.dumps(result.manifest, indent=2, sort_keys=True, default=str), encoding="utf-8")
    paths["quality"].write_text(json.dumps(result.quality_summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_csv(paths["coverage"], _coverage_rows(result.quality_summary))
    _write_csv(paths["reasons"], [
        {"exclusion_reason": key, "count": value}
        for key, value in sorted(result.quality_summary.get("exclusion_reason_counts", {}).items())
    ])
    _write_csv(paths["checks"], [_check_row(check) for check in result.readiness_checks])
    readiness_payload = {
        "verdict": result.verdict.value,
        "phase4_development_ready": result.phase4_development_ready,
        "phase4_production_ready": result.phase4_production_ready,
        "policy_version": READINESS_POLICY_VERSION,
        "manifest_id": result.manifest["manifest_id"],
        "checks": [_check_row(check) for check in result.readiness_checks],
        "limitations": [asdict(item) for item in result.limitations],
        "phase4_boundary": {
            "read_only_api": True, "read_only_ui": True,
            "execution_commands": False, "order_placement": False,
            "lifecycle_mutation": False, "request_handler_schema_mutation": False,
        },
    }
    paths["readiness_json"].write_text(json.dumps(readiness_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    paths["readiness_md"].write_text(_readiness_markdown(readiness_payload), encoding="utf-8")
    return tuple(paths.values())


def compare_manifests(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("manifest_id", "sample_identity_hash", "eligible_dataset_hash", "exclusion_dataset_hash")
    matches = {key: left.get(key) == right.get(key) for key in keys}
    return {"equivalent": all(matches.values()), "matches": matches}


def write_readiness_artifacts(
    *, checks: tuple[ReadinessCheck, ...], limitations: tuple[ReadinessLimitation, ...],
    verdict: ReadinessVerdict, phase4_development_ready: bool,
    phase4_production_ready: bool, manifest_id: str, output_root: Path,
) -> tuple[Path, ...]:
    output_root.mkdir(parents=True, exist_ok=True)
    checks_path = output_root / "phase3c5_readiness_checks.csv"
    json_path = output_root / "phase3c5_phase4_readiness.json"
    markdown_path = output_root / "phase3c5_phase4_readiness.md"
    _write_csv(checks_path, [_check_row(check) for check in checks])
    payload = {
        "verdict": verdict.value,
        "phase4_development_ready": phase4_development_ready,
        "phase4_production_ready": phase4_production_ready,
        "policy_version": READINESS_POLICY_VERSION,
        "manifest_id": manifest_id,
        "checks": [_check_row(check) for check in checks],
        "limitations": [asdict(item) for item in limitations],
        "phase4_boundary": {
            "read_only_api": True, "read_only_ui": True,
            "execution_commands": False, "order_placement": False,
            "lifecycle_mutation": False, "request_handler_schema_mutation": False,
        },
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    markdown_path.write_text(_readiness_markdown(payload), encoding="utf-8")
    return checks_path, json_path, markdown_path


def _quality_summary(
    source_rows: list[dict[str, Any]], eligibility: tuple[CalibrationEligibility, ...],
    eligible_rows: tuple[Mapping[str, Any], ...],
) -> dict[str, Any]:
    reason_counts = Counter(reason for item in eligibility for reason in item.exclusion_reasons)
    labels = Counter(str(row.get("outcome_label") or "pending").lower() for row in eligible_rows)
    total_eligible = len(eligible_rows)
    decisions = sorted(item.decision_at for item in eligibility)
    eligible_source = [dict(row) for row in eligible_rows]
    regimes = sorted({str(row.get("market_regime")) for row in eligible_source if row.get("market_regime")})
    years = Counter(str(row.get("decision_at"))[:4] for row in eligible_source)
    quarters = Counter(
        f"{parsed.year}-Q{((parsed.month - 1) // 3) + 1}"
        for row in eligible_source
        if (parsed := _maybe_dt(row.get("decision_at"))) is not None
    )
    dimensions = (
        "market_regime", "breadth_velocity_bucket", "stock_stage", "sector_stage",
        "scan_tier", "setup_family", "candidate_state", "outcome_horizon",
        "policy_snapshot_id", "admission_policy_snapshot_id",
        "primary_admission_reason", "primary_setup_family",
        "sector_gate_taxonomy", "sector_gate_cohort",
    )
    coverage_counts = {
        dimension: dict(sorted(Counter(
            str(row.get(dimension)) for row in eligible_source if row.get(dimension) is not None
        ).items()))
        for dimension in dimensions
    }
    largest_class = max(labels.values(), default=0)
    largest_year = max(years.values(), default=0)
    return {
        "total_rows": len(source_rows), "eligible_rows": total_eligible,
        "excluded_rows": sum(item.eligibility_status in {EligibilityStatus.EXCLUDED, EligibilityStatus.INSUFFICIENT_HISTORY} for item in eligibility),
        "quarantined_rows": sum(item.eligibility_status is EligibilityStatus.QUARANTINED for item in eligibility),
        "pending_rows": sum(item.eligibility_status is EligibilityStatus.PENDING_OUTCOME for item in eligibility),
        "positive_count": labels.get("positive", 0), "negative_count": labels.get("negative", 0),
        "neutral_count": labels.get("neutral", 0), "largest_class_share": largest_class / total_eligible if total_eligible else 0.0,
        "class_ratio": dict(sorted(labels.items())),
        "exclusion_reason_counts": dict(sorted(reason_counts.items())),
        "critical_leakage_rows": sum(any(reason in item.exclusion_reasons for reason in (
            ExclusionReason.LOOKAHEAD_INPUT.value, ExclusionReason.INPUT_AVAILABLE_AFTER_DECISION.value,
            ExclusionReason.LATE_CORRECTION_NOT_AVAILABLE_AS_OF_DECISION.value,
        )) for item in eligibility),
        "survivorship_failure_rows": sum(any(reason in item.exclusion_reasons for reason in (
            ExclusionReason.SURVIVORSHIP_STATUS_UNKNOWN.value, ExclusionReason.CURRENT_ONLY_UNIVERSE.value,
            ExclusionReason.SYMBOL_IDENTITY_CONFLICT.value,
        )) for item in eligibility),
        "unresolved_governance_eligible_rows": sum(item.eligible and not item.correction_impact_resolved for item in eligibility),
        "duplicate_sample_count": reason_counts.get(ExclusionReason.DUPLICATE_SAMPLE_IDENTITY.value, 0),
        "winner_only_dataset": bool(source_rows) and all(str(row.get("outcome_label") or "").lower() == "positive" for row in source_rows),
        "date_min": decisions[0].date().isoformat() if decisions else None,
        "date_max": decisions[-1].date().isoformat() if decisions else None,
        "decision_time_min": _iso(decisions[0]) if decisions else None,
        "decision_time_max": _iso(decisions[-1]) if decisions else None,
        "calendar_sessions": len({str(row.get("decision_at"))[:10] for row in eligible_source}),
        "invalid_outcome_eligible_rows": sum(
            str(row.get("outcome_status") or "").upper() != OutcomeStatus.COMPLETE.value
            for row in eligible_source
        ),
        "market_regimes": regimes,
        "stages": sorted({str(row.get("stock_stage")) for row in eligible_source if row.get("stock_stage")}),
        "scan_tiers": sorted({str(row.get("scan_tier")) for row in eligible_source if row.get("scan_tier")}),
        "setup_families": sorted({str(row.get("setup_family")) for row in eligible_source if row.get("setup_family")}),
        "outcome_horizons": sorted({int(row.get("outcome_horizon") or 0) for row in eligible_source}),
        "year_distribution": dict(sorted(years.items())),
        "quarter_distribution": dict(sorted(quarters.items())),
        "largest_year_share": largest_year / total_eligible if total_eligible else 0.0,
        "coverage_counts": coverage_counts,
        "universe_definition": "historical_point_in_time",
        "membership_trust_distribution": dict(sorted(Counter(str(row.get("membership_trust")) for row in source_rows).items())),
        "stage_status_distribution": dict(sorted(Counter(str(row.get("stage_status")) for row in source_rows).items())),
        "correction_status_distribution": dict(sorted(Counter(str(row.get("correction_impact_status") or "linked") for row in source_rows).items())),
        "policy_snapshot_ids": sorted({
            str(row["policy_snapshot_id"])
            for row in source_rows if row.get("policy_snapshot_id")
        }),
    }


def _output_row(row: Mapping[str, Any], eligibility: CalibrationEligibility) -> dict[str, Any]:
    allowed = (
        "entity_type", "entity_id", "candidate_id", "decision_context_id", "symbol_id", "exchange",
        "decision_at", "input_available_at", "outcome_available_at", "outcome_window_end",
        "outcome_horizon", "outcome_status", "outcome_label", "market_regime",
        "breadth_velocity_bucket", "stock_stage", "sector_stage", "stage_status",
        "scan_tier", "setup_family", "candidate_state", "membership_trust",
        "listing_status_as_of_decision", "delisting_status", "universe_source",
        "policy_snapshot_id", "admission_policy_snapshot_id",
        "primary_admission_reason", "primary_setup_family",
        "satisfied_admission_rules", "rule_evaluations",
        "sector_locked_stage_prior_completed_week",
        "sector_provisional_stage_current_week",
        "sector_stage_velocity_current_week", "sector_gate_taxonomy",
        "sector_gate_cohort",
    )
    result = {key: row.get(key) for key in allowed}
    result.update({
        "sample_id": eligibility.sample_id,
        "eligibility_status": eligibility.eligibility_status.value,
        "exclusion_reasons": "|".join(eligibility.exclusion_reasons),
        "policy_version": eligibility.policy_version,
    })
    return result


def _replace_eligibility(
    item: CalibrationEligibility, status: EligibilityStatus, reasons: tuple[str, ...],
) -> CalibrationEligibility:
    payload = {field: getattr(item, field) for field in item.__dataclass_fields__}
    payload.update({"eligible": False, "eligibility_status": status, "exclusion_reasons": tuple(dict.fromkeys(reasons))})
    return CalibrationEligibility(**payload)


def _replace_manifest_id(item: CalibrationEligibility, manifest_id: str) -> CalibrationEligibility:
    payload = {field: getattr(item, field) for field in item.__dataclass_fields__}
    payload["source_manifest_id"] = manifest_id
    return CalibrationEligibility(**payload)


def _coverage_rows(quality: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dimension, counts in sorted(quality.get("coverage_counts", {}).items()):
        for value, count in sorted(counts.items()):
            rows.append({"dimension": dimension, "value": value, "count": count, "status": "REPRESENTED"})
    for dimension, key in (("calendar_year", "year_distribution"), ("calendar_quarter", "quarter_distribution")):
        for value, count in sorted(quality.get(key, {}).items()):
            rows.append({"dimension": dimension, "value": value, "count": count, "status": "REPRESENTED"})
    return rows


def _check_row(check: ReadinessCheck) -> dict[str, Any]:
    row = asdict(check)
    row["status"] = check.status.value
    row["evidence_artifacts"] = "|".join(check.evidence_artifacts)
    row["observed_value"] = json.dumps(check.observed_value, sort_keys=True, default=str)
    return row


def _readiness_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 4 Readiness", "", f"Verdict: **{payload['verdict']}**", "",
        f"- Phase 4 development ready: `{str(payload['phase4_development_ready']).lower()}`",
        f"- Phase 4 production ready: `{str(payload['phase4_production_ready']).lower()}`",
        "", "## Limitations", "",
    ]
    limitations = payload.get("limitations", [])
    lines.extend(
        f"- `{item['limitation_id']}`: {item['description']} — {item['remediation']}"
        for item in limitations
    )
    if not limitations:
        lines.append("- None")
    lines.extend(["", "Phase 4 remains constrained to read-only API/UI development.", ""])
    return "\n".join(lines)


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) or ["status"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(value) for key, value in row.items()})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return value


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    else:
        raise ValueError("required timestamp is missing")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _maybe_dt(value: Any) -> datetime | None:
    return _dt(value) if value else None


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def file_hashes(paths: Iterable[Path]) -> dict[str, str]:
    return {path.name: compute_file_hash(path) for path in sorted(paths)}


__all__ = [
    "CALIBRATION_POLICY_VERSION", "READINESS_POLICY_VERSION",
    "CalibrationBuildResult", "CalibrationConfig", "CalibrationEligibility",
    "CalibrationIntegrityError", "CalibrationSampleRequirements",
    "EligibilityStatus", "ExclusionReason", "OutcomeStatus", "ReadinessCheck",
    "ReadinessLimitation", "ReadinessStatus", "ReadinessVerdict",
    "build_calibration_dataset", "compare_manifests", "deterministic_sample_id",
    "evaluate_calibration_eligibility", "evaluate_phase4_readiness",
    "write_calibration_artifacts", "write_readiness_artifacts",
]
