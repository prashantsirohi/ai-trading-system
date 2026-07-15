"""Framework-independent Phase 4A read service contracts and implementations."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import date, datetime, timezone
from typing import Any, Protocol

from ai_trading_system.domains.opportunities.calibration import (
    CALIBRATION_POLICY_VERSION,
    READINESS_POLICY_VERSION,
)
from ai_trading_system.domains.opportunities.routing import SCAN_ROUTING_POLICY_VERSION
from ai_trading_system.domains.opportunities.stage_governance import (
    STAGE_GOVERNANCE_AUTHORITY_POLICY_VERSION,
)
from ai_trading_system.platform.telemetry.performance import PERFORMANCE_POLICY_VERSION

from ..config import ApiSettings, SourceProfile
from ..repositories import ReadOnlyDataAccess, parse_json, utc


API_VERSION = "v1"
SCHEMA_VERSION = "phase4a-api-schema-v1"
LIMITATIONS = (
    "SINGLE_YEAR_CONCENTRATION",
    "COPIED_REALISTIC_BASELINE_MISSING",
    "OPERATOR_MIGRATIONS_NOT_APPLIED",
    "EMPTY_REAL_PHASE3B_HISTORY",
)


class SystemReadService(Protocol):
    def readiness(self) -> dict[str, Any]: ...
    def limitations(self) -> list[dict[str, Any]]: ...


class StageReadService(Protocol):
    def stages(self, scope: str, as_of: datetime) -> list[dict[str, Any]]: ...


class RoutingReadService(Protocol):
    def routing(self) -> list[dict[str, Any]]: ...


class CandidateReadService(Protocol):
    def candidates(self) -> list[dict[str, Any]]: ...


class PositionCoverageReadService(Protocol):
    def positions(self) -> list[dict[str, Any]]: ...


class AlertReadService(Protocol):
    def alerts(self, incidents: bool = False) -> list[dict[str, Any]]: ...


class GovernanceReadService(Protocol):
    def governance(self, resource: str) -> list[dict[str, Any]]: ...


class CalibrationReadService(Protocol):
    def calibration(self, resource: str) -> Any: ...


class PerformanceReadService(Protocol):
    def performance(self) -> list[dict[str, Any]]: ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fixture() -> dict[str, Any]:
    observed = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    stage = {
        "observation_id": "stock-stage-aaa", "exchange": "NSE", "symbol_id": "AAA",
        "sector_id": "TECH", "sector_name": "Technology", "effective_stage": "stage_2_advancing",
        "stage_status": "locked", "stage_confidence": 88.0, "as_of": observed,
        "available_at": observed, "source_week_start": date(2026, 7, 6),
        "source_week_end": date(2026, 7, 10), "membership_trust": "POINT_IN_TIME_VERIFIED",
        "governance_status": "AUTHORITATIVE",
    }
    sector = {**stage, "observation_id": "sector-stage-tech", "symbol_id": None}
    routing = {
        "decision_id": "route-aaa", "exchange": "NSE", "symbol_id": "AAA",
        "as_of": date(2026, 7, 14), "effective_scan_tier": "full_investigator",
        "winning_reason": "rank_selected", "all_reasons": ["rank_selected", "stage_promoted"],
        "policy_version": SCAN_ROUTING_POLICY_VERSION, "routing_input_hash": "a" * 64,
        "new_long_structural_block": False, "active_position_structural_risk": False,
        "risk_severity": "low", "selection_details": [],
    }
    candidate = {
        "candidate_id": "candidate-aaa", "symbol_id": "AAA", "exchange": "NSE",
        "setup_family": "breakout", "candidate_state": "confirmed", "opened_at": observed,
        "closed_at": None, "followthrough_status": "confirmed",
        "recovered_from_position_state": False, "pre_entry_history_available": True,
        "recovery_mode": None, "history_completeness": "COMPLETE",
        "latest_snapshot": {"snapshot_id": "snapshot-aaa", "as_of": observed.isoformat()},
        "correction_impact_status": "linked",
    }
    snapshot = {"snapshot_id": "snapshot-aaa", "candidate_id": "candidate-aaa", "as_of": observed, "lifecycle_state": "confirmed", "followthrough_status": "confirmed"}
    decision = {"decision_context_id": "decision-aaa", "candidate_id": "candidate-aaa", "decided_at": observed, "action": "watch", "eligibility": "conditionally_eligible", "reasons": ["shadow_only"]}
    outcome = {"attribution_id": "outcome-aaa", "candidate_id": "candidate-aaa", "category": "undetermined", "confidence": 50.0, "resolved_at": observed}
    position = {"position_cycle_id": "cycle-aaa", "symbol_id": "AAA", "exchange": "NSE", "coverage_status": "FULLY_MONITORED", "position_monitor_present": True, "market_data_complete": True, "evidence_complete": True, "missing_fields": [], "episode_compatibility": "compatible", "recovery_status": None, "positive_action_suppressed": False}
    alert = {"alert_id": "alert-aaa", "alert_code": "POSITION_DATA_INCOMPLETE", "severity": "critical", "status": "OPEN", "opened_at": observed, "resolved_at": None, "dedupe_key": "position:cycle-aaa", "position_cycle_id": "cycle-aaa", "symbol_id": "AAA", "missing_field_signature": "close", "recommended_operator_action": "Restore trusted market data", "occurrence_count": 1}
    correction = {"governance_event_id": "gov-aaa", "observation_id": "stock-stage-aaa", "authority": "reviewed_operator_correction", "policy_version": STAGE_GOVERNANCE_AUTHORITY_POLICY_VERSION, "superseded_observation_id": "stock-stage-old", "replacement_observation_id": "stock-stage-aaa", "recorded_at": observed, "available_at": observed}
    impact = {"impact_id": "impact-aaa", "candidate_id": "candidate-aaa", "impact_link_status": "linked", "review_required": False, "authoritative_calibration_eligible": True}
    membership = {"membership_observation_id": "membership-aaa", "exchange": "NSE", "symbol_id": "AAA", "sector_id": "TECH", "sector_name": "Technology", "membership_trust": "POINT_IN_TIME_VERIFIED", "valid_from": date(2026, 1, 1), "valid_to": date(2026, 12, 31), "recorded_at": observed}
    readiness_checks = [{"check_id": "development_path", "category": "phase4", "status": "PASS", "required_for_ready": True, "limitation": None}]
    performance = [{"run_id": "perf-small-fixture", "functional_status": "PASS", "performance_status": "PASS", "stage_runtimes": {"weekly_stage": 10.0}, "throughput": {"symbols_per_second": 100.0}, "peak_rss_mb": 128.0, "database_time_ms": 2.0, "artifact_metrics": [], "threshold_results": [], "replay_equivalence": "EXACT_REPLAY", "cache_mode": "COLD"}]
    return {"stock_stages": [stage], "sector_stages": [sector], "routing": [routing], "candidates": [candidate], "snapshots": [snapshot], "decisions": [decision], "outcomes": [outcome], "positions": [position], "alerts": [alert], "incidents": [alert], "corrections": [correction], "impacts": [impact], "memberships": [membership], "conflicts": [], "readiness_checks": readiness_checks, "performance": performance}


class Phase4ReadService:
    def __init__(self, settings: ApiSettings):
        self.settings = settings
        self.access = ReadOnlyDataAccess(settings)
        self.fixture = _fixture() if settings.source_profile is SourceProfile.SMALL_FIXTURE else {}

    def source_readable(self) -> bool:
        return self.access.source_readable()

    def limitations(self) -> list[dict[str, Any]]:
        descriptions = {
            "SINGLE_YEAR_CONCENTRATION": "Calibration evidence is concentrated in a single calendar year.",
            "COPIED_REALISTIC_BASELINE_MISSING": "No copied-realistic performance baseline is available.",
            "OPERATOR_MIGRATIONS_NOT_APPLIED": "Optional Phase 3C operator migrations remain unapplied.",
            "EMPTY_REAL_PHASE3B_HISTORY": "The operator store has no real Phase 3B history.",
        }
        return [{"limitation_id": item, "description": descriptions[item], "severity": "warning", "development_blocking": False, "production_blocking": True} for item in LIMITATIONS]

    def readiness(self) -> dict[str, Any]:
        return {"readiness_status": "READY_WITH_LIMITATIONS", "phase4_development_ready": True, "phase4_production_ready": False, "limitations": self.limitations()}

    def version(self) -> dict[str, Any]:
        try:
            commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=2).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            commit = "unknown"
        return {"api_version": API_VERSION, "application_version": "0.1.0", "git_commit": commit, "schema_version": SCHEMA_VERSION, "readiness_policy_version": READINESS_POLICY_VERSION, "routing_policy_version": SCAN_ROUTING_POLICY_VERSION, "performance_policy_version": PERFORMANCE_POLICY_VERSION, "calibration_policy_version": CALIBRATION_POLICY_VERSION}

    def stages(self, scope: str, as_of: datetime) -> list[dict[str, Any]]:
        if self.fixture:
            return [row for row in self.fixture[f"{scope}_stages"] if (row.get("as_of") or as_of) <= as_of]
        return self.access.stages(scope=scope, as_of=as_of)

    def routing(self) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture["routing"])
        result = []
        for row in self.access.rows("opportunity_scan_routing_history"):
            payload = parse_json(row.get("decision_json"), {})
            reasons = parse_json(row.get("reasons_json"), [])
            all_reasons = payload.get("all_reasons") or payload.get("selection_reasons") or reasons
            result.append({"decision_id": row["decision_id"], "exchange": row["exchange"], "symbol_id": row["symbol_id"], "as_of": row["as_of"], "effective_scan_tier": payload.get("effective_scan_tier") or row["scan_tier"], "winning_reason": payload.get("winning_reason") or (all_reasons[0] if all_reasons else "unknown"), "all_reasons": all_reasons, "policy_version": row["policy_version"], "routing_input_hash": payload.get("routing_input_hash"), "new_long_structural_block": bool(payload.get("new_long_structural_block", False)), "active_position_structural_risk": bool(payload.get("active_position_structural_risk", False)), "risk_severity": payload.get("risk_severity"), "selection_details": payload.get("selection_details", [])})
        return result

    def candidates(self) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture["candidates"])
        snapshots = {row["candidate_id"]: row for row in self.access.rows("candidate_snapshot")}
        result = []
        for row in self.access.rows("candidate_episode"):
            snapshot = snapshots.get(row["candidate_id"])
            recovery = str(row.get("setup_family")) == "position_state_recovery"
            result.append({"candidate_id": row["candidate_id"], "symbol_id": row["symbol_id"], "exchange": row["exchange"], "setup_family": row["setup_family"], "candidate_state": (snapshot or {}).get("lifecycle_state") or row["episode_status"], "opened_at": utc(row["episode_started_at"]), "closed_at": utc(row.get("episode_closed_at")), "followthrough_status": (snapshot or {}).get("followthrough_status"), "recovered_from_position_state": recovery, "pre_entry_history_available": not recovery, "recovery_mode": "report_only" if recovery else None, "history_completeness": "POSITION_ONLY" if recovery else "COMPLETE", "latest_snapshot": parse_json((snapshot or {}).get("snapshot_json"), None), "correction_impact_status": None})
        return result

    def candidate_children(self, resource: str) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture[resource])
        table = {"snapshots": "candidate_snapshot", "decisions": "candidate_decision_context", "outcomes": "candidate_outcome_attribution"}[resource]
        rows = self.access.rows(table)
        normalized = []
        for row in rows:
            if resource == "snapshots":
                normalized.append({"snapshot_id": row["snapshot_id"], "candidate_id": row["candidate_id"], "as_of": utc(row["as_of"]), "lifecycle_state": row["lifecycle_state"], "followthrough_status": row["followthrough_status"]})
            elif resource == "decisions":
                normalized.append({"decision_context_id": row["decision_context_id"], "candidate_id": row["candidate_id"], "decided_at": utc(row["decided_at"]), "action": row["action"], "eligibility": row["eligibility"], "reasons": parse_json(row.get("reasons_json"), [])})
            else:
                normalized.append({"attribution_id": row["attribution_id"], "candidate_id": row["candidate_id"], "category": row["attribution_category"], "confidence": row["attribution_confidence"], "resolved_at": utc(row["resolved_at"])})
        return normalized

    def positions(self) -> list[dict[str, Any]]:
        return list(self.fixture.get("positions", []))

    def recovery_proposals(self) -> list[dict[str, Any]]:
        if self.fixture:
            return []
        return [{**parse_json(row.get("payload_json"), {}), "recovery_proposal_id": row["recovery_proposal_id"], "position_cycle_id": row["position_cycle_id"], "symbol_id": row["symbol_id"], "exchange": row["exchange"], "recovery_status": row["proposal_status"], "recovery_mode": row["recovery_mode"]} for row in self.access.rows("position_recovery_proposal")]

    def alerts(self, incidents: bool = False) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture["incidents" if incidents else "alerts"])
        if incidents:
            result = []
            for row in self.access.rows("pipeline_alert_incident"):
                payload = parse_json(row.get("payload_json"), {})
                result.append({"alert_id": row["incident_id"], "alert_code": row["alert_type"], "severity": row["severity"], "status": row["status"], "opened_at": utc(row["opened_at"]), "resolved_at": utc(row.get("resolved_at")), "dedupe_key": row["dedupe_key"], "position_cycle_id": payload.get("position_cycle_id"), "symbol_id": payload.get("symbol_id"), "missing_field_signature": payload.get("missing_field_signature"), "recommended_operator_action": payload.get("recommended_operator_action"), "occurrence_count": row["occurrence_count"]})
            return result
        return [{"alert_id": row["alert_id"], "alert_code": row["alert_type"], "severity": row["severity"], "status": "OPEN", "opened_at": utc(row["created_at"]), "resolved_at": None} for row in self.access.rows("pipeline_alert")]

    def governance(self, resource: str) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture.get(resource, []))
        if resource == "corrections":
            return [{"governance_event_id": row["governance_event_id"], "observation_id": row["observation_id"], "authority": row["correction_authority"], "policy_version": row.get("governance_policy_version") or row["policy_version"], "superseded_observation_id": row.get("supersedes_observation_id"), "replacement_observation_id": row["observation_id"], "recorded_at": utc(row["recorded_at"]), "available_at": utc(row["recorded_at"])} for row in self.access.rows("stage_observation_governance") if row["governance_action"] == "CORRECTION"]
        if resource == "impacts":
            return [{"impact_id": row["impact_id"], "candidate_id": row.get("candidate_id"), "impact_link_status": row["impact_status"], "review_required": bool(row.get("review_required", True)), "authoritative_calibration_eligible": bool(row.get("authoritative_calibration_eligible", False))} for row in self.access.rows("stage_correction_impact")]
        if resource == "memberships":
            return self.access.rows("sector_membership_history")
        return []

    def calibration(self, resource: str) -> Any:
        if resource == "summary":
            return {"manifest_id": "fixture-manifest" if self.fixture else None, "eligible_count": 1 if self.fixture else 0, "excluded_count": 0, "quarantined_count": 0, "pending_count": 0, "top_exclusion_reasons": []}
        if resource == "manifest":
            return {"manifest_id": "fixture-manifest" if self.fixture else None, "policy_version": CALIBRATION_POLICY_VERSION, "source_hashes": {}, "replay_equivalent": True if self.fixture else None}
        if resource == "checks":
            return list(self.fixture.get("readiness_checks", []))
        return []

    def performance(self) -> list[dict[str, Any]]:
        return list(self.fixture.get("performance", []))

    def partial_limitations(self, resource: str, rows: list[Any]) -> list[str]:
        if self.fixture or rows:
            return list(LIMITATIONS)
        table_requirements = {
            "stages": "weekly_stock_stage_history", "routing": "opportunity_scan_routing_history",
            "candidates": "candidate_episode", "positions": "position_recovery_proposal",
            "alerts": "pipeline_alert_incident", "governance": "stage_observation_governance",
        }
        required = table_requirements.get(resource)
        if required and required not in self.access.tables():
            return [*LIMITATIONS, "SOURCE_NOT_MIGRATED"]
        return [*LIMITATIONS, "SOURCE_EMPTY"]

    @staticmethod
    def semantic_hash(value: Any) -> str:
        return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()
