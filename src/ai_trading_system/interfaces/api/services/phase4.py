"""Framework-independent Phase 4A read service contracts and implementations."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
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
from ..artifacts import ARTIFACT_SPECS, CanonicalArtifactLocator, LocatedArtifact
from ..limitations import PRODUCTION_LIMITATIONS
from ..repositories import ReadOnlyDataAccess, parse_json, utc
from ..telemetry import ApiMetrics


API_VERSION = "v1"
SCHEMA_VERSION = "phase4a-api-schema-v1"
FALLBACK_LIMITATIONS = PRODUCTION_LIMITATIONS


@dataclass(slots=True)
class ProjectionState:
    limitations: list[str] = field(default_factory=list)
    lineage: list[dict[str, Any]] = field(default_factory=list)
    freshness: dict[str, Any] = field(default_factory=dict)
    source_version_mismatch: bool = False


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    parsed = parse_json(value, None)
    if isinstance(parsed, list):
        return parsed
    if value in (None, "", "nan"):
        return []
    return [item for item in str(value).split("|") if item]


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "pass", "ready"}


def _unique_limitations(values: list[str] | tuple[str, ...]) -> list[str]:
    """Preserve artifact-owned codes while deduplicating in stable order."""
    return list(dict.fromkeys(str(value) for value in values if value))


def _stage_projection(scope: str, source: dict[str, Any]) -> dict[str, Any]:
    """Map governed canonical stage payloads onto the stable Phase 4 API schema."""
    row = dict(source)
    entity_id = row.get("symbol_id") if scope == "stock" else row.get("sector_id")
    if not row.get("observation_id"):
        identity = {
            "scope": scope,
            "exchange": row.get("exchange"),
            "entity_id": entity_id,
            "as_of": row.get("as_of"),
            "source_week_end": row.get("source_week_end"),
            "source_artifact_hash": row.get("source_artifact_hash"),
        }
        row["observation_id"] = "stage-" + hashlib.sha256(
            json.dumps(identity, sort_keys=True, default=str, separators=(",", ":")).encode()
        ).hexdigest()[:24]
    if row.get("stage_confidence") is None:
        row["stage_confidence"] = row.get("stage_confidence_score")
    if row.get("membership_trust") is None:
        row["membership_trust"] = row.get("sector_membership_trust")
    return row


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
    """Read projections use governed rows, immutable artifacts, then explicit partials."""

    def __init__(self, settings: ApiSettings, metrics: ApiMetrics | None = None):
        self.settings = settings
        self.metrics = metrics or ApiMetrics()
        self.access = ReadOnlyDataAccess(settings, self.metrics)
        self.fixture = _fixture() if settings.source_profile is SourceProfile.SMALL_FIXTURE else {}
        self.locator = CanonicalArtifactLocator(self.access, settings.artifact_roots(), self.metrics)
        self._states: dict[str, ProjectionState] = {}
        self._dynamic_conflicts: list[dict[str, Any]] = []

    def projection_state(self, family: str) -> ProjectionState:
        return self._states.get(family, ProjectionState(limitations=["LINEAGE_UNAVAILABLE", "FRESHNESS_UNKNOWN"]))

    def _artifacts(self, family: str, keys: tuple[str, ...]) -> dict[str, tuple[LocatedArtifact, Any]]:
        found: dict[str, tuple[LocatedArtifact, Any]] = {}
        state = ProjectionState()
        for key in keys:
            located = self.locator.locate_latest_successful(key)
            if located is None:
                self.metrics.record_source_unavailable(ARTIFACT_SPECS[key].artifact_family)
                continue
            try:
                found[key] = (located, self.locator.read(located))
                state.lineage.append(located.lineage())
            except (OSError, ValueError, json.JSONDecodeError):
                self.metrics.record_source_unavailable(ARTIFACT_SPECS[key].artifact_family)
        run_ids = {item.run_id for item, _ in found.values() if item.run_id}
        source_times = {item.source_as_of for item, _ in found.values() if item.source_as_of}
        state.source_version_mismatch = len(run_ids) > 1 or len(source_times) > 1
        if state.source_version_mismatch:
            state.limitations.append("SOURCE_VERSION_MISMATCH")
        semantic = [item for item, _ in found.values() if item.source_as_of or item.available_at]
        if semantic:
            latest = max(semantic, key=lambda item: item.source_as_of or item.available_at or datetime.min.replace(tzinfo=timezone.utc))
            state.freshness = {
                "source_as_of": latest.source_as_of,
                "last_successful_run_at": latest.available_at,
                "freshness_status": "FRESH" if latest.source_as_of else "UNKNOWN",
                "freshness_reasons": ["SEMANTIC_SOURCE_TIMESTAMP"] if latest.source_as_of else ["SOURCE_AS_OF_UNAVAILABLE"],
            }
        else:
            state.limitations.extend(("LINEAGE_UNAVAILABLE", "FRESHNESS_UNKNOWN"))
            state.freshness = {"freshness_status": "UNKNOWN", "freshness_reasons": ["SEMANTIC_TIMESTAMP_UNAVAILABLE"]}
        self._states[family] = state
        return found

    def source_readable(self) -> bool:
        return self.access.source_readable()

    def _db_state(self, family: str, source_id: str, rows: list[dict[str, Any]], *time_keys: str) -> None:
        timestamps = [utc(row.get(key)) for row in rows for key in time_keys if row.get(key) is not None]
        valid_timestamps: list[datetime] = [item for item in timestamps if item is not None]
        source_as_of = max(valid_timestamps) if valid_timestamps else None
        limitations = [] if rows else ["SOURCE_EMPTY"]
        if source_as_of is None:
            limitations.append("FRESHNESS_UNKNOWN")
        self._states[family] = ProjectionState(
            limitations=limitations,
            lineage=[{"source_type": "governed_database", "source_id": source_id, "source_as_of": source_as_of}] if rows else [],
            freshness={"source_as_of": source_as_of, "freshness_status": "FRESH" if source_as_of else ("UNAVAILABLE" if not rows else "UNKNOWN"), "freshness_reasons": ["GOVERNED_ROW_SEMANTIC_TIMESTAMP"] if source_as_of else ["SEMANTIC_TIMESTAMP_UNAVAILABLE"]},
        )

    def limitations(self) -> list[dict[str, Any]]:
        descriptions = {
            "SINGLE_YEAR_CONCENTRATION": "Calibration evidence is concentrated in a single calendar year.",
            "COPIED_REALISTIC_BASELINE_MISSING": "No copied-realistic performance baseline is available.",
            "OPERATOR_MIGRATIONS_NOT_APPLIED": "Optional Phase 3C operator migrations remain unapplied.",
            "EMPTY_REAL_PHASE3B_HISTORY": "The operator store has no real Phase 3B history.",
        }
        readiness = self._readiness_payload()
        values = readiness.get("limitations") if readiness else None
        if isinstance(values, list):
            return [dict(item) if isinstance(item, dict) else {"limitation_id": str(item), "description": descriptions.get(str(item), str(item)), "severity": "warning", "development_blocking": False, "production_blocking": True} for item in values]
        return [{"limitation_id": item, "description": descriptions[item], "severity": "warning", "development_blocking": False, "production_blocking": True} for item in FALLBACK_LIMITATIONS]

    def limitation_ids(self) -> list[str]:
        """Return the readiness artifact's limitation IDs, including an empty set."""
        return _unique_limitations([
            str(item.get("limitation_id")) for item in self.limitations()
            if item.get("limitation_id")
        ])

    def readiness(self) -> dict[str, Any]:
        payload = self._readiness_payload()
        return {"readiness_status": payload.get("verdict", "READY_WITH_LIMITATIONS"), "phase4_development_ready": _bool(payload.get("phase4_development_ready"), True), "phase4_production_ready": _bool(payload.get("phase4_production_ready"), False), "limitations": self.limitations()}

    def _readiness_payload(self) -> dict[str, Any]:
        if self.fixture:
            return {}
        found = self._artifacts("readiness", ("phase3c5_phase4_readiness",))
        payload: Any = found.get("phase3c5_phase4_readiness", (None, {}))[1]
        return payload if isinstance(payload, dict) else {}

    def version(self) -> dict[str, Any]:
        try:
            commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=2).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            commit = "unknown"
        return {"api_version": API_VERSION, "application_version": "0.1.0", "git_commit": commit, "schema_version": SCHEMA_VERSION, "readiness_policy_version": READINESS_POLICY_VERSION, "routing_policy_version": SCAN_ROUTING_POLICY_VERSION, "performance_policy_version": PERFORMANCE_POLICY_VERSION, "calibration_policy_version": CALIBRATION_POLICY_VERSION}

    def stages(self, scope: str, as_of: datetime) -> list[dict[str, Any]]:
        if self.fixture:
            return [row for row in self.fixture[f"{scope}_stages"] if (row.get("as_of") or as_of) <= as_of]
        try:
            rows = self.access.stages(scope=scope, as_of=as_of)
        except Exception as exc:
            conflict = getattr(exc, "conflict", None)
            if conflict is None:
                raise
            view = self._stage_conflict(conflict)
            self._dynamic_conflicts = [item for item in self._dynamic_conflicts if item["conflict_id"] != view["conflict_id"]]
            self._dynamic_conflicts.append(view)
            rows = []
        rows = [_stage_projection(scope, row) for row in rows]
        for conflict in self.access.stage_conflicts:
            view = self._stage_conflict(conflict)
            self._dynamic_conflicts = [item for item in self._dynamic_conflicts if item["conflict_id"] != view["conflict_id"]]
            self._dynamic_conflicts.append(view)
            rows.append({"observation_id": view["conflict_id"], "exchange": view.get("exchange"), "symbol_id": view.get("symbol_id"), "sector_id": view["entity_id"] if scope == "sector" else None, "effective_stage": "CONFLICT", "stage_status": "conflicted", "as_of": view["as_of"], "available_at": view["as_of"], "governance_status": "CONFLICT"})
        self._states["stages"] = ProjectionState(
            limitations=[] if rows else (["GOVERNANCE_CONFLICT_PRESENT"] if self._dynamic_conflicts else ["SOURCE_EMPTY"]),
            lineage=[{"source_type": "governed_database", "source_id": f"weekly_{scope}_stage_history", "policy_version": STAGE_GOVERNANCE_AUTHORITY_POLICY_VERSION, "source_as_of": as_of}],
            freshness={"source_as_of": as_of, "freshness_status": "FRESH", "freshness_reasons": ["CANONICAL_AS_OF_RESOLVER"]},
        )
        return rows

    @staticmethod
    def _stage_conflict(conflict: Any) -> dict[str, Any]:
        ids = tuple(str(item) for item in conflict.terminal_observation_ids)
        conflict_type = "SUPERSESSION_CYCLE" if "cycle" in conflict.conflict_reason.lower() else "COMPETING_TERMINAL_STAGE_OBSERVATIONS"
        return {"conflict_id": "stage-" + hashlib.sha256("|".join(ids).encode()).hexdigest()[:16], "conflict_type": conflict_type, "entity_type": conflict.scope, "entity_id": conflict.entity_id, "symbol_id": conflict.entity_id if conflict.scope == "stock" else None, "exchange": None, "as_of": conflict.requested_as_of, "severity": "high", "status": "OPEN", "message": conflict.conflict_reason, "observation_ids": list(ids), "authorities": [getattr(item, "value", str(item)) for item in conflict.authorities], "policy_version": conflict.policy_version, "source_refs": [{"source_type": "governed_database", "source_id": "stage_observation_governance", "policy_version": conflict.policy_version, "source_as_of": conflict.requested_as_of}]}

    def entity_conflict(self, scope: str, entity_id: str) -> dict[str, Any] | None:
        return next((row for row in self._dynamic_conflicts if row.get("entity_type") == scope and str(row.get("entity_id", "")).upper() == entity_id.upper()), None)

    def routing(self) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture["routing"])
        result = []
        for row in self.access.rows("opportunity_scan_routing_history"):
            payload = parse_json(row.get("decision_json"), {})
            reasons = parse_json(row.get("reasons_json"), [])
            all_reasons = payload.get("all_reasons") or payload.get("selection_reasons") or reasons
            result.append({"decision_id": row["decision_id"], "exchange": row["exchange"], "symbol_id": row["symbol_id"], "as_of": row["as_of"], "effective_scan_tier": payload.get("effective_scan_tier") or row["scan_tier"], "winning_reason": payload.get("winning_reason") or (all_reasons[0] if all_reasons else "unknown"), "all_reasons": all_reasons, "policy_version": row["policy_version"], "routing_input_hash": payload.get("routing_input_hash"), "new_long_structural_block": bool(payload.get("new_long_structural_block", False)), "active_position_structural_risk": bool(payload.get("active_position_structural_risk", False)), "risk_severity": payload.get("risk_severity"), "selection_details": payload.get("selection_details", [])})
        self._db_state("routing", "opportunity_scan_routing_history", result, "as_of")
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
        self._db_state("candidates", "candidate_episode", result, "opened_at", "closed_at")
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
        if self.fixture:
            return list(self.fixture["positions"])
        found = self._artifacts("positions", ("active_position_coverage", "position_monitor_reconciliation", "position_episode_compatibility"))
        state = self._states["positions"]
        if "active_position_coverage" not in found:
            state.limitations.append("POSITION_COVERAGE_ARTIFACT_MISSING")
            return []
        if "position_monitor_reconciliation" not in found:
            state.limitations.append("POSITION_RECONCILIATION_ARTIFACT_MISSING")
        if "position_episode_compatibility" not in found:
            state.limitations.append("POSITION_COMPATIBILITY_UNAVAILABLE")
        coverage = found["active_position_coverage"][1]
        reconciliation = {str(row.get("position_cycle_id")): row for row in found.get("position_monitor_reconciliation", (None, []))[1]}
        compatibility = {str(row.get("position_cycle_id")): row for row in found.get("position_episode_compatibility", (None, []))[1]}
        result = []
        for raw in coverage if isinstance(coverage, list) else []:
            row = dict(raw)
            cycle_id = str(row.get("position_cycle_id"))
            recon = reconciliation.get(cycle_id, {})
            compat = compatibility.get(cycle_id, {})
            missing = _list(row.get("missing_data_fields") or row.get("missing_fields"))
            episode_status = compat.get("compatibility_status") or recon.get("compatibility_status") or row.get("episode_match_status")
            result.append({
                **row, "position_cycle_id": cycle_id, "symbol_id": str(row.get("symbol_id") or ""), "exchange": str(row.get("exchange") or "NSE"),
                "coverage_status": row.get("coverage_status") or recon.get("coverage_status") or "UNKNOWN",
                "position_monitor_present": _bool(row.get("position_monitor_present"), bool(row.get("routing_decision_id"))),
                "market_data_available": _bool(row.get("market_data_available"), not missing),
                "market_data_complete": _bool(row.get("market_data_complete"), not missing),
                "evidence_complete": _bool(row.get("investigator_evidence_complete") or row.get("evidence_complete")),
                "investigator_evidence_complete": _bool(row.get("investigator_evidence_complete") or row.get("evidence_complete")),
                "missing_fields": missing, "missing_data_fields": missing,
                "episode_compatibility": episode_status or "unknown", "episode_match_status": episode_status,
                "opportunity_episode_id": compat.get("candidate_id") or row.get("opportunity_episode_id"),
                "positive_action_suppressed": _bool(row.get("positive_action_suppressed")),
                "suppression_reasons": _list(row.get("suppression_reasons")),
                "coverage_reasons": _list(row.get("coverage_reasons")),
                "recovery_status": row.get("recovery_status") or recon.get("outcome"),
            })
        return result

    def position_missing_data(self) -> list[dict[str, Any]]:
        if self.fixture:
            return [row for row in self.fixture["positions"] if row.get("missing_fields")]
        found = self._artifacts("positions", ("active_position_missing_data",))
        value: Any = found.get("active_position_missing_data", (None, []))[1]
        return [
            {**row, "missing_fields": _list(row.get("missing_data_fields") or row.get("missing_fields")), "missing_data_fields": _list(row.get("missing_data_fields") or row.get("missing_fields")), "staleness_sessions": int(row["staleness_sessions"]) if str(row.get("staleness_sessions", "")).isdigit() else None, "alert_incident_id": row.get("alert_incident_id") or row.get("incident_id")}
            for row in value if isinstance(value, list)
        ]

    def recovery_proposals(self) -> list[dict[str, Any]]:
        if self.fixture:
            return []
        database = [{**parse_json(row.get("payload_json"), {}), "recovery_proposal_id": row["recovery_proposal_id"], "position_cycle_id": row["position_cycle_id"], "symbol_id": row["symbol_id"], "exchange": row["exchange"], "recovery_status": row["proposal_status"], "recovery_mode": row["recovery_mode"]} for row in self.access.rows("position_recovery_proposal")]
        found = self._artifacts("position_recovery", ("position_recovery_proposals", "position_recovery_actions", "position_episode_compatibility"))
        state = self._states["position_recovery"]
        proposals = database or found.get("position_recovery_proposals", (None, []))[1]
        actions: Any = found.get("position_recovery_actions", (None, []))[1]
        compat: Any = found.get("position_episode_compatibility", (None, []))[1]
        if "position_recovery_actions" not in found:
            state.limitations.append("POSITION_RECOVERY_ACTIONS_UNAVAILABLE")
        if "position_episode_compatibility" not in found:
            state.limitations.append("POSITION_COMPATIBILITY_UNAVAILABLE")
        action_by = {str(row.get("recovery_proposal_id")): row for row in actions if isinstance(actions, list)}
        compat_by = {str(row.get("position_cycle_id")): row for row in compat if isinstance(compat, list)}
        result = []
        for proposal in proposals if isinstance(proposals, list) else []:
            action = action_by.get(str(proposal.get("recovery_proposal_id")))
            match = compat_by.get(str(proposal.get("position_cycle_id")), {})
            mode = (action or {}).get("recovery_mode") or proposal.get("recovery_mode") or "report_only"
            result.append({**proposal, "action_state": "reviewed_action" if action and str(mode).lower() == "reviewed" else ("automatic_action" if action else "proposal_only"), "action": action, "compatibility_status": match.get("compatibility_status"), "compatibility_conflict": str(match.get("compatibility_status", "")).lower() not in {"", "compatible"}, "ambiguous_episode": "ambiguous" in str(match.get("compatibility_status", "")).lower(), "report_only": not action or str(mode).lower() == "report_only"})
        self._states["positions"] = state
        return result

    def alerts(self, incidents: bool = False) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture["incidents" if incidents else "alerts"])
        if incidents:
            result = []
            for row in self.access.rows("pipeline_alert_incident"):
                payload = parse_json(row.get("payload_json"), {})
                result.append({"alert_id": row["incident_id"], "alert_code": row["alert_type"], "severity": row["severity"], "status": row["status"], "opened_at": utc(row["opened_at"]), "resolved_at": utc(row.get("resolved_at")), "dedupe_key": row["dedupe_key"], "position_cycle_id": payload.get("position_cycle_id"), "symbol_id": payload.get("symbol_id"), "missing_field_signature": payload.get("missing_field_signature"), "recommended_operator_action": payload.get("recommended_operator_action"), "occurrence_count": row["occurrence_count"]})
            self._db_state("alerts", "pipeline_alert_incident", result, "opened_at", "resolved_at")
            return result
        result = [{"alert_id": row["alert_id"], "alert_code": row["alert_type"], "severity": row["severity"], "status": "OPEN", "opened_at": utc(row["created_at"]), "resolved_at": None} for row in self.access.rows("pipeline_alert")]
        self._db_state("alerts", "pipeline_alert", result, "opened_at", "resolved_at")
        return result

    def governance(self, resource: str) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture.get(resource, []))
        if resource == "corrections":
            result = [{"governance_event_id": row["governance_event_id"], "observation_id": row["observation_id"], "authority": row["correction_authority"], "policy_version": row.get("governance_policy_version") or row["policy_version"], "superseded_observation_id": row.get("supersedes_observation_id"), "replacement_observation_id": row["observation_id"], "recorded_at": utc(row["recorded_at"]), "available_at": utc(row["recorded_at"])} for row in self.access.rows("stage_observation_governance") if row["governance_action"] == "CORRECTION"]
            self._db_state("governance", "stage_observation_governance", result, "recorded_at")
            return result
        if resource == "impacts":
            result = [{"impact_id": row["impact_id"], "candidate_id": row.get("candidate_id"), "impact_link_status": row.get("impact_link_status") or row["impact_status"], "review_required": bool(row.get("review_required", True)), "authoritative_calibration_eligible": bool(row.get("authoritative_calibration_eligible", False))} for row in self.access.rows("stage_correction_impact")]
            self._db_state("governance", "stage_correction_impact", result)
            return result
        if resource == "memberships":
            result = self.access.rows("sector_membership_history")
            self._db_state("governance", "sector_membership_history", result, "recorded_at")
            return result
        if resource == "conflicts":
            return self._governance_conflicts()
        return []

    def _governance_conflicts(self) -> list[dict[str, Any]]:
        conflicts = list(self._dynamic_conflicts)
        memberships = self.access.rows("sector_membership_history")
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in memberships:
            grouped.setdefault((str(row.get("exchange")), str(row.get("symbol_id"))), []).append(row)
            trust = str(row.get("membership_trust") or "")
            kind = "LATEST_ONLY_MEMBERSHIP" if trust == "LATEST_ONLY_BACKFILL" else ("OBSERVED_MEMBERSHIP_BEFORE_AVAILABLE" if trust == "OBSERVED_AT_RUN" and row.get("recorded_at") and str(row.get("valid_from")) < str(row.get("recorded_at"))[:10] else None)
            if not row.get("sector_id"):
                kind = "MEMBERSHIP_IDENTITY_UNRESOLVED"
            if kind:
                conflicts.append(self._simple_conflict(kind, "membership", str(row.get("membership_observation_id") or row.get("symbol_id")), row, trust))
        for key, rows in grouped.items():
            ordered = sorted(rows, key=lambda item: str(item.get("valid_from")))
            for left, right in zip(ordered, ordered[1:]):
                if str(right.get("valid_from")) <= str(left.get("valid_to")):
                    kind = "MULTIPLE_AUTHORITATIVE_MEMBERSHIPS" if left.get("sector_id") != right.get("sector_id") else "MEMBERSHIP_INTERVAL_OVERLAP"
                    conflicts.append(self._simple_conflict(kind, "membership", key[1], right, "overlapping effective-dated sector memberships"))
        for row in self.access.rows("stage_correction_impact"):
            link = str(row.get("impact_link_status") or row.get("link_status") or "").upper()
            statuses = []
            if link in {"UNRESOLVED_LEGACY_NO_MATCH", "UNRESOLVED_LEGACY_AMBIGUOUS"}:
                statuses.append(link)
            if _bool(row.get("review_required"), True):
                statuses.append("REVIEW_REQUIRED")
            if not _bool(row.get("authoritative_calibration_eligible")):
                statuses.append("AUTHORITATIVE_CALIBRATION_INELIGIBLE")
            for kind in statuses:
                conflicts.append(self._simple_conflict(kind, "correction_impact", str(row.get("impact_id")), row, kind.lower().replace("_", " ")))
        found = self._artifacts("governance", ("routing_conflicts", "registry_conflicts"))
        for artifact_key in ("routing_conflicts", "registry_conflicts"):
            value: Any = found.get(artifact_key, (None, []))[1]
            for row in value if isinstance(value, list) else []:
                kind = str(row.get("conflict_code") or row.get("conflict_type") or "ROUTING_CONFLICT")
                conflicts.append(self._simple_conflict(kind, "routing", str(row.get("symbol_id") or row.get("conflict_id") or "unknown"), row, str(row.get("validation_message") or row.get("message") or row.get("reason") or kind)))
        for family, state in self._states.items():
            if state.source_version_mismatch:
                conflicts.append(self._simple_conflict("SOURCE_VERSION_MISMATCH", family, family, {}, "canonical sources have different run or policy versions"))
        unique = {str(item["conflict_id"]): item for item in conflicts}
        self._states["governance"] = self._states.get("governance", ProjectionState())
        if unique:
            self._states["governance"].limitations.append("GOVERNANCE_CONFLICT_PRESENT")
        return list(unique.values())

    @staticmethod
    def _simple_conflict(kind: str, entity_type: str, entity_id: str, row: dict[str, Any], message: str) -> dict[str, Any]:
        raw = f"{kind}|{entity_type}|{entity_id}|{row.get('as_of') or row.get('created_at')}"
        return {"conflict_id": hashlib.sha256(raw.encode()).hexdigest()[:24], "conflict_type": kind, "entity_type": entity_type, "entity_id": entity_id, "symbol_id": row.get("symbol_id"), "exchange": row.get("exchange"), "as_of": row.get("as_of") or row.get("created_at"), "severity": row.get("severity") or "warning", "status": row.get("status") or "OPEN", "message": message, "observation_ids": _list(row.get("observation_ids")), "policy_version": row.get("policy_version"), "source_refs": [], **{key: row.get(key) for key in ("requested_tier", "effective_tier", "reason", "created_at", "validation_message", "source") if row.get(key) is not None}}

    def calibration(self, resource: str) -> Any:
        if self.fixture:
            if resource == "summary":
                return {"manifest_id": "fixture-manifest", "eligible_count": 1, "excluded_count": 0, "quarantined_count": 0, "pending_count": 0, "top_exclusion_reasons": [], "total_samples": 1}
            if resource == "manifest":
                return {"manifest_id": "fixture-manifest", "policy_version": CALIBRATION_POLICY_VERSION, "source_hashes": {}, "replay_equivalent": True}
            if resource == "checks":
                return list(self.fixture["readiness_checks"])
            return []
        found = self._artifacts("calibration", ("phase3c5_calibration_quality_summary", "phase3c5_calibration_manifest", "phase3c5_sample_coverage", "phase3c5_exclusion_reasons", "phase3c5_calibration_excluded", "phase3c5_calibration_quarantined", "phase3c5_readiness_checks", "phase3c5_phase4_readiness"))
        state = self._states["calibration"]
        if "phase3c5_calibration_manifest" not in found:
            state.limitations.append("CALIBRATION_MANIFEST_MISSING")
        if "phase3c5_sample_coverage" not in found:
            state.limitations.append("CALIBRATION_COVERAGE_MISSING")
        if "phase3c5_exclusion_reasons" not in found:
            state.limitations.append("CALIBRATION_EXCLUSIONS_MISSING")
        if "phase3c5_readiness_checks" not in found and not isinstance(found.get("phase3c5_phase4_readiness", (None, {}))[1], dict):
            state.limitations.append("READINESS_CHECKS_MISSING")
        quality: Any = found.get("phase3c5_calibration_quality_summary", (None, {}))[1]
        readiness: Any = found.get("phase3c5_phase4_readiness", (None, {}))[1]
        manifest: Any = found.get("phase3c5_calibration_manifest", (None, {}))[1]
        if resource == "summary":
            coverage = quality.get("coverage_counts", {}) if isinstance(quality, dict) else {}
            reasons = quality.get("exclusion_reason_counts", {}) if isinstance(quality, dict) else {}
            return {"manifest_id": readiness.get("manifest_id") or manifest.get("manifest_id"), "total_samples": int(quality.get("total_rows", 0)), "eligible_count": int(quality.get("eligible_rows", 0)), "excluded_count": int(quality.get("excluded_rows", 0)), "quarantined_count": int(quality.get("quarantined_rows", 0)), "pending_count": int(quality.get("pending_rows", 0)), "class_counts": quality.get("class_ratio", {}), "largest_class_share": quality.get("largest_class_share"), "date_range": {"min": quality.get("date_min"), "max": quality.get("date_max")}, "regime_coverage": coverage.get("market_regime", {}), "stage_coverage": {"stock": coverage.get("stock_stage", {}), "sector": coverage.get("sector_stage", {})}, "scan_tier_coverage": coverage.get("scan_tier", {}), "setup_family_coverage": coverage.get("setup_family", {}), "policy_snapshot_coverage": coverage.get("policy_snapshot_id", {}), "admission_reason_coverage": coverage.get("primary_admission_reason", {}), "top_exclusion_reasons": [{"reason": key, "count": value} for key, value in sorted(reasons.items(), key=lambda item: item[1], reverse=True)], "policy_version": readiness.get("policy_version") or manifest.get("policy_version"), "formal_verdict": readiness.get("verdict"), "phase4_development_ready": readiness.get("phase4_development_ready"), "phase4_production_ready": readiness.get("phase4_production_ready")}
        if resource == "manifest":
            source_hashes = manifest.get("source_hashes", {})
            return {"manifest_id": manifest.get("manifest_id"), "policy_version": manifest.get("policy_version", CALIBRATION_POLICY_VERSION), "source_hashes": source_hashes, "schema_versions": manifest.get("schema_versions", {}), "migration_lineage": manifest.get("migration_lineage", []), "configuration_hash": manifest.get("configuration_hash") or manifest.get("config_hash"), "policy_hashes": manifest.get("policy_hashes", {}), "dataset_hashes": manifest.get("dataset_hashes", {key: value for key, value in manifest.items() if key.endswith("dataset_hash")}), "row_counts": manifest.get("row_counts", {}), "date_bounds": manifest.get("date_bounds", {}), "reproducibility_status": manifest.get("reproducibility_status"), "replay_equivalent": manifest.get("replay_equivalent"), "policy_snapshot_ids": manifest.get("policy_snapshot_ids", [])}
        if resource == "coverage":
            return found.get("phase3c5_sample_coverage", (None, []))[1]
        if resource == "exclusions":
            return found.get("phase3c5_exclusion_reasons", (None, []))[1]
        if resource == "checks":
            checks = found.get("phase3c5_readiness_checks", (None, readiness.get("checks", [])))[1]
            if not checks:
                state.limitations.append("READINESS_CHECKS_MISSING")
            return [{**row, "required_for_ready": _bool(row.get("required_for_ready"), _bool(row.get("development_blocking")) or _bool(row.get("production_blocking"))), "development_blocking": _bool(row.get("development_blocking")), "production_blocking": _bool(row.get("production_blocking")), "policy_version": row.get("policy_version") or readiness.get("policy_version")} for row in checks]
        return []

    def performance(self) -> list[dict[str, Any]]:
        if self.fixture:
            return list(self.fixture["performance"])
        families = self.locator.locate_runs("phase3c4_performance_summary")
        result = []
        state = ProjectionState()
        for family in families:
            summary_artifact = family["phase3c4_performance_summary"]
            try:
                summary = self.locator.read(summary_artifact)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(summary, dict):
                continue
            state.lineage.extend(item.lineage() for item in family.values())
            for key, code in (
                ("phase3c4_performance_metrics", "PERFORMANCE_METRICS_MISSING"),
                ("phase3c4_database_metrics", "PERFORMANCE_DATABASE_METRICS_MISSING"),
                ("phase3c4_artifact_metrics", "PERFORMANCE_ARTIFACT_METRICS_MISSING"),
                ("phase3c4_replay_comparison", "REPLAY_COMPARISON_MISSING"),
            ):
                if key not in family:
                    state.limitations.append(code)
            metrics = self._family_payload(family, "phase3c4_performance_metrics", [])
            artifacts = self._family_payload(family, "phase3c4_artifact_metrics", summary.get("artifact_metrics", []))
            database = self._family_payload(family, "phase3c4_database_metrics", summary.get("database_metrics", []))
            replay = self._family_payload(family, "phase3c4_replay_comparison", {})
            result.append({**summary, "stage_runtimes": {key: value.get("duration_ms", 0.0) for key, value in summary.get("stage_metrics", {}).items()}, "throughput": {str(row.get("operation_name")): float(row.get("symbols_per_second") or row.get("rows_per_second") or 0) for row in metrics if row.get("symbols_per_second") or row.get("rows_per_second")}, "operation_metrics": metrics, "artifact_metrics": artifacts, "database_metrics": database, "threshold_results": summary.get("threshold_evaluations", {}), "replay_comparison": replay, "replay_equivalence": summary.get("replay_equivalence") or replay.get("equivalence") or replay.get("status")})
        if not result:
            state.limitations.append("PERFORMANCE_SUMMARY_MISSING")
        semantic = [item for item in state.lineage if item.get("source_as_of") or item.get("available_at")]
        state.freshness = {"source_as_of": semantic[0].get("source_as_of") if semantic else None, "last_successful_run_at": semantic[0].get("available_at") if semantic else None, "freshness_status": "FRESH" if semantic else "UNKNOWN", "freshness_reasons": ["BENCHMARK_RUN_TIMESTAMP"] if semantic else ["SEMANTIC_TIMESTAMP_UNAVAILABLE"]}
        self._states["performance"] = state
        return sorted(result, key=lambda row: (str(row.get("as_of") or ""), str(row.get("run_id") or "")))

    def performance_baselines(self) -> list[dict[str, Any]]:
        return [row for row in self.performance() if str(row.get("source_profile") or row.get("profile") or row.get("baseline_profile") or "").lower() in {"copied_realistic", "copied-realistic"}]

    def _family_payload(self, family: dict[str, LocatedArtifact], key: str, default: Any) -> Any:
        artifact = family.get(key)
        if artifact is None:
            code = {"phase3c4_performance_metrics": "PERFORMANCE_METRICS_MISSING", "phase3c4_database_metrics": "PERFORMANCE_DATABASE_METRICS_MISSING", "phase3c4_artifact_metrics": "PERFORMANCE_ARTIFACT_METRICS_MISSING", "phase3c4_replay_comparison": "REPLAY_COMPARISON_MISSING"}.get(key)
            if code:
                self._states.setdefault("performance", ProjectionState()).limitations.append(code)
            return default
        return self.locator.read(artifact)

    def partial_limitations(self, resource: str, rows: list[Any]) -> list[str]:
        family = {"positions": "positions", "governance": "governance"}.get(resource, resource)
        state = self.projection_state(family)
        readiness_limitations = self.limitation_ids()
        if self.fixture:
            return readiness_limitations
        if rows:
            return _unique_limitations([*readiness_limitations, *state.limitations])
        table_requirements = {
            "stages": "weekly_stock_stage_history", "routing": "opportunity_scan_routing_history",
            "candidates": "candidate_episode", "positions": "position_recovery_proposal",
            "alerts": "pipeline_alert_incident", "governance": "stage_observation_governance",
        }
        required = table_requirements.get(resource)
        if required and required not in self.access.tables():
            return _unique_limitations([*readiness_limitations, "SOURCE_NOT_MIGRATED"])
        return _unique_limitations([*readiness_limitations, *state.limitations, "SOURCE_EMPTY"])

    @staticmethod
    def semantic_hash(value: Any) -> str:
        return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()
