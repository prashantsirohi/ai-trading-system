"""Runtime policy fingerprints and version/content enforcement (ADR-0006 A3).

Every semantic policy value — runtime-suppliable thresholds and single-sourced
code constants alike — is fingerprinted per version label at stage start. A
label that reappears with different content fails the Phase 3 stage before any
stage-owned write; the overall pipeline continues because Phase 3 remains an
optional shadow path.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from ai_trading_system.domains.opportunities.contracts import (
    STAGE_CONFIDENCE_FORMULA_VERSION,
)
from ai_trading_system.domains.opportunities.coverage import SECTOR_AGGREGATION_RULES
from ai_trading_system.domains.opportunities.orchestration import contracts as admission_policy
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    ADMISSION_RULE_VERSION,
    LIFECYCLE_RULE_VERSION,
    RETENTION_RULE_VERSION,
    SECTOR_GATE_RULES,
    SETUP_FAMILY_RULE_VERSION,
    OpportunityShadowConfig,
)
from ai_trading_system.domains.opportunities.orchestration.matching import (
    SETUP_FAMILY_PROGRESSION,
    SETUP_FAMILY_SUPERSESSION,
)
from ai_trading_system.domains.opportunities.orchestration import retention as retention_policy
from ai_trading_system.domains.opportunities.routing import (
    REASON_MINIMUM_TIER,
    SCAN_TIER_PRECEDENCE,
    WINNING_REASON_TIE_BREAK,
    ScanRoutingConfig,
    StageCoverageConfig,
)
from ai_trading_system.domains.opportunities.serialization import to_dict
from ai_trading_system.domains.opportunities.validation import (
    STAGE_CONFIDENCE_BAND_BOUNDS,
    STAGE_CONFIDENCE_WEIGHTS,
    default_candidate_retention_policy,
)


class PolicyVersionContentMismatchError(RuntimeError):
    """A registered policy version label reappeared with different content."""

    def __init__(self, version_label: str, changed_fields: dict[str, tuple[Any, Any]]):
        self.version_label = version_label
        self.changed_fields = changed_fields
        changes = "; ".join(
            f"{field}: {old!r} -> {new!r}" for field, (old, new) in sorted(changed_fields.items())
        )
        super().__init__(
            f"POLICY_VERSION_CONTENT_MISMATCH: {version_label} already registered "
            f"with another policy snapshot. Create a successor version label "
            f"(e.g. bump {version_label}). Changed fields: {changes or 'unresolved'}"
        )


@dataclass(frozen=True, slots=True)
class PolicySnapshot:
    """Per-label content hashes plus the composite run-level snapshot ID."""

    policy_snapshot_id: str
    label_hashes: Mapping[str, str]
    content: Mapping[str, Mapping[str, Any]]

    def metadata(self) -> dict[str, Any]:
        return {
            "policy_snapshot_id": self.policy_snapshot_id,
            "label_hashes": dict(self.label_hashes),
        }


def _canonical_json(value: Any) -> str:
    return json.dumps(to_dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def policy_content(
    shadow: OpportunityShadowConfig,
    routing: ScanRoutingConfig,
    coverage: StageCoverageConfig,
) -> dict[str, dict[str, Any]]:
    """Return every policy label's semantic content from the live objects.

    Values are read from the same constants and config instances the policy
    code executes, so code-constant drift changes the fingerprint at runtime.
    """
    retention = shadow.retention_policy or default_candidate_retention_policy()
    return {
        ADMISSION_RULE_VERSION: {
            "admission_rule_precedence": list(
                admission_policy.ADMISSION_RULE_PRECEDENCE
            ),
            "rank_admission_percentile": shadow.rank_admission_percentile,
            "rank_velocity_floor": shadow.rank_velocity_floor,
            "rank_velocity_percentile_floor": shadow.rank_velocity_percentile_floor,
            "investigator_admission_score": shadow.investigator_admission_score,
            "accumulation_admission_score": shadow.accumulation_admission_score,
            "pattern_admission_score": shadow.pattern_admission_score,
            "breakout_admission_score": shadow.breakout_admission_score,
            "breakout_admission_tiers": list(shadow.breakout_admission_tiers),
            "early_trigger_stage_confidence_threshold": (
                shadow.early_trigger_stage_confidence_threshold
            ),
        },
        LIFECYCLE_RULE_VERSION: {
            "setup_forming_evidence_threshold": shadow.setup_forming_evidence_threshold,
            "ready_evidence_threshold": shadow.ready_evidence_threshold,
            "ready_stage_confidence_threshold": shadow.ready_stage_confidence_threshold,
            "early_trigger_evidence_threshold": shadow.early_trigger_evidence_threshold,
            "early_trigger_stage_confidence_threshold": shadow.early_trigger_stage_confidence_threshold,
            "allowed_market_regimes": list(shadow.allowed_market_regimes),
            "close_stage_4_without_position": shadow.close_stage_4_without_position,
            "sector_gate_rules": dict(SECTOR_GATE_RULES),
        },
        SETUP_FAMILY_RULE_VERSION: {
            "progression": list(SETUP_FAMILY_PROGRESSION),
            "supersession": dict(SETUP_FAMILY_SUPERSESSION),
            "setup_progression_max_days": shadow.setup_progression_max_days,
        },
        RETENTION_RULE_VERSION: {
            "rules": [to_dict(rule) for rule in retention.rules],
            "archive_failed_after_days": shadow.archive_failed_after_days,
            "counting_unit": retention_policy.RETENTION_COUNTING_UNIT,
            "counter_guard": retention_policy.RETENTION_COUNTER_GUARD,
        },
        routing.scan_policy_version: {
            "scan_tier_precedence": {tier.value: rank for tier, rank in SCAN_TIER_PRECEDENCE.items()},
            "reason_minimum_tier": {reason.value: tier.value for reason, tier in REASON_MINIMUM_TIER.items()},
            "winning_reason_tie_break": [reason.value for reason in WINNING_REASON_TIE_BREAK],
            "rank_deep_scan_limit": routing.rank_deep_scan_limit,
            "stage_promoted_scan_limit": routing.stage_promoted_scan_limit,
            "stage_discovery_confidence_threshold": routing.stage_discovery_confidence_threshold,
            "stage_promotion_confidence_threshold": routing.stage_promotion_confidence_threshold,
            "light_pattern_rule_version": routing.light_pattern_rule_version,
            "light_pattern_min_base_weeks": routing.light_pattern_min_base_weeks,
            "light_pattern_max_base_depth": routing.light_pattern_max_base_depth,
            "light_pattern_pivot_distance_threshold": routing.light_pattern_pivot_distance_threshold,
            "light_pattern_score_threshold": routing.light_pattern_score_threshold,
        },
        coverage.sector_stage_rule_version: {
            **dict(SECTOR_AGGREGATION_RULES),
            "minimum_sector_constituents": coverage.minimum_sector_constituents,
            "minimum_sector_stage_coverage_ratio": coverage.minimum_sector_stage_coverage_ratio,
        },
        STAGE_CONFIDENCE_FORMULA_VERSION: {
            "weights": dict(STAGE_CONFIDENCE_WEIGHTS),
            "band_bounds": dict(STAGE_CONFIDENCE_BAND_BOUNDS),
        },
    }


def build_policy_snapshot(
    shadow: OpportunityShadowConfig,
    routing: ScanRoutingConfig,
    coverage: StageCoverageConfig,
) -> PolicySnapshot:
    content = policy_content(shadow, routing, coverage)
    label_hashes = {label: _digest(values) for label, values in content.items()}
    composite = _digest(label_hashes)
    return PolicySnapshot(composite, label_hashes, content)


def compute_policy_snapshot(params: Mapping[str, Any]) -> PolicySnapshot:
    """Compute the full snapshot from pipeline params, as every stage does."""
    return build_policy_snapshot(
        OpportunityShadowConfig.from_mapping(params),
        ScanRoutingConfig.from_mapping(params),
        StageCoverageConfig.from_mapping(params),
    )


def append_policy_snapshot_event(
    registry: Any, snapshot: PolicySnapshot, *, run_id: str, stage_name: str
) -> None:
    """Stamp the run's metadata audit trail with the active policy snapshot.

    Missing pipeline_run rows occur only outside orchestrated runs (direct
    stage invocation, test harnesses); there the event has no home and the
    registry table plus summary artifacts remain the persisted evidence.
    """
    try:
        registry.append_run_metadata_event(
            run_id, {"event": "policy_snapshot", "stage": stage_name, **snapshot.metadata()},
        )
    except KeyError:
        return


def _content_diff(stored: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, tuple[Any, Any]]:
    changed: dict[str, tuple[Any, Any]] = {}
    for field in sorted(set(stored) | set(current)):
        if stored.get(field) != current.get(field):
            changed[field] = (stored.get(field), current.get(field))
    return changed


def register_or_verify_policy_snapshots(
    registry: Any,
    snapshot: PolicySnapshot,
    *,
    run_id: str,
) -> dict[str, int]:
    """Register unseen labels; verify known ones; raise on content mismatch.

    Runs before any other stage-owned write so a mismatch fails the optional
    Phase 3 stage fail-closed without touching canonical history.
    """
    registered = verified = 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute("BEGIN TRANSACTION")
        for label in sorted(snapshot.label_hashes):
            label_hash = snapshot.label_hashes[label]
            row = conn.execute(
                "SELECT policy_snapshot_id, content_json FROM policy_version_registry WHERE version_label = ?",
                [label],
            ).fetchone()
            if row is None:
                conn.execute(
                    """INSERT INTO policy_version_registry
                           (version_label, policy_snapshot_id, content_json, first_registered_at, first_run_id)
                       VALUES (?, ?, ?, ?, ?)""",
                    [label, label_hash, _canonical_json(snapshot.content[label]), now, run_id],
                )
                registered += 1
                continue
            if str(row[0]) == label_hash:
                verified += 1
                continue
            stored_content = json.loads(str(row[1])) if row[1] else {}
            raise PolicyVersionContentMismatchError(
                label, _content_diff(stored_content, to_dict(snapshot.content[label]))
            )
    return {"registered": registered, "verified": verified}
