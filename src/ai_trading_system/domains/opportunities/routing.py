"""Phase 3B/3C scan-routing contracts and deterministic policy."""

from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage

SCAN_ROUTING_POLICY_VERSION = "scan-routing-policy-v2"


class OpportunityScanRoutingMode(str, Enum):
    OFF = "off"
    COMPARE = "compare"
    SHADOW = "shadow"


class ScanTier(str, Enum):
    STAGE_ONLY = "stage_only"
    LIGHT_PATTERN = "light_pattern"
    FULL_INVESTIGATOR = "full_investigator"
    POSITION_MONITOR = "position_monitor"


class ScanProfile(str, Enum):
    LIGHT_DISCOVERY = "light_discovery"
    FULL_EVIDENCE = "full_evidence"
    POSITION_RISK = "position_risk"


class ScanReason(str, Enum):
    FULL_UNIVERSE_STRUCTURAL = "full_universe_structural"
    STAGE_1_DISCOVERY = "stage_1_discovery"
    STAGE_TRANSITION_DISCOVERY = "stage_transition_discovery"
    RANK_SELECTED = "rank_selected"
    STAGE_PROMOTED = "stage_promoted"
    ACTIVE_POSITION = "active_position"
    RECENT_EXIT = "recent_exit"
    TRIGGERED_CANDIDATE = "triggered_candidate"
    PENDING_FOLLOWTHROUGH = "pending_followthrough"
    MANUAL_OVERRIDE = "manual_override"


class RoutingConflictCode(str, Enum):
    UNKNOWN_SCAN_REASON = "UNKNOWN_SCAN_REASON"
    UNKNOWN_SCAN_TIER = "UNKNOWN_SCAN_TIER"
    REASON_TIER_MISMATCH = "REASON_TIER_MISMATCH"
    EFFECTIVE_TIER_TOO_LOW = "EFFECTIVE_TIER_TOO_LOW"
    INVALID_WINNING_REASON = "INVALID_WINNING_REASON"
    ACTIVE_POSITION_DEMOTION = "ACTIVE_POSITION_DEMOTION"
    RECENT_EXIT_DEMOTION = "RECENT_EXIT_DEMOTION"
    FOLLOWTHROUGH_DEMOTION = "FOLLOWTHROUGH_DEMOTION"
    INVALID_MANUAL_OVERRIDE = "INVALID_MANUAL_OVERRIDE"
    SECTOR_UNKNOWN_EARLY_ENTRY_BLOCK = "SECTOR_UNKNOWN_EARLY_ENTRY_BLOCK"
    ACTIVE_POSITION_MISSING_MARKET_DATA = "ACTIVE_POSITION_MISSING_MARKET_DATA"


class StageDiscoveryReason(str, Enum):
    STAGE_1_BASE = "stage_1_base"
    TRANSITION_1_TO_2 = "transition_1_to_2"
    BASE_CONTRACTION = "base_contraction"
    VOLUME_DRY_UP = "volume_dry_up"
    PIVOT_APPROACH = "pivot_approach"
    STAGE_CONFIDENCE_PROMOTION = "stage_confidence_promotion"


SCAN_TIER_PRECEDENCE: Mapping[ScanTier, int] = MappingProxyType(
    {
        ScanTier.STAGE_ONLY: 10,
        ScanTier.LIGHT_PATTERN: 20,
        ScanTier.FULL_INVESTIGATOR: 30,
        ScanTier.POSITION_MONITOR: 40,
    }
)

REASON_MINIMUM_TIER: Mapping[ScanReason, ScanTier] = MappingProxyType(
    {
        ScanReason.FULL_UNIVERSE_STRUCTURAL: ScanTier.STAGE_ONLY,
        ScanReason.STAGE_1_DISCOVERY: ScanTier.LIGHT_PATTERN,
        ScanReason.STAGE_TRANSITION_DISCOVERY: ScanTier.LIGHT_PATTERN,
        ScanReason.RANK_SELECTED: ScanTier.FULL_INVESTIGATOR,
        ScanReason.STAGE_PROMOTED: ScanTier.FULL_INVESTIGATOR,
        ScanReason.TRIGGERED_CANDIDATE: ScanTier.FULL_INVESTIGATOR,
        ScanReason.PENDING_FOLLOWTHROUGH: ScanTier.FULL_INVESTIGATOR,
        ScanReason.ACTIVE_POSITION: ScanTier.POSITION_MONITOR,
        ScanReason.RECENT_EXIT: ScanTier.POSITION_MONITOR,
    }
)

WINNING_REASON_TIE_BREAK: tuple[ScanReason, ...] = (
    ScanReason.ACTIVE_POSITION,
    ScanReason.RECENT_EXIT,
    ScanReason.PENDING_FOLLOWTHROUGH,
    ScanReason.TRIGGERED_CANDIDATE,
    ScanReason.STAGE_PROMOTED,
    ScanReason.RANK_SELECTED,
    ScanReason.STAGE_TRANSITION_DISCOVERY,
    ScanReason.STAGE_1_DISCOVERY,
    ScanReason.FULL_UNIVERSE_STRUCTURAL,
)

_REASON_ORDER = {reason: index for index, reason in enumerate(WINNING_REASON_TIE_BREAK)}
_STRUCTURAL_NEW_LONG_BLOCK_STAGES = {
    WeinsteinStage.TRANSITION_2_TO_3,
    WeinsteinStage.STAGE_3,
    WeinsteinStage.TRANSITION_3_TO_4,
    WeinsteinStage.STAGE_4,
}
_CRITICAL_STRUCTURAL_STAGES = {WeinsteinStage.TRANSITION_3_TO_4, WeinsteinStage.STAGE_4}
_HIGH_STRUCTURAL_STAGES = {WeinsteinStage.TRANSITION_2_TO_3, WeinsteinStage.STAGE_3}
_NON_DEMOTABLE_REASONS = {
    ScanReason.ACTIVE_POSITION: RoutingConflictCode.ACTIVE_POSITION_DEMOTION,
    ScanReason.RECENT_EXIT: RoutingConflictCode.RECENT_EXIT_DEMOTION,
    ScanReason.TRIGGERED_CANDIDATE: RoutingConflictCode.FOLLOWTHROUGH_DEMOTION,
    ScanReason.PENDING_FOLLOWTHROUGH: RoutingConflictCode.FOLLOWTHROUGH_DEMOTION,
}


@dataclass(frozen=True, slots=True)
class ScanSelection:
    reason: ScanReason
    required_tier: ScanTier
    source: str
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "metadata", MappingProxyType(dict(self.metadata or {}))
        )
        if self.reason is not ScanReason.MANUAL_OVERRIDE:
            expected = REASON_MINIMUM_TIER.get(self.reason)
            if expected is None:
                raise ValueError(f"unknown scan reason: {self.reason}")
            if self.required_tier is not expected:
                raise ValueError(
                    f"reason {self.reason.value} requires {expected.value}, got {self.required_tier.value}"
                )


@dataclass(frozen=True, slots=True)
class ManualScanOverride:
    requested_tier: ScanTier
    reviewer: str
    expires_at: datetime
    reason: str = ""
    source: str = "manual_override"

    def is_expired(self, now: datetime) -> bool:
        expiry = self.expires_at
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        current = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        return expiry <= current


@dataclass(frozen=True, slots=True)
class RoutingConflict:
    code: RoutingConflictCode
    severity: str
    message: str
    symbol_id: str = ""
    exchange: str = "NSE"
    field: str = ""
    observed_value: str = ""

    def as_row(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol_id": self.symbol_id,
            "severity": self.severity,
            "conflict": self.code.value,
            "field": self.field,
            "observed_value": self.observed_value,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class StageCoverageConfig:
    full_universe_stage_enabled: bool = True
    provisional_stage_refresh_enabled: bool = True
    weekly_lock_enabled: bool = True
    minimum_sector_constituents: int = 5
    minimum_sector_stage_coverage_ratio: float = 0.70
    minimum_liquidity_score: float = 0.20
    minimum_price: float = 20.0
    stage_classifier_version: str = "weekly-stage-v2"
    confidence_formula_version: str = "stage-confidence-v2"
    sector_stage_rule_version: str = "sector-stage-aggregation-v1"

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "StageCoverageConfig":
        return cls(
            full_universe_stage_enabled=bool(
                values.get("full_universe_stage_enabled", True)
            ),
            provisional_stage_refresh_enabled=bool(
                values.get("provisional_stage_refresh_enabled", True)
            ),
            weekly_lock_enabled=bool(values.get("weekly_lock_enabled", True)),
            minimum_sector_constituents=int(
                values.get("minimum_sector_constituents", 5)
            ),
            minimum_sector_stage_coverage_ratio=float(
                values.get("minimum_sector_stage_coverage_ratio", 0.70)
            ),
            minimum_liquidity_score=float(
                values.get("pattern_min_liquidity_score", 0.20)
            ),
            minimum_price=float(values.get("opportunity_stage_minimum_price", 20.0)),
        )


@dataclass(frozen=True, slots=True)
class ScanRoutingConfig:
    mode: OpportunityScanRoutingMode = OpportunityScanRoutingMode.OFF
    rank_deep_scan_limit: int = 250
    stage_promoted_scan_limit: int = 75
    stage_discovery_confidence_threshold: float = 75.0
    stage_promotion_confidence_threshold: float = 75.0
    light_pattern_min_base_weeks: int = 6
    light_pattern_max_base_depth: float = 35.0
    light_pattern_pivot_distance_threshold: float = 5.0
    light_pattern_score_threshold: float = 70.0
    scan_policy_version: str = SCAN_ROUTING_POLICY_VERSION
    light_pattern_rule_version: str = "light-pattern-v1"

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "ScanRoutingConfig":
        return cls(
            mode=OpportunityScanRoutingMode(
                str(values.get("opportunity_scan_routing_mode", "off")).lower()
            ),
            rank_deep_scan_limit=int(values.get("rank_deep_scan_limit", 250)),
            stage_promoted_scan_limit=int(values.get("stage_promoted_scan_limit", 75)),
            stage_discovery_confidence_threshold=float(
                values.get("stage_discovery_confidence_threshold", 75.0)
            ),
            stage_promotion_confidence_threshold=float(
                values.get("stage_promotion_confidence_threshold", 75.0)
            ),
            light_pattern_min_base_weeks=int(
                values.get("light_pattern_min_base_weeks", 6)
            ),
            light_pattern_max_base_depth=float(
                values.get("light_pattern_max_base_depth", 35.0)
            ),
            light_pattern_pivot_distance_threshold=float(
                values.get("light_pattern_pivot_distance_threshold", 5.0)
            ),
            light_pattern_score_threshold=float(
                values.get("light_pattern_score_threshold", 70.0)
            ),
        )


@dataclass(frozen=True, slots=True)
class PositionMonitoringConfig:
    recent_exit_cooling_sessions: int = 15
    recover_position_only_episodes: bool = False

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "PositionMonitoringConfig":
        sessions = int(values.get("recent_exit_cooling_sessions", 15))
        if not 10 <= sessions <= 20:
            raise ValueError("recent_exit_cooling_sessions must be between 10 and 20")
        return cls(
            recent_exit_cooling_sessions=sessions,
            recover_position_only_episodes=bool(
                values.get("recover_position_only_episodes", False)
            ),
        )


@dataclass(frozen=True, slots=True)
class ScanRoutingDecision:
    symbol_id: str
    exchange: str
    scan_tier: ScanTier
    reasons: tuple[ScanReason, ...]
    rank_selected: bool
    stage_selected: bool
    position_selected: bool
    recent_exit_selected: bool
    followthrough_selected: bool
    rank_position: int | None
    stock_stage: WeinsteinStage
    sector_stage: WeinsteinStage
    active_position: bool
    recently_exited: bool
    structural_long_blocked: bool
    market_data_available: bool
    policy_version: str = SCAN_ROUTING_POLICY_VERSION
    effective_scan_tier: ScanTier | None = None
    winning_reason: ScanReason | None = None
    all_selection_reasons: tuple[ScanReason, ...] = ()
    selection_details: tuple[Mapping[str, Any], ...] = ()
    routing_input_hash: str = ""
    routing_decision_id: str = ""
    new_long_structural_blocked: bool = False
    new_long_block_reasons: tuple[str, ...] = ()
    active_position_structural_risk: bool = False
    structural_risk_severity: str = ""
    structural_risk_reasons: tuple[str, ...] = ()
    validation_conflicts: tuple[RoutingConflict, ...] = ()

    def __post_init__(self) -> None:
        if self.effective_scan_tier is None:
            object.__setattr__(self, "effective_scan_tier", self.scan_tier)
        if not self.all_selection_reasons:
            object.__setattr__(self, "all_selection_reasons", self.reasons)
        if self.winning_reason is None and self.reasons:
            object.__setattr__(
                self, "winning_reason", _winning_reason(self.reasons, self.scan_tier)
            )
        if not self.selection_details:
            object.__setattr__(
                self,
                "selection_details",
                tuple(
                    MappingProxyType(
                        {
                            "reason": reason.value,
                            "required_tier": REASON_MINIMUM_TIER.get(
                                reason, self.scan_tier
                            ).value,
                            "source": "legacy",
                        }
                    )
                    for reason in self.reasons
                ),
            )
        if not self.new_long_structural_blocked and self.structural_long_blocked:
            object.__setattr__(self, "new_long_structural_blocked", True)
        if not self.new_long_block_reasons and self.new_long_structural_blocked:
            object.__setattr__(
                self, "new_long_block_reasons", ("stage_blocks_new_long",)
            )
        if not self.routing_input_hash:
            object.__setattr__(self, "routing_input_hash", _routing_hash(self))
        if not self.routing_decision_id:
            object.__setattr__(
                self,
                "routing_decision_id",
                hashlib.sha256(
                    f"{self.exchange}|{self.symbol_id}|{self.policy_version}|{self.routing_input_hash}".encode()
                ).hexdigest(),
            )
        conflicts = validate_scan_routing_decision(self)
        if conflicts:
            codes = ", ".join(conflict.code.value for conflict in conflicts)
            raise ValueError(f"invalid scan routing decision: {codes}")


def decide_scan_route(
    *,
    symbol_id: str,
    exchange: str = "NSE",
    rank_position: int | None = None,
    rank_selected: bool = False,
    stage_discovery: bool = False,
    stage_promoted: bool = False,
    active_position: bool = False,
    recently_exited: bool = False,
    triggered: bool = False,
    pending_followthrough: bool = False,
    stock_stage: WeinsteinStage = WeinsteinStage.UNKNOWN,
    sector_stage: WeinsteinStage = WeinsteinStage.UNKNOWN,
    market_data_available: bool = True,
    policy_version: str = SCAN_ROUTING_POLICY_VERSION,
    manual_overrides: Iterable[ManualScanOverride] = (),
    decided_at: datetime | None = None,
) -> ScanRoutingDecision:
    selections: list[ScanSelection] = [
        ScanSelection(
            ScanReason.FULL_UNIVERSE_STRUCTURAL,
            REASON_MINIMUM_TIER[ScanReason.FULL_UNIVERSE_STRUCTURAL],
            "full_universe",
        )
    ]
    if stock_stage is WeinsteinStage.STAGE_1 and stage_discovery:
        selections.append(
            ScanSelection(
                ScanReason.STAGE_1_DISCOVERY,
                REASON_MINIMUM_TIER[ScanReason.STAGE_1_DISCOVERY],
                "stage_discovery",
            )
        )
    if stock_stage is WeinsteinStage.TRANSITION_1_TO_2 and stage_discovery:
        selections.append(
            ScanSelection(
                ScanReason.STAGE_TRANSITION_DISCOVERY,
                REASON_MINIMUM_TIER[ScanReason.STAGE_TRANSITION_DISCOVERY],
                "stage_discovery",
            )
        )
    if rank_selected:
        selections.append(
            ScanSelection(
                ScanReason.RANK_SELECTED,
                REASON_MINIMUM_TIER[ScanReason.RANK_SELECTED],
                "rank",
                {"rank_position": rank_position},
            )
        )
    if stage_promoted:
        selections.append(
            ScanSelection(
                ScanReason.STAGE_PROMOTED,
                REASON_MINIMUM_TIER[ScanReason.STAGE_PROMOTED],
                "stage_promotion",
            )
        )
    if active_position:
        selections.append(
            ScanSelection(
                ScanReason.ACTIVE_POSITION,
                REASON_MINIMUM_TIER[ScanReason.ACTIVE_POSITION],
                "execution_position",
            )
        )
    if recently_exited:
        selections.append(
            ScanSelection(
                ScanReason.RECENT_EXIT,
                REASON_MINIMUM_TIER[ScanReason.RECENT_EXIT],
                "execution_recent_exit",
            )
        )
    if triggered:
        selections.append(
            ScanSelection(
                ScanReason.TRIGGERED_CANDIDATE,
                REASON_MINIMUM_TIER[ScanReason.TRIGGERED_CANDIDATE],
                "opportunity_lifecycle",
            )
        )
    if pending_followthrough:
        selections.append(
            ScanSelection(
                ScanReason.PENDING_FOLLOWTHROUGH,
                REASON_MINIMUM_TIER[ScanReason.PENDING_FOLLOWTHROUGH],
                "opportunity_lifecycle",
            )
        )

    now = decided_at or datetime.now(timezone.utc)
    conflicts: list[RoutingConflict] = []
    symbol = str(symbol_id).strip().upper()
    exch = str(exchange or "NSE").strip().upper()
    required_before_override = _effective_tier(
        item.required_tier for item in selections
    )
    for override in manual_overrides:
        if not override.reviewer.strip() or override.expires_at is None:
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.INVALID_MANUAL_OVERRIDE,
                    "error",
                    "manual override requires reviewer and expiry",
                    symbol,
                    exch,
                    "manual_override",
                )
            )
            continue
        if override.is_expired(now):
            continue
        if (
            SCAN_TIER_PRECEDENCE[override.requested_tier]
            < SCAN_TIER_PRECEDENCE[required_before_override]
        ):
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.INVALID_MANUAL_OVERRIDE,
                    "error",
                    "manual override cannot lower the required tier",
                    symbol,
                    exch,
                    "requested_tier",
                    override.requested_tier.value,
                )
            )
            for reason in {selection.reason for selection in selections} & set(
                _NON_DEMOTABLE_REASONS
            ):
                conflicts.append(
                    RoutingConflict(
                        _NON_DEMOTABLE_REASONS[reason],
                        "error",
                        f"{reason.value} cannot be demoted",
                        symbol,
                        exch,
                        "requested_tier",
                        override.requested_tier.value,
                    )
                )
            continue
        if (
            SCAN_TIER_PRECEDENCE[override.requested_tier]
            > SCAN_TIER_PRECEDENCE[required_before_override]
        ):
            selections.append(
                ScanSelection(
                    ScanReason.MANUAL_OVERRIDE,
                    override.requested_tier,
                    override.source,
                    {
                        "reviewer": override.reviewer,
                        "expires_at": override.expires_at.isoformat(),
                        "reason": override.reason,
                    },
                )
            )

    tier = _effective_tier(item.required_tier for item in selections)
    reasons = tuple(
        sorted(
            {item.reason for item in selections},
            key=lambda reason: _REASON_ORDER.get(reason, len(_REASON_ORDER)),
        )
    )
    structural_blocked = stock_stage in _STRUCTURAL_NEW_LONG_BLOCK_STAGES
    structural_reasons = (
        (f"stock_stage_{stock_stage.value}_blocks_new_long",)
        if structural_blocked
        else ()
    )
    active_structural_risk = bool(active_position and structural_blocked)
    severity = ""
    if active_structural_risk:
        severity = "CRITICAL" if stock_stage in _CRITICAL_STRUCTURAL_STAGES else "HIGH"
    selection_details = tuple(
        MappingProxyType(
            {
                "reason": item.reason.value,
                "required_tier": item.required_tier.value,
                "source": item.source,
                "metadata": dict(item.metadata or {}),
            }
        )
        for item in sorted(
            selections,
            key=lambda item: (
                -SCAN_TIER_PRECEDENCE[item.required_tier],
                _REASON_ORDER.get(item.reason, len(_REASON_ORDER)),
            ),
        )
    )
    winning_reason = _winning_reason(reasons, tier)
    return ScanRoutingDecision(
        symbol_id=symbol,
        exchange=exch,
        scan_tier=tier,
        reasons=reasons,
        rank_selected=rank_selected,
        stage_selected=stage_discovery or stage_promoted,
        position_selected=active_position,
        recent_exit_selected=recently_exited,
        followthrough_selected=triggered or pending_followthrough,
        rank_position=rank_position,
        stock_stage=stock_stage,
        sector_stage=sector_stage,
        active_position=active_position,
        recently_exited=recently_exited,
        structural_long_blocked=structural_blocked,
        market_data_available=market_data_available,
        policy_version=policy_version,
        effective_scan_tier=tier,
        winning_reason=winning_reason,
        all_selection_reasons=reasons,
        selection_details=selection_details,
        new_long_structural_blocked=structural_blocked,
        new_long_block_reasons=structural_reasons,
        active_position_structural_risk=active_structural_risk,
        structural_risk_severity=severity,
        structural_risk_reasons=structural_reasons if active_structural_risk else (),
        validation_conflicts=tuple(conflicts),
    )


def validate_scan_routing_decision(
    decision: ScanRoutingDecision,
) -> tuple[RoutingConflict, ...]:
    conflicts: list[RoutingConflict] = []
    symbol = decision.symbol_id
    exchange = decision.exchange
    tier = decision.effective_scan_tier or decision.scan_tier
    for value, field in (
        (tier, "effective_scan_tier"),
        (decision.scan_tier, "scan_tier"),
    ):
        if not isinstance(value, ScanTier):
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.UNKNOWN_SCAN_TIER,
                    "error",
                    "unknown scan tier",
                    symbol,
                    exchange,
                    field,
                    str(value),
                )
            )
    for reason in decision.reasons:
        if not isinstance(reason, ScanReason):
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.UNKNOWN_SCAN_REASON,
                    "error",
                    "unknown scan reason",
                    symbol,
                    exchange,
                    "scan_reasons",
                    str(reason),
                )
            )
            continue
        required = REASON_MINIMUM_TIER.get(reason)
        if required is None:
            if reason is ScanReason.MANUAL_OVERRIDE:
                continue
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.UNKNOWN_SCAN_REASON,
                    "error",
                    "unknown scan reason",
                    symbol,
                    exchange,
                    "scan_reasons",
                    reason.value,
                )
            )
            continue
        if SCAN_TIER_PRECEDENCE[tier] < SCAN_TIER_PRECEDENCE[required]:
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.EFFECTIVE_TIER_TOO_LOW,
                    "error",
                    f"{reason.value} requires at least {required.value}",
                    symbol,
                    exchange,
                    "effective_scan_tier",
                    tier.value,
                )
            )
        for detail in decision.selection_details:
            if (
                detail.get("reason") == reason.value
                and detail.get("required_tier")
                and detail.get("required_tier") != required.value
            ):
                conflicts.append(
                    RoutingConflict(
                        RoutingConflictCode.REASON_TIER_MISMATCH,
                        "error",
                        f"{reason.value} required tier mismatch",
                        symbol,
                        exchange,
                        "required_tier",
                        str(detail.get("required_tier")),
                    )
                )
    if decision.winning_reason is not None:
        if decision.winning_reason not in decision.reasons:
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.INVALID_WINNING_REASON,
                    "error",
                    "winning reason must be one of the selection reasons",
                    symbol,
                    exchange,
                    "winning_reason",
                    decision.winning_reason.value,
                )
            )
        elif (
            REASON_MINIMUM_TIER.get(decision.winning_reason, tier) is not tier
            and decision.winning_reason is not ScanReason.MANUAL_OVERRIDE
        ):
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.INVALID_WINNING_REASON,
                    "error",
                    "winning reason does not explain effective tier",
                    symbol,
                    exchange,
                    "winning_reason",
                    decision.winning_reason.value,
                )
            )
    return tuple(conflicts)


def validate_scan_routing_row(row: Mapping[str, Any]) -> tuple[RoutingConflict, ...]:
    conflicts: list[RoutingConflict] = []
    symbol = str(row.get("symbol_id") or "").strip().upper()
    exchange = str(row.get("exchange") or "NSE").strip().upper()
    try:
        tier = ScanTier(
            str(row.get("effective_scan_tier") or row.get("scan_tier") or "")
        )
    except ValueError:
        conflicts.append(
            RoutingConflict(
                RoutingConflictCode.UNKNOWN_SCAN_TIER,
                "error",
                "unknown scan tier",
                symbol,
                exchange,
                "scan_tier",
                str(row.get("effective_scan_tier") or row.get("scan_tier") or ""),
            )
        )
        return tuple(conflicts)
    reasons: list[ScanReason] = []
    for raw_reason in parse_scan_reasons(
        row.get("all_selection_reasons") or row.get("scan_reasons") or ()
    ):
        try:
            reasons.append(ScanReason(str(raw_reason)))
        except ValueError:
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.UNKNOWN_SCAN_REASON,
                    "error",
                    "unknown scan reason",
                    symbol,
                    exchange,
                    "scan_reasons",
                    str(raw_reason),
                )
            )
    for reason in reasons:
        required = REASON_MINIMUM_TIER.get(reason)
        if required is None:
            if reason is ScanReason.MANUAL_OVERRIDE:
                continue
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.UNKNOWN_SCAN_REASON,
                    "error",
                    "unknown scan reason",
                    symbol,
                    exchange,
                    "scan_reasons",
                    reason.value,
                )
            )
            continue
        if SCAN_TIER_PRECEDENCE[tier] < SCAN_TIER_PRECEDENCE[required]:
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.EFFECTIVE_TIER_TOO_LOW,
                    "error",
                    f"{reason.value} requires at least {required.value}",
                    symbol,
                    exchange,
                    "effective_scan_tier",
                    tier.value,
                )
            )
    raw_winning = row.get("winning_reason")
    if raw_winning not in (None, "", "nan"):
        try:
            winning = ScanReason(str(raw_winning))
            if winning not in reasons:
                conflicts.append(
                    RoutingConflict(
                        RoutingConflictCode.INVALID_WINNING_REASON,
                        "error",
                        "winning reason must be present in reasons",
                        symbol,
                        exchange,
                        "winning_reason",
                        winning.value,
                    )
                )
            elif (
                _winning_reason(tuple(reasons), tier) is not winning
                and winning is not ScanReason.MANUAL_OVERRIDE
            ):
                conflicts.append(
                    RoutingConflict(
                        RoutingConflictCode.INVALID_WINNING_REASON,
                        "error",
                        "winning reason does not match policy tie-break",
                        symbol,
                        exchange,
                        "winning_reason",
                        winning.value,
                    )
                )
        except ValueError:
            conflicts.append(
                RoutingConflict(
                    RoutingConflictCode.UNKNOWN_SCAN_REASON,
                    "error",
                    "unknown winning reason",
                    symbol,
                    exchange,
                    "winning_reason",
                    str(raw_winning),
                )
            )
    return tuple(conflicts)


def parse_scan_reasons(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item) for item in value if str(item))
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return ()
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple, set)):
            return tuple(str(item) for item in parsed if str(item))
    except (SyntaxError, ValueError):
        pass
    return tuple(item for item in text.split("|") if item)


def _effective_tier(tiers: Iterable[ScanTier]) -> ScanTier:
    return max(tiers, key=lambda tier: SCAN_TIER_PRECEDENCE[tier])


def _winning_reason(reasons: Iterable[ScanReason], tier: ScanTier) -> ScanReason:
    candidates = [
        reason
        for reason in reasons
        if reason is ScanReason.MANUAL_OVERRIDE
        or REASON_MINIMUM_TIER.get(reason) is tier
    ]
    if not candidates:
        candidates = list(reasons)
    return min(
        candidates, key=lambda reason: _REASON_ORDER.get(reason, len(_REASON_ORDER))
    )


def _routing_hash(decision: ScanRoutingDecision) -> str:
    payload = {
        "symbol_id": decision.symbol_id,
        "exchange": decision.exchange,
        "scan_tier": decision.scan_tier.value,
        "reasons": [reason.value for reason in decision.reasons],
        "rank_position": decision.rank_position,
        "stock_stage": decision.stock_stage.value,
        "sector_stage": decision.sector_stage.value,
        "active_position": decision.active_position,
        "recently_exited": decision.recently_exited,
        "market_data_available": decision.market_data_available,
        "policy_version": decision.policy_version,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()
