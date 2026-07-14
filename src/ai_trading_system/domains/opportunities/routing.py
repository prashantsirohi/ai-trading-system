"""Phase 3B scan-routing contracts and deterministic policy."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage


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


class StageDiscoveryReason(str, Enum):
    STAGE_1_BASE = "stage_1_base"
    TRANSITION_1_TO_2 = "transition_1_to_2"
    BASE_CONTRACTION = "base_contraction"
    VOLUME_DRY_UP = "volume_dry_up"
    PIVOT_APPROACH = "pivot_approach"
    STAGE_CONFIDENCE_PROMOTION = "stage_confidence_promotion"


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
            full_universe_stage_enabled=bool(values.get("full_universe_stage_enabled", True)),
            provisional_stage_refresh_enabled=bool(values.get("provisional_stage_refresh_enabled", True)),
            weekly_lock_enabled=bool(values.get("weekly_lock_enabled", True)),
            minimum_sector_constituents=int(values.get("minimum_sector_constituents", 5)),
            minimum_sector_stage_coverage_ratio=float(values.get("minimum_sector_stage_coverage_ratio", 0.70)),
            minimum_liquidity_score=float(values.get("pattern_min_liquidity_score", 0.20)),
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
    scan_policy_version: str = "scan-routing-v1"
    light_pattern_rule_version: str = "light-pattern-v1"

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "ScanRoutingConfig":
        return cls(
            mode=OpportunityScanRoutingMode(str(values.get("opportunity_scan_routing_mode", "off")).lower()),
            rank_deep_scan_limit=int(values.get("rank_deep_scan_limit", 250)),
            stage_promoted_scan_limit=int(values.get("stage_promoted_scan_limit", 75)),
            stage_discovery_confidence_threshold=float(values.get("stage_discovery_confidence_threshold", 75.0)),
            stage_promotion_confidence_threshold=float(values.get("stage_promotion_confidence_threshold", 75.0)),
            light_pattern_min_base_weeks=int(values.get("light_pattern_min_base_weeks", 6)),
            light_pattern_max_base_depth=float(values.get("light_pattern_max_base_depth", 35.0)),
            light_pattern_pivot_distance_threshold=float(values.get("light_pattern_pivot_distance_threshold", 5.0)),
            light_pattern_score_threshold=float(values.get("light_pattern_score_threshold", 70.0)),
        )


@dataclass(frozen=True, slots=True)
class PositionMonitoringConfig:
    recent_exit_cooling_sessions: int = 15
    recover_position_only_episodes: bool = True

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "PositionMonitoringConfig":
        sessions = int(values.get("recent_exit_cooling_sessions", 15))
        if not 10 <= sessions <= 20:
            raise ValueError("recent_exit_cooling_sessions must be between 10 and 20")
        return cls(
            recent_exit_cooling_sessions=sessions,
            recover_position_only_episodes=bool(values.get("recover_position_only_episodes", True)),
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
    policy_version: str = "scan-routing-v1"


_REASON_ORDER = {reason: index for index, reason in enumerate(ScanReason)}


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
    policy_version: str = "scan-routing-v1",
) -> ScanRoutingDecision:
    reasons = {ScanReason.FULL_UNIVERSE_STRUCTURAL}
    if stock_stage is WeinsteinStage.STAGE_1 and stage_discovery:
        reasons.add(ScanReason.STAGE_1_DISCOVERY)
    if stock_stage is WeinsteinStage.TRANSITION_1_TO_2 and stage_discovery:
        reasons.add(ScanReason.STAGE_TRANSITION_DISCOVERY)
    if rank_selected:
        reasons.add(ScanReason.RANK_SELECTED)
    if stage_promoted:
        reasons.add(ScanReason.STAGE_PROMOTED)
    if active_position:
        reasons.add(ScanReason.ACTIVE_POSITION)
    if recently_exited:
        reasons.add(ScanReason.RECENT_EXIT)
    if triggered:
        reasons.add(ScanReason.TRIGGERED_CANDIDATE)
    if pending_followthrough:
        reasons.add(ScanReason.PENDING_FOLLOWTHROUGH)

    if active_position or recently_exited:
        tier = ScanTier.POSITION_MONITOR
    elif triggered or pending_followthrough or stage_promoted or rank_selected:
        tier = ScanTier.FULL_INVESTIGATOR
    elif stage_discovery:
        tier = ScanTier.LIGHT_PATTERN
    else:
        tier = ScanTier.STAGE_ONLY
    return ScanRoutingDecision(
        symbol_id=str(symbol_id).strip().upper(),
        exchange=str(exchange or "NSE").strip().upper(),
        scan_tier=tier,
        reasons=tuple(sorted(reasons, key=_REASON_ORDER.__getitem__)),
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
        structural_long_blocked=(not active_position and stock_stage in {WeinsteinStage.STAGE_3, WeinsteinStage.STAGE_4}),
        market_data_available=market_data_available,
        policy_version=policy_version,
    )
