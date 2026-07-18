"""Frozen, serializable policies for ADR-0007 R0 replay.

The values in this module are intentionally isolated from operational rank
configuration. Changing one requires a successor policy version; manifests
bind both the version labels and their canonical content hash.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Literal


Lane = Literal[
    "stage2_continuation",
    "stage1_base",
    "young_listing_base",
    "ipo_early_base",
    "no_lane",
]
FamilyDisposition = Literal[
    "allowed",
    "excluded",
    "suppression_only",
    "not_applicable_due_to_history",
]

PATTERN_FAMILIES: tuple[str, ...] = (
    "cup_handle",
    "round_bottom",
    "double_bottom",
    "flag",
    "high_tight_flag",
    "ascending_triangle",
    "symmetrical_triangle",
    "ascending_base",
    "vcp",
    "flat_base",
    "stage2_reclaim",
    "darvas_box",
    "pocket_pivot",
    "ipo_base",
    "inside_week_breakout",
    "three_weeks_tight",
    "inside_day",
    "head_shoulders",
)


@dataclass(frozen=True)
class StandardLiquidityPolicy:
    version: str = "pattern-standard-liquidity-policy-v1"
    min_bars: int = 50
    min_close: float = 20.0
    min_liquidity_percentile: float = 0.20


@dataclass(frozen=True)
class EarlyIpoLiquidityPolicy:
    version: str = "ipo-early-liquidity-policy-v1"
    min_bars: int = 35
    max_bars: int = 49
    max_missing_session_ratio: float = 0.20
    min_median_turnover: float = 5_000_000.0
    min_median_volume: float = 50_000.0
    min_close: float = 20.0
    min_estimation_sessions: int = 20
    min_continuity_ratio: float = 0.80
    min_valid_ohlcv_ratio: float = 1.0
    allowed_exchanges: tuple[str, ...] = ("NSE",)
    require_latest_session_observation: bool = True
    reject_corporate_action_or_bad_data: bool = True


@dataclass(frozen=True)
class Stage2Policy:
    version: str = "pattern-stage2-validity-policy-v1"
    score_threshold: float = 70.0
    min_mature_bars: int = 180
    min_complete_long_history_bars: int = 200
    max_distance_from_52w_high_pct: float = 25.0
    require_close_above_sma150: bool = True
    require_close_above_sma200: bool = True
    require_sma150_above_sma200: bool = True
    require_positive_sma200_slope: bool = True


@dataclass(frozen=True)
class WeeklyStageFreshnessPolicy:
    version: str = "weekly-stage-freshness-policy-v2"
    max_age_trading_days: int = 10
    # Current stage and transition are separate facts: a fresh observation is
    # Stage-1 admissible when its current stage is S1, OR when the
    # observation itself records a permitted fresh transition (e.g. a current
    # S2 that just arrived from S1). v1 conflated the two by matching
    # transition strings inside the label vocabulary.
    allowed_stage1_labels: tuple[str, ...] = ("S1",)
    allowed_stage1_transitions: tuple[str, ...] = ("S1_TO_S2",)


@dataclass(frozen=True)
class Stage1StructurePolicy:
    version: str = "pattern-stage1-structure-policy-v1"
    sma150_band_pct: float = 0.15
    sma150_slope_abs_max_pct: float = 2.0
    base_lookback_bars: int = 65
    max_base_depth_pct: float = 0.35
    contraction_window_bars: int = 20
    max_range_contraction_ratio: float = 0.90
    max_pivot_distance_pct: float = 0.10
    rs_short_bars: int = 20
    rs_long_bars: int = 60
    min_rs_trend_delta_pct: float = 0.0
    max_volume_dry_up_ratio: float = 0.90
    min_close_to_sma200_ratio: float = 0.85
    min_sma200_slope_pct: float = -1.0


def _all(status: FamilyDisposition) -> dict[str, FamilyDisposition]:
    return {family: status for family in PATTERN_FAMILIES}


def _family_matrix() -> dict[str, dict[str, FamilyDisposition]]:
    early = _all("not_applicable_due_to_history")
    early["ipo_base"] = "allowed"
    young_short = dict(early)

    base_allowed = {
        "cup_handle",
        "round_bottom",
        "double_bottom",
        "ascending_triangle",
        "symmetrical_triangle",
        "ascending_base",
        "vcp",
        "flat_base",
        "darvas_box",
        "ipo_base",
        "inside_week_breakout",
        "inside_day",
    }
    young_long = _all("excluded")
    stage1 = _all("excluded")
    for family in base_allowed:
        young_long[family] = "allowed"
        stage1[family] = "allowed"
    young_long["head_shoulders"] = "suppression_only"
    stage1["head_shoulders"] = "suppression_only"

    stage2 = _all("allowed")
    stage2["ipo_base"] = "not_applicable_due_to_history"
    stage2["head_shoulders"] = "suppression_only"
    return {
        "ipo_early_base:35_49": early,
        "young_listing_base:50_119": young_short,
        "young_listing_base:120_179": young_long,
        "stage1_base:180_plus": stage1,
        "stage2_continuation:180_plus": stage2,
    }


@dataclass(frozen=True)
class FamilyPolicy:
    version: str = "pattern-family-policy-v1"
    detector_floor_bars: int = 120
    ipo_detector_floor_bars: int = 35
    matrix: dict[str, dict[str, FamilyDisposition]] = field(default_factory=_family_matrix)


@dataclass(frozen=True)
class OutcomePolicy:
    version: str = "pattern-r0-outcome-policy-v2"
    horizons: tuple[int, ...] = (5, 10, 20)
    benchmark_symbol: str = "UNIV_TOP1000_EW"
    benchmark_source: str = "universe_index_daily:UNIV_TOP1000_MCAP:equal_weight"
    breakout_buffer_pct: float = 0.0
    failed_breakout_close_below_invalidation: bool = True
    matched_control_method: str = "same_date_lane_history_band_liquidity_decile_nearest_symbol"
    confidence_level: float = 0.95
    minimum_observations_per_lane_family: int = 30


@dataclass(frozen=True)
class ReconstructionPolicy:
    version: str = "pattern-r0-reconstruction-policy-v1"
    history_lookback_bars: int = 420
    inclusive_as_of: bool = True
    weekly_stage_selection: str = "latest_week_end_on_or_before_as_of"
    source_table: str = "_catalog"
    future_rows_allowed_only_for_outcomes: bool = True


@dataclass(frozen=True)
class R0Policy:
    version: str = "pattern-lane-r0-policy-v1"
    standard_liquidity: StandardLiquidityPolicy = field(default_factory=StandardLiquidityPolicy)
    early_ipo_liquidity: EarlyIpoLiquidityPolicy = field(default_factory=EarlyIpoLiquidityPolicy)
    stage2: Stage2Policy = field(default_factory=Stage2Policy)
    weekly_freshness: WeeklyStageFreshnessPolicy = field(default_factory=WeeklyStageFreshnessPolicy)
    stage1: Stage1StructurePolicy = field(default_factory=Stage1StructurePolicy)
    families: FamilyPolicy = field(default_factory=FamilyPolicy)
    outcomes: OutcomePolicy = field(default_factory=OutcomePolicy)
    reconstruction: ReconstructionPolicy = field(default_factory=ReconstructionPolicy)

    def to_metadata(self) -> dict[str, object]:
        return asdict(self)

    def canonical_json(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True, separators=(",", ":"))

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def validate(self) -> None:
        expected = set(PATTERN_FAMILIES)
        required_keys = {
            "ipo_early_base:35_49",
            "young_listing_base:50_119",
            "young_listing_base:120_179",
            "stage1_base:180_plus",
            "stage2_continuation:180_plus",
        }
        if set(self.families.matrix) != required_keys:
            raise ValueError("family matrix does not cover every lane/history band")
        for key, row in self.families.matrix.items():
            if set(row) != expected:
                raise ValueError(f"family matrix row {key!r} is incomplete")
        if self.families.matrix["ipo_early_base:35_49"]["ipo_base"] != "allowed":
            raise ValueError("early IPO lane must allow ipo_base")
        if any(
            row["head_shoulders"] != "suppression_only"
            for key, row in self.families.matrix.items()
            if key not in {"ipo_early_base:35_49", "young_listing_base:50_119"}
        ):
            raise ValueError("head_shoulders must remain suppression-only where history permits")


def default_r0_policy() -> R0Policy:
    policy = R0Policy()
    policy.validate()
    return policy
