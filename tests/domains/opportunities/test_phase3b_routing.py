from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage
from ai_trading_system.domains.opportunities.coverage import build_light_pattern_scan, build_sector_coverage
from ai_trading_system.domains.opportunities.routing import (
    ScanReason,
    ScanRoutingConfig,
    ScanTier,
    StageCoverageConfig,
    decide_scan_route,
)


def test_position_overrides_rank_and_preserves_reasons() -> None:
    decision = decide_scan_route(
        symbol_id="AAA",
        rank_position=1200,
        rank_selected=True,
        stage_promoted=True,
        active_position=True,
        stock_stage=WeinsteinStage.STAGE_2,
    )
    assert decision.scan_tier is ScanTier.POSITION_MONITOR
    assert set(decision.reasons) >= {
        ScanReason.ACTIVE_POSITION,
        ScanReason.RANK_SELECTED,
        ScanReason.STAGE_PROMOTED,
    }


def test_stage4_ranked_is_deep_scanned_but_long_blocked() -> None:
    decision = decide_scan_route(
        symbol_id="DECLINE",
        rank_position=50,
        rank_selected=True,
        stock_stage=WeinsteinStage.STAGE_4,
    )
    assert decision.scan_tier is ScanTier.FULL_INVESTIGATOR
    assert decision.structural_long_blocked is True


def _stage_row(symbol: str, sector: str, stage: WeinsteinStage, confidence: float = 85.0) -> dict:
    return {
        "symbol_id": symbol,
        "exchange": "NSE",
        "sector_id": sector,
        "sector_name": sector,
        "as_of": "2026-07-10",
        "source_week_start": "2026-07-06",
        "source_week_end": "2026-07-10",
        "effective_stage": stage.value,
        "stage_status": "locked",
        "stage_confidence_score": confidence,
        "weekly_ma_30_slope": 0.02,
        "weekly_ma_30_slope_acceleration": 0.01,
        "price_vs_weekly_ma_30_pct": 5.0,
        "weekly_rs_slope": 2.0,
        "base_duration_weeks": 12,
        "base_depth_pct": 20.0,
        "weekly_range_contraction": 0.8,
        "volume_dry_up": 0.7,
        "distance_to_pivot_pct": 3.0,
    }


def test_sector_uses_all_constituents_and_low_coverage_is_unknown() -> None:
    stock = pd.DataFrame([
        _stage_row("A", "TECH", WeinsteinStage.STAGE_2),
        _stage_row("B", "TECH", WeinsteinStage.STAGE_2),
        _stage_row("C", "TECH", WeinsteinStage.STAGE_2),
        _stage_row("D", "TECH", WeinsteinStage.STAGE_1),
        _stage_row("E", "TECH", WeinsteinStage.STAGE_1),
    ])
    sector = build_sector_coverage(stock, config=StageCoverageConfig())
    assert sector.iloc[0]["effective_stage"] == WeinsteinStage.STAGE_2.value
    assert sector.iloc[0]["eligible_constituents"] == 5

    too_small = build_sector_coverage(stock.head(4), config=StageCoverageConfig())
    assert too_small.iloc[0]["effective_stage"] == WeinsteinStage.UNKNOWN.value


def test_light_scan_promotes_high_quality_transition() -> None:
    stock = pd.DataFrame([_stage_row("EARLY", "TECH", WeinsteinStage.TRANSITION_1_TO_2)])
    light, promoted = build_light_pattern_scan(stock, config=ScanRoutingConfig())
    assert len(light) == 1
    assert light.iloc[0]["stage_discovery_eligible"]
    assert promoted.iloc[0]["symbol_id"] == "EARLY"
