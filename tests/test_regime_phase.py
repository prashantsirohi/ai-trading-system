from __future__ import annotations

from ai_trading_system.analytics.regime.regime_phase import (
    RegimePhase,
    compute_regime_phase,
)


def test_regime_phase_bear_when_market_stage_s4():
    result = compute_regime_phase(
        market_stage="S4",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.20,
    )
    assert result.regime_phase == RegimePhase.BEAR_STAGE4


def test_regime_phase_bear_when_risk_off():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="risk_off",
        breadth_velocity_bucket="very_positive",
        s2_pct=0.35,
    )
    assert result.regime_phase == RegimePhase.BEAR_STAGE4


def test_regime_phase_base_forming_when_neutral_positive_velocity_low_s2():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.138,
    )
    assert result.regime_phase == RegimePhase.BASE_FORMING_STAGE1
    assert result.phase_label == "Base forming (S1)"


def test_regime_phase_transition_when_neutral_positive_velocity_s2_above_threshold():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="very_positive",
        s2_pct=0.31,
    )
    assert result.regime_phase == RegimePhase.TRANSITION_S1_TO_S2


def test_regime_phase_transition_when_cautious_bull_positive_velocity_s2_above_threshold():
    result = compute_regime_phase(
        market_stage="S1",
        regime="cautious_bull",
        breadth_velocity_bucket="positive",
        s2_pct=0.30,
    )
    assert result.regime_phase == RegimePhase.TRANSITION_S1_TO_S2


def test_regime_phase_confirmed_bull_when_bull_and_market_s2():
    result = compute_regime_phase(
        market_stage="S2",
        regime="bull",
        breadth_velocity_bucket="neutral",
        s2_pct=0.45,
    )
    assert result.regime_phase == RegimePhase.CONFIRMED_STAGE2_BULL


def test_regime_phase_strong_bull_requires_market_stage_s2_for_confirmed_phase():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="strong_bull",
        breadth_velocity_bucket="positive",
        s2_pct=0.25,
    )
    assert result.regime_phase == RegimePhase.MIXED_WAIT


def test_regime_phase_handles_missing_inputs():
    result = compute_regime_phase(
        market_stage=None,
        regime=None,
        breadth_velocity_bucket=None,
        s2_pct=0.0,
    )
    assert result.regime_phase == RegimePhase.MIXED_WAIT


def test_regime_phase_threshold_is_configurable():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.26,
        transition_s2_threshold=0.25,
    )
    assert result.regime_phase == RegimePhase.TRANSITION_S1_TO_S2


def test_regime_phase_bear_when_pct_above_200dma_capitulation():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="very_positive",
        s2_pct=0.10,
        pct_above_200dma=0.19,
    )
    assert result.regime_phase == RegimePhase.BEAR_STAGE4
    assert result.driven_by["breadth_level_zone"] == "capitulation"
    assert result.driven_by["pct_above_200dma"] == 0.19


def test_regime_phase_base_forming_in_bear_bottom_zone_with_positive_velocity():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.20,
        pct_above_200dma=0.25,
    )
    assert result.regime_phase == RegimePhase.BASE_FORMING_STAGE1
    assert result.driven_by["breadth_level_zone"] == "bear_bottom_zone"


def test_regime_phase_transition_in_base_recovery_zone_with_s2_above_threshold():
    result = compute_regime_phase(
        market_stage="S1",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.35,
        pct_above_200dma=0.40,
    )
    assert result.regime_phase == RegimePhase.TRANSITION_S1_TO_S2
    assert result.driven_by["breadth_level_zone"] == "base_recovery_zone"


def test_regime_phase_confirmed_bull_requires_risk_on_breadth_level():
    result = compute_regime_phase(
        market_stage="S2",
        regime="bull",
        breadth_velocity_bucket="neutral",
        s2_pct=0.45,
        pct_above_200dma=0.60,
    )
    assert result.regime_phase == RegimePhase.CONFIRMED_STAGE2_BULL
    assert result.driven_by["breadth_level_zone"] == "risk_on_zone"


def test_regime_phase_overheated_breadth_is_warning_not_bearish_override():
    result = compute_regime_phase(
        market_stage="S2",
        regime="strong_bull",
        breadth_velocity_bucket="positive",
        s2_pct=0.70,
        pct_above_200dma=0.92,
    )
    assert result.regime_phase == RegimePhase.CONFIRMED_STAGE2_BULL
    assert result.driven_by["breadth_level_zone"] == "overheated_breadth"


def test_regime_phase_invalid_pct_above_200dma_keeps_existing_behavior():
    result = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.20,
        pct_above_200dma="bad",  # type: ignore[arg-type]
    )
    assert result.regime_phase == RegimePhase.BASE_FORMING_STAGE1
    assert result.driven_by["breadth_level_zone"] is None
    assert result.driven_by["pct_above_200dma"] is None
