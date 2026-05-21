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
