"""Phase 8 market-direction helper tests."""

from __future__ import annotations

import pytest

from ai_trading_system.analytics.regime.direction import build_market_direction
from ai_trading_system.analytics.regime.profiles import BreadthImpulseRiskMatrix, RiskCell


def _matrix(*, regime: str, bucket: str, action: str = "hold", exposure: float = 0.8) -> BreadthImpulseRiskMatrix:
    return BreadthImpulseRiskMatrix(
        name="test_matrix",
        cells={
            (regime, bucket): RiskCell(
                regime=regime,
                velocity_bucket=bucket,
                gross_exposure=exposure,
                allow_new_buys=True,
                min_score=64,
                require_breakout_tier="normal",
                require_setup_quality_gte=0.65,
                allow_pyramiding=False,
                action=action,
            )
        },
    )


@pytest.mark.parametrize(
    "regime,bucket,leadership,bias",
    [
        ("risk_off", "very_negative", False, "Bearish / capital protection"),
        ("risk_off", "very_positive", True, "Recovery attempt"),
        ("bull", "positive", True, "Confirmed uptrend"),
        ("strong_bull", "very_negative", False, "Late-cycle warning"),
    ],
)
def test_market_direction_headline_cases(regime: str, bucket: str, leadership: bool, bias: str) -> None:
    out = build_market_direction(
        market_regime={
            "regime": regime,
            "breadth_velocity_bucket": bucket,
            "regime_age_days": 21,
            "regime_confidence": 0.9,
            "leadership_velocity_confirmed": leadership,
        },
        risk_matrix=_matrix(regime=regime, bucket=bucket, action="scale_in", exposure=1.0),
    )

    assert out["direction_bias"] == bias
    assert out["matrix_active"] is True
    assert out["allowed_exposure"] == 0.85
    assert out["required_min_score"] == 64
    assert out["applied_live"] is False


def test_positive_velocity_without_leadership_is_selective() -> None:
    out = build_market_direction(
        market_regime={
            "regime": "neutral",
            "breadth_velocity_bucket": "positive",
            "leadership_velocity_confirmed": False,
        },
        risk_matrix=_matrix(regime="neutral", bucket="positive"),
    )
    assert out["direction_bias"] == "Early risk-on (selective; leadership unconfirmed)"


def test_missing_matrix_falls_back_to_legacy_profile() -> None:
    out = build_market_direction(
        market_regime={"regime": "bull", "breadth_velocity_bucket": "positive"},
        regime_profile={
            "regime": "bull",
            "max_exposure": 0.85,
            "max_positions": 10,
            "min_score": 64,
            "breakout_mode": "normal",
        },
        risk_matrix=None,
    )

    assert out["matrix_active"] is False
    assert out["action"] == "legacy_profile"
    assert out["allowed_exposure"] == 0.85
    assert out["new_buys_allowed"] is True
    assert out["required_breakout_tier"] == "normal"
