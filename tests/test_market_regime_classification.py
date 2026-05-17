from __future__ import annotations

from ai_trading_system.analytics.regime.breadth import classify_regime, confirmed_regime


def test_market_regime_classification_rules() -> None:
    assert classify_regime({"pct_above_200dma": 0.39}) == "risk_off"
    assert classify_regime({"pct_above_200dma": 0.50}) == "neutral"
    assert classify_regime({"pct_above_200dma": 0.60, "top1000_above_200dma": True}) == "bull"
    assert (
        classify_regime(
            {
                "pct_above_200dma": 0.75,
                "pct_above_50dma": 0.70,
                "top1000_above_50dma": True,
                "top1000_above_200dma": True,
            }
        )
        == "strong_bull"
    )


def test_confirmed_regime_uses_three_day_hysteresis() -> None:
    assert confirmed_regime(["bull", "neutral", "bull"]) == "bull"
    assert confirmed_regime(["risk_off", "neutral", "risk_off"]) == "risk_off"
    assert confirmed_regime(["bull", "neutral", "risk_off"]) == "neutral"
