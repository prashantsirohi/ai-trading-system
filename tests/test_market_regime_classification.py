"""Classification + hysteresis tests for the 5-tier regime ladder.

Phase 4b changed:
  - risk_off requires BOTH 200DMA<30% AND new_highs<5% (recovery
    periods with improving 200DMA but lagging highs are now `neutral`).
  - bull requires pct_at_52w_high>=12% in addition to 200DMA>=55%.
  - cautious_bull (new) takes 200DMA>=55% when leadership is thin.
  - strong_bull thresholds raised: 200DMA>=75%, 50DMA>=60%, highs>=15%.
"""

from __future__ import annotations

from ai_trading_system.analytics.regime.breadth import classify_regime, confirmed_regime


def test_risk_off_requires_both_weak_200dma_and_weak_leadership() -> None:
    # Both weak: classic risk_off
    assert (
        classify_regime({"pct_above_200dma": 0.20, "pct_at_52w_high": 0.02})
        == "risk_off"
    )
    # 200DMA weak but new highs are present (recovery starting): NOT risk_off,
    # falls to neutral. This is the user's explicit recovery-period intent.
    assert (
        classify_regime({"pct_above_200dma": 0.25, "pct_at_52w_high": 0.10})
        == "neutral"
    )


def test_neutral_band() -> None:
    # 200DMA 30-55% → neutral regardless of leadership
    assert classify_regime({"pct_above_200dma": 0.35}) == "neutral"
    assert classify_regime({"pct_above_200dma": 0.50}) == "neutral"


def test_cautious_bull_for_thin_leadership() -> None:
    # 200DMA healthy, top1000 confirms, but new-high participation < 12%
    assert (
        classify_regime(
            {
                "pct_above_200dma": 0.60,
                "pct_at_52w_high": 0.08,
                "top1000_above_200dma": True,
            }
        )
        == "cautious_bull"
    )


def test_full_bull_requires_leadership_breadth() -> None:
    assert (
        classify_regime(
            {
                "pct_above_200dma": 0.60,
                "pct_at_52w_high": 0.14,
                "top1000_above_200dma": True,
            }
        )
        == "bull"
    )


def test_strong_bull_needs_all_three_conditions() -> None:
    base = {
        "pct_above_200dma": 0.80,
        "pct_above_50dma": 0.65,
        "pct_at_52w_high": 0.18,
        "top1000_above_50dma": True,
        "top1000_above_200dma": True,
    }
    assert classify_regime(base) == "strong_bull"
    # Drop leadership: degrades to bull (still has 50DMA + 200DMA)
    assert classify_regime({**base, "pct_at_52w_high": 0.13}) == "bull"
    # Drop 50DMA: degrades to bull
    assert classify_regime({**base, "pct_above_50dma": 0.55}) == "bull"


def test_confirmed_regime_uses_three_day_hysteresis() -> None:
    # Existing semantics still hold
    assert confirmed_regime(["bull", "neutral", "bull"]) == "bull"
    assert confirmed_regime(["risk_off", "neutral", "risk_off"]) == "risk_off"
    assert confirmed_regime(["bull", "neutral", "risk_off"]) == "neutral"


def test_confirmed_regime_cautious_bull_window() -> None:
    """Phase 4b: cautious_bull confirms when ≥2 of last 3 are
    cautious_bull-or-better (bull/strong_bull count too)."""
    assert (
        confirmed_regime(["cautious_bull", "neutral", "cautious_bull"])
        == "cautious_bull"
    )
    # A bull→cautious_bull jitter still confirms cautious_bull, not neutral
    assert (
        confirmed_regime(["bull", "neutral", "cautious_bull"]) == "cautious_bull"
    )
