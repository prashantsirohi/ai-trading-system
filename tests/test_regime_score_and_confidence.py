"""Tests for the Phase 4b derived metrics: regime_score and regime_confidence."""

from __future__ import annotations

from ai_trading_system.analytics.regime.breadth import (
    compute_regime_confidence,
    compute_regime_score,
)


# ── regime_score: 0..100 weighted blend ───────────────────────────────────


def test_regime_score_clamped_to_zero_when_all_inputs_zero() -> None:
    assert compute_regime_score({}) == 0.0
    assert (
        compute_regime_score(
            {"pct_above_200dma": 0.0, "pct_above_50dma": 0.0, "pct_at_52w_high": 0.0}
        )
        == 0.0
    )


def test_regime_score_hits_100_when_all_inputs_one() -> None:
    assert (
        compute_regime_score(
            {
                "pct_above_200dma": 1.0,
                "pct_above_50dma": 1.0,
                "pct_at_52w_high": 1.0,
            }
        )
        == 100.0
    )


def test_regime_score_is_monotone_in_200dma() -> None:
    base = {"pct_above_50dma": 0.4, "pct_at_52w_high": 0.05}
    scores = [
        compute_regime_score({**base, "pct_above_200dma": pct})
        for pct in (0.2, 0.4, 0.6, 0.8)
    ]
    assert scores == sorted(scores)  # non-decreasing
    assert scores[0] < scores[-1]


def test_regime_score_clamps_out_of_range_inputs() -> None:
    """Negative or >1 inputs shouldn't push the score outside [0, 100]."""
    out_low = compute_regime_score(
        {"pct_above_200dma": -0.5, "pct_above_50dma": -1.0, "pct_at_52w_high": -0.2}
    )
    out_high = compute_regime_score(
        {"pct_above_200dma": 1.5, "pct_above_50dma": 2.0, "pct_at_52w_high": 1.3}
    )
    assert out_low == 0.0
    assert out_high == 100.0


def test_regime_score_weights_emphasize_200dma() -> None:
    """200DMA should dominate over 50DMA (50% vs 20% weight)."""
    s_200 = compute_regime_score({"pct_above_200dma": 1.0, "pct_above_50dma": 0.0, "pct_at_52w_high": 0.0})
    s_50 = compute_regime_score({"pct_above_200dma": 0.0, "pct_above_50dma": 1.0, "pct_at_52w_high": 0.0})
    s_high = compute_regime_score({"pct_above_200dma": 0.0, "pct_above_50dma": 0.0, "pct_at_52w_high": 1.0})
    assert s_200 > s_high > s_50  # 50 > 30 > 20


# ── regime_confidence: distance from band edges ──────────────────────────


def test_confidence_high_at_band_center() -> None:
    """neutral band is [0.30, 0.55] — center is 0.425."""
    conf = compute_regime_confidence({"pct_above_200dma": 0.425}, "neutral")
    assert 0.95 <= conf <= 1.0


def test_confidence_low_at_band_edges() -> None:
    """Right on the 0.30 / 0.55 boundaries — confidence near 0."""
    low_edge = compute_regime_confidence({"pct_above_200dma": 0.30}, "neutral")
    high_edge = compute_regime_confidence({"pct_above_200dma": 0.55}, "neutral")
    assert low_edge <= 0.05
    assert high_edge <= 0.05


def test_confidence_uses_band_edges_from_rules_when_present() -> None:
    """When a rules dict is passed, confidence respects custom thresholds."""
    rules = {
        "neutral": {"pct_above_200dma_gte": 0.40, "pct_above_200dma_lt": 0.60},
    }
    # Center of [0.40, 0.60] is 0.50 → confidence near 1
    center = compute_regime_confidence({"pct_above_200dma": 0.50}, "neutral", rules)
    assert center >= 0.9
    # Right at 0.40 → near 0
    edge = compute_regime_confidence({"pct_above_200dma": 0.40}, "neutral", rules)
    assert edge <= 0.05


def test_confidence_unknown_regime_returns_zero() -> None:
    """Defensive: if the regime label isn't in the default ladder, return 0."""
    assert compute_regime_confidence({"pct_above_200dma": 0.5}, "panic") == 0.0


def test_confidence_zero_when_band_is_degenerate() -> None:
    """Custom rules where lower >= upper — return 0 gracefully."""
    rules = {"weird": {"pct_above_200dma_gte": 0.60, "pct_above_200dma_lt": 0.40}}
    assert compute_regime_confidence({"pct_above_200dma": 0.50}, "weird", rules) == 0.0
