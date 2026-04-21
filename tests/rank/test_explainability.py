from __future__ import annotations

from ai_trading_system.domains.ranking.payloads import (
    build_rejection_reasons,
    build_score_breakdown,
    build_top_factors,
)


def test_build_score_breakdown_and_top_factors() -> None:
    row = {
        "rel_strength_score": 90.0,
        "vol_intensity_score": 75.0,
        "trend_score_score": 60.0,
        "prox_high_score": 40.0,
        "delivery_pct_score": 55.0,
        "sector_strength_score": 80.0,
        "penalty_score": 10.0,
        "relative_strength": 1.0,
        "volume_intensity": 2.0,
        "trend_persistence": 3.0,
        "proximity_to_highs": 4.0,
        "delivery_pct": 5.0,
        "sector_strength": 6.0,
    }

    breakdown = build_score_breakdown(row)
    top_factors = build_top_factors(row)

    assert breakdown["penalty_score"] == 10.0
    assert top_factors == ["relative_strength", "sector_strength", "volume_intensity"]


def test_build_rejection_reasons_includes_eligibility_and_explicit_reasons() -> None:
    reasons = build_rejection_reasons(
        {
            "eligible_rank": False,
            "rejection_reasons": ["min_price", "insufficient_liquidity"],
        }
    )
    assert reasons == ["failed_eligibility", "min_price", "insufficient_liquidity"]
