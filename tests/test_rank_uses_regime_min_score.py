from __future__ import annotations

from ai_trading_system.analytics.regime.profiles import RegimeProfile
from ai_trading_system.domains.ranking.service import apply_regime_profile_to_rank_params


def test_rank_uses_regime_min_score_and_top_n() -> None:
    profile = RegimeProfile(
        regime="bull",
        name="test_profile",
        min_score=64,
        rank_top_n=40,
        max_exposure=0.85,
        max_positions=10,
        max_sector_exposure=0.32,
        max_single_stock_weight=0.10,
        atr_stop_mult=2.6,
        breakout_mode="normal",
        allow_pyramiding=True,
    )

    params = apply_regime_profile_to_rank_params({"min_score": 72, "top_n": 20}, profile)

    assert params["min_score"] == 64
    assert params["top_n"] == 40
    assert params["regime_profile"]["regime"] == "bull"
