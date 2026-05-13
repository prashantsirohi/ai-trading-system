"""Profile loader tests."""

import pytest

from ai_trading_system.domains.risk import RiskPolicyConfig, load_profile


@pytest.mark.parametrize("name", ["aggressive_momentum", "balanced_swing", "positional_trend", "stage1_watchlist"])
def test_named_profiles_load(name):
    cfg = load_profile(name)
    assert isinstance(cfg, RiskPolicyConfig)
    assert cfg.name == name


def test_aggressive_uses_tight_atr_stop():
    cfg = load_profile("aggressive_momentum")
    assert cfg.stop.method == "atr"
    assert cfg.stop.atr_multiple == 1.5
    assert cfg.exit.dma_exit_window == 11


def test_balanced_uses_hybrid_stop_and_20dma():
    cfg = load_profile("balanced_swing")
    assert cfg.stop.method == "hybrid"
    assert cfg.exit.dma_exit_window == 20


def test_positional_uses_swing_low_and_50dma():
    cfg = load_profile("positional_trend")
    assert cfg.stop.method == "swing_low"
    assert cfg.exit.dma_exit_window == 50
    assert cfg.exit.time_stop_days == 180


def test_stage1_watchlist_uses_relaxed_discovery_gates():
    cfg = load_profile("stage1_watchlist")
    assert cfg.entry.require_stage_2 is False
    assert cfg.entry.require_price_above_sma50 is True
    assert cfg.entry.require_price_above_ema20 is True
    assert cfg.entry.require_sma50_above_sma200_or_rising_20d is True
    assert cfg.entry.min_close_to_52w_high == 0.75
    assert cfg.entry.min_return_20_pct == 8.0
    assert cfg.entry.min_return_50_pct == 15.0
    assert cfg.entry.min_volume_ratio == 1.5
    assert cfg.entry.max_drawdown_from_recent_high_pct == 25.0
    assert cfg.entry.max_below_ema20_days_20 == 6


def test_unknown_profile_falls_back_to_balanced():
    cfg = load_profile("does_not_exist")
    assert cfg.name == "balanced_swing"


def test_strict_unknown_profile_raises():
    with pytest.raises(FileNotFoundError):
        load_profile("does_not_exist", strict=True)
