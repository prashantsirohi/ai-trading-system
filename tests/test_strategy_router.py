"""Unit tests for strategy_router.py — pure lookup, no DB needed."""
from __future__ import annotations

import pytest

from ai_trading_system.domains.ranking.strategy_router import StrategyConfig, route


def test_s2_bull_config():
    cfg = route("S2")
    assert cfg.rank_mode == "stage2_breakout"
    assert cfg.weekly_stage_gate is True
    assert cfg.breakout_active is True
    assert cfg.position_regime == "TREND"
    assert cfg.position_multiplier == 1.0
    assert "BULLISH" in cfg.breakout_bias_allowlist
    assert cfg.breakout_min_breadth == 45.0
    assert cfg.market_stage == "S2"


def test_s3_defensive_config():
    cfg = route("S3")
    assert cfg.rank_mode == "defensive"
    assert cfg.weekly_stage_gate is False
    assert cfg.breakout_active is True
    assert cfg.position_regime == "BEARISH_MIXED"
    assert cfg.position_multiplier == pytest.approx(0.7)
    assert cfg.breakout_bias_allowlist == "NEUTRAL"
    assert cfg.breakout_min_breadth == 55.0
    assert cfg.market_stage == "S3"


def test_s4_watchlist_config():
    cfg = route("S4")
    assert cfg.rank_mode == "watchlist"
    assert cfg.weekly_stage_gate is False
    assert cfg.breakout_active is False
    assert cfg.position_regime == "STRONG_BEAR_TREND"
    assert cfg.position_multiplier == pytest.approx(0.5)
    assert cfg.market_stage == "S4"


def test_s1_default_config():
    cfg = route("S1")
    assert cfg.rank_mode == "default"
    assert cfg.weekly_stage_gate is False
    assert cfg.breakout_active is True
    assert cfg.position_regime == "TREND"
    assert cfg.position_multiplier == 1.0


def test_mixed_same_as_s1():
    """MIXED regime should behave identically to S1 (neutral default)."""
    s1 = route("S1")
    mx = route("MIXED")
    assert s1.rank_mode == mx.rank_mode
    assert s1.weekly_stage_gate == mx.weekly_stage_gate
    assert s1.breakout_active == mx.breakout_active
    assert s1.position_regime == mx.position_regime
    assert s1.position_multiplier == mx.position_multiplier


def test_unknown_label_falls_back_to_mixed():
    cfg = route("UNKNOWN_STAGE")
    mixed = route("MIXED")
    assert cfg.rank_mode == mixed.rank_mode
    assert cfg.breakout_active == mixed.breakout_active


def test_s2_gate_is_true():
    """Explicit assertion from plan: route('S2').weekly_stage_gate is True."""
    assert route("S2").weekly_stage_gate is True


def test_s4_breakout_is_false():
    """Explicit assertion from plan: route('S4').breakout_active is False."""
    assert route("S4").breakout_active is False


def test_s3_multiplier():
    """Explicit assertion from plan: route('S3').position_multiplier == 0.7."""
    assert route("S3").position_multiplier == pytest.approx(0.7)


def test_config_is_frozen():
    """StrategyConfig must be immutable (frozen dataclass)."""
    cfg = route("S2")
    with pytest.raises((AttributeError, TypeError)):
        cfg.rank_mode = "something_else"   # type: ignore[misc]
