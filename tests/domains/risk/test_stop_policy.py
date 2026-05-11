"""Stop method tests — one per supported method + error path."""

import pytest

from ai_trading_system.domains.risk.config import StopConfig
from ai_trading_system.domains.risk.stop_policy import (
    StopMethodUnavailable,
    calculate_initial_stop,
)


def test_atr_stop(market):
    cfg = StopConfig(method="atr", atr_multiple=2.0)
    stop, method = calculate_initial_stop(market, cfg)
    assert method == "atr"
    assert stop == pytest.approx(market.close - market.atr_14 * 2.0)


def test_percent_stop(market):
    cfg = StopConfig(method="percent", stop_pct=0.05)
    stop, method = calculate_initial_stop(market, cfg)
    assert method == "percent"
    assert stop == pytest.approx(market.close * 0.95)


def test_swing_low_stop(market):
    cfg = StopConfig(method="swing_low")
    stop, method = calculate_initial_stop(market, cfg)
    assert method == "swing_low"
    assert stop == market.swing_low_20


def test_breakout_candle_low_stop(market):
    cfg = StopConfig(method="breakout_candle_low")
    stop, method = calculate_initial_stop(market, cfg)
    assert method == "breakout_candle_low"
    assert stop == market.breakout_candle_low


def test_hybrid_stop_picks_tighter(market):
    cfg = StopConfig(method="hybrid", hybrid_atr_multiple=2.5)
    stop, method = calculate_initial_stop(market, cfg)
    assert method == "hybrid"
    # hybrid: max(swing_low_20, close - atr*mult) → tighter to entry
    atr_stop = market.close - market.atr_14 * 2.5
    assert stop == pytest.approx(max(market.swing_low_20, atr_stop))


def test_atr_stop_raises_without_atr(make_market):
    cfg = StopConfig(method="atr")
    with pytest.raises(StopMethodUnavailable):
        calculate_initial_stop(make_market(atr_14=0.0), cfg)


def test_swing_low_raises_without_data(make_market):
    cfg = StopConfig(method="swing_low")
    with pytest.raises(StopMethodUnavailable):
        calculate_initial_stop(make_market(swing_low_20=None), cfg)
