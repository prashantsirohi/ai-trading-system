from datetime import date

import pandas as pd
import pytest

from ai_trading_system.domains.events.triggers import Trigger
from ai_trading_system.domains.ranking.volume_shocker import (
    VolumeShockerConfig,
    detect_volume_shockers,
    to_triggers,
)


def _frame(rows):
    return pd.DataFrame(rows)


def test_detector_keeps_only_above_threshold():
    df = _frame([
        {"symbol": "RELIANCE", "volume_zscore_20": 4.2,
         "volume": 5_000_000, "close": 2400.0, "market_cap_cr": 1_500_000},
        {"symbol": "QUIET",    "volume_zscore_20": 1.5,
         "volume": 100_000,   "close": 100.0,  "market_cap_cr": 5000},
    ])
    out = detect_volume_shockers(df, config=VolumeShockerConfig(z_threshold=3.0))
    assert list(out["symbol"]) == ["RELIANCE"]
    assert out.iloc[0]["shock_intensity"] == pytest.approx(4.2 / 3.0)


def test_detector_applies_market_cap_gate():
    df = _frame([
        {"symbol": "BIG", "volume_zscore_20": 5.0,
         "volume": 1_000_000, "close": 1000.0, "market_cap_cr": 100_000},
        {"symbol": "SMALL", "volume_zscore_20": 5.0,
         "volume": 1_000_000, "close": 50.0,   "market_cap_cr": 100},
    ])
    out = detect_volume_shockers(
        df, config=VolumeShockerConfig(z_threshold=3.0, min_market_cap_cr=500)
    )
    assert list(out["symbol"]) == ["BIG"]


def test_detector_applies_turnover_gate():
    df = _frame([
        {"symbol": "ENOUGH", "volume_zscore_20": 4.0,
         "volume": 500_000, "close": 1000.0, "market_cap_cr": 50_000},
        {"symbol": "TINY",   "volume_zscore_20": 4.0,
         "volume": 100,      "close": 1.0,   "market_cap_cr": 50_000},
    ])
    out = detect_volume_shockers(
        df, config=VolumeShockerConfig(z_threshold=3.0, min_turnover_cr=1.0)
    )
    assert list(out["symbol"]) == ["ENOUGH"]


def test_detector_universe_filter():
    df = _frame([
        {"symbol": "A", "volume_zscore_20": 5.0,
         "volume": 1_000_000, "close": 1000.0, "market_cap_cr": 5000},
        {"symbol": "B", "volume_zscore_20": 5.0,
         "volume": 1_000_000, "close": 1000.0, "market_cap_cr": 5000},
    ])
    out = detect_volume_shockers(
        df, config=VolumeShockerConfig(
            z_threshold=3.0, universe_symbols=frozenset({"A"})
        ),
    )
    assert list(out["symbol"]) == ["A"]


def test_empty_input_returns_empty_result():
    out = detect_volume_shockers(pd.DataFrame())
    assert out.empty
    assert "symbol" in out.columns


def test_missing_zscore_column_returns_empty():
    df = _frame([{"symbol": "X", "volume": 1, "close": 1}])
    out = detect_volume_shockers(df)
    assert out.empty


def test_to_triggers_projects_metadata():
    df = _frame([{
        "symbol": "RELIANCE", "volume_zscore_20": 4.2,
        "volume": 5_000_000, "close": 2400.0, "vol_20_avg": 1_500_000,
        "market_cap_cr": 1_500_000,
    }])
    out = detect_volume_shockers(df, config=VolumeShockerConfig(z_threshold=3.0))
    triggers = to_triggers(out, as_of=date(2026, 5, 2))
    assert len(triggers) == 1
    t: Trigger = triggers[0]
    assert t.symbol == "RELIANCE"
    assert t.trigger_type == "volume_shock"
    assert t.as_of_date == date(2026, 5, 2)
    assert t.trigger_metadata["z_score"] == pytest.approx(4.2)
    assert t.trigger_metadata["vol_20_avg"] == pytest.approx(1_500_000.0)
    assert t.dedupe_key() == ("RELIANCE", "volume_shock", "2026-05-02")
