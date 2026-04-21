from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.features.feature_store import add_cross_sectional_features, add_liquidity_features
from ai_trading_system.domains.features.sector_rs import add_benchmark_relative_features


def test_add_liquidity_and_cross_sectional_features() -> None:
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-15", "2026-04-15", "2026-04-15"]),
            "symbol_id": ["AAA", "BBB", "CCC"],
            "sector": ["IT", "IT", "BANK"],
            "close": [100.0, 200.0, 50.0],
            "volume": [1_000, 500, 2_000],
            "return_20d": [0.10, 0.05, 0.20],
        }
    )

    out = add_liquidity_features(frame)
    out = add_cross_sectional_features(out, metric="return_20d")

    assert set(["turnover", "liquidity_score", "rank_in_universe", "percentile_score", "rank_in_sector"]).issubset(
        out.columns
    )
    assert out.loc[out["symbol_id"] == "CCC", "rank_in_universe"].iloc[0] == 1.0
    assert out.loc[out["symbol_id"] == "BBB", "rank_in_sector"].iloc[0] == 2.0


def test_add_benchmark_relative_features() -> None:
    stock = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-15", "2026-04-16"]),
            "symbol_id": ["AAA", "AAA"],
            "close": [100.0, 105.0],
        }
    )
    benchmark = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-15", "2026-04-16"]),
            "symbol_id": ["NIFTY_500", "NIFTY_500"],
            "close": [20_000.0, 20_100.0],
        }
    )

    out = add_benchmark_relative_features(stock, benchmark, benchmark_symbol="NIFTY_500")

    assert set(["benchmark_close", "stock_vs_benchmark"]).issubset(out.columns)
    assert out["benchmark_close"].tolist() == [20_000.0, 20_100.0]
    assert out["stock_vs_benchmark"].notna().all()
