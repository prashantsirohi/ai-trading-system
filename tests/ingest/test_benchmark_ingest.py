from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ingest.benchmark_ingest import (
    BenchmarkSpec,
    benchmark_lookup,
    ingest_benchmarks,
)


class _DummyNSECollector:
    def get_bhavcopy(self, trade_date: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "SYMBOL": "NIFTY 50",
                    "OPEN_PRICE": 22000.0,
                    "HIGH_PRICE": 22100.0,
                    "LOW_PRICE": 21900.0,
                    "CLOSE_PRICE": 22050.0,
                    "TTL_TRD_QNTY": 0,
                }
            ]
        )


def test_ingest_benchmarks_loads_rows() -> None:
    frame = ingest_benchmarks(
        date_range=["2026-04-07"],
        specs=[BenchmarkSpec(symbol="NIFTY_50", label="NIFTY 50")],
        nse_collector=_DummyNSECollector(),
    )
    assert not frame.empty
    assert frame.iloc[0]["symbol_id"] == "NIFTY_50"
    assert bool(frame.iloc[0]["is_benchmark"]) is True
    assert frame.iloc[0]["instrument_type"] == "index"
    assert frame.iloc[0]["exchange"] == "NSE"


def test_benchmark_lookup_returns_close_projection() -> None:
    source = pd.DataFrame(
        [
            {
                "symbol_id": "NIFTY_500",
                "timestamp": "2026-04-07",
                "close": 18750.0,
            }
        ]
    )
    lookup = benchmark_lookup(source)
    assert list(lookup.columns) == ["symbol_id", "timestamp", "benchmark_close"]
    assert lookup.iloc[0]["benchmark_close"] == 18750.0
