"""Recipe parsing — structured and legacy benchmark forms."""

from __future__ import annotations

from ai_trading_system.research.optimization.recipe import (
    Benchmark,
    OptimizationRecipe,
)


_BASE = {
    "name": "x",
    "strategy_id": "y",
    "baseline_pack_path": "config/strategies/momentum_breakout_v1.yaml",
    "from_date": "2024-01-01",
    "to_date": "2024-06-30",
}


def test_default_benchmark_is_univ_top1000():
    recipe = OptimizationRecipe.from_dict(_BASE)
    assert recipe.benchmark.symbol == "UNIV_TOP1000"
    assert recipe.benchmark.source == "index_catalog"
    assert recipe.benchmark.blend == 0.35


def test_structured_benchmark_block_parses():
    payload = {
        **_BASE,
        "benchmark": {"symbol": "NIFTY_50", "source": "index_catalog", "blend": 0.20},
    }
    recipe = OptimizationRecipe.from_dict(payload)
    assert recipe.benchmark == Benchmark(symbol="NIFTY_50", source="index_catalog", blend=0.20)


def test_legacy_benchmark_symbol_string_parses(caplog):
    """Old flat benchmark_symbol: X form must still load and emit a deprecation log."""
    payload = {**_BASE, "benchmark_symbol": "NIFTY_50"}
    import logging

    with caplog.at_level(logging.WARNING):
        recipe = OptimizationRecipe.from_dict(payload)
    assert recipe.benchmark.symbol == "NIFTY_50"
    assert recipe.benchmark.source == "index_catalog"  # default
    # Deprecation warning emitted.
    assert any("benchmark_symbol" in r.message and "deprecated" in r.message for r in caplog.records)
