from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.ranking.patterns.universe import (
    _load_latest_universe_snapshot,
    _identify_unusual_movers,
    apply_pattern_liquidity_gate,
    build_pattern_seed_universe,
)


def test_apply_pattern_liquidity_gate_requires_feature_ready_liquidity_and_price() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "feature_ready": True, "liquidity_score": 0.8, "close": 120.0},
            {"symbol_id": "BBB", "feature_ready": False, "liquidity_score": 0.8, "close": 120.0},
            {"symbol_id": "CCC", "feature_ready": True, "liquidity_score": 0.1, "close": 120.0},
            {"symbol_id": "DDD", "feature_ready": True, "liquidity_score": 0.8, "close": 10.0},
        ]
    )

    out = apply_pattern_liquidity_gate(frame, min_liquidity_score=0.2)

    assert out["symbol_id"].tolist() == ["AAA"]


def test_identify_unusual_movers_requires_score_threshold_and_supports_fallbacks() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "PRICE_ONLY",
                "liquidity_score": 0.6,
                "close": 100.0,
                "vol_20_avg": 200_000.0,
                "return_1d_abs_pct": 2.0,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 0.0,
                "volume_ratio_20": 1.0,
                "true_range_today": 1.0,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
            },
            {
                "symbol_id": "VOLUME_ONLY_WEAK",
                "liquidity_score": 0.7,
                "close": 100.0,
                "vol_20_avg": 200_000.0,
                "return_1d_abs_pct": 0.5,
                "atr_20_pct": 1.0,
                "volume_zscore_20": None,
                "volume_ratio_20": 1.6,
                "true_range_today": 1.0,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
            },
            {
                "symbol_id": "RANGE_ONLY",
                "liquidity_score": 0.7,
                "close": 100.0,
                "vol_20_avg": 200_000.0,
                "return_1d_abs_pct": 0.5,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 0.0,
                "volume_ratio_20": 1.0,
                "true_range_today": 3.2,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
            },
            {
                "symbol_id": "FALLBACK_VOLUME_RANGE",
                "liquidity_score": 0.9,
                "close": 100.0,
                "vol_20_avg": 300_000.0,
                "return_1d_abs_pct": 0.4,
                "atr_20_pct": None,
                "volume_zscore_20": None,
                "volume_ratio_20": 1.7,
                "true_range_today": 2.0,
                "atr_20": None,
                "range_expansion_20": 1.6,
            },
            {
                "symbol_id": "STRONG_VOLUME_ONLY",
                "liquidity_score": 0.9,
                "close": 100.0,
                "vol_20_avg": 300_000.0,
                "return_1d_abs_pct": 0.5,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 2.5,
                "volume_ratio_20": 1.2,
                "true_range_today": 1.0,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
            },
        ]
    )

    out = _identify_unusual_movers(
        frame,
        min_liquidity_score=0.2,
        unusual_mover_min_vol20_avg=100_000.0,
    )

    assert set(out["symbol_id"]) == {"FALLBACK_VOLUME_RANGE", "STRONG_VOLUME_ONLY"}
    scores = dict(zip(out["symbol_id"], out["unusual_mover_score"]))
    assert scores["FALLBACK_VOLUME_RANGE"] == 2
    assert scores["STRONG_VOLUME_ONLY"] == 2


def test_build_pattern_seed_universe_orders_sources_and_preserves_cached_symbols(
    monkeypatch,
) -> None:
    snapshot = pd.DataFrame(
        [
            {
                "symbol_id": "CACHED",
                "feature_ready": False,
                "liquidity_score": 0.05,
                "close": 110.0,
                "vol_20_avg": 200_000.0,
                "return_1d_abs_pct": 0.1,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 0.0,
                "volume_ratio_20": 1.0,
                "true_range_today": 1.0,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
                "sma_150": 100.0,
                "sma_200": 101.0,
                "sma200_slope_20d_pct": -0.1,
                "near_52w_high_pct": 40.0,
            },
            {
                "symbol_id": "STRUCT",
                "feature_ready": True,
                "liquidity_score": 0.9,
                "close": 120.0,
                "vol_20_avg": 250_000.0,
                "return_1d_abs_pct": 0.6,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 0.2,
                "volume_ratio_20": 1.0,
                "true_range_today": 1.0,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
                "sma_150": 100.0,
                "sma_200": 90.0,
                "sma200_slope_20d_pct": 1.2,
                "near_52w_high_pct": 5.0,
            },
            {
                "symbol_id": "UNUSUAL",
                "feature_ready": True,
                "liquidity_score": 0.8,
                "close": 140.0,
                "vol_20_avg": 300_000.0,
                "return_1d_abs_pct": 2.0,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 0.0,
                "volume_ratio_20": 1.6,
                "true_range_today": 3.5,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
                "sma_150": 120.0,
                "sma_200": 115.0,
                "sma200_slope_20d_pct": 0.1,
                "near_52w_high_pct": 12.0,
            },
            {
                "symbol_id": "LIQUID",
                "feature_ready": True,
                "liquidity_score": 0.7,
                "close": 95.0,
                "vol_20_avg": 220_000.0,
                "return_1d_abs_pct": 0.4,
                "atr_20_pct": 1.0,
                "volume_zscore_20": 0.0,
                "volume_ratio_20": 1.0,
                "true_range_today": 1.0,
                "atr_20": 2.0,
                "range_expansion_20": 1.0,
                "sma_150": 90.0,
                "sma_200": 91.0,
                "sma200_slope_20d_pct": -0.2,
                "near_52w_high_pct": 28.0,
            },
        ]
    )

    monkeypatch.setattr(
        "ai_trading_system.domains.ranking.patterns.universe._load_latest_universe_snapshot",
        lambda **_kwargs: snapshot.copy(),
    )
    monkeypatch.setattr(
        "ai_trading_system.domains.ranking.patterns.universe.merge_cached_pattern_symbols",
        lambda *args, **kwargs: (["CACHED"], {"latest_cached_signal_date": "2026-04-22", "cached_symbol_count": 1}),
    )

    symbols, metadata = build_pattern_seed_universe(
        project_root=".",
        ohlcv_db_path="ignored.duckdb",
        signal_date="2026-04-23",
        max_symbols=3,
        min_liquidity_score=0.2,
        unusual_mover_min_vol20_avg=100_000.0,
    )

    assert symbols == ["CACHED", "STRUCT", "UNUSUAL"]
    assert metadata["seed_source_counts"]["cached"] == 1
    assert metadata["seed_source_counts"]["stage2_structural"] == 2
    assert metadata["seed_source_counts"]["unusual_movers"] == 1


def test_load_latest_universe_snapshot_returns_one_latest_row_per_symbol_without_groupby_tail_bug(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    timestamps = pd.date_range("2025-09-01", periods=220, freq="D")
    rows: list[dict[str, object]] = []
    for offset, symbol in enumerate(("AAA", "BBB"), start=1):
        for idx, timestamp in enumerate(timestamps):
            close = 100.0 + float(offset) + (idx * 0.5)
            rows.append(
                {
                    "symbol_id": symbol,
                    "exchange": "NSE",
                    "timestamp": timestamp,
                    "open": close - 1.0,
                    "high": close + 1.5,
                    "low": close - 1.5,
                    "close": close,
                    "volume": 100_000 + (offset * 10_000) + idx,
                }
            )

    frame = pd.DataFrame(rows)
    conn = duckdb.connect(str(db_path))
    try:
        conn.register("catalog_rows", frame)
        conn.execute(
            """
            CREATE TABLE _catalog AS
            SELECT symbol_id, exchange, timestamp, open, high, low, close, volume
            FROM catalog_rows
            """
        )
    finally:
        conn.close()

    snapshot = _load_latest_universe_snapshot(
        ohlcv_db_path=db_path,
        signal_date="2026-04-24",
        exchange="NSE",
    )

    assert snapshot["symbol_id"].tolist() == ["AAA", "BBB"]
    assert snapshot["feature_ready"].astype(bool).tolist() == [True, True]
    assert {"liquidity_score", "volume_zscore_20", "sma_150", "sma_200", "near_52w_high_pct"}.issubset(
        snapshot.columns
    )
