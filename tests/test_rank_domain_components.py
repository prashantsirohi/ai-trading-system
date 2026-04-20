import json

import pandas as pd
import pytest

from analytics.ranker import StockRanker
from ai_trading_system.domains.ranking.composite import (
    apply_rank_stability,
    compute_factor_scores,
    filter_ranked_scores,
    load_factor_weights,
    select_rank_output_columns,
)
from ai_trading_system.domains.ranking.contracts import DEFAULT_FACTOR_WEIGHTS
from ai_trading_system.domains.ranking.factors import apply_sector_strength, apply_trend_persistence
from ai_trading_system.domains.ranking.factors import apply_delivery
from ai_trading_system.domains.ranking.input_loader import RankerInputLoader


def test_load_factor_weights_overrides_defaults(tmp_path):
    config_path = tmp_path / "rank_factor_weights.json"
    config_path.write_text(
        json.dumps(
            {
                "relative_strength": 0.40,
                "volume_intensity": 0.10,
                "sector_strength": 0.05,
            }
        ),
        encoding="utf-8",
    )

    weights = load_factor_weights(config_path)

    assert weights["relative_strength"] == pytest.approx(0.40)
    assert weights["volume_intensity"] == pytest.approx(0.10)
    assert weights["sector_strength"] == pytest.approx(0.05)
    assert weights["trend_persistence"] == pytest.approx(DEFAULT_FACTOR_WEIGHTS["trend_persistence"])


def test_compute_factor_scores_preserves_rank_order_and_output_contract():
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "close": 100.0,
                "rel_strength": 10.0,
                "vol_intensity": 1.0,
                "trend_score": 20.0,
                "prox_high": 10.0,
                "delivery_pct": 40.0,
                "sector_rs_value": 1.0,
                "stock_vs_sector_value": 5.0,
                "sector_name": "Finance",
                "high_52w": 110.0,
                "vol_20_avg": 1000.0,
                "adx_14": 20.0,
                "sma_20": 95.0,
                "sma_50": 90.0,
                "volume": 1200.0,
                "timestamp": "2026-04-15T09:15:00",
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "close": 110.0,
                "rel_strength": 20.0,
                "vol_intensity": 2.0,
                "trend_score": 40.0,
                "prox_high": 20.0,
                "delivery_pct": 50.0,
                "sector_rs_value": 2.0,
                "stock_vs_sector_value": 6.0,
                "sector_name": "Tech",
                "high_52w": 120.0,
                "vol_20_avg": 1100.0,
                "adx_14": 30.0,
                "sma_20": 100.0,
                "sma_50": 95.0,
                "volume": 2200.0,
                "timestamp": "2026-04-15T09:15:00",
            },
            {
                "symbol_id": "CCC",
                "exchange": "NSE",
                "close": 120.0,
                "rel_strength": 30.0,
                "vol_intensity": 3.0,
                "trend_score": 60.0,
                "prox_high": 30.0,
                "delivery_pct": 60.0,
                "sector_rs_value": 3.0,
                "stock_vs_sector_value": 7.0,
                "sector_name": "Energy",
                "high_52w": 130.0,
                "vol_20_avg": 1200.0,
                "adx_14": 40.0,
                "sma_20": 105.0,
                "sma_50": 100.0,
                "volume": 3200.0,
                "timestamp": "2026-04-15T09:15:00",
            },
        ]
    )

    scored = compute_factor_scores(frame, weights=DEFAULT_FACTOR_WEIGHTS)
    ranked = filter_ranked_scores(scored, min_score=0.0, top_n=None)
    projected = select_rank_output_columns(ranked)

    assert projected["symbol_id"].tolist() == ["CCC", "BBB", "AAA"]
    assert projected["composite_score"].tolist() == pytest.approx(
        sorted(projected["composite_score"].tolist(), reverse=True)
    )
    assert projected.columns.tolist()[:10] == [
        "symbol_id",
        "exchange",
        "close",
        "composite_score",
        "rel_strength_score",
        "vol_intensity_score",
        "trend_score_score",
        "prox_high_score",
        "delivery_pct_score",
        "sector_strength_score",
    ]


def test_apply_trend_persistence_blends_strength_and_alignment():
    data = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "close": 95.0},
            {"symbol_id": "BBB", "exchange": "NSE", "close": 110.0},
        ]
    )
    adx_frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "adx_14": 40.0},
            {"symbol_id": "BBB", "exchange": "NSE", "adx_14": 10.0},
        ]
    )
    sma_frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "sma_20": 90.0, "sma_50": 100.0},
            {"symbol_id": "BBB", "exchange": "NSE", "sma_20": 100.0, "sma_50": 95.0},
        ]
    )

    scored = apply_trend_persistence(data, adx_frame=adx_frame, sma_frame=sma_frame)

    aaa = scored.loc[scored["symbol_id"] == "AAA"].iloc[0]
    bbb = scored.loc[scored["symbol_id"] == "BBB"].iloc[0]

    assert aaa["adx_score"] == pytest.approx(80.0)
    assert aaa["sma20_aligned"] == 1
    assert aaa["sma50_aligned"] == 0
    assert aaa["trend_score"] == pytest.approx(68.0)

    assert bbb["adx_score"] == pytest.approx(20.0)
    assert bbb["sma20_aligned"] == 1
    assert bbb["sma50_aligned"] == 1
    assert bbb["trend_score"] == pytest.approx(44.0)
    assert aaa["trend_score"] > bbb["trend_score"]


def test_apply_sector_strength_falls_back_when_inputs_missing():
    data = pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "close": 100.0}])

    scored = apply_sector_strength(
        data,
        sector_rs=pd.DataFrame(),
        stock_vs_sector=pd.DataFrame(),
        sector_map={},
        date="2026-04-15",
    )

    assert scored["sector_rs_value"].iloc[0] == pytest.approx(0.5)
    assert scored["stock_vs_sector_value"].iloc[0] == pytest.approx(0.0)


def test_apply_delivery_imputes_from_sector_then_universe_median():
    data = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "sector_name": "Tech"},
            {"symbol_id": "BBB", "exchange": "NSE", "close": 101.0, "sector_name": "Tech"},
            {"symbol_id": "CCC", "exchange": "NSE", "close": 102.0, "sector_name": "Energy"},
            {"symbol_id": "DDD", "exchange": "NSE", "close": 103.0, "sector_name": None},
        ]
    )
    delivery_frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "delivery_pct": 30.0},
            {"symbol_id": "CCC", "exchange": "NSE", "delivery_pct": 50.0},
        ]
    )

    scored = apply_delivery(data, delivery_frame=delivery_frame)

    assert scored.loc[scored["symbol_id"] == "AAA", "delivery_pct"].iloc[0] == pytest.approx(30.0)
    assert scored.loc[scored["symbol_id"] == "BBB", "delivery_pct"].iloc[0] == pytest.approx(30.0)
    assert scored.loc[scored["symbol_id"] == "DDD", "delivery_pct"].iloc[0] == pytest.approx(40.0)
    assert scored.loc[scored["symbol_id"] == "BBB", "delivery_pct_imputed"].iloc[0]
    assert scored.loc[scored["symbol_id"] == "DDD", "delivery_pct_imputed"].iloc[0]
    assert scored.loc[scored["symbol_id"] == "AAA", "delivery_pct_imputed"].iloc[0] == False
    assert scored.loc[scored["symbol_id"] == "DDD", "delivery_pct_filled"].iloc[0] == pytest.approx(40.0)


def test_compute_factor_scores_sector_demeans_sector_sensitive_raw_factors():
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "TECH_A",
                "exchange": "NSE",
                "close": 100.0,
                "rel_strength": 90.0,
                "vol_intensity": 90.0,
                "trend_score": 90.0,
                "prox_high": 50.0,
                "delivery_pct": 40.0,
                "sector_rs_value": 0.6,
                "stock_vs_sector_value": 0.2,
                "sector_name": "Tech",
            },
            {
                "symbol_id": "TECH_B",
                "exchange": "NSE",
                "close": 101.0,
                "rel_strength": 80.0,
                "vol_intensity": 80.0,
                "trend_score": 80.0,
                "prox_high": 50.0,
                "delivery_pct": 40.0,
                "sector_rs_value": 0.6,
                "stock_vs_sector_value": 0.2,
                "sector_name": "Tech",
            },
            {
                "symbol_id": "UTIL_A",
                "exchange": "NSE",
                "close": 102.0,
                "rel_strength": 60.0,
                "vol_intensity": 60.0,
                "trend_score": 60.0,
                "prox_high": 50.0,
                "delivery_pct": 40.0,
                "sector_rs_value": 0.6,
                "stock_vs_sector_value": 0.2,
                "sector_name": "Utilities",
            },
            {
                "symbol_id": "UTIL_B",
                "exchange": "NSE",
                "close": 103.0,
                "rel_strength": 50.0,
                "vol_intensity": 50.0,
                "trend_score": 50.0,
                "prox_high": 50.0,
                "delivery_pct": 40.0,
                "sector_rs_value": 0.6,
                "stock_vs_sector_value": 0.2,
                "sector_name": "Utilities",
            },
        ]
    )

    scored = compute_factor_scores(frame, weights=DEFAULT_FACTOR_WEIGHTS)

    tech_a = scored.loc[scored["symbol_id"] == "TECH_A"].iloc[0]
    util_a = scored.loc[scored["symbol_id"] == "UTIL_A"].iloc[0]
    tech_b = scored.loc[scored["symbol_id"] == "TECH_B"].iloc[0]
    util_b = scored.loc[scored["symbol_id"] == "UTIL_B"].iloc[0]

    assert tech_a["rel_strength_score"] == pytest.approx(util_a["rel_strength_score"])
    assert tech_a["vol_intensity_score"] == pytest.approx(util_a["vol_intensity_score"])
    assert tech_a["trend_score_score"] == pytest.approx(util_a["trend_score_score"])
    assert tech_b["rel_strength_score"] == pytest.approx(util_b["rel_strength_score"])
    assert tech_b["vol_intensity_score"] == pytest.approx(util_b["vol_intensity_score"])
    assert tech_b["trend_score_score"] == pytest.approx(util_b["trend_score_score"])
    assert tech_a["prox_high_score"] == pytest.approx(util_a["prox_high_score"])
    assert tech_a["delivery_pct_score"] == pytest.approx(util_a["delivery_pct_score"])


def test_apply_rank_stability_uses_previous_score_order_for_positions():
    current = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "composite_score": 95.0},
            {"symbol_id": "BBB", "exchange": "NSE", "composite_score": 90.0},
            {"symbol_id": "CCC", "exchange": "NSE", "composite_score": 85.0},
        ]
    )
    previous = pd.DataFrame(
        [
            {"symbol_id": "BBB", "exchange": "NSE", "composite_score": 70.0},
            {"symbol_id": "CCC", "exchange": "NSE", "composite_score": 80.0},
            {"symbol_id": "AAA", "exchange": "NSE", "composite_score": 90.0},
        ]
    )

    stabilized = apply_rank_stability(current, previous_frame=previous)

    previous_positions = stabilized.set_index("symbol_id")["previous_rank_position"].to_dict()

    assert previous_positions["AAA"] == 1
    assert previous_positions["CCC"] == 2
    assert previous_positions["BBB"] == 3


def test_ranker_input_loader_normalizes_swapped_columns():
    loader = RankerInputLoader(
        ohlcv_db_path="unused.duckdb",
        feature_store_dir="unused",
        master_db_path="unused.db",
    )
    frame = pd.DataFrame(
        [
            {"symbol_id": "NSE", "exchange": "ABC"},
            {"symbol_id": "XYZ", "exchange": "NSE"},
        ]
    )

    normalized = loader.normalize_symbol_exchange_columns(frame)

    assert normalized.to_dict("records") == [
        {"symbol_id": "ABC", "exchange": "NSE"},
        {"symbol_id": "XYZ", "exchange": "NSE"},
    ]


def test_stock_ranker_exposes_externalized_default_weights():
    assert StockRanker.WEIGHTS == DEFAULT_FACTOR_WEIGHTS
