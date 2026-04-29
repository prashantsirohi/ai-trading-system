import json

import pandas as pd
import pytest

from ai_trading_system.analytics.ranker import StockRanker
from ai_trading_system.domains.ranking.composite import (
    apply_rank_stability,
    compute_factor_scores,
    filter_ranked_scores,
    load_factor_weights,
    select_rank_output_columns,
)
from ai_trading_system.domains.ranking.contracts import DEFAULT_FACTOR_WEIGHTS
from ai_trading_system.domains.ranking.factors import apply_sector_strength, apply_trend_persistence
from ai_trading_system.domains.ranking.factors import (
    apply_delivery,
    apply_momentum_acceleration,
    apply_volume_intensity,
    compute_penalty_score,
)
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


def test_default_factor_weights_sum_to_one_and_include_momentum_acceleration():
    assert sum(DEFAULT_FACTOR_WEIGHTS.values()) == pytest.approx(1.0)
    assert DEFAULT_FACTOR_WEIGHTS["momentum_acceleration"] == pytest.approx(0.08)


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
        "momentum_acceleration_score",
        "prox_high_score",
        "delivery_pct_score",
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


def test_momentum_acceleration_scores_improving_roc_higher():
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "IMPROVING",
                "exchange": "NSE",
                "return_5": 8.0,
                "return_10": 5.0,
                "return_20": 1.0,
                "rel_strength": 70.0,
                "vol_intensity": 1.5,
                "trend_score": 70.0,
                "prox_high": 10.0,
                "delivery_pct": 45.0,
                "sector_rs_value": 0.5,
                "stock_vs_sector_value": 0.1,
            },
            {
                "symbol_id": "FADING",
                "exchange": "NSE",
                "return_5": 1.0,
                "return_10": 4.0,
                "return_20": 8.0,
                "rel_strength": 70.0,
                "vol_intensity": 1.5,
                "trend_score": 70.0,
                "prox_high": 10.0,
                "delivery_pct": 45.0,
                "sector_rs_value": 0.5,
                "stock_vs_sector_value": 0.1,
            },
        ]
    )

    accelerated = apply_momentum_acceleration(frame)
    scored = compute_factor_scores(accelerated, weights=DEFAULT_FACTOR_WEIGHTS).set_index("symbol_id")

    assert scored.loc["IMPROVING", "momentum_acceleration"] > scored.loc["FADING", "momentum_acceleration"]
    assert scored.loc["IMPROVING", "momentum_acceleration_score"] > scored.loc["FADING", "momentum_acceleration_score"]


def test_volume_intensity_normalization_caps_outlier_influence():
    data = pd.DataFrame(
        [
            {"symbol_id": "NORMAL", "exchange": "NSE", "volume": 2000.0},
            {"symbol_id": "OUTLIER", "exchange": "NSE", "volume": 100000.0},
        ]
    )
    volume_frame = pd.DataFrame(
        [
            {
                "symbol_id": "NORMAL",
                "exchange": "NSE",
                "vol_20_avg": 1000.0,
                "vol_20_max": 2500.0,
                "volume_zscore_20": 2.0,
            },
            {
                "symbol_id": "OUTLIER",
                "exchange": "NSE",
                "vol_20_avg": 1000.0,
                "vol_20_max": 100000.0,
                "volume_zscore_20": 50.0,
            },
        ]
    )

    scored = apply_volume_intensity(data, volume_frame=volume_frame).set_index("symbol_id")

    assert scored.loc["OUTLIER", "vol_intensity"] == pytest.approx(100.0)
    assert scored.loc["OUTLIER", "volume_intensity_normalized"] == pytest.approx(4.4)
    assert scored.loc["NORMAL", "volume_intensity_normalized"] == pytest.approx(2.0)


def test_exhaustion_penalty_lowers_adjusted_composite_score():
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "CLIMAX",
                "close": 125.0,
                "sma_20": 100.0,
                "volume_zscore_20": 4.2,
                "composite_score": 90.0,
            },
            {
                "symbol_id": "CLEAN",
                "close": 102.0,
                "sma_20": 100.0,
                "volume_zscore_20": 0.5,
                "composite_score": 90.0,
            },
        ]
    )

    out = compute_penalty_score(frame)
    out.loc[:, "composite_score_adjusted"] = out["composite_score"] - out["penalty_score"]
    out = out.set_index("symbol_id")

    assert out.loc["CLIMAX", "exhaustion_penalty"] == pytest.approx(8.0)
    assert out.loc["CLIMAX", "exhaustion_flag"] == "strong_exhaustion"
    assert out.loc["CLIMAX", "composite_score_adjusted"] < out.loc["CLEAN", "composite_score_adjusted"]


def test_pivot_distance_penalty_requires_valid_pivot_and_atr():
    frame = pd.DataFrame(
        [
            {"symbol_id": "NEAR", "close": 105.0, "breakout_level": 100.0, "atr_14": 4.0},
            {"symbol_id": "FAR", "close": 112.0, "breakout_level": 100.0, "atr_14": 4.0},
            {"symbol_id": "NO_PIVOT", "close": 112.0, "atr_14": 4.0},
            {"symbol_id": "NO_ATR", "close": 112.0, "breakout_level": 100.0},
        ]
    )

    out = compute_penalty_score(frame).set_index("symbol_id")

    assert out.loc["NEAR", "pivot_distance_penalty"] == pytest.approx(0.0)
    assert out.loc["FAR", "distance_from_pivot_atr"] == pytest.approx(3.0)
    assert out.loc["FAR", "pivot_distance_penalty"] == pytest.approx(6.0)
    assert out.loc["NO_PIVOT", "pivot_distance_penalty"] == pytest.approx(0.0)
    assert out.loc["NO_ATR", "pivot_distance_penalty"] == pytest.approx(0.0)


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


def test_stock_ranker_rank_all_preserves_stage2_columns_from_stage2_enrichment(tmp_path):
    ranker = StockRanker(
        ohlcv_db_path=str(tmp_path / "ohlcv.duckdb"),
        feature_store_dir=str(tmp_path / "feature_store"),
    )

    ranker.input_loader.load_latest_market_data = lambda exchanges: pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "timestamp": "2026-04-23",
                "close": 100.0,
                "volume": 1200.0,
                "high": 105.0,
                "low": 95.0,
                "open": 98.0,
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "timestamp": "2026-04-23",
                "close": 110.0,
                "volume": 1600.0,
                "high": 116.0,
                "low": 104.0,
                "open": 108.0,
            },
        ]
    )
    ranker.input_loader.load_return_frame_multi = lambda periods: pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "return_20": 8.0, "return_60": 16.0, "return_120": 24.0},
            {"symbol_id": "BBB", "exchange": "NSE", "return_20": 10.0, "return_60": 18.0, "return_120": 28.0},
        ]
    )
    ranker.input_loader.load_volume_frame = lambda: pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "vol_20_avg": 1000.0, "vol_20_max": 1500.0},
            {"symbol_id": "BBB", "exchange": "NSE", "vol_20_avg": 1100.0, "vol_20_max": 1700.0},
        ]
    )
    ranker.input_loader.load_latest_adx = lambda date: pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "adx_14": 22.0},
            {"symbol_id": "BBB", "exchange": "NSE", "adx_14": 28.0},
        ]
    )
    ranker.input_loader.load_latest_sma = lambda date: pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "sma_20": 97.0, "sma_50": 92.0},
            {"symbol_id": "BBB", "exchange": "NSE", "sma_20": 106.0, "sma_50": 100.0},
        ]
    )
    ranker.input_loader.load_latest_highs = lambda date, window: pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "high_52w": 110.0},
            {"symbol_id": "BBB", "exchange": "NSE", "high_52w": 120.0},
        ]
    )
    ranker.input_loader.load_latest_delivery = lambda date: pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "delivery_pct": 40.0},
            {"symbol_id": "BBB", "exchange": "NSE", "delivery_pct": 45.0},
        ]
    )
    ranker.input_loader.load_latest_stage2 = lambda date, exchanges, rel_strength_frame=None: pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "timestamp": "2026-04-23",
                "close": 100.0,
                "sma_200": 90.0,
                "sma_150": 94.0,
                "sma200_slope_20d_pct": 2.5,
                "stage2_score": 88.0,
                "is_stage2_structural": True,
                "is_stage2_candidate": True,
                "is_stage2_uptrend": True,
                "stage2_label": "strong_stage2",
                "stage2_hard_fail_reason": "",
                "stage2_fail_reason": "",
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "timestamp": "2026-04-23",
                "close": 110.0,
                "sma_200": 108.0,
                "sma_150": 109.0,
                "sma200_slope_20d_pct": -1.0,
                "stage2_score": 58.0,
                "is_stage2_structural": False,
                "is_stage2_candidate": True,
                "is_stage2_uptrend": False,
                "stage2_label": "stage1_to_stage2",
                "stage2_hard_fail_reason": "sma200_slope_negative",
                "stage2_fail_reason": "sma200_slope_negative",
            },
        ]
    )
    ranker._load_sector_inputs = lambda: (pd.DataFrame(), pd.DataFrame(), {})

    ranked = ranker.rank_all(date="2026-04-23", exchanges=["NSE"], min_score=0.0, top_n=None)

    assert {"stage2_score", "is_stage2_structural", "stage2_label", "stage2_score_bonus"}.issubset(ranked.columns)
    row_aaa = ranked.loc[ranked["symbol_id"] == "AAA"].iloc[0]
    row_bbb = ranked.loc[ranked["symbol_id"] == "BBB"].iloc[0]

    assert row_aaa["stage2_score"] == pytest.approx(88.0)
    assert bool(row_aaa["is_stage2_structural"]) is True
    assert row_aaa["stage2_label"] == "strong_stage2"
    assert row_aaa["stage2_score_bonus"] == pytest.approx(4.4)

    assert row_bbb["stage2_score"] == pytest.approx(58.0)
    assert bool(row_bbb["is_stage2_structural"]) is False
    assert row_bbb["stage2_label"] == "stage1_to_stage2"


def test_stage2_age_bonuses_and_warnings_are_added() -> None:
    ranker = StockRanker.__new__(StockRanker)
    frame = pd.DataFrame(
        [
            {"symbol_id": "FRESH", "weekly_stage_label": "S2", "bars_in_stage": 4, "weekly_stage_transition": "NONE"},
            {"symbol_id": "MID", "weekly_stage_label": "S2", "bars_in_stage": 12, "weekly_stage_transition": "NONE"},
            {"symbol_id": "MATURE", "weekly_stage_label": "S2", "bars_in_stage": 18, "weekly_stage_transition": "NONE"},
            {"symbol_id": "TRANS", "weekly_stage_label": "S2", "bars_in_stage": 3, "weekly_stage_transition": "S1_TO_S2"},
        ]
    )

    out = ranker._apply_stage2_age_bonuses(frame).set_index("symbol_id")

    assert out.loc["FRESH", "stage2_freshness_bonus"] == pytest.approx(4.0)
    assert out.loc["MID", "stage2_freshness_bonus"] == pytest.approx(2.0)
    assert out.loc["MATURE", "stage2_freshness_bonus"] == pytest.approx(0.0)
    assert out.loc["MATURE", "stage2_age_warning"] == "mature_stage2"
    assert out.loc["TRANS", "stage2_transition_bonus"] == pytest.approx(5.0)


def test_stage2_transition_bonus_ranks_above_mature_s2_and_clips_adjusted_score() -> None:
    ranker = StockRanker.__new__(StockRanker)
    frame = pd.DataFrame(
        [
                {
                    "symbol_id": "MATURE",
                    "composite_score": 96.0,
                    "stage2_score_bonus": 0.0,
                "penalty_score": 0.0,
                "weekly_stage_label": "S2",
                "bars_in_stage": 20,
                "weekly_stage_transition": "NONE",
            },
            {
                "symbol_id": "TRANS",
                "composite_score": 96.0,
                "stage2_score_bonus": 4.0,
                "penalty_score": 0.0,
                "weekly_stage_label": "S2",
                "bars_in_stage": 2,
                "weekly_stage_transition": "S1_TO_S2",
            },
        ]
    )
    scored = ranker._apply_stage2_age_bonuses(frame)
    scored.loc[:, "composite_score_adjusted"] = (
        scored["composite_score"]
        + scored["stage2_score_bonus"]
        + scored["stage2_freshness_bonus"]
        + scored["stage2_transition_bonus"]
        - scored["penalty_score"]
    ).clip(0.0, 100.0)

    ranked = filter_ranked_scores(scored, min_score=0.0, top_n=None)

    assert ranked.iloc[0]["symbol_id"] == "TRANS"
    assert ranked.iloc[0]["composite_score_adjusted"] == pytest.approx(100.0)
    assert ranked.iloc[1]["stage2_age_warning"] == "mature_stage2"


def test_rank_projection_includes_optional_stage2_age_columns() -> None:
    projected = select_rank_output_columns(
        pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "exchange": "NSE",
                    "close": 100.0,
                    "composite_score": 90.0,
                    "weekly_stage_label": "S2",
                    "weekly_stage_confidence": 0.9,
                    "weekly_stage_transition": "S1_TO_S2",
                    "bars_in_stage": 2,
                    "stage_entry_date": "2026-03-20",
                    "stage2_freshness_bonus": 4.0,
                    "stage2_transition_bonus": 5.0,
                    "stage2_age_warning": "",
                }
            ]
        )
    )

    assert {
        "weekly_stage_label",
        "weekly_stage_confidence",
        "weekly_stage_transition",
        "bars_in_stage",
        "stage_entry_date",
        "stage2_freshness_bonus",
        "stage2_transition_bonus",
        "stage2_age_warning",
    }.issubset(projected.columns)


def test_rank_projection_includes_optional_momentum_quality_columns() -> None:
    projected = select_rank_output_columns(
        pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "exchange": "NSE",
                    "close": 100.0,
                    "composite_score": 90.0,
                    "momentum_acceleration": 4.0,
                    "momentum_acceleration_score": 80.0,
                    "return_5": 5.0,
                    "return_10": 4.0,
                    "volume_zscore_20": 2.5,
                    "volume_intensity_normalized": 2.1,
                    "exhaustion_penalty": 3.0,
                    "exhaustion_flag": "mild_exhaustion",
                    "pivot_distance_penalty": 0.0,
                    "distance_from_pivot_atr": 1.0,
                }
            ]
        )
    )

    assert {
        "momentum_acceleration",
        "momentum_acceleration_score",
        "return_5",
        "return_10",
        "volume_zscore_20",
        "volume_intensity_normalized",
        "exhaustion_penalty",
        "exhaustion_flag",
        "pivot_distance_penalty",
        "distance_from_pivot_atr",
    }.issubset(projected.columns)
