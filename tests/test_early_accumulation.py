"""Early accumulation sidecar tests."""

from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.early_accumulation import (
    EarlyAccumulationConfig,
    build_early_accumulation_scan,
    pattern_age_multiplier,
)
from ai_trading_system.domains.ranking.payloads import build_dashboard_payload
from ai_trading_system.pipeline.contracts import StageContext


def _ranked() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "close": 100.0,
                "sector_name": "Tech",
                "sma_50": 90.0,
                "sma_200": 96.0,
                "sma50_slope_20d_pct": 4.0,
                "sma200_slope_20d_pct": 2.0,
                "delivery_pct": 72.0,
                "delivery_pct_imputed": False,
                "volume_ratio_20": 2.2,
                "volume_zscore_20": 2.0,
                "momentum_acceleration": 8.0,
                "return_20": 12.0,
                "return_60": -5.0,
                "trend_score": 66.0,
                "adx_14": 28.0,
                "rel_strength_score": 68.0,
                "composite_score": 55.0,
                "composite_score_adjusted": 55.0,
                "avg_value_traded_20": 10_000_000.0,
                "liquidity_score": 1.0,
            },
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "close": 8.0,
                "sma_50": 9.0,
                "sma_200": 10.0,
                "delivery_pct": None,
                "composite_score": 10.0,
                "composite_score_adjusted": 10.0,
            },
            {
                "symbol_id": "CCC",
                "exchange": "NSE",
                "close": 60.0,
                "sma_50": 55.0,
                "sma_200": 70.0,
                "delivery_pct": None,
                "delivery_pct_imputed": True,
                "volume_ratio_20": 0.9,
                "volume_zscore_20": 0.0,
                "momentum_acceleration": -2.0,
                "return_20": -4.0,
                "return_60": -20.0,
                "trend_score": 35.0,
                "adx_14": 16.0,
                "rel_strength_score": 35.0,
                "composite_score": 30.0,
                "composite_score_adjusted": 30.0,
                "avg_value_traded_20": 5_000_000.0,
                "liquidity_score": 1.0,
            },
        ]
    )


def test_pattern_decay_buckets() -> None:
    assert pattern_age_multiplier(5) == 1.0
    assert pattern_age_multiplier(20) == 0.80
    assert pattern_age_multiplier(40) == 0.55
    assert pattern_age_multiplier(60) == 0.30
    assert pattern_age_multiplier(61) == 0.0


def test_scoring_excludes_head_shoulders_and_clips_outputs() -> None:
    patterns = pd.DataFrame(
        [
            {"symbol_id": "AAA", "exchange": "NSE", "pattern_family": "pocket_pivot", "pattern_state": "confirmed", "signal_date": "2026-07-03"},
            {"symbol_id": "AAA", "exchange": "NSE", "pattern_family": "flat_base", "pattern_state": "confirmed", "signal_date": "2026-06-20"},
            {"symbol_id": "AAA", "exchange": "NSE", "pattern_family": "head_shoulders", "pattern_state": "confirmed", "signal_date": "2026-07-04"},
            {"symbol_id": "CCC", "exchange": "NSE", "pattern_family": "head_shoulders", "pattern_state": "confirmed", "signal_date": "2026-07-04"},
        ]
    )
    scan, summary = build_early_accumulation_scan(
        ranked_universe=_ranked(),
        pattern_df=patterns,
        breakout_df=pd.DataFrame(),
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10),
    )

    by_symbol = scan.set_index("symbol_id")
    assert "BBB" not in by_symbol.index
    assert bool(by_symbol.loc["AAA", "head_shoulders_diagnostic"]) is True
    assert by_symbol.loc["AAA", "base_pattern_freshness_score"] > 95.0
    assert by_symbol.loc["CCC", "base_pattern_freshness_score"] == 0.0
    assert scan["early_accumulation_score"].between(0, 100).all()
    assert summary["rows"] == len(scan)


def test_missing_delivery_fallback_and_imputed_cap() -> None:
    scan, summary = build_early_accumulation_scan(
        ranked_universe=_ranked().drop(columns=["delivery_pct"]),
        pattern_df=pd.DataFrame(),
        breakout_df=pd.DataFrame(),
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10),
    )

    assert "delivery_pct unavailable" in "; ".join(summary["warnings"])
    assert scan["delivery_accumulation_score"].eq(50.0).all()


def test_graduation_precedence_and_required_columns() -> None:
    breakouts = pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "breakout_state": "qualified", "breakout_score": 99.0}])
    scan, _summary = build_early_accumulation_scan(
        ranked_universe=_ranked(),
        pattern_df=pd.DataFrame(),
        breakout_df=breakouts,
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10),
    )

    row = scan.set_index("symbol_id").loc["AAA"]
    assert row["graduation_status"] == "breakout_qualified"
    for column in [
        "early_accumulation_score",
        "above_200dma_reclaim_score",
        "delivery_accumulation_score",
        "watchlist_reason",
    ]:
        assert column in scan.columns


def test_dashboard_payload_includes_early_accumulation_preview(tmp_path) -> None:
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-07-05-test",
        run_date="2026-07-05",
        stage_name="rank",
        attempt_number=1,
        params={},
    )
    scan, _summary = build_early_accumulation_scan(
        ranked_universe=_ranked(),
        pattern_df=pd.DataFrame(),
        breakout_df=pd.DataFrame(),
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10),
    )

    payload = build_dashboard_payload(
        context=context,
        ranked_df=_ranked().head(1),
        breakout_df=pd.DataFrame(),
        pattern_df=pd.DataFrame(),
        stock_scan_df=pd.DataFrame(),
        sector_dashboard_df=pd.DataFrame(),
        warnings=[],
        early_accumulation_df=scan,
    )

    assert payload["summary"]["early_accumulation_enabled"] is True
    assert payload["summary"]["early_accumulation_count"] == len(scan)
    assert payload["early_accumulation_preview"]
