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


def test_config_defaults_target_investigator_research_mode() -> None:
    config = EarlyAccumulationConfig()

    assert config.min_score == 45.0
    assert config.top_n == 150
    assert config.true_early_top_n == 50
    assert config.emerging_top_n == 50
    assert config.near_graduation_top_n == 30
    assert config.already_graduated_top_n == 20
    assert config.min_true_early_rows == 25
    assert config.min_emerging_rows == 25


def test_early_purity_buckets_sort_before_score() -> None:
    ranked = pd.DataFrame(
        [
            {**_ranked().iloc[0].to_dict(), "symbol_id": "EARLY", "active_rank_pctile": 30, "composite_score": 30, "rel_strength_score": 30},
            {**_ranked().iloc[0].to_dict(), "symbol_id": "EMERGE", "active_rank_pctile": 60, "composite_score": 60, "rel_strength_score": 60},
            {**_ranked().iloc[0].to_dict(), "symbol_id": "NEAR", "active_rank_pctile": 75, "composite_score": 75, "rel_strength_score": 75},
            {**_ranked().iloc[0].to_dict(), "symbol_id": "GRAD", "active_rank_pctile": 90, "composite_score": 90, "rel_strength_score": 90},
        ]
    )
    scan, summary = build_early_accumulation_scan(
        ranked_universe=ranked,
        pattern_df=pd.DataFrame(),
        breakout_df=pd.DataFrame(),
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10, min_true_early_rows=1, min_emerging_rows=1),
    )

    assert scan["early_purity_bucket"].tolist() == ["true_early", "emerging", "near_graduation", "already_graduated"]
    assert set(scan["early_purity_bucket"]).issubset(
        {"true_early", "emerging", "near_graduation", "already_graduated", "unknown_rank_context"}
    )
    assert summary["bucket_counts"] == {
        "true_early": 1,
        "emerging": 1,
        "near_graduation": 1,
        "already_graduated": 1,
    }


def test_bucket_caps_and_soft_score_fallback() -> None:
    rows = []
    for idx in range(6):
        rows.append(
            {
                **_ranked().iloc[0].to_dict(),
                "symbol_id": f"E{idx}",
                "active_rank_pctile": 10 + idx,
                "composite_score": 10 + idx,
                "rel_strength_score": 10 + idx,
                "return_20": -20,
                "momentum_acceleration": -20,
                "volume_ratio_20": 0,
                "volume_zscore_20": -2,
            }
        )
    scan, summary = build_early_accumulation_scan(
        ranked_universe=pd.DataFrame(rows),
        pattern_df=pd.DataFrame(),
        breakout_df=pd.DataFrame(),
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(
            min_score=99,
            top_n=10,
            true_early_top_n=2,
            emerging_top_n=2,
            near_graduation_top_n=2,
            already_graduated_top_n=2,
            min_true_early_rows=1,
            min_emerging_rows=0,
        ),
    )

    assert len(scan) == 2
    assert scan["early_purity_bucket"].eq("true_early").all()
    assert "no candidates passed early_accumulation_min_score" in "; ".join(summary["warnings"])


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
    assert scan["early_purity_bucket"].notna().all()
    assert summary["rows"] == len(scan)
    assert "score_distribution" in summary
    assert "active_rank_pctile_distribution" in summary


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


def test_missing_rank_breakout_and_technical_inputs_warn() -> None:
    minimal = pd.DataFrame(
        [
            {
                "symbol_id": "MISS",
                "exchange": "NSE",
                "close": 100.0,
                "sector_name": "Tech",
                "delivery_pct": 50.0,
            }
        ]
    )
    scan, summary = build_early_accumulation_scan(
        ranked_universe=minimal,
        pattern_df=pd.DataFrame(),
        breakout_df=pd.DataFrame(),
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10, min_true_early_rows=0, min_emerging_rows=0),
    )

    assert scan.loc[0, "early_purity_bucket"] == "unknown_rank_context"
    assert scan.loc[0, "breakout_qualified"] is False or bool(scan.loc[0, "breakout_qualified"]) is False
    warning_text = "; ".join(summary["warnings"])
    assert "sma_200 missing" in warning_text
    assert "breakout_scan artifact missing" in warning_text
    assert "active rank percentile and composite_score missing" in warning_text
    assert {"sma_200", "breakout_state", "active_rank_pctile", "composite_score"}.issubset(
        set(summary["missing_input_columns"])
    )


def test_graduation_precedence_and_required_columns() -> None:
    breakouts = pd.DataFrame([{"symbol_id": "AAA", "exchange": "NSE", "breakout_state": "qualified", "breakout_type": "A", "breakout_score": 99.0}])
    scan, _summary = build_early_accumulation_scan(
        ranked_universe=_ranked(),
        pattern_df=pd.DataFrame(),
        breakout_df=breakouts,
        as_of_date="2026-07-05",
        config=EarlyAccumulationConfig(min_score=0, top_n=10),
    )

    row = scan.set_index("symbol_id").loc["AAA"]
    assert row["graduation_status"] == "breakout_qualified"
    assert row["early_purity_bucket"] == "already_graduated"
    assert bool(row["breakout_qualified"]) is True
    assert row["breakout_score"] == 99.0
    for column in [
        "early_accumulation_score",
        "early_purity_bucket",
        "breakout_qualified",
        "breakout_type",
        "breakout_score",
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

    assert payload["summary"]["investigator_early_accumulation_enabled"] is True
    assert payload["summary"]["investigator_early_accumulation_count"] == len(scan)
    assert payload["summary"]["investigator_early_accumulation_bucket_counts"]
    assert payload["investigator_early_accumulation_preview"]
    assert "early_purity_bucket" in payload["investigator_early_accumulation_preview"][0]
