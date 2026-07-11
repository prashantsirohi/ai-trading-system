from __future__ import annotations

import pandas as pd
import pytest

from ai_trading_system.domains.ranking.stage1 import Stage1ModelConfig, build_stage1_scan


def _candidate(symbol: str = "AAA", **overrides):
    row = {
        "symbol_id": symbol,
        "exchange": "NSE",
        "sma_20": 105.0,
        "sma_50": 99.0,
        "sma_150": 98.0,
        "sma_200": 100.0,
        "sma50_slope_20d_pct": 1.0,
        "sma150_slope_20d_pct": 0.5,
        "sma200_slope_20d_pct": 0.2,
        "sma50_sma200_gap_delta_5d": 0.5,
        "sma50_sma200_gap_delta_20d": 2.0,
        "sma50_sma200_gap_delta_60d": 4.0,
        "sma20_sma50_gap_delta_10d": 1.0,
        "trend_repair_score": 80.0,
        "delivery_accumulation_score": 80.0,
        "volume_confirmation_score": 80.0,
        "relative_strength_score_early": 80.0,
        "base_pattern_freshness_score": 80.0,
        "sector_strength_score": 80.0,
        "liquidity_score": 1.0,
        "top_pattern_state": "mature",
    }
    row.update(overrides)
    return row


def test_config_validation_and_hash_are_stable():
    config = Stage1ModelConfig().validate()
    assert config.config_hash == Stage1ModelConfig().config_hash
    with pytest.raises(ValueError, match="sum to 100"):
        Stage1ModelConfig(maturity_weights={**config.maturity_weights, "structural_repair": 24}).validate()
    with pytest.raises(ValueError, match="unknown"):
        Stage1ModelConfig(maturity_weights={**config.maturity_weights, "bogus": 0}).validate()


def test_stage1_scan_is_explainable_ranked_and_research_only():
    frame = pd.DataFrame([_candidate("BBB", relative_strength_score_early=70), _candidate("AAA")])
    out, summary = build_stage1_scan(frame)
    assert out.iloc[0]["symbol_id"] == "AAA"
    assert out.iloc[0]["stage1_emerging_rank"] == 1
    assert out.iloc[0]["stage1_substate"] in {"STAGE_1_LATE", "STAGE_1_BREAKOUT_READY"}
    assert out["execution_eligible"].eq(False).all()
    assert out["model_status"].eq("RESEARCH_ONLY").all()
    assert out["stage1_config_hash"].nunique() == 1
    assert summary["eligible_rows"] == 2
    assert "stage1_structural_repair_raw" in out
    assert "stage1_structural_repair_score" in out
    assert "stage1_structural_repair_max" in out


def test_hard_guard_excludes_candidate_from_emerging_rank():
    out, _ = build_stage1_scan(pd.DataFrame([_candidate(weekly_stage_label="STAGE_4_DECLINE")]))
    assert not bool(out.loc[0, "stage1_eligible"])
    assert "STAGE4_HARD_GUARD" in out.loc[0, "stage1_block_reasons"]
    assert out.loc[0, "stage1_score_band"] in {"LATE_RANGE", "BREAKOUT_READY_RANGE"}
    assert out.loc[0, "stage1_substate"] == "NOT_STAGE1"
    assert out.loc[0, "stage1_operational_status"] == "STRUCTURALLY_BLOCKED"
    assert pd.isna(out.loc[0, "stage1_emerging_rank"])


def test_golden_cross_status_and_pattern_state_are_orthogonal():
    out, _ = build_stage1_scan(pd.DataFrame([_candidate(top_pattern_state="failed")]))
    assert out.loc[0, "golden_cross_status"] == "IMMINENT"
    assert out.loc[0, "pattern_promotion_state"] == "FAILED"
    assert out.loc[0, "stage1_operational_status"] == "REGRESSED"
    assert out.loc[0, "stage1_substate"].startswith("STAGE_1_")


def test_missing_components_are_not_rescaled():
    row = _candidate()
    for column in ("trend_repair_score", "delivery_accumulation_score", "volume_confirmation_score", "relative_strength_score_early", "base_pattern_freshness_score"):
        row[column] = None
    out, _ = build_stage1_scan(pd.DataFrame([row]))
    assert out.loc[0, "stage1_score_confidence"] == "INSUFFICIENT_DATA"
    assert out.loc[0, "stage1_maturity_score"] < 50
    assert not bool(out.loc[0, "stage1_eligible"])


def test_blocked_confirmed_pattern_is_visible_but_never_promoted():
    out, summary = build_stage1_scan(pd.DataFrame([_candidate(
        weekly_stage_label="STAGE_4_DECLINE", top_pattern_state="confirmed",
    )]))
    row = out.iloc[0]
    assert row["pattern_promotion_state"] == "CONFIRMED"
    assert row["stage1_operational_status"] == "STRUCTURALLY_BLOCKED"
    assert not bool(row["promotion_eligibility"])
    assert "STAGE4_HARD_GUARD" in row["promotion_block_reasons"]
    assert summary["invariant_validation"] is True


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"sma_50": 80, "sma_200": 100, "sma50_sma200_gap_delta_20d": 0}, "FAR_BELOW"),
        ({"sma_50": 95, "sma_200": 100, "sma50_sma200_gap_delta_20d": 1}, "APPROACHING"),
        ({}, "IMMINENT"),
        ({"sma_50": 101, "sma_200": 100, "golden_cross_days_since": 4}, "CROSSED_RECENTLY"),
        ({"sma_50": 101, "sma_200": 100, "golden_cross_days_since": 30}, "CROSSED_ESTABLISHED"),
        ({"golden_cross_failed": True}, "FAILED_CROSS"),
        ({"sma_50": 105, "sma_200": 100, "sma50_sma200_gap_delta_20d": -1, "sma50_slope_20d_pct": 0, "sma200_slope_20d_pct": 1}, "DEATH_CROSS_RISK"),
        ({"sma_200": None}, "UNKNOWN"),
    ],
)
def test_canonical_golden_cross_statuses(overrides, expected):
    out, _ = build_stage1_scan(pd.DataFrame([_candidate(**overrides)]))
    assert out.loc[0, "golden_cross_status"] == expected


def test_ma_gap_quality_and_summary_invariants():
    out, summary = build_stage1_scan(pd.DataFrame([
        _candidate("POS", sma_50=160, sma_200=100),
        _candidate("NEG", sma_50=60, sma_200=100),
        _candidate("NORMAL"),
    ]))
    flags = dict(zip(out["symbol_id"], out["ma_gap_quality_flag"], strict=True))
    assert flags["POS"] == "EXTREME_POSITIVE_GAP"
    assert flags["NEG"] == "EXTREME_NEGATIVE_GAP"
    assert summary["extreme_ma_gap_count"] == 2
    assert summary["median_ma_gap_pct"] is not None
    assert summary["invariant_validation_errors"] == []
