from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.fundamentals.enrich_rank import _first_available, enrich_rank_artifacts


def _write_base_files(rank_dir: Path, scores_path: Path, trends_path: Path | None = None) -> None:
    rank_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "composite_score": 82,
                "rel_strength": 35,
                "vol_intensity": 1.7,
                "trend_score": 80,
                "prox_high": 4,
                "delivery_pct": 55,
                "sector_strength_score": 75,
            },
            {
                "symbol_id": "BAD",
                "composite_score": 85,
                "rel_strength": 30,
                "vol_intensity": 1.5,
                "trend_score": 70,
                "prox_high": 5,
                "delivery_pct": 50,
                "sector_strength_score": 60,
            },
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "breakout_score": 90, "candidate_tier": "A", "qualified": True, "setup_family": "flat_base"},
            {"symbol_id": "BAD", "breakout_score": 88, "candidate_tier": "A", "qualified": True, "setup_family": "flat_base"},
        ]
    ).to_csv(rank_dir / "breakout_scan.csv", index=False)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "pattern_family": "vcp", "pattern_state": "confirmed", "pattern_score": 82, "setup_quality": 75},
            {"symbol_id": "BAD", "pattern_family": "flag", "pattern_state": "confirmed", "pattern_score": 80, "setup_quality": 70},
        ]
    ).to_csv(rank_dir / "pattern_scan.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "name": "Alpha",
                "industry_group": "Capital Goods",
                "industry": "Industrial Products",
                "quality_score": 80,
                "growth_score": 75,
                "balance_sheet_score": 90,
                "valuation_score": 65,
                "ownership_score": 85,
                "fundamental_score": 79,
                "fundamental_tier": "A",
                "red_flags": "",
                "hard_red_flag": False,
            },
            {
                "symbol": "BAD",
                "name": "Bad Co",
                "industry_group": "Capital Goods",
                "industry": "Industrial Products",
                "quality_score": 30,
                "growth_score": 20,
                "balance_sheet_score": 10,
                "valuation_score": 50,
                "ownership_score": 10,
                "fundamental_score": 25,
                "fundamental_tier": "Reject",
                "red_flags": "pledged_pct > 10",
                "hard_red_flag": True,
            },
        ]
    ).to_csv(scores_path, index=False)
    if trends_path is not None:
        pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "fundamental_score_delta": 8,
                    "fundamental_trend_label": "IMPROVING",
                    "trend_reason": "Fundamental score improved",
                },
                {
                    "symbol": "BAD",
                    "fundamental_score_delta": -10,
                    "fundamental_trend_label": "DETERIORATING",
                    "trend_reason": "Fundamental score deteriorated",
                },
            ]
        ).to_csv(trends_path, index=False)


def test_enrich_rank_assigns_buckets_and_scores(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    scores_path = tmp_path / "fundamental_scores_latest.csv"
    trends_path = tmp_path / "fundamental_trends_latest.csv"
    output = tmp_path / "watchlist_candidates_latest.csv"
    _write_base_files(rank_dir, scores_path, trends_path)

    result = enrich_rank_artifacts(rank_dir=rank_dir, fundamental_scores=scores_path, fundamental_trends=trends_path, output=output)

    assert output.exists()
    assert "final_watchlist_score" in result.columns
    by_symbol = result.set_index("symbol")
    assert by_symbol.loc["AAA", "watchlist_bucket"] == "ADD_TO_WATCHLIST"
    assert by_symbol.loc["BAD", "watchlist_bucket"] == "AVOID_RED_FLAG"
    assert by_symbol.loc["AAA", "fundamental_trend_label"] == "IMPROVING"
    assert "improving fundamentals" in by_symbol.loc["AAA", "watchlist_reason"]
    assert "deteriorating fundamentals" in by_symbol.loc["BAD", "watchlist_reason"]
    assert by_symbol.loc["AAA", "next_action"] == "Add to watchlist and review chart"


def test_first_available_text_default_preserves_object_dtype() -> None:
    frame = pd.DataFrame({"industry_group": pd.Series([float("nan"), float("nan")], dtype="float64")})

    result = _first_available(frame, ["industry_group", "industry_group_fundamental"], "")

    assert result.dtype == object
    assert result.tolist() == ["", ""]


def test_enrich_rank_tolerates_missing_optional_scan_files(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    scores_path = tmp_path / "fundamental_scores_latest.csv"
    output = tmp_path / "watchlist_candidates_latest.csv"
    _write_base_files(rank_dir, scores_path)
    (rank_dir / "breakout_scan.csv").unlink()
    (rank_dir / "pattern_scan.csv").unlink()

    result = enrich_rank_artifacts(rank_dir=rank_dir, fundamental_scores=scores_path, fundamental_trends=None, output=output)

    assert output.exists()
    assert not result.empty
    assert "final_watchlist_score" in result.columns


def test_value_trap_risk_is_penalized(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    scores_path = tmp_path / "fundamental_scores_latest.csv"
    trends_path = tmp_path / "fundamental_trends_latest.csv"
    output = tmp_path / "watchlist_candidates_latest.csv"
    _write_base_files(rank_dir, scores_path)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "fundamental_score_delta": -2,
                "fundamental_trend_label": "VALUE_TRAP_RISK",
                "trend_reason": "Valuation improved while quality deteriorated",
            }
        ]
    ).to_csv(trends_path, index=False)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores_path,
        fundamental_trends=trends_path,
        output=output,
    )

    row = result.set_index("symbol").loc["AAA"]
    assert row["watchlist_bucket"] == "STUDY_ONLY"
    assert "value-trap risk" in row["watchlist_reason"]


def test_catalyst_score_adjusts_final_score_only_for_symbols_with_catalysts(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    scores_path = tmp_path / "fundamental_scores_latest.csv"
    catalysts_path = tmp_path / "catalyst_scores_latest.csv"
    output = tmp_path / "watchlist_candidates_latest.csv"
    _write_base_files(rank_dir, scores_path)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "catalyst_score": 90,
                "catalyst_type": "ORDER_WIN",
                "catalyst_summary": "Large order win",
                "evidence_source": "artifact",
                "confidence": 0.8,
            }
        ]
    ).to_csv(catalysts_path, index=False)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores_path,
        fundamental_trends=None,
        catalysts=catalysts_path,
        output=output,
    )

    by_symbol = result.set_index("symbol")
    assert by_symbol.loc["AAA", "catalyst_score"] == 90
    assert by_symbol.loc["AAA", "catalyst_type"] == "ORDER_WIN"
    assert by_symbol.loc["AAA", "final_watchlist_score"] == 83.82
    assert pd.isna(by_symbol.loc["BAD", "catalyst_score"])
