from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.fundamentals.enrich_rank import enrich_rank_artifacts


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    rank_dir = tmp_path / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 82, "sector_strength_score": 72, "prox_high": 5},
            {"symbol_id": "EXP", "composite_score": 75, "sector_strength_score": 70, "prox_high": 4},
            {"symbol_id": "DOWN", "composite_score": 80, "sector_strength_score": 75, "prox_high": 4},
            {"symbol_id": "BAD", "composite_score": 90, "sector_strength_score": 80, "prox_high": 3},
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)
    pd.DataFrame(
        [
            {"symbol_id": symbol, "breakout_score": 85, "candidate_tier": "A", "qualified": True}
            for symbol in ["AAA", "EXP", "DOWN", "BAD"]
        ]
    ).to_csv(rank_dir / "breakout_scan.csv", index=False)
    pd.DataFrame(
        [
            {"symbol_id": symbol, "pattern_score": 80, "setup_quality": 75}
            for symbol in ["AAA", "EXP", "DOWN", "BAD"]
        ]
    ).to_csv(rank_dir / "pattern_scan.csv", index=False)
    scores = tmp_path / "scores.csv"
    pd.DataFrame(
        [
            {"symbol": "AAA", "fundamental_score": 78, "fundamental_tier": "A", "hard_red_flag": False},
            {"symbol": "EXP", "fundamental_score": 78, "fundamental_tier": "A", "hard_red_flag": False},
            {"symbol": "DOWN", "fundamental_score": 78, "fundamental_tier": "A", "hard_red_flag": False},
            {"symbol": "BAD", "fundamental_score": 20, "fundamental_tier": "Reject", "hard_red_flag": True, "red_flags": "pledge"},
        ]
    ).to_csv(scores, index=False)
    results = tmp_path / "quarterly.csv"
    pd.DataFrame(
        [
            {"symbol": "AAA", "available_at": "2026-05-01", "quarterly_result_score": 82, "quarterly_result_bucket": "GREAT_RESULT"},
            {"symbol": "EXP", "available_at": "2026-05-01", "quarterly_result_score": 82, "quarterly_result_bucket": "GREAT_RESULT"},
            {"symbol": "DOWN", "available_at": "2026-05-01", "quarterly_result_score": 40, "quarterly_result_bucket": "DETERIORATING"},
            {"symbol": "BAD", "available_at": "2026-05-01", "quarterly_result_score": 95, "quarterly_result_bucket": "BLOWOUT_RESULT"},
        ]
    ).to_csv(results, index=False)
    bands = tmp_path / "bands.csv"
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "date": "2026-05-31",
                "valuation_history_score": 70,
                "valuation_history_bucket": "BELOW_OWN_MEDIAN",
                "valuation_reason": "PE/PS below own 5Y median",
                "pe_ttm": 15,
                "ps_ttm": 2,
                "pb": 3,
            },
            {
                "symbol": "EXP",
                "date": "2026-05-31",
                "valuation_history_score": 60,
                "valuation_history_bucket": "EXPENSIVE_VS_HISTORY",
                "valuation_reason": "Expensive: PE/PS above 80th percentile",
            },
            {"symbol": "DOWN", "date": "2026-05-31", "valuation_history_score": 70, "valuation_history_bucket": "BELOW_OWN_MEDIAN"},
            {"symbol": "BAD", "date": "2026-05-31", "valuation_history_score": 90, "valuation_history_bucket": "DEEPLY_BELOW_HISTORY"},
        ]
    ).to_csv(bands, index=False)
    return rank_dir, scores, results, bands


def test_great_result_below_median_strong_technical_gets_f4(tmp_path: Path) -> None:
    rank_dir, scores, results, bands = _write_inputs(tmp_path)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        quarterly_result_scores=results,
        stock_valuation_bands=bands,
        watchlist_mode="fundamental_tracking",
        output=tmp_path / "watchlist.csv",
    )

    row = result.set_index("symbol").loc["AAA"]
    assert row["watchlist_bucket"] == "F4_ACTION_CANDIDATE"
    assert row["valuation_history_bucket"] == "BELOW_OWN_MEDIAN"


def test_expensive_valuation_blocks_f4(tmp_path: Path) -> None:
    rank_dir, scores, results, bands = _write_inputs(tmp_path)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        quarterly_result_scores=results,
        stock_valuation_bands=bands,
        watchlist_mode="fundamental_tracking",
        output=tmp_path / "watchlist.csv",
    )

    assert result.set_index("symbol").loc["EXP", "watchlist_bucket"] != "F4_ACTION_CANDIDATE"


def test_fair_valuation_with_strong_result_and_technical_gets_f3(tmp_path: Path) -> None:
    rank_dir, scores, results, bands = _write_inputs(tmp_path)
    pd.DataFrame(
        [
            {"symbol_id": "FAIR", "composite_score": 76, "sector_strength_score": 68, "prox_high": 8},
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)
    pd.DataFrame([{"symbol_id": "FAIR", "breakout_score": 70, "candidate_tier": "B", "qualified": False}]).to_csv(
        rank_dir / "breakout_scan.csv",
        index=False,
    )
    pd.DataFrame([{"symbol_id": "FAIR", "pattern_score": 70, "setup_quality": 70}]).to_csv(
        rank_dir / "pattern_scan.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "FAIR", "fundamental_score": 78, "fundamental_tier": "A", "hard_red_flag": False}]).to_csv(
        scores,
        index=False,
    )
    pd.DataFrame(
        [
            {
                "symbol": "FAIR",
                "available_at": "2026-05-01",
                "quarterly_result_score": 82,
                "quarterly_result_bucket": "GREAT_RESULT",
            }
        ]
    ).to_csv(results, index=False)
    pd.DataFrame(
        [
            {
                "symbol": "FAIR",
                "date": "2026-05-31",
                "valuation_history_score": 50,
                "valuation_history_bucket": "FAIR_VALUE",
                "valuation_reason": "Fair versus own valuation history",
            }
        ]
    ).to_csv(bands, index=False)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        quarterly_result_scores=results,
        stock_valuation_bands=bands,
        watchlist_mode="fundamental_tracking",
        output=tmp_path / "watchlist.csv",
    )

    assert result.set_index("symbol").loc["FAIR", "watchlist_bucket"] == "F3_FUND_VALUE_TECH_READY"


def test_deteriorating_result_gets_d1(tmp_path: Path) -> None:
    rank_dir, scores, results, bands = _write_inputs(tmp_path)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        quarterly_result_scores=results,
        stock_valuation_bands=bands,
        watchlist_mode="fundamental_tracking",
        output=tmp_path / "watchlist.csv",
    )

    assert result.set_index("symbol").loc["DOWN", "watchlist_bucket"] == "D1_RESULT_DOWNTURN"


def test_hard_red_flag_gets_d2(tmp_path: Path) -> None:
    rank_dir, scores, results, bands = _write_inputs(tmp_path)

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        quarterly_result_scores=results,
        stock_valuation_bands=bands,
        watchlist_mode="fundamental_tracking",
        output=tmp_path / "watchlist.csv",
    )

    assert result.set_index("symbol").loc["BAD", "watchlist_bucket"] == "D2_AVOID_RED_FLAG"
