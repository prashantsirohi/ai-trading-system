from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.candidate_tracker import CandidateTrackerConfig, run_candidate_tracker


def _config(tmp_path: Path, run_date: str) -> CandidateTrackerConfig:
    return CandidateTrackerConfig(db_path=tmp_path / "tracker.duckdb", run_date=run_date)


def _final(close: float, rs: float, sma200: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "candidate_group": "FUND_VALUE_TECH_READY",
                "composite_score": 90,
                "rel_strength_score": rs,
                "sector": "Capital Goods",
                "close": close,
                "sma_50": 105,
                "sma_200": sma200,
                "near_52w_high_pct": 3,
                "stage2_label": "stage2",
            }
        ]
    )


def _result(available_at: str, score: float, bucket: str, profit: float, opm: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "report_date": available_at,
                "available_at": available_at,
                "quarterly_result_score": score,
                "quarterly_result_bucket": bucket,
                "sales_yoy_pct": 25,
                "operating_profit_yoy_pct": 35,
                "profit_yoy_pct": profit,
                "opm_yoy_change_bps": opm,
            }
        ]
    )


def test_new_quarterly_result_creates_review_and_delta(tmp_path: Path) -> None:
    run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=_final(120, 82),
        quarterly_result_scores=_result("2026-06-01", 70, "GREAT_RESULT", 20, 150),
    )
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-09-01"),
        final_candidates=_final(130, 90),
        quarterly_result_scores=_result("2026-09-01", 90, "GREAT_RESULT", 40, 300),
    )

    assert len(result.fundamental_reviews) == 1
    assert result.fundamental_reviews.iloc[0]["result_score_delta"] == 20
    assert result.current.iloc[0]["status"] == "STRONG_IMPROVING"


def test_deteriorating_result_and_sma200_break_remove_from_tracking(tmp_path: Path) -> None:
    run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=_final(120, 82),
        quarterly_result_scores=_result("2026-06-01", 75, "GREAT_RESULT", 25, 200),
    )
    bad = _final(70, 30)
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-09-01"),
        final_candidates=bad,
        quarterly_result_scores=_result("2026-09-01", 25, "DETERIORATING", -10, -300),
    )

    assert result.current.iloc[0]["status"] == "REMOVE_FROM_TRACKING"
