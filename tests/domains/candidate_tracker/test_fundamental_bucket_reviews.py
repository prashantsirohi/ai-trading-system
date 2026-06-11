from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.candidate_tracker import CandidateTrackerConfig, run_candidate_tracker


def _config(tmp_path: Path, run_date: str) -> CandidateTrackerConfig:
    return CandidateTrackerConfig(db_path=tmp_path / "candidate_tracker.duckdb", run_date=run_date, run_id=f"run-{run_date}")


def _shortlist(
    symbol: str = "AAA",
    *,
    bucket_as_of: str = "2026-06-01",
    business_bucket: str = "HIGH_GROWTH",
    score: float = 72.0,
    opportunity: str = "GOOD_RESULTS_BELOW_HISTORY",
    manual: bool = False,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "bucket_as_of": bucket_as_of,
                "business_bucket": business_bucket,
                "secondary_bucket_tags": "Turnaround Candidate",
                "opportunity_label": opportunity,
                "bucket_reason": "revenue growth > 30%",
                "manual_review_flag": manual,
                "watchlist_bucket": "F2_RESULT_VALUE_ACCUMULATION",
                "final_watchlist_score": score,
            }
        ]
    )


def test_bucket_shortlist_creates_episode_and_current_fields(tmp_path: Path) -> None:
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=pd.DataFrame(),
        fundamental_bucket_shortlist=_shortlist(),
    )

    assert len(result.current) == 1
    assert len(result.bucket_reviews) == 1
    row = result.current.iloc[0]
    assert row["symbol"] == "AAA"
    assert row["business_bucket"] == "HIGH_GROWTH"
    assert row["opportunity_label"] == "GOOD_RESULTS_BELOW_HISTORY"
    assert row["final_watchlist_score"] == 72.0


def test_bucket_review_is_idempotent_for_same_as_of(tmp_path: Path) -> None:
    run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=pd.DataFrame(),
        fundamental_bucket_shortlist=_shortlist(),
    )
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=pd.DataFrame(),
        fundamental_bucket_shortlist=_shortlist(score=75),
    )

    assert len(result.bucket_reviews) == 0
    assert result.current.iloc[0]["final_watchlist_score"] == 72.0


def test_next_quarter_bucket_change_and_score_drop_alerts(tmp_path: Path) -> None:
    run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=pd.DataFrame(),
        fundamental_bucket_shortlist=_shortlist(bucket_as_of="2026-06-01", business_bucket="HIGH_GROWTH", score=82),
    )
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-09-01"),
        final_candidates=pd.DataFrame(),
        fundamental_bucket_shortlist=_shortlist(
            bucket_as_of="2026-09-01",
            business_bucket="QUALITY_COMPOUNDER",
            score=68,
        ),
    )

    assert len(result.bucket_reviews) == 1
    review = result.bucket_reviews.iloc[0]
    assert review["prior_business_bucket"] == "HIGH_GROWTH"
    assert bool(review["bucket_changed"]) is True
    assert review["final_watchlist_score_delta"] == -14
    alert_types = set(result.alerts["alert_type"].tolist())
    assert "BUCKET_CHANGE" in alert_types
    assert "FUNDAMENTAL_SCORE_DROP" in alert_types


def test_manual_review_bucket_alert(tmp_path: Path) -> None:
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=pd.DataFrame(),
        fundamental_bucket_shortlist=_shortlist(opportunity="MANUAL_REVIEW", manual=True),
    )

    assert "MANUAL_REVIEW_REQUIRED" in set(result.alerts["alert_type"].tolist())
