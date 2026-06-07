from __future__ import annotations

from ai_trading_system.domains.candidate_tracker.service import classify_candidate


def test_price_action_breaks_sma200_or_rs_or_drawdown_flags_technical_failure() -> None:
    snapshot = {
        "tracking_health_score": 60,
        "relative_strength": 70,
        "composite_score": 70,
        "close_above_sma50": False,
        "close_above_sma200": False,
        "drawdown_from_tracking_high": 8,
    }

    status, reasons = classify_candidate(
        snapshot=snapshot,
        latest_review=None,
        prior_snapshot=None,
        last_seen_date="2026-06-01",
        run_date="2026-06-01",
        review_window_days=120,
    )

    assert status == "TECHNICAL_FAILURE"
    assert "technical failure" in reasons


def test_improving_price_action_can_be_strong_improving() -> None:
    snapshot = {
        "tracking_health_score": 84,
        "relative_strength": 88,
        "composite_score": 90,
        "close_above_sma50": True,
        "close_above_sma200": True,
        "drawdown_from_tracking_high": 2,
    }
    prior = {"relative_strength": 80, "composite_score": 82}
    review = {"quarterly_result_score": 82, "result_score_delta": 5, "profit_yoy_pct": 20, "opm_yoy_change_bps": 150}

    status, _ = classify_candidate(
        snapshot=snapshot,
        latest_review=review,
        prior_snapshot=prior,
        last_seen_date="2026-06-01",
        run_date="2026-06-01",
        review_window_days=120,
    )

    assert status == "STRONG_IMPROVING"
