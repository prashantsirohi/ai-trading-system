from __future__ import annotations

from ai_trading_system.domains.publish.signal_classification import classify_signal


def test_classify_signal_uses_score_thresholds() -> None:
    assert classify_signal({"composite_score": 90}) == "actionable"
    assert classify_signal({"composite_score": 70}) == "watchlist"
    assert classify_signal({"composite_score": 40}) == "informational"
