"""Tests for the raw vs confirmed regime disagreement helper + alert path."""

from __future__ import annotations

from ai_trading_system.analytics.regime import regime_disagreement


def test_same_regime_is_not_a_disagreement() -> None:
    out = regime_disagreement("bull", "bull")
    assert out["present"] is False
    assert out["dangerous"] is False
    assert out["direction"] == "same"


def test_dangerous_when_raw_risk_off_and_confirmed_bull() -> None:
    out = regime_disagreement("bull", "risk_off")
    assert out["present"] is True
    assert out["dangerous"] is True
    assert out["direction"] == "raw_worse"
    assert out["confirmed"] == "bull"
    assert out["raw"] == "risk_off"


def test_dangerous_when_raw_risk_off_and_confirmed_strong_bull() -> None:
    out = regime_disagreement("strong_bull", "risk_off")
    assert out["dangerous"] is True
    assert out["direction"] == "raw_worse"


def test_not_dangerous_when_raw_better_than_confirmed() -> None:
    """raw=bull, confirmed=risk_off — breadth is improving but confirmed lags.
    This is the opposite case — opportunity, not danger."""
    out = regime_disagreement("risk_off", "bull")
    assert out["present"] is True
    assert out["dangerous"] is False
    assert out["direction"] == "raw_better"


def test_not_dangerous_when_raw_only_one_step_worse() -> None:
    """raw=neutral while confirmed=bull is a softening, but not the
    catastrophic 'breadth collapsed' scenario the alert targets."""
    out = regime_disagreement("bull", "neutral")
    assert out["present"] is True
    assert out["dangerous"] is False
    assert out["direction"] == "raw_worse"


def test_not_dangerous_when_confirmed_is_neutral() -> None:
    """neutral confirmed → no aggressive position sizing was in play, so
    raw=risk_off doesn't trigger the 'dangerous' label."""
    out = regime_disagreement("neutral", "risk_off")
    assert out["present"] is True
    assert out["dangerous"] is False


def test_handles_missing_or_empty_inputs() -> None:
    assert regime_disagreement(None, None)["present"] is False
    assert regime_disagreement("", "")["present"] is False
    assert regime_disagreement("bull", None)["present"] is False
    assert regime_disagreement(None, "risk_off")["present"] is False


def test_unknown_regime_label_falls_back_to_same_direction() -> None:
    """Unknown labels can't be ranked but the disagreement flag still works."""
    out = regime_disagreement("bull", "panic")
    # confirmed != raw, so present=True; direction unknown
    assert out["present"] is True
    assert out["direction"] == "same"
    # dangerous requires raw=risk_off specifically
    assert out["dangerous"] is False
