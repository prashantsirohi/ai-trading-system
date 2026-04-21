from __future__ import annotations

from ai_trading_system.domains.execution.entry_policy import select_entry_policy


def test_select_entry_policy_returns_explicit_entry_scaffold() -> None:
    payload = select_entry_policy({"close": 123.45}, policy_name="breakout")

    assert payload["entry_policy"] == "breakout"
    assert payload["entry_price"] == 123.45
    assert payload["entry_trigger"] is None
    assert payload["entry_note"] == "policy=breakout"
