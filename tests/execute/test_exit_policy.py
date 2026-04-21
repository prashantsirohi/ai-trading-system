from __future__ import annotations

import pytest

from ai_trading_system.domains.execution.exit_policy import build_exit_plan


def test_build_exit_plan_uses_atr_multiple_and_time_stop() -> None:
    plan = build_exit_plan(
        {"close": 100.0, "atr_14": 2.5},
        atr_multiple=2.0,
        max_holding_days=15,
    )

    assert plan["stop_loss"] == pytest.approx(95.0)
    assert plan["trailing_stop"] is None
    assert plan["time_stop_days"] == 15
    assert plan["exit_reason"] is None
