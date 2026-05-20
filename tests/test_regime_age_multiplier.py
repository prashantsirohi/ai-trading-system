"""Phase 8: regime_age_multiplier decay table."""

from __future__ import annotations

import pytest

from ai_trading_system.analytics.regime.profiles import regime_age_multiplier


@pytest.mark.parametrize(
    "age_days,expected",
    [
        (0, 1.00),
        (20, 1.00),
        (21, 0.85),
        (40, 0.85),
        (41, 0.70),
        (60, 0.70),
        (61, 0.50),
        (200, 0.50),
    ],
)
def test_regime_age_multiplier_decay_table(age_days: int, expected: float) -> None:
    assert regime_age_multiplier(age_days) == expected


def test_regime_age_multiplier_clamps_negative_to_youngest() -> None:
    # Defensive: a negative age (shouldn't happen in practice) folds to the
    # ≤20-day bucket rather than blowing up.
    assert regime_age_multiplier(-5) == 1.00
