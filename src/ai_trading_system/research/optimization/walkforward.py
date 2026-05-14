"""Walk-forward window generation. Each Optuna trial is evaluated as the mean
of per-fold fitness — never on a single full-period in-sample run, otherwise
the optimizer overfits to whichever year happened to look favourable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class WalkForwardFold:
    index: int
    train_start: date
    train_end: date
    val_start: date
    val_end: date


def _add_months(d: date, months: int) -> date:
    # Calendar arithmetic without external deps.
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    # Clamp to month end.
    day = min(d.day, 28)
    return date(year, month, day)


def build_folds(
    start: date,
    end: date,
    *,
    train_months: int = 12,
    validation_months: int = 3,
    step_months: int = 3,
) -> list[WalkForwardFold]:
    """Rolling train/validation windows.

    A fold is valid only if its full validation window fits inside ``[start, end]``.
    """
    if train_months <= 0 or validation_months <= 0 or step_months <= 0:
        raise ValueError("month spans must be positive")
    if end <= start:
        return []

    folds: list[WalkForwardFold] = []
    cursor = start
    index = 0
    while True:
        train_end = _add_months(cursor, train_months) - timedelta(days=1)
        val_start = train_end + timedelta(days=1)
        val_end = _add_months(val_start, validation_months) - timedelta(days=1)
        if val_end > end:
            break
        folds.append(
            WalkForwardFold(
                index=index,
                train_start=cursor,
                train_end=train_end,
                val_start=val_start,
                val_end=val_end,
            )
        )
        cursor = _add_months(cursor, step_months)
        index += 1
    return folds
