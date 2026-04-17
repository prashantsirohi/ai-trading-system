"""Execution exit policy helpers."""

from __future__ import annotations


def build_exit_plan(
    candidate: dict,
    atr_multiple: float = 2.0,
    max_holding_days: int = 20,
) -> dict:
    """Build an inspectable exit scaffold without changing order semantics."""
    close = candidate.get("close")
    atr = candidate.get("atr_14") or 0.0
    stop_loss = None if close is None else float(close) - (float(atr) * atr_multiple)
    return {
        "stop_loss": stop_loss,
        "trailing_stop": None,
        "time_stop_days": int(max_holding_days),
        "exit_reason": None,
    }
