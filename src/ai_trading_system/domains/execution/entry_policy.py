"""Execution entry policy helpers."""

from __future__ import annotations


def select_entry_policy(candidate: dict, policy_name: str = "breakout") -> dict:
    """Return additive entry policy metadata for execution inspection."""
    close = candidate.get("close")
    return {
        "entry_policy": policy_name,
        "entry_price": close,
        "entry_trigger": None,
        "entry_note": f"policy={policy_name}",
    }
