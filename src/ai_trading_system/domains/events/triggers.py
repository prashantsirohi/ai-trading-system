"""Common ``Trigger`` schema consumed by the events stage.

Every trigger source (volume shocker, bulk-deal feed, breakout scanner)
projects its native rows into a ``Trigger`` so the enrichment service can
treat them uniformly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

TriggerType = Literal["volume_shock", "bulk_deal", "breakout"]


@dataclass(frozen=True)
class Trigger:
    """A single (symbol, reason) row that should be enriched with corp-actions.

    ``trigger_strength`` is normalized to roughly [0, 1+]; the enrichment
    service uses it to rank-order triggers when truncating to a budget.
    Implementations may exceed 1.0 for extreme cases (z=8 volume shock); the
    publish payload formatter is expected to clamp for display.
    """

    symbol: str
    trigger_type: TriggerType
    as_of_date: date
    trigger_strength: float = 1.0
    trigger_metadata: dict[str, Any] = field(default_factory=dict)

    def dedupe_key(self) -> tuple[str, str, str]:
        """Stable key for in-memory dedup across trigger sources."""
        return (self.symbol, self.trigger_type, self.as_of_date.isoformat())
