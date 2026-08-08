"""Explainable journal analytics helpers."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

from .config import JournalAnalyticsConfig


def score_components(
    components: dict[str, Decimal | None],
    config: JournalAnalyticsConfig | None = None,
    *,
    weights: dict[str, Decimal] | None = None,
) -> dict[str, Any]:
    cfg = config or JournalAnalyticsConfig()
    available = {key: value for key, value in components.items() if value is not None}
    coverage = Decimal(len(available)) / Decimal(max(1, len(components)))
    normalized_weights = weights or {key: Decimal("1") for key in components}
    contributions = {
        key: (value * normalized_weights.get(key, Decimal("1")) if value is not None else None)
        for key, value in components.items()
    }
    if coverage < cfg.minimum_score_coverage:
        return {
            "status": "insufficient_data", "score": None,
            "coverage": format(coverage, "f"), "components": components,
            "contributions": contributions,
        }
    denominator = sum(
        (normalized_weights.get(key, Decimal("1")) for key in available), Decimal("0")
    )
    score = sum(
        (value * normalized_weights.get(key, Decimal("1")) for key, value in available.items()),
        Decimal("0"),
    ) / denominator
    return {
        "status": "scored", "score": format(score, "f"),
        "coverage": format(coverage, "f"), "components": components,
        "contributions": contributions,
    }


def behaviour_rate(
    occurrences: int, eligible: int, config: JournalAnalyticsConfig | None = None
) -> dict[str, Any]:
    cfg = config or JournalAnalyticsConfig()
    if eligible < cfg.behaviour_minimum_sample or occurrences < cfg.behaviour_minimum_occurrences:
        return {
            "status": "insufficient_sample", "numerator": occurrences,
            "denominator": eligible, "occurrences": occurrences, "eligible": eligible,
            "prevalence": None, "wilson_95": None,
        }
    p = occurrences / eligible
    z = 1.959963984540054
    denominator = 1 + z * z / eligible
    centre = (p + z * z / (2 * eligible)) / denominator
    radius = z * math.sqrt(p * (1 - p) / eligible + z * z / (4 * eligible * eligible)) / denominator
    return {
        "status": "reportable", "numerator": occurrences, "denominator": eligible,
        "occurrences": occurrences, "eligible": eligible, "prevalence": p,
        "wilson_95": [max(0.0, centre - radius), min(1.0, centre + radius)],
    }
