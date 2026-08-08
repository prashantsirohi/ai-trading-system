"""Versioned analytical and reconciliation defaults."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class JournalAnalyticsConfig:
    logic_version: str = "journal-analytics-v3"
    cost_absolute_tolerance: Decimal = Decimal("50")
    cost_relative_tolerance: Decimal = Decimal("0.001")
    minimum_score_coverage: Decimal = Decimal("0.80")
    behaviour_minimum_sample: int = 5
    behaviour_minimum_occurrences: int = 3
    max_position_weight: Decimal = Decimal("0.12")
    max_sector_weight: Decimal = Decimal("0.30")
    entry_component_weights: tuple[tuple[str, Decimal], ...] = (
        ("trend", Decimal("1.25")), ("momentum", Decimal("1.00")),
        ("trend_strength", Decimal("0.75")), ("volume", Decimal("1.00")),
        ("delivery", Decimal("0.75")), ("relative_strength", Decimal("1.00")),
        ("stage", Decimal("1.00")), ("pattern", Decimal("1.00")),
        ("sector_regime", Decimal("0.75")),
    )
    exit_component_weights: tuple[tuple[str, Decimal], ...] = (
        ("trend_break", Decimal("1.25")), ("momentum_deterioration", Decimal("1.00")),
        ("volume_confirmation", Decimal("0.75")), ("stage_deterioration", Decimal("1.00")),
        ("pattern_invalidation", Decimal("1.00")), ("regime_risk", Decimal("0.75")),
    )
