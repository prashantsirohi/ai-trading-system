"""Pydantic models for a strategy rule pack.

Phase 1 narrow scope: only fields the compiler can actually route somewhere
today. ``risk`` reuses the existing ``RiskPolicyConfig`` schema verbatim — no
duplicate definitions.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


FACTOR_KEYS: tuple[str, ...] = (
    "relative_strength",
    "volume_intensity",
    "trend_persistence",
    "momentum_acceleration",
    "proximity_highs",
    "delivery_pct",
    "sector_strength",
)


class RankingConfig(BaseModel):
    """Composite-score factor weights. Must sum to 1.0."""

    model_config = ConfigDict(extra="forbid")

    weights: dict[str, float] = Field(
        default_factory=lambda: {
            "relative_strength": 0.38,
            "volume_intensity": 0.0,
            "trend_persistence": 0.22,
            "momentum_acceleration": 0.0,
            "proximity_highs": 0.18,
            "delivery_pct": 0.0,
            "sector_strength": 0.22,
        }
    )

    @field_validator("weights")
    @classmethod
    def _validate_weights(cls, value: dict[str, float]) -> dict[str, float]:
        unknown = set(value) - set(FACTOR_KEYS)
        if unknown:
            raise ValueError(f"unknown ranking factors: {sorted(unknown)}")
        if any(v < 0 for v in value.values()):
            raise ValueError("ranking weights must be non-negative")
        total = sum(value.values())
        if not (0.99 <= total <= 1.01):
            raise ValueError(f"ranking weights must sum to 1.0 (got {total:.4f})")
        return {k: float(value.get(k, 0.0)) for k in FACTOR_KEYS}


class StrategyRulePack(BaseModel):
    """Declarative strategy bundle. Drives the engine via the compiler."""

    model_config = ConfigDict(extra="forbid")

    strategy_id: str
    version: int = 1
    description: str = ""
    ranking: RankingConfig = Field(default_factory=RankingConfig)
    # Risk policy is loaded as a raw dict and converted via the compiler so we
    # don't duplicate the RiskPolicyConfig schema in two places.
    risk: dict[str, Any] = Field(default_factory=dict)
