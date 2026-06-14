"""Sector rotation analytics for rank-stage sidecar artifacts."""

from ai_trading_system.analytics.sector_rotation.compute import compute_sector_rotation
from ai_trading_system.analytics.sector_rotation.contracts import (
    ACCUMULATION_LABEL,
    DISTRIBUTION_LABEL,
    NEUTRAL_LABEL,
    ROTATION_ARTIFACTS,
    SectorRotationResult,
    bucket_outperformance,
    classify_quadrant,
)

__all__ = [
    "ACCUMULATION_LABEL",
    "DISTRIBUTION_LABEL",
    "NEUTRAL_LABEL",
    "ROTATION_ARTIFACTS",
    "SectorRotationResult",
    "bucket_outperformance",
    "classify_quadrant",
    "compute_sector_rotation",
]
