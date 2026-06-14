"""Contracts and small pure helpers for sector rotation outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


LEADING = "Leading"
WEAKENING = "Weakening"
LAGGING = "Lagging"
IMPROVING = "Improving"
QUADRANTS = (LEADING, WEAKENING, LAGGING, IMPROVING)

ACCUMULATION_LABEL = "Accumulation"
DISTRIBUTION_LABEL = "Distribution"
NEUTRAL_LABEL = "Neutral"

ROTATION_ARTIFACTS = {
    "sector_rotation": "sector_rotation.csv",
    "stock_rotation": "stock_rotation.csv",
    "accumulation_distribution": "accumulation_distribution.csv",
    "sector_custom_indices": "sector_custom_indices.csv",
    "sector_rotation_payload": "sector_rotation_payload.json",
}


@dataclass(frozen=True)
class SectorRotationResult:
    """All artifacts produced by the sector rotation sidecar."""

    sector_rotation: pd.DataFrame
    stock_rotation: pd.DataFrame
    accumulation_distribution: pd.DataFrame
    sector_custom_indices: pd.DataFrame
    payload: dict[str, Any]
    metadata: dict[str, Any]


def classify_quadrant(rs_ratio: float | int | None, rs_momentum: float | int | None) -> str:
    """Classify an RRG point using the 100/100 JdK-style thresholds."""
    rs = pd.to_numeric(pd.Series([rs_ratio]), errors="coerce").iloc[0]
    momentum = pd.to_numeric(pd.Series([rs_momentum]), errors="coerce").iloc[0]
    if pd.isna(rs) or pd.isna(momentum):
        return LAGGING
    if rs >= 100 and momentum >= 100:
        return LEADING
    if rs >= 100 and momentum < 100:
        return WEAKENING
    if rs < 100 and momentum < 100:
        return LAGGING
    return IMPROVING


def bucket_outperformance(alpha_20d: float | int | None) -> str:
    """Bucket 20-day alpha into the operator-facing labels from the spec."""
    alpha = pd.to_numeric(pd.Series([alpha_20d]), errors="coerce").iloc[0]
    if pd.isna(alpha):
        return "Same as Benchmark"
    if alpha >= 0.10:
        return "Major Outperformance"
    if alpha >= 0.05:
        return "Significant Outperformance"
    if alpha >= 0.02:
        return "Minor Outperformance"
    if alpha > -0.02:
        return "Same as Benchmark"
    if alpha > -0.05:
        return "Minor Underperformance"
    if alpha > -0.10:
        return "Significant Underperformance"
    return "Major Underperformance"


def score_quadrant(quadrant: str) -> float:
    """Map a quadrant to a 0-100 overlay score."""
    return {
        LEADING: 100.0,
        IMPROVING: 75.0,
        WEAKENING: 45.0,
        LAGGING: 20.0,
    }.get(str(quadrant), 20.0)
