"""Composite scoring helpers for rank outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pandas as pd

from core.logging import logger
from services.rank.contracts import (
    DEFAULT_FACTOR_WEIGHTS,
    PRIMARY_FACTORS,
    RANKED_SIGNAL_COLUMNS,
)


RANK_FACTOR_WEIGHTS_PATH = Path(__file__).resolve().parents[2] / "config" / "rank_factor_weights.json"


def load_factor_weights(config_path: Path | None = None) -> dict[str, float]:
    """Load rank factor weights from config, falling back to defaults."""
    weights = dict(DEFAULT_FACTOR_WEIGHTS)
    path = config_path or RANK_FACTOR_WEIGHTS_PATH
    if not path.exists():
        return weights

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not load rank factor weights from %s: %s", path, exc)
        return weights

    if not isinstance(payload, dict):
        logger.warning("Rank factor weight config at %s is not an object; using defaults", path)
        return weights

    for key in DEFAULT_FACTOR_WEIGHTS:
        if key in payload:
            try:
                weights[key] = float(payload[key])
            except (TypeError, ValueError):
                logger.warning("Ignoring invalid rank factor weight for %s in %s", key, path)
    return weights


def compute_factor_scores(
    frame: pd.DataFrame,
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    """Normalize factor inputs and compute the composite ranking score."""
    scores = frame.copy()

    for factor in PRIMARY_FACTORS:
        scores[factor.score_column] = scores[factor.raw_column].rank(pct=True) * 100

    scores["sector_rs_score"] = scores["sector_rs_value"].rank(pct=True) * 100
    scores["stock_vs_sector_score"] = scores["stock_vs_sector_value"].rank(pct=True) * 100
    scores["sector_strength_score"] = (
        scores["sector_rs_score"] * 0.6 + scores["stock_vs_sector_score"] * 0.4
    )

    scores["composite_score"] = sum(
        scores[factor.score_column] * float(weights[factor.weight_key])
        for factor in PRIMARY_FACTORS
    ) + scores["sector_strength_score"] * float(weights["sector_strength"])
    return scores


def filter_ranked_scores(
    frame: pd.DataFrame,
    *,
    min_score: float,
    top_n: int | None,
) -> pd.DataFrame:
    """Apply output ordering and score cutoffs while preserving existing semantics."""
    ranked = frame.sort_values("composite_score", ascending=False)
    ranked = ranked[ranked["composite_score"] >= min_score]
    if top_n:
        ranked = ranked.head(top_n)
    return ranked


def select_rank_output_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Project rank output into the backward-compatible artifact contract."""
    available = [column for column in RANKED_SIGNAL_COLUMNS if column in frame.columns]
    return frame[available].reset_index(drop=True)
