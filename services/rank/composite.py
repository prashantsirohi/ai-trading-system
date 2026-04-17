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


def compute_rank_confidence(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive rank confidence from readiness, eligibility, and penalties."""
    output = frame.copy()
    if output.empty:
        output["rank_confidence"] = pd.Series(dtype=float)
        return output

    output["rank_confidence"] = 1.0

    if "feature_confidence" in output.columns:
        output["rank_confidence"] *= pd.to_numeric(
            output["feature_confidence"], errors="coerce"
        ).fillna(0.0)

    if "eligible_rank" in output.columns:
        output.loc[~output["eligible_rank"].fillna(False), "rank_confidence"] = 0.0

    if "penalty_score" in output.columns:
        penalties = pd.to_numeric(output["penalty_score"], errors="coerce").fillna(0.0)
        output["rank_confidence"] *= (1.0 - penalties.clip(lower=0.0, upper=50.0) / 100.0)

    output["rank_confidence"] = output["rank_confidence"].clip(lower=0.0, upper=1.0)
    return output


def apply_rank_stability(
    current_frame: pd.DataFrame,
    previous_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Attach optional rank stability metadata without mutating current ordering."""
    output = current_frame.copy()
    output["rank_change_limit"] = pd.NA
    output["previous_rank_position"] = pd.NA
    output["rank_delta"] = pd.NA
    output["score_delta"] = pd.NA

    if previous_frame is None or previous_frame.empty or output.empty:
        return output
    if "symbol_id" not in output.columns or "symbol_id" not in previous_frame.columns:
        return output

    prev = previous_frame.copy()
    prev = prev.reset_index(drop=True)
    prev["previous_rank_position"] = prev.index + 1
    prev_cols = ["symbol_id", "previous_rank_position"]
    if "exchange" in prev.columns and "exchange" in output.columns:
        prev_cols.insert(1, "exchange")
    if "composite_score" in prev.columns and "composite_score" in output.columns:
        prev = prev.rename(columns={"composite_score": "previous_composite_score"})
        prev_cols.append("previous_composite_score")

    merged = output.merge(prev[prev_cols], on=[c for c in ["symbol_id", "exchange"] if c in prev_cols and c in output.columns], how="left")
    merged = merged.reset_index(drop=True)
    merged["current_rank_position"] = merged.index + 1
    merged["rank_delta"] = merged["previous_rank_position"] - merged["current_rank_position"]
    if "previous_composite_score" in merged.columns and "composite_score" in merged.columns:
        merged["score_delta"] = merged["composite_score"] - merged["previous_composite_score"]
    merged["rank_change_limit"] = pd.NA
    return merged


def select_rank_output_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Project rank output into the backward-compatible artifact contract."""
    available = [column for column in RANKED_SIGNAL_COLUMNS if column in frame.columns]
    return frame[available].reset_index(drop=True)
