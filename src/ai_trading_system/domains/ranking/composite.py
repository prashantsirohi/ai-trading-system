"""Composite scoring helpers for rank outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pandas as pd
import ai_trading_system.platform.config as platform_config

from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.domains.ranking.contracts import (
    DEFAULT_FACTOR_WEIGHTS,
    PRIMARY_FACTORS,
    RANKED_SIGNAL_COLUMNS,
)


_PLATFORM_CONFIG_DIR = Path(platform_config.__file__).resolve().parent
_LEGACY_CONFIG_DIR = Path(__file__).resolve().parents[4] / "config"
RANK_FACTOR_WEIGHTS_PATH = _PLATFORM_CONFIG_DIR / "rank_factor_weights.json"
LEGACY_RANK_FACTOR_WEIGHTS_PATH = _LEGACY_CONFIG_DIR / "rank_factor_weights.json"
_SECTOR_DEMEAN_FACTORS = frozenset({"rel_strength", "volume_intensity_normalized", "trend_score"})


def load_factor_weights(config_path: Path | None = None) -> dict[str, float]:
    """Load rank factor weights using configured defaults when needed."""
    weights = dict(DEFAULT_FACTOR_WEIGHTS)
    path = config_path or (RANK_FACTOR_WEIGHTS_PATH if RANK_FACTOR_WEIGHTS_PATH.exists() else LEGACY_RANK_FACTOR_WEIGHTS_PATH)
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


def winsorize_series(
    values: pd.Series,
    *,
    lower_quantile: float = 0.05,
    upper_quantile: float = 0.95,
) -> pd.Series:
    """Clip outliers so a few extreme rows do not dominate percentile scoring."""
    numeric = pd.to_numeric(values, errors="coerce")
    non_null = numeric.dropna()
    if non_null.empty:
        return numeric

    lower = non_null.quantile(lower_quantile)
    upper = non_null.quantile(upper_quantile)
    if pd.isna(lower) or pd.isna(upper):
        return numeric
    return numeric.clip(lower=lower, upper=upper)


def demean_by_sector(values: pd.Series, sector_names: pd.Series | None) -> pd.Series:
    """Subtract sector medians so sector baselines do not overwhelm stock selection."""
    numeric = pd.to_numeric(values, errors="coerce")
    if sector_names is None:
        return numeric

    sectors = sector_names.where(sector_names.notna()).astype("object")
    sector_medians = numeric.groupby(sectors).transform("median")
    return numeric - sector_medians.fillna(0.0)


def normalize_raw_factor_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    """Prepare raw factors for percentile scoring without changing the output schema."""
    normalized = frame.copy()
    sector_names = normalized["sector_name"] if "sector_name" in normalized.columns else None

    if "volume_intensity_normalized" not in normalized.columns and "vol_intensity" in normalized.columns:
        normalized.loc[:, "volume_intensity_normalized"] = normalized["vol_intensity"]

    for factor in PRIMARY_FACTORS:
        if factor.raw_column not in normalized.columns:
            normalized.loc[:, factor.raw_column] = 0.0
        raw_values = winsorize_series(normalized[factor.raw_column])
        if factor.raw_column in _SECTOR_DEMEAN_FACTORS:
            raw_values = demean_by_sector(raw_values, sector_names)
        normalized.loc[:, factor.raw_column] = raw_values

    return normalized


def compute_factor_scores(
    frame: pd.DataFrame,
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    """Normalize factor inputs and compute the composite ranking score."""
    scores = normalize_raw_factor_inputs(frame)

    for factor in PRIMARY_FACTORS:
        rank_method = "average" if factor.raw_column == "delivery_pct" else "average"
        scores.loc[:, factor.score_column] = scores[factor.raw_column].rank(
            pct=True,
            method=rank_method,
        ) * 100

    scores.loc[:, "sector_rs_score"] = scores["sector_rs_value"].rank(pct=True) * 100
    scores.loc[:, "stock_vs_sector_score"] = scores["stock_vs_sector_value"].rank(pct=True) * 100
    scores.loc[:, "sector_strength_score"] = (
        scores["sector_rs_score"] * 0.6 + scores["stock_vs_sector_score"] * 0.4
    )

    scores.loc[:, "composite_score"] = sum(
        scores[factor.score_column] * float(weights[factor.weight_key])
        for factor in PRIMARY_FACTORS
    ) + scores["sector_strength_score"] * float(weights["sector_strength"])

    if "sector_name" in scores.columns:
        scores.loc[:, "sector_rank_within_sector"] = scores.groupby("sector_name")["composite_score"].rank(
            ascending=False, method="min"
        )
        scores.loc[:, "sector_total_symbols"] = scores.groupby("sector_name")["sector_name"].transform("count")

    return scores


def filter_ranked_scores(
    frame: pd.DataFrame,
    *,
    min_score: float,
    top_n: int | None,
) -> pd.DataFrame:
    """Apply output ordering and score cutoffs while preserving existing semantics."""
    score_column = "composite_score_adjusted" if "composite_score_adjusted" in frame.columns else "composite_score"
    ranked = frame.sort_values(score_column, ascending=False)
    ranked = ranked[pd.to_numeric(ranked[score_column], errors="coerce").fillna(0.0) >= min_score]
    if top_n:
        ranked = ranked.head(top_n)
    return ranked


def compute_rank_confidence(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive rank confidence from readiness, eligibility, and penalties."""
    output = frame.copy()
    if output.empty:
        output["rank_confidence"] = pd.Series(dtype=float)
        return output

    output.loc[:, "rank_confidence"] = 1.0

    if "feature_confidence" in output.columns:
        output.loc[:, "rank_confidence"] = output["rank_confidence"] * pd.to_numeric(
            output["feature_confidence"], errors="coerce"
        ).fillna(0.0)

    if "eligible_rank" in output.columns:
        output.loc[~output["eligible_rank"].fillna(False), "rank_confidence"] = 0.0

    if "penalty_score" in output.columns:
        penalties = pd.to_numeric(output["penalty_score"], errors="coerce").fillna(0.0)
        output.loc[:, "rank_confidence"] = output["rank_confidence"] * (
            1.0 - penalties.clip(lower=0.0, upper=50.0) / 100.0
        )

    output.loc[:, "rank_confidence"] = output["rank_confidence"].clip(lower=0.0, upper=1.0)
    return output


def apply_rank_stability(
    current_frame: pd.DataFrame,
    previous_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Attach optional rank stability metadata without mutating current ordering."""
    output = current_frame.copy()
    output.loc[:, "rank_change_limit"] = pd.NA
    output.loc[:, "previous_rank_position"] = pd.NA
    output.loc[:, "rank_delta"] = pd.NA
    output.loc[:, "score_delta"] = pd.NA

    if previous_frame is None or previous_frame.empty or output.empty:
        return output
    if "symbol_id" not in output.columns or "symbol_id" not in previous_frame.columns:
        return output

    prev = previous_frame.copy()
    if "composite_score" in prev.columns:
        prev = prev.sort_values("composite_score", ascending=False, kind="stable")
    prev = prev.reset_index(drop=True)
    prev.loc[:, "previous_rank_position"] = prev.index + 1
    prev_cols = ["symbol_id", "previous_rank_position"]
    if "exchange" in prev.columns and "exchange" in output.columns:
        prev_cols.insert(1, "exchange")
    if "composite_score" in prev.columns and "composite_score" in output.columns:
        prev = prev.rename(columns={"composite_score": "previous_composite_score"})
        prev_cols.append("previous_composite_score")

    merge_base = output.drop(columns=["previous_rank_position", "rank_delta", "score_delta"], errors="ignore")
    merged = merge_base.merge(
        prev[prev_cols],
        on=[c for c in ["symbol_id", "exchange"] if c in prev_cols and c in output.columns],
        how="left",
    )
    merged = merged.reset_index(drop=True)
    merged.loc[:, "current_rank_position"] = merged.index + 1
    merged.loc[:, "rank_delta"] = merged["previous_rank_position"] - merged["current_rank_position"]
    if "previous_composite_score" in merged.columns and "composite_score" in merged.columns:
        merged.loc[:, "score_delta"] = merged["composite_score"] - merged["previous_composite_score"]
    merged.loc[:, "rank_change_limit"] = pd.NA
    return merged


def select_rank_output_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Project rank output into the backward-compatible artifact contract."""
    available = [column for column in RANKED_SIGNAL_COLUMNS if column in frame.columns]
    return frame[available].reset_index(drop=True)


FACTOR_COLUMNS = [
    "rel_strength_score",
    "vol_intensity_score",
    "trend_score_score",
    "momentum_acceleration_score",
    "prox_high_score",
    "delivery_pct_score",
]


def compute_factor_turnover(
    current_frame: pd.DataFrame,
    previous_frame: pd.DataFrame | None = None,
) -> dict:
    """Compute factor turnover metrics comparing current vs previous rank.

    Returns dict with:
    - turnover_pct: percentage of symbols that changed rank position
    - symbols_changed: count of symbols with rank position change
    - symbols_unchanged: count of symbols with same rank position
    - total_symbols: total symbols in comparison
    """
    if previous_frame is None or previous_frame.empty or current_frame.empty:
        return {
            "turnover_pct": 0.0,
            "symbols_changed": 0,
            "symbols_unchanged": 0,
            "total_symbols": 0,
            "previous_frame_available": False,
        }

    prev = previous_frame.copy().reset_index(drop=True)
    curr = current_frame.copy().reset_index(drop=True)

    if "eligible_rank" not in prev.columns or "eligible_rank" not in curr.columns:
        if "composite_score" in prev.columns and "composite_score" in curr.columns:
            prev["rank_by_score"] = prev["composite_score"].rank(ascending=False, method="min")
            curr["rank_by_score"] = curr["composite_score"].rank(ascending=False, method="min")
            compare_col = "rank_by_score"
        else:
            return {
                "turnover_pct": 0.0,
                "symbols_changed": 0,
                "symbols_unchanged": 0,
                "total_symbols": 0,
                "previous_frame_available": True,
            }
    else:
        compare_col = "eligible_rank"

    merge_keys = ["symbol_id"]
    if "exchange" in prev.columns and "exchange" in curr.columns:
        merge_keys.append("exchange")

    prev_cols_to_select = merge_keys + [compare_col]
    merged = curr.merge(
        prev[prev_cols_to_select],
        on=merge_keys,
        how="inner",
        suffixes=("", "_prev"),
    )

    prev_col = f"{compare_col}_prev"
    changed = (merged[compare_col] != merged[prev_col]).sum() if prev_col in merged.columns else 0
    total = len(merged)
    unchanged = total - changed

    return {
        "turnover_pct": round((changed / total * 100) if total > 0 else 0.0, 2),
        "symbols_changed": int(changed),
        "symbols_unchanged": int(unchanged),
        "total_symbols": int(total),
        "previous_frame_available": True,
    }


def compute_factor_correlations(frame: pd.DataFrame) -> dict:
    """Compute correlation matrix for primary factor scores.

    Returns dict with:
    - correlation_matrix: DataFrame of pairwise correlations
    - violations: list of (factor1, factor2, correlation) where |corr| > 0.70
    - has_violations: bool indicating if any threshold exceeded
    """
    available_factors = [f for f in FACTOR_COLUMNS if f in frame.columns]

    if len(available_factors) < 2:
        return {
            "correlation_matrix": pd.DataFrame(),
            "violations": [],
            "has_violations": False,
        }

    corr_matrix = frame[available_factors].corr()

    violations = []
    for i, f1 in enumerate(available_factors):
        for f2 in available_factors[i + 1:]:
            corr_val = corr_matrix.loc[f1, f2]
            if abs(corr_val) > 0.70:
                violations.append({
                    "factor_1": f1,
                    "factor_2": f2,
                    "correlation": round(float(corr_val), 4),
                })

    corr_dict = corr_matrix.to_dict()
    return {
        "correlation_matrix": corr_dict if isinstance(corr_dict, dict) else {},
        "violations": violations,
        "has_violations": len(violations) > 0,
    }
