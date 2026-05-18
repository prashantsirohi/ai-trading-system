"""Candidate-config writer and side-by-side comparison report.

The writer enforces a hard "never overwrite production config" invariant: if
the target resolves to either production path, ``ValueError`` is raised.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from ai_trading_system.domains.ranking.composite import (
    LEGACY_RANK_FACTOR_WEIGHTS_PATH as _LEGACY_PRODUCTION_CONFIG_PATH,
    RANK_FACTOR_WEIGHTS_PATH as _PRODUCTION_CONFIG_PATH,
    load_factor_weights,
)
from ai_trading_system.research.ranking_optimisation.data_v2 import (
    SCORE_TO_WEIGHT_KEY,
    WEIGHT_KEYS,
)
from ai_trading_system.research.ranking_optimisation.runner_v2 import (
    WalkForwardResultV2,
)


PRODUCTION_CONFIG_PATH = _PRODUCTION_CONFIG_PATH
LEGACY_PRODUCTION_CONFIG_PATH = _LEGACY_PRODUCTION_CONFIG_PATH
DEFAULT_CANDIDATE_CONFIG_PATH = Path("config/rank_factor_weights.candidate.json")
DEFAULT_COMPARISON_REPORT_PATH = Path("reports/ranking_optimisation/walkforward_live_v2_comparison.md")
DEFAULT_WALKFORWARD_JSON_PATH  = Path("reports/ranking_optimisation/walkforward_live_v2.json")


def _resolved_production_paths() -> set[Path]:
    paths = set()
    for raw in (PRODUCTION_CONFIG_PATH, LEGACY_PRODUCTION_CONFIG_PATH):
        try:
            paths.add(Path(raw).resolve())
        except (FileNotFoundError, OSError):
            paths.add(Path(raw).absolute())
    return paths


def write_candidate_config(
    weights: Mapping[str, float],
    target: Path | str = DEFAULT_CANDIDATE_CONFIG_PATH,
) -> Path:
    """Write candidate weights to ``target``. Refuses to write to production paths."""
    target_path = Path(target)
    try:
        resolved = target_path.resolve()
    except (FileNotFoundError, OSError):
        resolved = target_path.absolute()
    production_paths = _resolved_production_paths()
    if resolved in production_paths:
        raise ValueError(
            f"refusing to write candidate to production config path: {resolved}. "
            f"Candidate configs must be written to a distinct path "
            f"(e.g. config/rank_factor_weights.candidate.json)."
        )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {k: float(weights.get(k, 0.0)) for k in WEIGHT_KEYS}
    target_path.write_text(json.dumps(payload, indent=2))
    return target_path


def load_production_weights() -> dict[str, float]:
    """Read whichever production config is active (same logic the live ranker uses)."""
    return dict(load_factor_weights())


def _mean_weights_across_folds(result: WalkForwardResultV2) -> dict[str, float]:
    if not result.folds:
        return {k: 0.0 for k in WEIGHT_KEYS}
    df = pd.DataFrame([f.best_weights for f in result.folds])
    means = df.reindex(columns=list(WEIGHT_KEYS)).fillna(0.0).mean(axis=0)
    # Re-normalise the mean to sum to 1.0 (means may drift from exact 1.0 due to
    # per-fold rounding when degenerate factors zero out a different subset).
    total = float(means.sum())
    if total <= 0:
        return {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    return {k: float(v / total) for k, v in means.items()}


def _weight_stability(result: WalkForwardResultV2) -> pd.DataFrame:
    df = pd.DataFrame([f.best_weights for f in result.folds])
    df = df.reindex(columns=list(WEIGHT_KEYS)).fillna(0.0)
    stats = pd.DataFrame(
        {
            "mean": df.mean(axis=0),
            "std":  df.std(axis=0, ddof=0),
            "min":  df.min(axis=0),
            "max":  df.max(axis=0),
        }
    )
    return stats


def build_comparison_report(
    result: WalkForwardResultV2,
    *,
    target: Path | str = DEFAULT_COMPARISON_REPORT_PATH,
    production_weights: Mapping[str, float] | None = None,
    candidate_weights: Mapping[str, float] | None = None,
) -> Path:
    """Render the side-by-side markdown comparison report."""
    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    prod = dict(production_weights) if production_weights is not None else load_production_weights()
    cand = (
        dict(candidate_weights) if candidate_weights is not None else _mean_weights_across_folds(result)
    )

    lines: list[str] = []
    lines.append("# Ranking weights — walk-forward v2 comparison\n")
    lines.append(
        f"- Objective mode: `{result.objective_mode}`\n"
        f"- Horizon: {result.horizon_days} trading days\n"
        f"- Top-N: {result.top_n}\n"
    )

    lines.append("\n## Side-by-side weights (production → candidate)\n")
    lines.append("| factor | production | candidate | Δ |")
    lines.append("|---|---:|---:|---:|")
    for key in WEIGHT_KEYS:
        p = float(prod.get(key, 0.0))
        c = float(cand.get(key, 0.0))
        lines.append(f"| `{key}` | {p:.3f} | {c:.3f} | {c - p:+.3f} |")

    lines.append("\n## Per-fold OOS\n")
    lines.append("| test year | train years | n_train | n_test | active | OOS IC | OOS lift | OOS hit |")
    lines.append("|---:|:---|---:|---:|---:|---:|---:|---:|")
    for f in result.folds:
        active_count = f"{len(f.active_factors)}/8"
        lines.append(
            f"| {f.test_year} | {','.join(str(y) for y in f.train_years)} "
            f"| {f.train_panel_count} | {f.test_panel_count} | {active_count} "
            f"| {f.oos_mean_ic:+.3f} | {f.oos_mean_lift:+.1%} | {f.oos_mean_hit:.0%} |"
        )

    lines.append("\n## Weight stability across folds\n")
    lines.append("| factor | mean | std | min | max |")
    lines.append("|---|---:|---:|---:|---:|")
    stats = _weight_stability(result)
    for key, row in stats.iterrows():
        lines.append(
            f"| `{key}` | {row['mean']:.3f} | {row['std']:.3f} | {row['min']:.3f} | {row['max']:.3f} |"
        )

    lines.append("\n## Top movers\n")
    deltas = sorted(
        ((k, float(cand.get(k, 0.0)) - float(prod.get(k, 0.0))) for k in WEIGHT_KEYS),
        key=lambda kv: kv[1],
        reverse=True,
    )
    risers = [kv for kv in deltas if kv[1] > 0][:3]
    fallers = sorted(
        [kv for kv in deltas if kv[1] < 0], key=lambda kv: kv[1]
    )[:3]
    lines.append("\n**Top 3 risers**\n")
    for k, d in risers:
        lines.append(f"- `{k}`: {d:+.3f}")
    lines.append("\n**Top 3 fallers**\n")
    for k, d in fallers:
        lines.append(f"- `{k}`: {d:+.3f}")

    target_path.write_text("\n".join(lines) + "\n")
    return target_path


def write_walkforward_json(
    result: WalkForwardResultV2,
    *,
    target: Path | str = DEFAULT_WALKFORWARD_JSON_PATH,
    production_weights: Mapping[str, float] | None = None,
    candidate_weights: Mapping[str, float] | None = None,
) -> Path:
    """Persist the walk-forward result as JSON for downstream tooling."""
    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    cand = (
        dict(candidate_weights) if candidate_weights is not None else _mean_weights_across_folds(result)
    )
    prod = dict(production_weights) if production_weights is not None else load_production_weights()
    payload = {
        "objective_mode": result.objective_mode,
        "horizon_days": result.horizon_days,
        "top_n": result.top_n,
        "candidate_weights": cand,
        "production_weights": {k: float(prod.get(k, 0.0)) for k in WEIGHT_KEYS},
        "folds": [
            {
                "test_year": f.test_year,
                "train_years": list(f.train_years),
                "train_panel_count": f.train_panel_count,
                "test_panel_count": f.test_panel_count,
                "active_factors": list(f.active_factors),
                "best_weights": f.best_weights,
                "train_mean_ic": f.train_mean_ic,
                "oos_mean_ic": f.oos_mean_ic,
                "oos_mean_lift": f.oos_mean_lift,
                "oos_mean_hit": f.oos_mean_hit,
                "oos_ic_per_panel": list(f.oos_ic_per_panel),
            }
            for f in result.folds
        ],
        "summary": {
            "mean_oos_ic":   float(np.nanmean([f.oos_mean_ic for f in result.folds])) if result.folds else float("nan"),
            "mean_oos_lift": float(np.nanmean([f.oos_mean_lift for f in result.folds])) if result.folds else float("nan"),
            "mean_oos_hit":  float(np.nanmean([f.oos_mean_hit for f in result.folds])) if result.folds else float("nan"),
        },
    }
    target_path.write_text(json.dumps(payload, indent=2, default=float))
    return target_path
