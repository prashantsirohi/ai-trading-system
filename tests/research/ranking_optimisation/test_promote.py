"""Tests for the v2 promote module: candidate-config writer + comparison report.

Critical invariant: ``write_candidate_config`` must REFUSE to write to either
production config path. These tests guard that invariant.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.research.ranking_optimisation import (
    WEIGHT_KEYS,
    V2FoldOutcome,
    WalkForwardResultV2,
)
from ai_trading_system.research.ranking_optimisation import promote as promote_module
from ai_trading_system.research.ranking_optimisation.promote import (
    build_comparison_report,
    write_candidate_config,
    write_walkforward_json,
)


def _make_fold(test_year: int, weights: dict[str, float]) -> V2FoldOutcome:
    return V2FoldOutcome(
        test_year=test_year,
        train_years=tuple(range(2020, test_year)),
        train_panel_count=12,
        test_panel_count=4,
        active_factors=("rel_strength_score", "trend_score_score", "above_200dma_score"),
        best_weights={**{k: 0.0 for k in WEIGHT_KEYS}, **weights},
        train_objective_breakdown={"combined": 0.15, "mean_ic": 0.18},
        train_mean_ic=0.18,
        oos_mean_ic=0.21,
        oos_mean_lift=0.22,
        oos_mean_hit=0.27,
        oos_ic_per_panel=(0.18, 0.22, 0.20, 0.24),
    )


def _make_result() -> WalkForwardResultV2:
    return WalkForwardResultV2(
        folds=[
            _make_fold(2023, {"relative_strength": 0.3, "trend_persistence": 0.4, "above_200dma": 0.3}),
            _make_fold(2024, {"relative_strength": 0.2, "trend_persistence": 0.5, "above_200dma": 0.3}),
        ],
    )


# ---------------- write_candidate_config -------------------------------------


def test_write_candidate_config_writes_valid_json(tmp_path: Path):
    target = tmp_path / "candidate.json"
    weights = {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    out = write_candidate_config(weights, target=target)
    assert out == target
    payload = json.loads(target.read_text())
    assert set(payload.keys()) == set(WEIGHT_KEYS)
    assert sum(payload.values()) == pytest.approx(1.0)


def test_write_candidate_config_refuses_production_config_path(tmp_path: Path, monkeypatch):
    """Both production paths are rejected — even when the target uses a different
    spelling that resolves to the same absolute path."""
    fake_prod = tmp_path / "rank_factor_weights.json"
    fake_legacy = tmp_path / "config" / "rank_factor_weights.json"
    fake_legacy.parent.mkdir(parents=True, exist_ok=True)
    fake_prod.write_text('{"existing": "production"}')
    fake_legacy.write_text('{"existing": "legacy"}')

    monkeypatch.setattr(promote_module, "PRODUCTION_CONFIG_PATH", fake_prod)
    monkeypatch.setattr(promote_module, "LEGACY_PRODUCTION_CONFIG_PATH", fake_legacy)

    weights = {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    with pytest.raises(ValueError, match="refusing to write candidate to production"):
        write_candidate_config(weights, target=fake_prod)
    with pytest.raises(ValueError, match="refusing to write candidate to production"):
        write_candidate_config(weights, target=fake_legacy)

    # Production files unchanged.
    assert json.loads(fake_prod.read_text()) == {"existing": "production"}
    assert json.loads(fake_legacy.read_text()) == {"existing": "legacy"}


def test_write_candidate_config_refuses_path_aliases(tmp_path: Path, monkeypatch):
    """Symlink / relative-path aliases that resolve to the production path are rejected."""
    fake_prod = tmp_path / "real_production.json"
    fake_prod.write_text('{"existing": "production"}')
    alias = tmp_path / "alias_to_prod.json"
    alias.symlink_to(fake_prod)

    monkeypatch.setattr(promote_module, "PRODUCTION_CONFIG_PATH", fake_prod)
    monkeypatch.setattr(promote_module, "LEGACY_PRODUCTION_CONFIG_PATH", tmp_path / "noexist.json")

    weights = {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    with pytest.raises(ValueError):
        write_candidate_config(weights, target=alias)
    assert json.loads(fake_prod.read_text()) == {"existing": "production"}


def test_write_candidate_config_creates_parent_dirs(tmp_path: Path):
    target = tmp_path / "nested" / "deeper" / "candidate.json"
    weights = {k: 1.0 / len(WEIGHT_KEYS) for k in WEIGHT_KEYS}
    write_candidate_config(weights, target=target)
    assert target.exists()


def test_write_candidate_config_normalises_missing_keys_to_zero(tmp_path: Path):
    target = tmp_path / "candidate.json"
    weights = {"relative_strength": 0.5, "trend_persistence": 0.5}
    write_candidate_config(weights, target=target)
    payload = json.loads(target.read_text())
    assert set(payload.keys()) == set(WEIGHT_KEYS)
    assert payload["volume_intensity"] == 0.0
    assert payload["above_200dma"] == 0.0


# ---------------- build_comparison_report ------------------------------------


def test_build_comparison_report_renders_all_required_sections(tmp_path: Path):
    result = _make_result()
    target = tmp_path / "comparison.md"
    out = build_comparison_report(
        result,
        target=target,
        production_weights={"relative_strength": 0.38, "trend_persistence": 0.22, "proximity_highs": 0.18},
        candidate_weights={"relative_strength": 0.25, "trend_persistence": 0.45, "above_200dma": 0.30},
    )
    assert out == target
    body = target.read_text()
    assert "# Ranking weights — walk-forward v2 comparison" in body
    assert "Side-by-side weights" in body
    assert "Per-fold OOS" in body
    assert "Weight stability across folds" in body
    assert "Top movers" in body
    assert "above_200dma" in body  # candidate key shown


def test_build_comparison_report_handles_empty_result(tmp_path: Path):
    target = tmp_path / "empty.md"
    out = build_comparison_report(
        WalkForwardResultV2(folds=[]),
        target=target,
        production_weights={k: 0.0 for k in WEIGHT_KEYS},
        candidate_weights={k: 0.0 for k in WEIGHT_KEYS},
    )
    assert out.exists()


# ---------------- write_walkforward_json -------------------------------------


def test_write_walkforward_json_contains_per_fold_and_summary(tmp_path: Path):
    target = tmp_path / "wf.json"
    result = _make_result()
    write_walkforward_json(
        result,
        target=target,
        production_weights={k: 0.0 for k in WEIGHT_KEYS},
        candidate_weights={k: 0.5 if k == "trend_persistence" else 0.0 for k in WEIGHT_KEYS},
    )
    payload = json.loads(target.read_text())
    assert payload["objective_mode"] == "combined"
    assert payload["horizon_days"] == 20
    assert len(payload["folds"]) == 2
    fold0 = payload["folds"][0]
    assert fold0["test_year"] == 2023
    assert "oos_ic_per_panel" in fold0
    assert "summary" in payload
    assert "mean_oos_ic" in payload["summary"]
