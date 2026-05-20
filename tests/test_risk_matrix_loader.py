"""Phase 8: risk matrix YAML loader + 25-cell completeness."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ai_trading_system.analytics.regime.profiles import (
    REGIMES,
    VELOCITY_BUCKETS,
    BreadthImpulseRiskMatrix,
    RiskCell,
    load_risk_matrix,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = REPO_ROOT / "config" / "strategies" / "regime" / "risk_matrix.yaml"


def test_shipped_matrix_loads_and_has_all_25_cells(tmp_path: Path) -> None:
    # Symlink shipped matrix into a tmp project so we don't depend on the
    # repo having `config/active_risk_matrix.yaml` symlinked in CI.
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "active_risk_matrix.yaml").symlink_to(MATRIX_PATH)
    matrix = load_risk_matrix(project_root=tmp_path)
    assert isinstance(matrix, BreadthImpulseRiskMatrix)
    assert matrix.name == "risk_matrix_v1"
    for regime in REGIMES:
        for bucket in VELOCITY_BUCKETS:
            cell = matrix.lookup(regime, bucket)
            assert isinstance(cell, RiskCell)
            assert cell.regime == regime
            assert cell.velocity_bucket == bucket


def test_load_returns_none_when_symlink_missing(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    assert load_risk_matrix(project_root=tmp_path) is None


def test_missing_cell_raises_at_load_time(tmp_path: Path) -> None:
    payload = yaml.safe_load(MATRIX_PATH.read_text(encoding="utf-8"))
    # Drop one cell.
    del payload["bull"]["positive"]
    bad = tmp_path / "config" / "strategies" / "regime"
    bad.mkdir(parents=True)
    bad_path = bad / "risk_matrix.yaml"
    bad_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    (tmp_path / "config" / "active_risk_matrix.yaml").symlink_to(bad_path)
    with pytest.raises(ValueError, match="missing cell 'bull.positive'"):
        load_risk_matrix(project_root=tmp_path)


def test_missing_regime_block_raises(tmp_path: Path) -> None:
    payload = yaml.safe_load(MATRIX_PATH.read_text(encoding="utf-8"))
    del payload["strong_bull"]
    bad = tmp_path / "config" / "strategies" / "regime"
    bad.mkdir(parents=True)
    bad_path = bad / "risk_matrix.yaml"
    bad_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    (tmp_path / "config" / "active_risk_matrix.yaml").symlink_to(bad_path)
    with pytest.raises(ValueError, match="missing regime block 'strong_bull'"):
        load_risk_matrix(project_root=tmp_path)
