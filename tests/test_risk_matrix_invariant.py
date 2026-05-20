"""Phase 8: shipped risk_matrix.yaml encodes the headline invariants.

The matrix may never grant aggressive exposure on regime level alone — a
stale strong_bull with deteriorating breadth must size smaller than a
healthy bull with positive breadth velocity.
"""

from __future__ import annotations

from pathlib import Path

from ai_trading_system.analytics.regime.profiles import (
    REGIMES,
    VELOCITY_BUCKETS,
    load_risk_matrix,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = REPO_ROOT / "config" / "strategies" / "regime" / "risk_matrix.yaml"


def _load(tmp_path: Path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "active_risk_matrix.yaml").symlink_to(MATRIX_PATH)
    matrix = load_risk_matrix(project_root=tmp_path)
    assert matrix is not None
    return matrix


def test_strong_bull_very_negative_smaller_than_bull_positive(tmp_path: Path) -> None:
    """Headline invariant: regime level alone never unlocks max exposure."""
    matrix = _load(tmp_path)
    stale_top = matrix.lookup("strong_bull", "very_negative").gross_exposure
    healthy_mid = matrix.lookup("bull", "positive").gross_exposure
    assert stale_top < healthy_mid, (
        f"strong_bull×very_negative ({stale_top}) must be smaller than "
        f"bull×positive ({healthy_mid}) — otherwise the matrix degenerates "
        f"to the legacy 1-D level-only policy."
    )


def test_each_regime_row_monotone_in_velocity(tmp_path: Path) -> None:
    matrix = _load(tmp_path)
    for regime in REGIMES:
        exposures = [
            matrix.lookup(regime, bucket).gross_exposure for bucket in VELOCITY_BUCKETS
        ]
        assert exposures == sorted(exposures), (
            f"{regime}: exposure must be non-decreasing across velocity "
            f"buckets very_negative→very_positive, got {exposures}"
        )


def test_each_bucket_column_monotone_in_regime(tmp_path: Path) -> None:
    matrix = _load(tmp_path)
    for bucket in VELOCITY_BUCKETS:
        exposures = [
            matrix.lookup(regime, bucket).gross_exposure for regime in REGIMES
        ]
        assert exposures == sorted(exposures), (
            f"{bucket}: exposure must be non-decreasing across regimes "
            f"risk_off→strong_bull, got {exposures}"
        )


def test_risk_off_row_is_flat(tmp_path: Path) -> None:
    matrix = _load(tmp_path)
    for bucket in VELOCITY_BUCKETS:
        cell = matrix.lookup("risk_off", bucket)
        assert cell.gross_exposure == 0.0
        assert cell.allow_new_buys is False
