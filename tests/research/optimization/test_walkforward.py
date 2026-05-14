"""Walk-forward fold generation."""

from __future__ import annotations

from datetime import date

from ai_trading_system.research.optimization.walkforward import build_folds


def test_basic_12_3_3_fold_schedule():
    folds = build_folds(date(2021, 1, 1), date(2024, 12, 31))
    assert len(folds) >= 1
    f0 = folds[0]
    assert f0.train_start == date(2021, 1, 1)
    assert f0.train_end == date(2021, 12, 31)
    assert f0.val_start == date(2022, 1, 1)
    assert f0.val_end == date(2022, 3, 31)
    # Step is 3 months.
    f1 = folds[1]
    assert f1.train_start == date(2021, 4, 1)


def test_no_fold_when_validation_overruns_end():
    folds = build_folds(date(2024, 1, 1), date(2024, 6, 30))
    # 12 month train doesn't fit before validation in 6 month window.
    assert folds == []


def test_fold_indices_are_contiguous():
    folds = build_folds(date(2021, 1, 1), date(2023, 12, 31))
    for i, f in enumerate(folds):
        assert f.index == i


def test_validation_window_inside_bounds():
    folds = build_folds(date(2021, 1, 1), date(2024, 6, 30))
    for f in folds:
        assert f.val_end <= date(2024, 6, 30)
        assert f.train_start >= date(2021, 1, 1)
        assert f.val_start > f.train_end
