from __future__ import annotations

import pandas as pd
import pytest

from ai_trading_system.analytics.shadow_monitor import build_shadow_overlay, compute_matured_outcomes
from ai_trading_system.research.shadow_monitor import compute_rolling_spearman_ic, compute_spearman_ic


class _DummyModel:
    def __init__(self, feature_names):
        self.feature_names_ = feature_names

    def predict_proba(self, X):
        values = X["signal"].to_numpy()
        return pd.DataFrame({"neg": 1.0 - values, "pos": values}).to_numpy()


class _DummyScorer:
    def score_frame(self, dataset_df, *, model, horizon: int):
        scored = dataset_df.copy()
        scored["probability"] = model.predict_proba(scored[model.feature_names_])[:, 1]
        scored["prediction"] = (scored["probability"] >= 0.5).astype(int)
        scored["horizon"] = horizon
        return scored


def test_build_shadow_overlay_adds_ranks_and_top_deciles():
    current_df = pd.DataFrame(
        {
            "symbol_id": [f"SYM{i}" for i in range(10)],
            "exchange": ["NSE"] * 10,
            "timestamp": pd.to_datetime(["2026-03-31"] * 10),
            "close": [100 + i for i in range(10)],
            "technical_score": [90 - i for i in range(10)],
            "signal": [0.9 - (i * 0.05) for i in range(10)],
        }
    )
    overlay = build_shadow_overlay(
        current_df,
        scorer=_DummyScorer(),
        model_5d=_DummyModel(["signal"]),
        model_20d=_DummyModel(["signal"]),
    )

    assert overlay.loc[0, "technical_rank"] == 1
    assert overlay.loc[0, "ml_5d_rank"] == 1
    assert int(overlay["technical_top_decile"].sum()) == 1
    assert int(overlay["ml_20d_top_decile"].sum()) == 1
    assert "blend_20d_score" in overlay.columns


def test_compute_matured_outcomes_uses_forward_bars():
    price_history = pd.DataFrame(
        {
            "symbol_id": ["AAA"] * 7,
            "exchange": ["NSE"] * 7,
            "trade_date": pd.date_range("2026-03-01", periods=7, freq="D"),
            "close": [100, 101, 102, 103, 104, 105, 106],
        }
    )
    predictions = [
        {
            "prediction_id": "pred-1",
            "prediction_date": "2026-03-01",
            "symbol_id": "AAA",
            "exchange": "NSE",
        }
    ]

    outcomes = compute_matured_outcomes(price_history, predictions, horizon=5)

    assert len(outcomes) == 1
    assert outcomes[0]["future_date"] == "2026-03-06"
    assert round(outcomes[0]["realized_return"], 4) == 0.05
    assert outcomes[0]["hit"] is True


def test_compute_spearman_ic_returns_value_with_enough_observations():
    frame = pd.DataFrame(
        {
            "probability": [0.9, 0.8, 0.4, 0.2, 0.1],
            "realized_return": [0.12, 0.08, 0.03, -0.01, -0.04],
        }
    )

    ic_value = compute_spearman_ic(frame, min_observations=5)

    assert ic_value == pytest.approx(1.0)


def test_compute_spearman_ic_returns_nan_when_observations_are_insufficient():
    frame = pd.DataFrame(
        {
            "probability": [0.9, 0.8, 0.4],
            "realized_return": [0.12, 0.08, 0.03],
        }
    )

    ic_value = compute_spearman_ic(frame, min_observations=5)

    assert pd.isna(ic_value)


def test_compute_rolling_spearman_ic_returns_expected_schema():
    frame = pd.DataFrame(
        {
            "prediction_date": [
                "2026-04-01", "2026-04-01", "2026-04-01", "2026-04-01", "2026-04-01",
                "2026-04-02", "2026-04-02", "2026-04-02", "2026-04-02", "2026-04-02",
                "2026-04-03", "2026-04-03", "2026-04-03", "2026-04-03", "2026-04-03",
            ],
            "horizon": [5] * 15,
            "probability": [
                0.9, 0.8, 0.6, 0.3, 0.1,
                0.85, 0.7, 0.55, 0.25, 0.05,
                0.88, 0.75, 0.5, 0.2, 0.1,
            ],
            "realized_return": [
                0.10, 0.06, 0.04, -0.01, -0.03,
                0.11, 0.05, 0.02, -0.02, -0.05,
                0.09, 0.07, 0.01, -0.03, -0.04,
            ],
        }
    )

    rolling = compute_rolling_spearman_ic(frame, window=2, min_observations=5)

    assert rolling.columns.tolist() == [
        "prediction_date",
        "horizon",
        "observations",
        "ic_spearman",
        "rolling_ic_spearman",
        "window",
    ]
    assert len(rolling) == 3
    assert pd.isna(rolling.iloc[0]["rolling_ic_spearman"])
    assert rolling.iloc[1]["window"] == 2
