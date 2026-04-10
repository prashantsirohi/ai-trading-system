from __future__ import annotations

import pandas as pd

from analytics.shadow_monitor import build_shadow_overlay, compute_matured_outcomes


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
