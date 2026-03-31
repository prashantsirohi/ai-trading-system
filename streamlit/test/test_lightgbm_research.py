from __future__ import annotations

import pandas as pd

from analytics.lightgbm_research import add_technical_baseline_scores, walk_forward_compare


class _DummyModel:
    def predict_proba(self, X):
        return X[["signal"]].to_numpy().repeat(2, axis=1)[:, ::-1]


class _DummyEngine:
    def train(self, train_df, horizon=5, **kwargs):
        return _DummyModel(), {}

    def evaluate_frame(self, dataset_df, *, model, horizon: int):
        return {"validation_auc": 0.61}

    def score_frame(self, dataset_df, *, model, horizon: int):
        scored = dataset_df.copy()
        scored["probability"] = scored["signal"]
        scored["prediction"] = (scored["probability"] >= 0.5).astype(int)
        scored["horizon"] = horizon
        return scored


def test_add_technical_baseline_scores_creates_score_column():
    df = pd.DataFrame(
        {
            "rel_strength_pct": [90.0],
            "vol_intensity_pct": [70.0],
            "trend_score_pct": [60.0],
            "prox_high_pct": [80.0],
            "delivery_pct_pct": [50.0],
            "sector_rs_pct": [75.0],
            "stock_vs_sector_pct": [65.0],
        }
    )
    result = add_technical_baseline_scores(df)
    assert "technical_score" in result.columns
    assert result.loc[0, "technical_score"] > 0


def test_walk_forward_compare_returns_fold_summary():
    rows = []
    for year in range(2015, 2022):
        for idx in range(10):
            ts = pd.Timestamp(f"{year}-01-0{(idx % 9) + 1}")
            rows.append(
                {
                    "symbol_id": f"SYM{idx}",
                    "timestamp": ts,
                    "target_5d": 1 if idx < 3 else 0,
                    "return_5d": 0.05 if idx < 3 else -0.01,
                    "signal": 0.9 if idx < 3 else 0.1,
                    "rel_strength_pct": 90 - idx,
                    "vol_intensity_pct": 50 + idx,
                    "trend_score_pct": 55 + idx,
                    "prox_high_pct": 60 + idx,
                    "delivery_pct_pct": 45 + idx,
                    "sector_rs_pct": 50 + idx,
                    "stock_vs_sector_pct": 48 + idx,
                }
            )

    df = pd.DataFrame(rows)
    result = walk_forward_compare(
        df,
        engine=_DummyEngine(),
        horizon=5,
        min_train_years=5,
    )
    assert result["summary"]["fold_count"] >= 1
    assert "avg_ml_precision_at_10pct" in result["summary"]
