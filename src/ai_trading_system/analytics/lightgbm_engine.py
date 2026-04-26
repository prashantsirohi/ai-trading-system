"""LightGBM-based alpha model training and inference."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ai_trading_system.analytics.ml_engine import AlphaEngine
from ai_trading_system.platform.logging.logger import logger

try:
    import lightgbm as lgb
except ImportError:  # pragma: no cover - optional dependency boundary
    lgb = None


class LightGBMAlphaEngine(AlphaEngine):
    """Gradient-boosted tree alpha engine using LightGBM."""

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        model_dir: str = None,
        data_domain: str = "research",
    ):
        super().__init__(
            ohlcv_db_path=ohlcv_db_path,
            feature_store_dir=feature_store_dir,
            model_dir=model_dir,
            data_domain=data_domain,
        )
        self.engine_name = "lightgbm"

    def _require_lightgbm(self) -> None:
        if lgb is None:
            raise ImportError(
                "lightgbm is not installed. Install it with `python -m pip install lightgbm`."
            )

    def _default_model_path(self, horizon: int = 5) -> str:
        return os.path.join(self.model_dir, f"{self.engine_name}_h{horizon}.txt")

    @dataclass
    class BoosterAdapter:
        booster_: "lgb.Booster"
        feature_names_: List[str]

        @property
        def feature_importances_(self) -> np.ndarray:
            return self.booster_.feature_importance(importance_type="gain")

        def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
            positive = np.asarray(self.booster_.predict(X))
            return np.column_stack([1.0 - positive, positive])

        def predict(self, X: pd.DataFrame) -> np.ndarray:
            positive = np.asarray(self.booster_.predict(X))
            return (positive >= 0.5).astype(int)

    @staticmethod
    def _progress_callback(period: int):
        """Log LightGBM boosting-round progress at a readable interval."""

        def _callback(env) -> None:
            current_round = env.iteration + 1
            total_rounds = env.end_iteration
            should_log = current_round == 1 or current_round == total_rounds
            if period > 0 and current_round % period == 0:
                should_log = True
            if not should_log:
                return

            metrics = []
            for data_name, metric_name, value, _ in env.evaluation_result_list:
                metrics.append(f"{data_name}_{metric_name}={value:.5f}")
            logger.info(
                "LightGBM progress round=%s/%s %s",
                current_round,
                total_rounds,
                " ".join(metrics) if metrics else "training",
            )

        _callback.order = 20
        return _callback

    def train(
        self,
        train_df: pd.DataFrame,
        horizon: int = 5,
        regime: str = "ALL",
        validation_start: str | None = None,
        validation_fraction: float = 0.2,
        show_progress: bool = True,
        progress_interval: int = 25,
        num_boost_round: int = 300,
        early_stopping_rounds: int = 50,
        **_: Dict[str, Any],
    ) -> Tuple["BoosterAdapter", Dict]:
        self._require_lightgbm()

        target_col = f"target_{horizon}d"
        feature_cols = self._feature_cols(train_df)

        ordered = train_df.copy()
        ordered["timestamp"] = pd.to_datetime(ordered["timestamp"])
        ordered = ordered.sort_values(["timestamp", "symbol_id"]).reset_index(drop=True)
        X = ordered[feature_cols].copy()
        y = ordered[target_col].copy()
        X = X.replace([np.inf, -np.inf], np.nan).fillna(0)

        valid_mask = None
        if validation_start:
            valid_mask = ordered["timestamp"] >= pd.Timestamp(validation_start)
        else:
            unique_dates = ordered["timestamp"].dt.normalize().sort_values().unique()
            if len(unique_dates) > 5:
                split_index = max(1, int(len(unique_dates) * (1 - validation_fraction)))
                start = pd.Timestamp(unique_dates[min(split_index, len(unique_dates) - 1)])
                valid_mask = ordered["timestamp"] >= start

        if valid_mask is None or valid_mask.sum() == 0 or valid_mask.sum() == len(ordered):
            valid_mask = pd.Series(False, index=ordered.index)

        X_train = X.loc[~valid_mask]
        y_train = y.loc[~valid_mask]
        X_valid = X.loc[valid_mask]
        y_valid = y.loc[valid_mask]

        train_set = lgb.Dataset(X_train, label=y_train, feature_name=feature_cols, free_raw_data=True)
        valid_sets = [train_set]
        valid_names = ["train"]
        callbacks = []
        evals_result: Dict[str, Dict[str, List[float]]] = {}
        callbacks.append(lgb.record_evaluation(evals_result))
        if show_progress:
            callbacks.append(self._progress_callback(progress_interval))

        if len(X_valid) > 0:
            valid_set = lgb.Dataset(X_valid, label=y_valid, feature_name=feature_cols, free_raw_data=True)
            valid_sets.append(valid_set)
            valid_names.append("valid")
            callbacks.append(lgb.early_stopping(early_stopping_rounds, verbose=False))

        booster = lgb.train(
            {
                "objective": "binary",
                "metric": "auc",
                "learning_rate": 0.05,
                "num_leaves": 31,
                "feature_fraction": 0.8,
                "bagging_fraction": 0.8,
                "bagging_freq": 1,
                "min_data_in_leaf": 50,
                "lambda_l1": 0.1,
                "lambda_l2": 1.0,
                "seed": 42,
                "verbosity": -1,
            },
            train_set,
            num_boost_round=num_boost_round,
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=callbacks,
        )
        model = self.BoosterAdapter(
            booster_=booster,
            feature_names_=feature_cols,
        )

        importance = dict(zip(feature_cols, model.feature_importances_))
        top_features = sorted(importance.items(), key=lambda item: item[1], reverse=True)[:15]

        logger.info("Trained LightGBM model for %sd/%s", horizon, regime)
        logger.info("  Top features: %s", [feature[0] for feature in top_features[:5]])

        return model, {
            "importance": importance,
            "top_features": top_features,
            "best_iteration": int(getattr(booster, "best_iteration", 0) or num_boost_round),
            "training_history": evals_result,
            "validation_rows": int(len(X_valid)),
            "train_rows": int(len(X_train)),
        }

    def save_model(self, model: "BoosterAdapter", horizon: int = 5):
        self._require_lightgbm()
        path = self._default_model_path(horizon)
        model.booster_.save_model(path)
        logger.info("Model saved to %s", path)
        return path

    def load_model(self, horizon: int = 5) -> Optional["BoosterAdapter"]:
        self._require_lightgbm()
        path = self._default_model_path(horizon)
        if not os.path.exists(path):
            return None
        return self.load_model_from_uri(path)

    def load_model_from_uri(self, model_uri: str) -> "BoosterAdapter":
        self._require_lightgbm()
        path = os.fspath(model_uri)
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        booster = lgb.Booster(model_file=path)
        model = self.BoosterAdapter(
            booster_=booster,
            feature_names_=booster.feature_name(),
        )
        logger.info("Model loaded from %s", path)
        return model

    def _predict_positive_class(self, model, X: pd.DataFrame) -> np.ndarray:
        if hasattr(model, "predict_proba"):
            return model.predict_proba(X)[:, 1]
        return np.asarray(model.predict(X))

    def predict(
        self,
        symbols: List[str] = None,
        exchange: str = "NSE",
        horizon: int = 5,
        model=None,
        date: str = None,
    ) -> pd.DataFrame:
        self._require_lightgbm()

        if symbols is None:
            conn = self._get_conn()
            try:
                syms = conn.execute(
                    """
                    SELECT DISTINCT symbol_id FROM _catalog
                    WHERE exchange = 'NSE'
                    ORDER BY symbol_id LIMIT 100
                    """
                ).fetchdf()
                symbols = syms["symbol_id"].tolist()
            finally:
                conn.close()

        features = self.prepare_training_data(
            symbols=symbols,
            to_date=date,
            exchange=exchange,
            horizons=[horizon],
        )

        if model is None:
            model = self.load_model(horizon)
            if model is None:
                logger.warning("No trained LightGBM model found. Run training first.")
                return pd.DataFrame()

        features["timestamp"] = pd.to_datetime(features["timestamp"])
        latest = features["timestamp"].max()
        latest_df = features[features["timestamp"] == latest].copy()

        feature_cols = self._feature_cols(latest_df)
        X = latest_df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

        probs = self._predict_positive_class(model, X)
        preds = (probs >= 0.5).astype(int)

        result = latest_df[["symbol_id", "exchange", "timestamp", "close"]].copy()
        result["probability"] = probs
        result["prediction"] = preds
        result["direction"] = result["prediction"].map({1: "LONG", 0: "NO_SIGNAL"})
        result["horizon"] = horizon
        return result.sort_values("probability", ascending=False).reset_index(drop=True)

    def evaluate(
        self,
        dataset_df: pd.DataFrame,
        *,
        model: "BoosterAdapter",
        horizon: int = 5,
        validation_start: str | None = None,
        validation_fraction: float = 0.2,
    ) -> Dict[str, float]:
        """Evaluate the model on the validation portion of a prepared dataset."""
        ordered = dataset_df.copy()
        ordered["timestamp"] = pd.to_datetime(ordered["timestamp"])
        ordered = ordered.sort_values(["timestamp", "symbol_id"]).reset_index(drop=True)

        if validation_start:
            valid_mask = ordered["timestamp"] >= pd.Timestamp(validation_start)
        else:
            unique_dates = ordered["timestamp"].dt.normalize().sort_values().unique()
            split_index = max(1, int(len(unique_dates) * (1 - validation_fraction)))
            start = pd.Timestamp(unique_dates[min(split_index, len(unique_dates) - 1)])
            valid_mask = ordered["timestamp"] >= start

        valid_df = ordered.loc[valid_mask].copy()
        if valid_df.empty:
            return {"validation_rows": 0.0}

        target_col = f"target_{horizon}d"
        return_col = f"return_{horizon}d"
        feature_cols = self._feature_cols(valid_df)
        X_valid = valid_df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)
        probs = model.predict_proba(X_valid)[:, 1]
        y_valid = valid_df[target_col].fillna(0).astype(int)

        auc = self._safe_auc(y_valid, probs)
        ranked = valid_df.assign(probability=probs).sort_values("probability", ascending=False)
        top_n = max(1, int(len(ranked) * 0.1))
        top_bucket = ranked.head(top_n)
        precision_at_10pct = float(top_bucket[target_col].mean()) if target_col in top_bucket else 0.0
        avg_return_top_10pct = float(top_bucket[return_col].mean()) if return_col in top_bucket else 0.0
        baseline_positive_rate = float(valid_df[target_col].mean()) if target_col in valid_df else 0.0

        return {
            "validation_rows": float(len(valid_df)),
            "validation_auc": float(round(auc, 4)),
            "precision_at_10pct": float(round(precision_at_10pct, 4)),
            "avg_return_top_10pct": float(round(avg_return_top_10pct, 6)),
            "baseline_positive_rate": float(round(baseline_positive_rate, 4)),
        }

    def score_frame(
        self,
        dataset_df: pd.DataFrame,
        *,
        model: "BoosterAdapter",
        horizon: int,
    ) -> pd.DataFrame:
        """Score an already prepared dataset frame without retraining."""
        scored = dataset_df.copy()
        feature_cols = list(getattr(model, "feature_names_", []) or self._feature_cols(scored))
        for column in feature_cols:
            if column not in scored.columns:
                scored[column] = 0.0
        X = scored[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)
        scored["probability"] = model.predict_proba(X)[:, 1]
        scored["prediction"] = (scored["probability"] >= 0.5).astype(int)
        scored["horizon"] = horizon
        return scored

    def evaluate_frame(
        self,
        dataset_df: pd.DataFrame,
        *,
        model: "BoosterAdapter",
        horizon: int,
    ) -> Dict[str, float]:
        """Evaluate a trained model on a provided out-of-sample frame."""
        if dataset_df.empty:
            return {"validation_rows": 0.0}

        scored = self.score_frame(dataset_df, model=model, horizon=horizon)
        target_col = f"target_{horizon}d"
        return_col = f"return_{horizon}d"
        y_valid = scored[target_col].fillna(0).astype(int)
        auc = self._safe_auc(y_valid, scored["probability"].to_numpy())
        ranked = scored.sort_values("probability", ascending=False)
        top_n = max(1, int(len(ranked) * 0.1))
        top_bucket = ranked.head(top_n)
        precision_at_10pct = float(top_bucket[target_col].mean()) if target_col in top_bucket else 0.0
        avg_return_top_10pct = float(top_bucket[return_col].mean()) if return_col in top_bucket else 0.0
        baseline_positive_rate = float(scored[target_col].mean()) if target_col in scored else 0.0
        return {
            "validation_rows": float(len(scored)),
            "validation_auc": float(round(auc, 4)),
            "precision_at_10pct": float(round(precision_at_10pct, 4)),
            "avg_return_top_10pct": float(round(avg_return_top_10pct, 6)),
            "baseline_positive_rate": float(round(baseline_positive_rate, 4)),
        }
