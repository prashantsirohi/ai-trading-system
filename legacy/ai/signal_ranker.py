import os
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np
import pickle
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, classification_report
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("Sklearn not available, using rule-based ranking")

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False


class SignalRanker:
    """
    AI Ranking Layer - Ranks signals using ML models.
    """

    def __init__(self, data_dir: str = "data", model_path: Optional[str] = None):
        self.data_dir = data_dir
        self.model_path = model_path or "models"
        self.model = None
        self.scaler = None
        self.is_trained = False

        os.makedirs(self.model_path, exist_ok=True)

    def prepare_training_data(self, symbols: List[str]) -> pd.DataFrame:
        """Prepare features and labels for training"""
        all_data = []

        for symbol in symbols:
            signals_path = os.path.join(self.data_dir, "signals", f"{symbol}_signals.parquet")
            features_path = os.path.join(self.data_dir, "features", f"{symbol}_features.parquet")

            if not (os.path.exists(signals_path) and os.path.exists(features_path)):
                continue

            signals_df = pd.read_parquet(signals_path)
            features_df = pd.read_parquet(features_path)

            if signals_df.empty or features_df.empty:
                continue

            merged = pd.merge(signals_df, features_df, left_index=True, right_index=True, how="inner")

            merged["future_return"] = merged["close"].shift(-5) / merged["close"] - 1
            merged["label"] = (merged["future_return"] > 0).astype(int)

            all_data.append(merged)

        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    def get_feature_columns(self) -> List[str]:
        """Get feature columns for model training"""
        return [
            "RSI", "MACD", "MACD_signal", "MACD_hist", "SUPERT_10_3", "SUPERTd_10_3",
            "ATR", "EMA_20", "EMA_50", "SMA_20", "SMA_50", "SMA_200", "VWAP",
            "returns", "volatility_20", "volatility_60", "price_change_1d",
            "price_change_5d", "price_change_20d", "volume_ratio", "volume_spike",
            "trend_strength", "momentum_10", "momentum_20", "range_pct"
        ]

    def train(
        self,
        symbols: List[str],
        model_type: str = "random_forest"
    ) -> Dict:
        """Train the ranking model"""
        if not SKLEARN_AVAILABLE:
            return {"error": "Sklearn not available"}

        logger.info(f"Training {model_type} model...")

        df = self.prepare_training_data(symbols)
        if df.empty:
            return {"error": "No training data available"}

        feature_cols = [col for col in self.get_feature_columns() if col in df.columns]
        X = df[feature_cols].fillna(0)
        y = df["label"]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        if model_type == "random_forest":
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
        elif model_type == "xgboost" and XGB_AVAILABLE:
            self.model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
        elif model_type == "gradient_boosting":
            self.model = GradientBoostingClassifier(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                random_state=42
            )
        else:
            self.model = RandomForestClassifier(n_estimators=100, random_state=42)

        self.model.fit(X_train_scaled, y_train)

        y_pred = self.model.predict(X_test_scaled)
        accuracy = accuracy_score(y_test, y_pred)

        self.is_trained = True
        self._save_model()

        return {
            "model_type": model_type,
            "accuracy": accuracy,
            "training_samples": len(X_train),
            "test_samples": len(X_test),
            "feature_count": len(feature_cols)
        }

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        """Predict probability of profit for given features"""
        if not self.is_trained or self.model is None:
            return np.array([])

        feature_cols = [col for col in self.get_feature_columns() if col in features.columns]
        X = features[feature_cols].fillna(0)

        X_scaled = self.scaler.transform(X)
        predictions = self.model.predict_proba(X_scaled)

        return predictions[:, 1] if predictions.shape[1] > 1 else predictions.flatten()

    def rank_signals(self, signals_df: pd.DataFrame) -> pd.DataFrame:
        """Rank signals based on AI prediction"""
        if not self.is_trained or signals_df.empty:
            return signals_df

        features = signals_df.copy()
        features["ranking_score"] = self.predict(features)

        features = features.sort_values("ranking_score", ascending=False)

        return features

    def get_top_signals(
        self,
        signals: pd.DataFrame,
        top_n: int = 10,
        min_score: float = 0.5
    ) -> pd.DataFrame:
        """Get top N signals above threshold"""
        ranked = self.rank_signals(signals)

        if "ranking_score" not in ranked.columns:
            return signals.head(top_n)

        filtered = ranked[ranked["ranking_score"] >= min_score]
        return filtered.head(top_n)

    def calculate_market_regime(self, features: pd.DataFrame) -> str:
        """Calculate current market regime"""
        if features.empty:
            return "unknown"

        latest = features.iloc[-1] if len(features) > 0 else None
        if latest is None:
            return "unknown"

        volatility = latest.get("volatility_20", 0)
        trend = latest.get("trend_strength", 0)

        if abs(trend) > 2 and volatility < 0.02:
            return "trending_low_vol"
        elif abs(trend) > 2 and volatility >= 0.02:
            return "trending_high_vol"
        elif abs(trend) <= 2 and volatility < 0.015:
            return "range_low_vol"
        else:
            return "range_high_vol"

    def _save_model(self):
        """Save trained model to disk"""
        if self.model is None:
            return

        model_file = os.path.join(self.model_path, "signal_ranker.pkl")
        scaler_file = os.path.join(self.model_path, "scaler.pkl")

        with open(model_file, "wb") as f:
            pickle.dump(self.model, f)

        if self.scaler:
            with open(scaler_file, "wb") as f:
                pickle.dump(self.scaler, f)

        logger.info(f"Model saved to {model_file}")

    def load_model(self) -> bool:
        """Load trained model from disk"""
        model_file = os.path.join(self.model_path, "signal_ranker.pkl")
        scaler_file = os.path.join(self.model_path, "scaler.pkl")

        if not (os.path.exists(model_file) and os.path.exists(scaler_file)):
            return False

        try:
            with open(model_file, "rb") as f:
                self.model = pickle.load(f)

            with open(scaler_file, "rb") as f:
                self.scaler = pickle.load(f)

            self.is_trained = True
            logger.info("Model loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False
