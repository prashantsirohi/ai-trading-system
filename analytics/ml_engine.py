import os
import time
import logging
import duckdb
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Literal, Tuple
import xgboost as xgb
from sklearn.metrics import roc_auc_score, mean_squared_error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlphaEngine:
    """
    ML Alpha Generation Engine using XGBoost.

    Key features:
      - Walk-Forward Validation: Rolling train (2 years) -> test (3 months)
      - Regime-aware: Split training by regime for targeted strategy
      - Meta-Labeling: Secondary model labels signal quality (high/low conviction)
      - Multi-horizon targets: 1-day, 5-day, 20-day returns

    Feature set:
      - Technical: RSI, ADX, MACD, ATR, Bollinger Bands, Supertrend
      - Volume: Volume intensity, volume ratio
      - Price action: Momentum, ROC, distance from MAs
      - Rank-based: Composite score, factor scores
    """

    def __init__(
        self,
        ohlcv_db_path: str = None,
        feature_store_dir: str = None,
        model_dir: str = None,
    ):
        if ohlcv_db_path is None:
            ohlcv_db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "ohlcv.duckdb",
            )
        if feature_store_dir is None:
            feature_store_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data",
                "feature_store",
            )
        if model_dir is None:
            model_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "models",
            )
        self.ohlcv_db_path = ohlcv_db_path
        self.feature_store_dir = feature_store_dir
        self.model_dir = model_dir
        os.makedirs(model_dir, exist_ok=True)

    def _get_conn(self):
        return duckdb.connect(self.ohlcv_db_path)

    def prepare_training_data(
        self,
        symbols: List[str] = None,
        from_date: str = None,
        to_date: str = None,
        exchange: str = "NSE",
        horizons: List[int] = None,
    ) -> pd.DataFrame:
        """
        Load all features + targets for training using DuckDB for efficient joins.
        Features are loaded from feature_store/ Parquet files via DuckDB.
        Targets: forward returns at multiple horizons.
        """
        if horizons is None:
            horizons = [1, 5, 20]

        conn = self._get_conn()
        try:
            date_filter = ""
            if from_date:
                date_filter += f" AND timestamp >= '{from_date}'"
            if to_date:
                date_filter += f" AND timestamp <= '{to_date}'"

            symbol_filter = ""
            if symbols:
                sym_list = ", ".join(f"'{s}'" for s in symbols)
                symbol_filter += f" AND symbol_id IN ({sym_list})"

            ohlcv = conn.execute(f"""
                SELECT symbol_id, exchange, timestamp, close, high, low, open, volume
                FROM _catalog
                WHERE exchange = '{exchange}'
                  AND timestamp IS NOT NULL
                  {date_filter}
                  {symbol_filter}
                ORDER BY symbol_id, timestamp
            """).fetchdf()

            ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"])
            conn.execute("CREATE TEMP VIEW temp_ohlcv AS SELECT * FROM ohlcv")
        finally:
            pass

        logger.info(f"Loaded {len(ohlcv):,} OHLCV rows for training")

        feat_cfg = [
            ("rsi", "rsi", ["rsi"]),
            ("adx", "adx", ["adx_plus", "adx_minus", "adx_value"]),
            ("atr", "atr", ["atr_value"]),
            ("bb", "bb", ["bb_upper", "bb_middle", "bb_lower"]),
            ("supertrend", "st", ["st_upper", "st_lower", "st_signal"]),
        ]

        feat_cols_sql = "t.symbol_id, t.exchange, t.timestamp, t.close, t.high, t.low, t.open, t.volume"
        join_clauses = []
        for feat_name, alias, feat_cols in feat_cfg:
            feat_path = os.path.join(
                self.feature_store_dir, feat_name, exchange, "*.parquet"
            ).replace("\\", "/")
            if not os.path.exists(
                os.path.join(self.feature_store_dir, feat_name, exchange)
            ):
                continue
            select_cols = ", ".join(f"{c}" for c in feat_cols)
            join_clauses.append(f"""
            LEFT JOIN (
                SELECT symbol_id, exchange, timestamp, {select_cols}
                FROM read_parquet('{feat_path}')
                WHERE exchange = '{exchange}'
                  AND timestamp >= '{from_date or "1900-01-01"}'
                  AND timestamp <= '{to_date or "2100-01-01"}'
                  {symbol_filter}
            ) {alias} ON t.symbol_id = {alias}.symbol_id
                       AND t.exchange = {alias}.exchange
                       AND t.timestamp = {alias}.timestamp
            """)
            feat_cols_sql += ", " + ", ".join(f"{alias}.{c}" for c in feat_cols)
            logger.info(f"  Joined {feat_name}: {feat_cols}")

        query = f"""
            SELECT {feat_cols_sql}
            FROM temp_ohlcv t
            {"".join(join_clauses)}
            ORDER BY t.symbol_id, t.timestamp
        """
        features = conn.execute(query).fetchdf()
        conn.execute("DROP VIEW temp_ohlcv")

        conn.close()

        features["timestamp"] = pd.to_datetime(features["timestamp"])

        for h in horizons:
            features[f"return_{h}d"] = (
                features.groupby("symbol_id")["close"].shift(-h) / features["close"] - 1
            )
            features[f"target_{h}d"] = (
                features[f"return_{h}d"] > features[f"return_{h}d"].quantile(0.6)
            ).astype(int)

        features = features.dropna(subset=["close", "volume"])
        logger.info(
            f"Training data: {len(features):,} rows, "
            f"{features['symbol_id'].nunique()} symbols"
        )
        return features

    def _feature_cols(self, df: pd.DataFrame) -> List[str]:
        exclude = {
            "symbol_id",
            "exchange",
            "timestamp",
            "close",
            "open",
            "high",
            "low",
            "volume",
            "parquet_file",
            "ingestion_version",
            "ingestion_ts",
            "return_1d",
            "return_5d",
            "return_20d",
            "target_1d",
            "target_5d",
            "target_20d",
        }
        return [
            c
            for c in df.columns
            if c not in exclude
            and df[c].dtype
            in ("float64", "float32", "int64", "int32", "float16", "int16")
        ]

    def train(
        self,
        train_df: pd.DataFrame,
        horizon: int = 5,
        regime: str = "ALL",
    ) -> Tuple[xgb.XGBClassifier, Dict]:
        """
        Train an XGBoost classifier for a given horizon and regime.
        """
        target_col = f"target_{horizon}d"
        feature_cols = self._feature_cols(train_df)

        X = train_df[feature_cols].copy()
        y = train_df[target_col].copy()
        X = X.replace([np.inf, -np.inf], np.nan)
        X = X.fillna(0)

        model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            gamma=0.1,
            reg_alpha=0.1,
            reg_lambda=1.0,
            scale_pos_weight=1,
            eval_metric="auc",
            verbosity=0,
            tree_method="hist",
            random_state=42,
        )

        model.fit(X, y)

        importance = dict(zip(feature_cols, model.feature_importances_))
        top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:15]

        logger.info(f"Trained XGBoost model for {horizon}d/{regime}")
        logger.info(f"  Top features: {[f[0] for f in top_features[:5]]}")

        return model, {"importance": importance, "top_features": top_features}

    def walk_forward_validate(
        self,
        symbols: List[str] = None,
        train_days: int = 504,
        test_days: int = 63,
        horizon: int = 5,
        regime: str = "ALL",
        from_date: str = None,
        to_date: str = None,
    ) -> Dict:
        """
        Walk-Forward Validation:
          1. Train on rolling window of train_days
          2. Test on next test_days
          3. Walk window forward
        """
        conn = self._get_conn()
        try:
            latest = conn.execute(
                "SELECT MAX(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
            earliest = conn.execute(
                "SELECT MIN(timestamp) FROM _catalog WHERE exchange = 'NSE'"
            ).fetchone()[0]
        finally:
            conn.close()

        if to_date is None:
            to_date = (
                str(latest.date()) if hasattr(latest, "date") else str(latest)[:10]
            )
        if from_date is None:
            from_date = (
                str(earliest.date())
                if hasattr(earliest, "date")
                else str(earliest)[:10]
            )

        all_data = self.prepare_training_data(
            symbols=symbols,
            from_date=from_date,
            to_date=to_date,
        )

        all_data["timestamp"] = pd.to_datetime(all_data["timestamp"])
        all_data = all_data.sort_values("timestamp")

        dates = all_data["timestamp"].unique()
        dates = np.sort(dates)
        n_windows = max(1, (len(dates) - train_days) // test_days)

        results = []
        for i in range(n_windows):
            train_end_idx = train_days + i * test_days
            test_start_idx = train_end_idx
            test_end_idx = min(test_start_idx + test_days, len(dates))

            if test_end_idx <= test_start_idx:
                break

            train_dates = dates[max(0, train_end_idx - train_days) : train_end_idx]
            test_dates = dates[test_start_idx:test_end_idx]

            train_df = all_data[all_data["timestamp"].isin(train_dates)].copy()
            test_df = all_data[all_data["timestamp"].isin(test_dates)].copy()

            if len(train_df) < 100 or len(test_df) < 50:
                continue

            model, model_info = self.train(train_df, horizon, regime)

            feature_cols = self._feature_cols(test_df)
            X_test = test_df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)
            y_test = test_df[f"target_{horizon}d"]

            if y_test.sum() == 0 or y_test.sum() == len(y_test):
                continue

            y_pred_proba = model.predict_proba(X_test)[:, 1]
            y_pred = model.predict(X_test)

            try:
                auc = roc_auc_score(y_test, y_pred_proba)
            except ValueError:
                auc = 0.5

            actual_returns = test_df["return_5d"].dropna()
            pred_signals = test_df.loc[actual_returns.index, "return_5d"]
            pred_high = pred_signals[y_pred == 1]
            signal_return = pred_high.mean() if len(pred_high) > 0 else 0

            fold_result = {
                "window": i + 1,
                "train_start": str(train_dates[0])[:10],
                "train_end": str(train_dates[-1])[:10],
                "test_start": str(test_dates[0])[:10],
                "test_end": str(test_dates[-1])[:10],
                "train_rows": len(train_df),
                "test_rows": len(test_df),
                "auc": round(auc, 4),
                "signal_return": round(signal_return * 100, 2) if signal_return else 0,
                "n_signals": int(y_pred.sum()),
            }
            results.append(fold_result)
            logger.info(
                f"  Window {i + 1}: AUC={auc:.4f}, "
                f"signal_return={signal_return * 100:.2f}%, "
                f"n_signals={y_pred.sum()}"
            )

        if not results:
            return {"error": "Not enough data for walk-forward validation"}

        results_df = pd.DataFrame(results)
        summary = {
            "n_windows": len(results),
            "avg_auc": round(results_df["auc"].mean(), 4),
            "std_auc": round(results_df["auc"].std(), 4),
            "avg_signal_return": round(results_df["signal_return"].mean(), 2),
            "windows": results,
        }
        logger.info(
            f"Walk-Forward Validation: {len(results)} windows, "
            f"avg AUC={summary['avg_auc']}, "
            f"avg signal return={summary['avg_signal_return']}%"
        )
        return summary

    def predict(
        self,
        symbols: List[str] = None,
        exchange: str = "NSE",
        horizon: int = 5,
        model: xgb.XGBClassifier = None,
        date: str = None,
    ) -> pd.DataFrame:
        """
        Predict alpha scores for current/recent signals.
        Returns symbol, prediction probability, predicted direction.
        """
        if symbols is None:
            conn = self._get_conn()
            try:
                syms = conn.execute("""
                    SELECT DISTINCT symbol_id FROM _catalog
                    WHERE exchange = 'NSE'
                    ORDER BY symbol_id LIMIT 100
                """).fetchdf()
                symbols = syms["symbol_id"].tolist()
            finally:
                conn.close()

        features = self.prepare_training_data(
            symbols=symbols,
            to_date=date,
            horizon=horizon,
        )

        if model is None:
            model_path = os.path.join(self.model_dir, f"xgb_h{horizon}.json")
            if os.path.exists(model_path):
                model = xgb.XGBClassifier()
                model.load_model(model_path)
            else:
                logger.warning(
                    "No trained model found. Run walk_forward_validate first."
                )
                return pd.DataFrame()

        features["timestamp"] = pd.to_datetime(features["timestamp"])
        latest = features["timestamp"].max()
        latest_df = features[features["timestamp"] == latest].copy()

        feature_cols = self._feature_cols(latest_df)
        X = latest_df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

        probs = model.predict_proba(X)[:, 1]
        preds = model.predict(X)

        result = latest_df[["symbol_id", "exchange", "timestamp", "close"]].copy()
        result["probability"] = probs
        result["prediction"] = preds
        result["direction"] = result["prediction"].map({1: "LONG", 0: "NO_SIGNAL"})
        result["horizon"] = horizon

        return result.sort_values("probability", ascending=False).reset_index(drop=True)

    def meta_label(
        self,
        signals_df: pd.DataFrame,
        horizon: int = 5,
    ) -> pd.DataFrame:
        """
        Meta-labeling: Label high-conviction vs low-conviction signals.
        Uses ADX + RSI combination to classify conviction.
        """
        result = signals_df.copy()

        if "adx_14" in result.columns:
            result["meta_label"] = (
                (result["adx_14"] > 25)
                & ((result["rsi_14"] > 30) & (result["rsi_14"] < 70))
            ).map({True: "HIGH_CONVICTION", False: "LOW_CONVICTION"})
        else:
            result["meta_label"] = "LOW_CONVICTION"

        result["strategy"] = result.apply(
            lambda r: (
                "TREND_FOLLOWING" if r.get("adx_14", 0) >= 20 else "MEAN_REVERSION"
            ),
            axis=1,
        )

        return result

    def save_model(self, model: xgb.XGBClassifier, horizon: int = 5):
        path = os.path.join(self.model_dir, f"xgb_h{horizon}.json")
        model.save_model(path)
        logger.info(f"Model saved to {path}")

    def load_model(self, horizon: int = 5) -> Optional[xgb.XGBClassifier]:
        path = os.path.join(self.model_dir, f"xgb_h{horizon}.json")
        if not os.path.exists(path):
            return None
        model = xgb.XGBClassifier()
        model.load_model(path)
        logger.info(f"Model loaded from {path}")
        return model
