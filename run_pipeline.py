"""
Pipeline to calculate features and signals for all downloaded symbols.
Usage: python run_pipeline.py
"""

import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)
sys.path.insert(0, script_dir)

import sqlite3
from features.indicators import FeatureEngine
from signals.pattern_detector import SignalDetector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_downloaded_symbols():
    """Get list of symbols that have parquet data"""
    conn = sqlite3.connect("data/masterdata.db")
    cursor = conn.cursor()
    cursor.execute("SELECT symbol_id FROM download_tracker WHERE status = 'completed'")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols


def main():
    symbols = get_downloaded_symbols()
    logger.info(f"Found {len(symbols)} downloaded symbols")

    feature_engine = FeatureEngine(data_dir="data")
    signal_detector = SignalDetector(data_dir="data")

    # Create directories
    os.makedirs("data/features", exist_ok=True)
    os.makedirs("data/signals", exist_ok=True)

    success_count = 0
    error_count = 0

    for i, symbol in enumerate(symbols):
        try:
            # Calculate features
            df = feature_engine.load_raw_data(symbol)
            if df.empty:
                logger.warning(f"No data for {symbol}, skipping")
                continue

            df_features = feature_engine.calculate_indicators(df)
            df_features = feature_engine.calculate_trend_strength(df_features)
            df_features = feature_engine.calculate_momentum(df_features)
            df_features = feature_engine.calculate_volume_indicators(df_features)

            # Save features
            feature_engine.save_features(df_features, symbol)

            # Calculate signals
            signals = signal_detector.detect_all_signals(df_features)
            signals["symbol"] = symbol
            signals["close"] = df_features["close"]

            signal_cols = [
                col for col in signals.columns if col not in ["symbol", "close"]
            ]
            signals["signal_strength"] = signals[signal_cols].sum(axis=1)

            # Save signals
            signal_detector.save_signals(signals, symbol)

            success_count += 1
            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(symbols)} symbols")

        except Exception as e:
            error_count += 1
            logger.error(f"Error processing {symbol}: {e}")

    logger.info(f"Pipeline complete: {success_count} success, {error_count} errors")


if __name__ == "__main__":
    main()
