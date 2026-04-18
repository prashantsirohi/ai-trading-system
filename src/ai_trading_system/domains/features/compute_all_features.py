"""Convenience script to recompute the full operational feature store."""

from __future__ import annotations

import time
from pathlib import Path

from core.bootstrap import ensure_project_root_on_path

project_root = ensure_project_root_on_path(__file__)

from ai_trading_system.domains.features.feature_store import FeatureStore


def main() -> None:
    fs = FeatureStore(
        ohlcv_db_path=Path(project_root) / "data" / "ohlcv.duckdb",
        feature_store_dir=Path(project_root) / "data" / "feature_store",
    )

    conn = fs._get_conn()
    syms_df = conn.execute(
        """
        SELECT DISTINCT symbol_id FROM _catalog
        WHERE exchange = 'NSE'
        ORDER BY symbol_id
        """
    ).fetchdf()
    conn.close()

    symbols = syms_df["symbol_id"].tolist()
    print(f"Total symbols: {len(symbols)}")

    feature_types = [
        "rsi",
        "adx",
        "sma",
        "ema",
        "macd",
        "atr",
        "bb",
        "roc",
        "supertrend",
    ]

    t0 = time.time()
    result = fs.compute_and_store_features(
        symbols=symbols,
        exchanges=["NSE"],
        feature_types=feature_types,
    )

    total_rows = sum(v for v in result.values())
    elapsed = time.time() - t0

    print(f"\nAll features computed and stored in {elapsed:.1f}s")
    print(f"Total rows written: {total_rows:,}")
    for key, value in result.items():
        print(f"  {key}: {value:,} rows")

    reg = fs.list_features()
    print(f"\nFeature Registry: {len(reg)} entries total")


if __name__ == "__main__":
    main()
