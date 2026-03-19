import sys
import time

sys.path.insert(0, r"C:\Users\DIO\Opencode\ai-trading-system")

from features.feature_store import FeatureStore

fs = FeatureStore(
    ohlcv_db_path=r"C:\Users\DIO\Opencode\ai-trading-system\data\ohlcv.duckdb",
    feature_store_dir=r"C:\Users\DIO\Opencode\ai-trading-system\data\feature_store",
)

conn = fs._get_conn()
syms_df = conn.execute("""
    SELECT DISTINCT symbol_id FROM _catalog
    WHERE exchange = 'NSE'
    ORDER BY symbol_id
""").fetchdf()
conn.close()

symbols = syms_df["symbol_id"].tolist()
print(f"Total symbols: {len(symbols)}")

feature_types = ["rsi", "adx", "sma", "ema", "macd", "atr", "bb", "roc", "supertrend"]

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
for k, v in result.items():
    print(f"  {k}: {v:,} rows")

reg = fs.list_features()
print(f"\nFeature Registry: {len(reg)} entries total")
