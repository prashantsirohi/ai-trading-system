import sys

sys.path.insert(0, r"C:\Users\DIO\Opencode\ai-trading-system")
from features.feature_store import FeatureStore
import time

fs = FeatureStore(
    ohlcv_db_path=r"C:\Users\DIO\Opencode\ai-trading-system\data\ohlcv.duckdb",
    feature_store_dir=r"C:\Users\DIO\Opencode\ai-trading-system\data\feature_store",
)

conn = fs._get_conn()
stats = conn.execute("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT symbol_id) AS total_symbols,
        MIN(timestamp) AS earliest,
        MAX(timestamp) AS latest
    FROM _catalog
""").fetchdf()
conn.close()

print("=" * 60)
print("Feature Store & Compute Layer — AI Trading System")
print("=" * 60)

print("\nOHLCV Catalog:")
print(f"  Total rows:     {stats['total_rows'][0]:,}")
print(f"  Total symbols: {stats['total_symbols'][0]:,}")
print(f"  Date range:    {stats['earliest'][0]} -> {stats['latest'][0]}")

reg = fs.list_features()
print(f"\nFeature Registry: {len(reg)} entries")

test_sym = "AARTIIND"
test_exc = "NSE"

print(f"\n{'=' * 60}")
print(f"Computing all technicals for {test_sym}/{test_exc}...")
t0 = time.time()
df = fs.compute_all_technicals(test_sym, test_exc)
print(f"Computed in {time.time() - t0:.1f}s")
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"\nSample rows:")
print(
    df[
        [
            "timestamp",
            "close",
            "rsi_14",
            "adx_14",
            "macd_line",
            "macd_signal_9",
            "macd_histogram",
            "atr_14",
            "roc_1",
            "roc_5",
            "roc_20",
        ]
    ]
    .head(5)
    .to_string(index=False)
)

print(f"\n{'=' * 60}")
print("Computing individual features...")
for feat_name, method in [
    ("RSI", lambda: fs.compute_rsi(test_sym, test_exc)),
    ("ADX", lambda: fs.compute_adx(test_sym, test_exc)),
    ("SMA", lambda: fs.compute_sma(test_sym, test_exc, windows=[20, 50, 200])),
    ("EMA", lambda: fs.compute_ema(test_sym, test_exc, windows=[12, 26])),
    ("MACD", lambda: fs.compute_macd(test_sym, test_exc)),
    ("ATR", lambda: fs.compute_atr(test_sym, test_exc)),
    ("Bollinger", lambda: fs.compute_bollinger_bands(test_sym, test_exc)),
    ("ROC", lambda: fs.compute_roc(test_sym, test_exc, periods=[1, 5, 20])),
]:
    t0 = time.time()
    d = method()
    feat_cols = [
        c for c in d.columns if c not in ("symbol_id", "exchange", "timestamp", "close")
    ]
    print(f"  {feat_name}: {len(d):,} rows, {time.time() - t0:.1f}s  |  {feat_cols}")

print(f"\n{'=' * 60}")
print("Point-in-time join (no look-ahead bias)...")
rsi_df = fs.compute_rsi(test_sym, test_exc)
pit = fs.point_in_time_join(rsi_df, test_sym, test_exc)
print(f"  Joined shape: {pit.shape}")
print(f"  Columns: {list(pit.columns)}")
print(f"  Sample:")
print(pit[["timestamp", "close", "rsi_14"]].head(5).to_string(index=False))

print(f"\n{'=' * 60}")
print("Computing & storing features for 10 symbols...")
syms = [
    "AARTIIND",
    "ABB",
    "STYRENIX",
    "ACC",
    "ADANIENT",
    "ADOR",
    "AEGISLOG",
    "HAPPSTMNDS",
    "ALEMBICLTD",
    "ARE&M",
]
t0 = time.time()
result = fs.compute_and_store_features(
    symbols=syms,
    exchanges=["NSE"],
    feature_types=["rsi", "sma", "atr", "supertrend"],
)
conn = fs._get_conn()
conn.close()
print(f"Bulk compute: {time.time() - t0:.1f}s")
for k, v in result.items():
    print(f"  {k}: {v:,} rows")

reg = fs.list_features()
print(f"\nFeature Registry: {len(reg)} entries")
if not reg.empty:
    print(reg.groupby("feature_name")[["rows_computed"]].sum())

loaded = fs.load_feature("rsi", "AARTIIND", "NSE")
print(f"\nLoaded RSI from Parquet: {len(loaded):,} rows")

print(f"\n{'=' * 60}")
print("Computing Supertrend for AARTIIND...")
st = fs.compute_supertrend(test_sym, test_exc, period=10, multiplier=3.0)
print(f"  Supertrend rows: {len(st):,}")
print(f"  Columns: {list(st.columns)}")
print(f"  Sample:")
print(
    st[["timestamp", "close", "supertrend_10_3", "supertrend_dir_10_3"]]
    .head(10)
    .to_string(index=False)
)

print(f"\n{'=' * 60}")
print("Computing fundamental features from stock_details...")
fund = fs.compute_fundamental_features(exchanges=["NSE"])
print(f"  Fundamental records: {len(fund):,}")
print(f"  Columns: {list(fund.columns)}")
if not fund.empty:
    print(f"  MCAP categories:")
    print(fund["mcap_category"].value_counts().to_string())
    print(f"  Sample:")
    print(
        fund[["symbol_id", "name", "industry_group", "industry", "mcap_category"]]
        .head(5)
        .to_string(index=False)
    )

fund_stored = fs.store_fundamental_features(exchanges=["NSE"])
print(f"\nStored fundamental features: {fund_stored} symbols")

print(f"\n{'=' * 60}")
print("Feature Store: ALL OK")
print("=" * 60)
