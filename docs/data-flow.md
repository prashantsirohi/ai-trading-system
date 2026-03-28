# Data Flow

## Overview

The AI Trading System uses an Iceberg-lite architecture with DuckDB for fast SQL queries and incremental computation.

## 1. Symbol Setup

```
all-stock-non-sme.csv (1,346 rows)
  → masterdata.py
  → masterdata.db::stock_details (1,000 matched NSE symbols)
  → masterdata.db::symbols (security_id + metadata)
```

## 2. OHLCV Ingestion

```
collectors/dhan_collector.py
  → TokenManager._ensure_valid_token() [auto-renews via DHAN_TOTP]
  → DhanHQ Market Feed API
  → DuckDB ohlcv.duckdb::_catalog (238,607 rows, 1,000 symbols)
```

**Note:** Dhan API returns corrupted data from March 19, 2026. Using yfinance as fallback.

## 3. Daily EOD Update (Incremental)

```
run/daily_pipeline.py
  │
  ├── 1. Check holiday/weekend
  │
  ├── 2. OHLCV Update
  │     ├── DhanCollector.run_daily_update()
  │     │     → _get_last_dates() from _catalog
  │     │     → Fetch from (last_date + 1) → today
  │     │     → INSERT OR IGNORE (dedup)
  │     │
  │     └── yfinance fallback for current prices
  │
  ├── 3. Feature Computation (Incremental)
  │     ├── FeatureStore.compute_incremental()
  │     │     → get_last_feature_date() per symbol
  │     │     → compute_* with start_date filter
  │     │     → Append to DuckDB tables
  │     │
  │     └── Feature tables: feat_rsi, feat_adx, feat_atr, etc.
  │
  ├── 4. Create Snapshot
  │     └── create_snapshot() → _snapshots table
  │
  └── 5. Google Sheets Update
        ├── Stock Scan
        ├── Sector Dashboard
        └── Portfolio Analysis
```

## 4. Feature Computation (Iceberg-lite)

```
features/feature_store.py
  │
  ├── compute_rsi()           # RSI(14)
  ├── compute_adx()           # ADX(14) with +DI/-DI
  ├── compute_sma()           # SMA(20,50,200)
  ├── compute_ema()           # EMA(12,26)
  ├── compute_macd()          # MACD(12,26,9)
  ├── compute_atr()           # ATR(14)
  ├── compute_bollinger_bands()  # BB(20,2)
  ├── compute_roc()           # ROC(1,5,10,20)
  │
  ├── compute_incremental()   # Wrapper for incremental updates
  ├── store_features_duckdb() # Bulk insert to DuckDB
  └── store_partitioned()     # Atomic parquet writes
```

### Feature Storage

All features stored in DuckDB tables (Iceberg-lite):

| Feature | Table | Rows | Columns |
|---------|-------|------|---------|
| RSI | feat_rsi | 238K | close, rs, rsi_14 |
| ADX | feat_adx | 237K | plus_di_14, minus_di_14, adx_14 |
| ATR | feat_atr | 238K | atr_14 |
| EMA | feat_ema | 238K | ema_12, ema_26 |
| ROC | feat_roc | 237K | roc_1, roc_5, roc_10, roc_20 |
| SMA | feat_sma | 47K | sma_20, sma_50, sma_200 |
| MACD | feat_macd | 210K | macd, signal, histogram |
| BB | feat_bb | 220K | bb_middle, bb_upper, bb_lower |

**Total: 1.67M feature rows**

### Incremental Logic

```python
# For each symbol:
last_date = get_last_feature_date('rsi', 'RELIANCE')

if last_date:
    # Add 50-day lookback for rolling calculations
    start = last_date - 50 days
    df = compute_rsi('RELIANCE', start_date=start)
    df = df[df['date'] > last_date]  # Only new rows
else:
    df = compute_rsi('RELIANCE')

store_features_duckdb('rsi', df)  # Append-only
```

## 5. Metadata Tables (Iceberg-lite)

```
ohlcv.duckdb
  │
  ├── _catalog                 # OHLCV data
  ├── _snapshots              # Version snapshots
  ├── _feature_registry        # Feature tracking
  ├── _file_registry          # Parquet file tracking
  ├── _ingestion_status       # Per-symbol update status
  │
  └── feat_*                   # Feature tables
```

### _snapshots

```sql
CREATE TABLE _snapshots (
    snapshot_id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    description TEXT,
    ohlcv_min_date DATE,
    ohlcv_max_date DATE,
    features_count BIGINT,
    status TEXT
);
```

### _ingestion_status

```sql
CREATE TABLE _ingestion_status (
    symbol_id VARCHAR,
    exchange VARCHAR,
    table_name VARCHAR,
    last_updated TIMESTAMP,
    last_date DATE,
    status VARCHAR,
    PRIMARY KEY (symbol_id, exchange, table_name)
);
```

## 6. Screener Pipeline

```
analytics/screener.py
  │
  ├── RegimeDetector.get_market_regime()
  │     ADX median → TREND (>25) | MEAN_REV (<=25)
  │
  ├── StockRanker.rank_stocks()
  │     5-factor: 30% RS, 20% volume, 15% trend, 20% highs, 15% delivery
  │
  ├── AlphaEngine.get_ml_signals()
  │     XGBoost walk-forward validation
  │
  ├── RiskManager.size_positions()
  │     ATR-based position sizing
  │
  └── EventBacktester.backtest()
        5 strategies with QuantStats
```

## 7. Dashboard

```
dashboard/app.py (Streamlit)
  │
  ├── Overview: Stats, sector distribution
  ├── Ranking: 5-factor ranked stocks
  ├── Chart: Interactive OHLCV + indicators
  └── Portfolio: Position sizing, risk budget
```

## 8. Query Examples

### Load RSI for a symbol
```python
from features.feature_store import FeatureStore
fs = FeatureStore()
df = fs.load_features_duckdb('rsi', symbol_id='RELIANCE')
```

### Incremental update
```python
rows = fs.compute_incremental('rsi', 'RELIANCE', 'NSE', fs.compute_rsi)
print(f'Added {rows} new rows')
```

### Check pending updates
```python
conn = duckdb.connect('data/ohlcv.duckdb')
pending = conn.execute("""
    SELECT * FROM _ingestion_status 
    WHERE status = 'pending'
""").fetchdf()
```

### Time travel (load from snapshot)
```python
# Load data as of a specific snapshot
df = fs.load_partitioned('rsi', snapshot_id=42)
```
