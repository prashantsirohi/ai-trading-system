> ARCHIVED - superseded by the canonical docs in /docs. Do not use this file as the current source of truth.

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

Delivery ingestion path (separate from OHLCV):

```
collectors/delivery_collector.py
  → Source `mto` (NSE archive)
  → Fallback `nse_securitywise` when archive dates are missing
  → DuckDB ohlcv.duckdb::_delivery
  → feature_store/delivery/NSE/*.parquet
```

## 3. Operational Pipeline (Staged)

```
run/orchestrator.py
  │
  ├── Stage: ingest
  │     ├── OHLCV update from configured market collector(s)
  │     └── Delivery collection (`mto` with `nse_securitywise` fallback)
  │
  ├── Stage: features
  │     ├── FeatureStore.compute_incremental()
  │     │     → get_last_feature_date() per symbol
  │     │     → compute_* with start_date filter
  │     │     → Append to DuckDB tables
  │     │
  │     └── Sector leadership artifacts
  │
  ├── Stage: rank
  │     ├── technical ranking (`ranked_signals.csv`)
  │     ├── breakout scan (`breakout_scan.csv`)
  │     ├── stock scan (`stock_scan.csv`)
  │     └── sector dashboard (`sector_dashboard.csv`)
  │
  └── Stage: publish
        ├── Google Sheets targets
        ├── Telegram summary
        ├── dashboard payload publish
        └── QuantStats dashboard tear sheet
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
  │     Directional regime using:
  │       - ADX median / trending participation
  │       - % above 50 DMA
  │       - % above 200 DMA
  │       - short and medium breadth
  │     Output examples:
  │       - STRONG_BULL_TREND
  │       - STRONG_BEAR_TREND
  │       - RANGE_BOUND
  │
  ├── StockRanker.rank_all()
  │     6-factor technical score:
  │       25% relative strength
  │       18% volume intensity
  │       15% trend persistence
  │       17% proximity to highs
  │       10% delivery
  │       15% sector strength
  │
  ├── LightGBM research workflow
  │     - prepared OHLCV + engineered technical datasets
  │     - separate 5D and 20D models
  │     - walk-forward validation
  │     - shadow-monitor technical vs ML/blended evaluation
  │
  ├── RiskManager.size_positions()
  │     ATR-based position sizing
  │
  └── EventBacktester / RankBacktester
        event-driven and cross-sectional backtest workflows
```

## 7. UI Surfaces

```
ui/research/app.py (Streamlit)
  │
  ├── Overview: stats, freshness, pipeline health
  ├── Long-Term Breadth: % above 20/50/200 SMA since 2010
  ├── Ranking: 6-factor technical ranked stocks
  ├── Chart: Interactive OHLCV + indicators
  ├── Pipeline: run payload, sectors, breakout scan, warnings
  ├── ML: LightGBM review, feature importance, shadow monitor
  └── Portfolio: Position sizing, risk budget

ui/execution/app.py (NiceGUI)
  │
  ├── Control: run launcher, publish retry, Streamlit launcher
  ├── Ranking: latest technical ranks and charts
  ├── Market: breakouts, sectors, market summary
  ├── Operations: run inspection and alerts
  ├── Shadow: weekly/monthly technical vs ML/blended summaries
  ├── Tasks: background jobs and live logs
  └── Processes: project process listing and safe termination

ui/services/
  │
  └── shared read/query and control layer for execution and research UIs
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
