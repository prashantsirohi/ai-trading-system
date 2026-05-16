> ARCHIVED - superseded by the canonical docs in /docs. Do not use this file as the current source of truth.

# Data Architecture - Iceberg-lite Implementation

## Overview

This document describes the current Iceberg-lite architecture that provides 90% of Apache Iceberg's benefits without the complexity.

## Scope Note

- This document is storage-focused (feature/OHLCV layout, snapshots, metadata tables).
- Pipeline control-plane behavior (stage gating, DQ semantics, retry behavior, publish isolation) is documented in:
  - `docs/ops_runbook.md`
  - `docs/dq_rules.md`
  - `docs/data-flow.md`

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ICEBERG-LITE ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      METADATA LAYER (DuckDB)                         │   │
│  │  ┌─────────────────┐ ┌──────────────────┐ ┌────────────────────┐   │   │
│  │  │ _file_registry │ │_ingestion_status│ │    _snapshots     │   │   │
│  │  │ Tracks parquet  │ │ Per-symbol      │ │ Version snapshots │   │   │
│  │  │ files           │ │ update status   │ │ for time travel   │   │   │
│  │  └─────────────────┘ └──────────────────┘ └────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    FEATURE STORAGE LAYER                              │   │
│  │                                                                      │   │
│  │  DuckDB Tables (Primary)          Partitioned Parquet (Backup)      │   │
│  │  ┌──────────────┐                 data/feature_store/               │   │
│  │  │ feat_rsi    │                 ├── rsi/year=2025/month=03/       │   │
│  │  │ feat_adx    │                 ├── adx/year=2025/month=03/       │   │
│  │  │ feat_ema    │                 └── ...                            │   │
│  │  │ feat_atr    │                                                      │   │
│  │  │ feat_sma    │                                                      │   │
│  │  │ feat_macd   │                                                      │   │
│  │  │ feat_bb     │                                                      │   │
│  │  │ feat_roc   │                                                      │   │
│  │  └──────────────┘                                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    OHLCV DATA LAYER (DuckDB)                         │   │
│  │  _catalog: 238,607 rows, 1,000 symbols, Mar 2025 - Mar 2026         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Components

### 1. Metadata Tables

```sql
-- File registry: tracks all parquet files
CREATE TABLE _file_registry (
    file_id INTEGER PRIMARY KEY,
    file_path VARCHAR,
    table_name VARCHAR,
    feature_name VARCHAR,
    min_date DATE,
    max_date DATE,
    row_count INTEGER,
    snapshot_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ingestion status: tracks per-symbol update state
CREATE TABLE _ingestion_status (
    symbol_id VARCHAR,
    exchange VARCHAR,
    table_name VARCHAR,
    last_updated TIMESTAMP,
    last_date DATE,
    status VARCHAR DEFAULT 'pending',
    PRIMARY KEY (symbol_id, exchange, table_name)
);

-- Snapshots: version control for reproducibility
CREATE TABLE _snapshots (
    snapshot_id BIGINT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT,
    ohlcv_min_date DATE,
    ohlcv_max_date DATE,
    features_count BIGINT,
    status TEXT DEFAULT 'active'
);
```

### 2. Feature Tables (DuckDB)

All computed features stored in DuckDB tables with the same schema:

```sql
-- Example: feat_rsi
CREATE TABLE feat_rsi (
    symbol_id VARCHAR,
    exchange VARCHAR,
    timestamp TIMESTAMP,
    date DATE,
    close DOUBLE,
    rs DOUBLE,
    rsi_14 DOUBLE,
    PRIMARY KEY (symbol_id, exchange, timestamp)
);
```

**Current Feature Tables:**

| Table | Rows | Symbols | Description |
|-------|------|---------|-------------|
| feat_rsi | 238,225 | 1000 | Relative Strength Index |
| feat_adx | 237,597 | 1000 | Average Directional Index |
| feat_atr | 238,607 | 1000 | Average True Range |
| feat_ema | 238,365 | 1000 | Exponential Moving Average |
| feat_roc | 237,393 | 1000 | Rate of Change |
| feat_sma | 46,798 | 919 | Simple Moving Average |
| feat_macd | 209,792 | 979 | MACD |
| feat_bb | 219,572 | 997 | Bollinger Bands |

**Total: 1.67M feature rows**

### 3. Partitioned Storage (for backup/archival)

```
data/feature_store/
├── rsi/
│   ├── year=2025/
│   │   └── month=03/
│   │       ├── RELIANCE.parquet
│   │       ├── TCS.parquet
│   │       └── ...
│   └── year=2025/
│       └── month=04/
├── adx/
│   └── ...
└── (etc.)
```

---

## Key Patterns

### 1. Atomic Writes

```python
def store_partitioned(self, table_name, df):
    # Write to temp file
    temp_path = f"/path/{symbol}.tmp.parquet"
    df.to_parquet(temp_path, index=False)
    
    # Atomic rename (no partial states)
    final_path = f"/path/{symbol}.parquet"
    if os.path.exists(final_path):
        os.remove(final_path)
    os.rename(temp_path, final_path)
```

### 2. Incremental Computation

```python
def compute_incremental(self, feature_name, symbol_id, compute_method):
    # Get last computed date
    last_date = self.get_last_feature_date(feature_name, symbol_id)
    
    if last_date:
        # Add lookback for rolling indicators (RSI needs prior data)
        lookback_date = last_date - 50 days
        df = compute_method(symbol_id, start_date=lookback_date)
        
        # Filter to only new rows
        df = df[df["date"] > last_date]
    else:
        df = compute_method(symbol_id)
    
    # Store (append-only)
    self.store_features_duckdb(feature_name, df)
```

### 3. Time Travel

```python
# Load from specific snapshot
df = fs.load_partitioned('rsi', snapshot_id=42)

# Or load current data with filters
df = fs.load_features_duckdb('rsi', symbol_id='RELIANCE', start_date='2026-01-01')
```

### 4. Append + Deduplicate

```python
# Never delete + insert
# Always append + use primary key for deduplication

# DuckDB handles this with INSERT OR IGNORE
conn.execute("""
    INSERT OR IGNORE INTO feat_rsi 
    SELECT * FROM staging_table
""")
```

---

## Benefits vs Before

| Aspect | Before | After (Iceberg-lite) |
|--------|--------|---------------------|
| **Storage** | 8,800+ parquet files | DuckDB tables + partitioned parquet |
| **Updates** | Full file rewrite | Incremental append |
| **Atomicity** | None (partial writes possible) | Atomic rename |
| **Reproducibility** | None | Snapshot-based |
| **Query Speed** | Slow (file I/O) | Fast (DuckDB SQL) |
| **Concurrency** | Risky | Safe |
| **File Count** | Exploding | Stable |

---

## Implementation Status

### ✅ Completed

1. **DuckDB Feature Tables**
   - All 8 technical indicators stored in DuckDB
   - Primary key on (symbol_id, exchange, timestamp)
   - 1.67M total rows

2. **Incremental Computation**
   - `compute_incremental()` method
   - `start_date` / `end_date` filtering on all compute methods
   - Only computes new rows + lookback for rolling indicators

3. **Metadata Tables**
   - `_file_registry` - tracks parquet files
   - `_ingestion_status` - tracks per-symbol status
   - `_snapshots` - version snapshots

4. **Atomic Writes**
   - Temp file + rename pattern implemented
   - No partial state on failure

5. **Time Travel Support**
   - `load_partitioned()` with snapshot_id
   - `create_snapshot()` for reproducible states

### ⏳ Future Enhancements

- Full Iceberg migration (if DuckDB adds native support)
- Spark/Flink for distributed processing
- Cloud storage integration

---

## Query Examples

### Load RSI for a symbol
```python
from features.feature_store import FeatureStore
fs = FeatureStore()
df = fs.load_features_duckdb('rsi', symbol_id='RELIANCE')
```

### Incremental update
```python
rows = fs.compute_incremental('rsi', 'RELIANCE', 'NSE', fs.compute_rsi)
```

### Get table info
```python
df = fs.get_table_info()  # All tables
df = fs.get_table_info('rsi')  # Specific table
```

### Check ingestion status
```python
conn = duckdb.connect('data/ohlcv.duckdb')
status = conn.execute("SELECT * FROM _ingestion_status WHERE status = 'pending'").fetchdf()
```
