> ARCHIVED - superseded by the canonical docs in /docs. Do not use this file as the current source of truth.

# Database Documentation

This document describes the databases used in the AI Trading System.

## Overview

| Database | Type | Path | Size |
|----------|------|------|------|
| masterdata.db | SQLite | data/masterdata.db | ~1 MB |
| ohlcv.duckdb | DuckDB | data/ohlcv.duckdb | ~50 MB |

---

## Master Database (SQLite)

**Path:** `data/masterdata.db`

### Tables

| Table | Rows | Description |
|-------|------|-------------|
| stock_details | 1,000 | Main stock list with sector mapping |
| symbols | 4,028 | Complete symbol master with exchange info |
| nse500 | 500 | Nifty 50 constituents |
| nse_holidays | 16 | NSE holidays for 2026 |
| sectors | 23 | Sector definitions |

### stock_details

Main reference table for stocks with sector mapping.

| Column | Type | Description |
|--------|------|-------------|
| Security_id | INTEGER | NSE Security ID |
| Name | TEXT | Company name |
| Symbol | TEXT | NSE Ticker (e.g., TCS, INFY) |
| Industry Group | TEXT | Broader industry category |
| Industry | TEXT | Specific industry |
| MCAP | REAL | Market capitalization |
| Sector | TEXT | Mapped sector (23 categories) |
| exchange | TEXT | Exchange (NSE) |

**Sector Mapping (23 categories):**
IT, Banks, Finance, Automobiles, Pharma, Healthcare, FMCG, Consumer, Energy, Power, Metals, Mining, Chemicals, Infrastructure, Realty, Aerospace, Services, Industrial, Diversified, Auto Components, Agri, Materials, Other

---

## OHLCV Database (DuckDB)

**Path:** `data/ohlcv.duckdb`

### Tables

#### _catalog

Main OHLCV data table.

| Column | Type | Description |
|--------|------|-------------|
| symbol_id | TEXT | Stock ticker |
| security_id | TEXT | Security ID |
| exchange | TEXT | Exchange (NSE/BSE) |
| timestamp | TIMESTAMP | Date/time |
| open | DOUBLE | Open price |
| high | DOUBLE | High price |
| low | DOUBLE | Low price |
| close | DOUBLE | Close price |
| volume | BIGINT | Trading volume |

**Data Status:**
- Total rows: 238,607
- Date range: Mar 2025 - Mar 2026
- Symbols: 1,000

#### _snapshots

Version snapshots for reproducibility.

| Column | Type | Description |
|--------|------|-------------|
| snapshot_id | BIGINT | Unique snapshot ID |
| created_at | TIMESTAMP | Creation time |
| description | TEXT | Snapshot description |
| ohlcv_min_date | DATE | Earliest OHLCV date |
| ohlcv_max_date | DATE | Latest OHLCV date |
| features_count | BIGINT | Number of features |
| status | TEXT | Status (active/completed) |

#### _feature_registry

Feature computation tracking.

| Column | Type | Description |
|--------|------|-------------|
| feature_id | BIGINT | Unique ID |
| feature_name | TEXT | Feature name |
| symbol_id | TEXT | Stock ticker |
| exchange | TEXT | Exchange |
| computed_at | TIMESTAMP | Computation time |
| rows_computed | BIGINT | Row count |
| lookback_days | INTEGER | Lookback period |
| params | TEXT | Parameters JSON |
| feature_file | TEXT | Storage location |
| status | TEXT | Status |
| note | TEXT | Notes |

#### _file_registry (Iceberg-lite)

Tracks all parquet files for time travel.

| Column | Type | Description |
|--------|------|-------------|
| file_id | INTEGER | Unique ID |
| file_path | TEXT | File path |
| table_name | TEXT | Table name |
| feature_name | TEXT | Feature name |
| min_date | DATE | Earliest date in file |
| max_date | DATE | Latest date in file |
| row_count | INTEGER | Rows in file |
| snapshot_id | INTEGER | Snapshot reference |
| created_at | TIMESTAMP | Creation time |

#### _ingestion_status (Iceberg-lite)

Per-symbol update status.

| Column | Type | Description |
|--------|------|-------------|
| symbol_id | TEXT | Stock ticker |
| exchange | TEXT | Exchange |
| table_name | TEXT | Table name |
| last_updated | TIMESTAMP | Last update time |
| last_date | DATE | Last updated date |
| status | TEXT | Status (pending/completed) |

---

## Feature Tables

All features stored as DuckDB tables with the same base schema:

```sql
CREATE TABLE feat_<name> (
    symbol_id VARCHAR,
    exchange VARCHAR,
    timestamp TIMESTAMP,
    date DATE,
    -- feature-specific columns --
    PRIMARY KEY (symbol_id, exchange, timestamp)
);
```

### Feature Summary

| Table | Rows | Symbols | Columns |
|-------|------|---------|---------|
| feat_rsi | 238,225 | 1000 | close, rs, rsi_14 |
| feat_adx | 237,597 | 1000 | plus_di_14, minus_di_14, adx_14 |
| feat_atr | 238,607 | 1000 | atr_14 |
| feat_ema | 238,365 | 1000 | ema_12, ema_26, ema_50, ema_200 |
| feat_roc | 237,393 | 1000 | roc_1, roc_5, roc_10, roc_20 |
| feat_sma | 46,798 | 919 | sma_20, sma_50, sma_200 |
| feat_macd | 209,792 | 979 | macd, signal, histogram |
| feat_bb | 219,572 | 997 | bb_middle_20, bb_upper_20_2sd, bb_lower_20_2sd |

**Total: 1,666,349 feature rows**

---

## Data Relationships

```
stock_details (1000 symbols)
    ↓ Symbol
symbols (4028 entries)
    ↓ symbol_id
_catalog (DuckDB)
    ↓ symbol_id + timestamp
feat_* tables (DuckDB)
    ↓ for features
```

---

## Query Examples

### Get OHLCV for a symbol
```python
import duckdb
conn = duckdb.connect('data/ohlcv.duckdb')
df = conn.execute("""
    SELECT * FROM _catalog 
    WHERE symbol_id = 'RELIANCE' 
    ORDER BY timestamp DESC 
    LIMIT 100
""").fetchdf()
```

### Get latest RSI values
```python
df = conn.execute("""
    SELECT * FROM feat_rsi 
    WHERE symbol_id = 'TCS' 
    ORDER BY timestamp DESC 
    LIMIT 10
""").fetchdf()
```

### Check ingestion status
```python
df = conn.execute("""
    SELECT * FROM _ingestion_status 
    WHERE status = 'pending'
""").fetchdf()
```

### Get table statistics
```python
df = conn.execute("""
    SELECT 
        table_name,
        COUNT(*) as row_count,
        COUNT(DISTINCT symbol_id) as symbols
    FROM information_schema.tables 
    WHERE table_name LIKE 'feat_%'
    GROUP BY table_name
""").fetchdf()
```

---

## Notes

- All feature tables use composite primary key `(symbol_id, exchange, timestamp)` for deduplication
- DuckDB provides fast SQL queries without data loading
- Incremental updates use `INSERT OR IGNORE` pattern
- Snapshots enable reproducible backtests
