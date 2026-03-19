# Data Flow

## 1. Symbol Setup

```
all-stock-non-sme.csv (1346 rows)
  → match_and_create_table.py
  → masterdata.db::stock_details (1306 matched NSE symbols)
  → masterdata.db::symbols (Dhan security_id + metadata)
```

## 2. OHLCV Ingestion

```
DhanCollector.ingest()
  → DhanHQ Market Feed API (rate: 5 req/s)
  → DuckDB ohlcv.duckdb::_catalog (symbol, exchange, timestamp, parquet_path)
  → Parquet: data/features/run_000001/<symbol>.parquet
  → DuckDB ohlcv.duckdb::snapshots (run metadata + time-travel)
```

- 1,306 symbols → 342,172 OHLCV rows
- ACID writes via DuckDB transactions
- Immutable append-only Parquet files

## 3. Feature Computation

```
FeatureStore.compute_all_features()
  → DuckDB SQL queries on ohlcv.duckdb::_catalog
  → 9 technical indicators (RSI, ADX, SMA, EMA, MACD, ATR, BB, ROC, Supertrend)
  → Parquet per feature per symbol: data/feature_store/<feature>/NSE/<symbol>.parquet
  → Feature registry: ohlcv.duckdb::_feature_registry
```

- Supertrend: hybrid DuckDB + pandas (recursive state)
- Total: 2,759,578 rows, 149.75 MB across 1,306 symbols

## 4. Screener Pipeline

```
AIQScreener.screen()
  ├── RegimeDetector.get_market_regime()
  │     ADX median → TREND (>25) | MEAN_REV (<=25) | RANGE_BOUND (~0)
  ├── StockRanker.rank_stocks()
  │     40% relative strength, 25% volume intensity
  │     20% trend persistence, 15% proximity to 52W highs
  ├── AlphaEngine.get_ml_signals()
  │     XGBoost 2yr train / 3mo test walk-forward
  │     6 windows, avg AUC ~0.54, top features: BB + Supertrend + ATR
  ├── RiskManager.size_positions()
  │     ATR-based, 1-2% risk per trade
  │     Regime-aware multipliers (MEAN_REV: 0.5x, TREND: 1.0x)
  ├── EventBacktester.backtest()
  │     5 strategies: BREAKOUT, TREND_FOLLOW, MEAN_REV, MA_CROSS, PULLBACK
  │     Per-symbol processing (memory-efficient)
  └── Visualizer.generate_report()
        Plotly OHLCV + indicators, equity curve, QuantStats tearsheet
        HTML output to reports/
```
