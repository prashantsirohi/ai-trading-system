# Data Flow

## 1. Symbol Setup

```
all-stock-non-sme.csv (1346 rows)
  → match_and_create_table.py
  → masterdata.db::stock_details (1306 matched NSE symbols)
  → masterdata.db::symbols (Dhan security_id + metadata)
```

## 2. OHLCV Ingestion (Initial / Full)

```
DhanCollector.ingest()
  → TokenManager._ensure_valid_token() [auto-renews expired tokens]
  → DhanHQ Market Feed API (rate: 5 req/s)
  → DuckDB ohlcv.duckdb::_catalog (symbol, exchange, timestamp, parquet_path)
  → Parquet: data/features/run_000001/<symbol>.parquet
  → DuckDB ohlcv.duckdb::snapshots (run metadata + time-travel)
```

- 1,306 symbols → 342,172 OHLCV rows
- ACID writes via DuckDB transactions
- Immutable append-only Parquet files
- Token auto-renewal on 401/403/expiry detection + proactive 1-hour expiry check

## 3. Daily EOD Update (Incremental — after market close)

```
run_daily_update.ps1
  → DhanCollector.run_daily_update()
  │     → _get_last_dates() — reads last stored date per symbol from DuckDB
  │     → 2 batches of 700 symbols
  │     → Per symbol: from (last_date + 1) → today
  │     → _fetch_daily_batch() [async, 10 concurrent]
  │     → _upsert_ohlcv() [DELETE + INSERT per symbol]
  │     → Status report: up-to-date / stale (>1 day gap) / no-data counts
  → FeatureStore.compute_and_store_features()
      → DuckDB SQL queries on ohlcv.duckdb::_catalog
      → 9 technical indicators (RSI, ADX, SMA, EMA, MACD, ATR, BB, ROC, Supertrend)
      → Parquet: data/feature_store/<feature>/NSE/<symbol>.parquet
```

**Stale data handling:**
- Reads `MAX(timestamp)` per symbol from `_catalog`
- Computes `gap = today - last_date` for each symbol
- Fetches from `(last_date + 1)` → today (handles weekends, holidays, missed days)
- Symbols with no prior data: 7-day lookback
- Symbols already up-to-date: skipped silently

## 4. Feature Computation

```
FeatureStore.compute_all_features()
  → DuckDB SQL queries on ohlcv.duckdb::_catalog
  → 9 technical indicators (RSI, ADX, SMA, EMA, MACD, ATR, BB, ROC, Supertrend)
  → Parquet per feature per symbol: data/feature_store/<feature>/NSE/<symbol>.parquet
  → Feature registry: ohlcv.duckdb::_feature_registry
```

- Supertrend: hybrid DuckDB + pandas (recursive state)
- Total: 2,759,578 rows, 149.75 MB across 1,306 symbols

## 5. Screener Pipeline

```
AIQScreener.screen()
  ├── RegimeDetector.get_market_regime()
  │     ADX median → TREND (>25) | MEAN_REV (<=25) | RANGE_BOUND (~0)
  ├── StockRanker.rank_stocks()
  │     35% relative strength, 25% volume intensity
  │     15% trend persistence, 30% proximity to 52W highs
  │     ⚠️ 1-year penalty: -9 pts (30% × 30) for stocks down >30% over 1 year
  │     ⚠️ prox_high_score halved for downtrending stocks
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

## 6. Streamlit Dashboard

```
dashboard/app.py (Streamlit Command Center)
  ├── Overview Tab
  │     DuckDB stats (symbols, rows, latest date)
  │     Sector distribution (top 100 ranked stocks only)
  │     Score histogram
  │     → Auto-runs StockRanker.rank_all() on page load
  ├── Ranking Tab
  │     DuckDB query all 1,306 stocks with current weights
  │     Weight sliders (35/25/15/30) → auto-refresh on change
  │     Sector filter, min-score filter (default 40), top-N selector
  │     Click to select → navigates to Chart tab
  ├── Chart Tab
  │     DuckDB OHLCV fetch for selected symbol
  │     Plotly candlestick + SMA 20/50 + EMA 20 + Supertrend
  │     Volume bars, RSI(14) with 30/70 lines, MACD + signal + histogram
  │     Factor radar chart from ranked scores
  └── Portfolio Tab
        ATR-based position sizing for top 20 signals
        Exposure %, risk budget, per-symbol stop-loss and targets
```

## Token Renewal Flow

```
DhanCollector._ensure_valid_token()
  ├── is_token_expired() → True?
  │     → TokenManager.renew_token() [calls Dhan OAuth API]
  │     → Saves new access_token to .env
  │     → DhanCollector._init_dhan_client() [reinitializes dhanhq object]
  ├── is_token_expiring_soon(hours=1)?
  │     → Proactive renewal (once per session)
  └── On 401/403 in _fetch_sync:
        → Attempt token renewal + retry once
```
