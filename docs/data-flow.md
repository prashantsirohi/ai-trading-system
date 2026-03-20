# Data Flow

## 1. Symbol Setup

```
all-stock-non-sme.csv (1,346 rows)
  → masterdata.py / match_and_create_table.py
  → masterdata.db::stock_details (1,306 matched NSE symbols)
  → masterdata.db::symbols (Dhan security_id + metadata)
```

## 2. OHLCV Ingestion (Full)

```
ingest_full.py / DhanCollector.ingest()
  → TokenManager._ensure_valid_token() [auto-renews via DHAN_TOTP base32 secret]
  → DhanHQ Market Feed API
  → DuckDB ohlcv.duckdb::_catalog (4,029,570 rows, 1,306 symbols)
  → DuckDB snapshots for time-travel
```

- **API quirks discovered**: timestamps are Unix epoch seconds (not date strings), nested `data["data"]` dict structure
- API returns from actual listing date (inception) — 1000 calls/day, RELIANCE from 2001-12-31
- `ON CONFLICT` upsert for deduplication
- ACID writes via DuckDB transactions

## 3. Daily EOD Update (Incremental)

```
run_daily_update.ps1
  → DhanCollector.run_daily_update()
  │     → _get_last_dates() — reads MAX(timestamp) per symbol from _catalog
  │     → 2 batches of 700 symbols
  │     → Per symbol: from (last_date + 1) → today
  │     → _fetch_daily_batch() [async, concurrent]
  │     → ON CONFLICT upsert per symbol
  │     → Status report: up-to-date / stale (>1 day gap) / no-data counts
  → compute_features_batch.py
      → DuckDB SQL on _catalog
      → 9 indicators (RSI, ADX, SMA, ATR, BB, ROC, Supertrend: DuckDB COPY fast path)
      → EMA/MACD: pandas fallback (per-symbol parquet)
      → Partitioned parquet: feature_store/<feature>/NSE/data_*.parquet
```

**Stale data handling:**
- Reads `MAX(timestamp)` per symbol from `_catalog`
- Computes `gap = today - last_date` for each symbol
- Fetches from `(last_date + 1)` → today (handles weekends, holidays, missed days)
- Symbols with no prior data: full inception-date lookback (2001-2004)
- Symbols already up-to-date: skipped silently

## 4. Feature Computation

```
compute_features_batch.py
  → DuckDB SQL on ohlcv.duckdb::_catalog
  → 9 technical indicators
  → Partitioned parquet: feature_store/<feature>/NSE/*.parquet
  → DuckDB _feature_registry table
```

### Feature Storage Strategy

| Feature | Method | Files | Size | Columns |
|---------|--------|-------|------|---------|
| RSI(14) | DuckDB COPY | 6 partitioned | 48.7 MB | `symbol_id, exchange, timestamp, close, rsi` |
| SMA(20,50,200) | DuckDB COPY | 6 partitioned | 140.2 MB | `symbol_id, exchange, timestamp, close, period, sma_value` |
| ATR(14) | DuckDB COPY | 6 partitioned | 42.8 MB | `symbol_id, exchange, timestamp, close, atr_value, atr_period` |
| ADX(14) | DuckDB COPY | 6 partitioned | 98.4 MB | `symbol_id, exchange, timestamp, close, adx_plus, adx_minus, adx_value, adx_period` |
| BB(20,2) | DuckDB COPY | 6 partitioned | 95.0 MB | `symbol_id, exchange, timestamp, close, bb_upper, bb_middle, bb_lower, bb_period, bb_std` |
| ROC(1,3,5,10,20) | DuckDB COPY | 6 partitioned | 244.6 MB | `symbol_id, exchange, timestamp, close, roc_period, roc_value` |
| Supertrend(10,3) | DuckDB COPY | 6 partitioned | 79.2 MB | `symbol_id, exchange, timestamp, close, atr_value, st_upper, st_lower, st_signal, st_period, st_multiplier` |
| EMA(12,26,50,200) | Pandas fallback | 1,306 per-symbol | 24.0 MB | `symbol_id, exchange, timestamp, close, ema_12, ema_26, ema_50, ema_200` |
| MACD(12,26,9) | Pandas fallback | 1,306 per-symbol | 18.6 MB | `symbol_id, exchange, timestamp, close, macd_line, macd_signal_9, macd_histogram` |

**Total: 791.6 MB**

**Why split?** DuckDB lacks `EXPONENTIAL_MOVING_AVERAGE()` function. EMA/MACD must use pandas per-symbol rolling (slow but correct). All other features use vectorized DuckDB SQL via `COPY TO PARQUET`.

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
  │     → DuckDB SQL on partitioned parquet, ~13s for 614 stocks
  ├── AlphaEngine.get_ml_signals()
  │     XGBoost 2yr train / 3mo test walk-forward
  │     DuckDB SQL joins for efficient feature loading
  │     Top features: BB + Supertrend + ATR
  ├── RiskManager.size_positions()
  │     ATR-based, 1-2% risk per trade
  │     Regime-aware multipliers (MEAN_REV: 0.5x, TREND: 1.0x)
  ├── EventBacktester.backtest()
  │     5 strategies: BREAKOUT, TREND_FOLLOW, MEAN_REV, MA_CROSS, PULLBACK
  │     DuckDB for partitioned ATR/ADX/Supertrend loading
  │     Per-symbol processing (memory-efficient)
  └── Visualizer.generate_report()
        Plotly OHLCV + indicators, equity curve, QuantStats tearsheet
        HTML output to reports/
```

## 6. Streamlit Dashboard

```
dashboard/app.py (Streamlit Command Center)
  ├── Overview Tab
  │     DuckDB stats (1,306 symbols, 4M rows, latest 2026-03-18)
  │     Sector distribution (top ranked stocks)
  │     Score histogram
  ├── Ranking Tab
  │     DuckDB SQL query all 1,306 stocks with current weights
  │     Weight sliders (35/25/15/30) → auto-refresh
  │     Sector filter, min-score filter, top-N selector
  │     Click to select → navigates to Chart tab
  ├── Chart Tab
  │     DuckDB OHLCV fetch for selected symbol
  │     DuckDB SQL on partitioned parquet → load_features()
  │     Plotly candlestick + SMA 20/50 + EMA 20
  │     Supertrend upper/lower bands
  │     Volume bars, RSI(14) with 30/70 lines
  │     MACD + signal + histogram
  │     Factor radar chart from ranked scores
  └── Portfolio Tab
        ATR-based position sizing for top signals
        Exposure %, risk budget, per-symbol stop-loss and targets
```

## 7. FeatureReader Utility

```
analytics/feature_reader.py
  → DuckDB SQL on partitioned parquet
  → Methods: read_feature(), read_latest(), read_ohlcv(), read_per_symbol()
  → Used by ranker, ML engine, backtester, dashboard
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

**TOTP-based renewal**: `DHAN_TOTP` in `.env` is a 32-char base32 secret. `pyotp` auto-generates the 6-digit code — no manual PIN entry needed.
