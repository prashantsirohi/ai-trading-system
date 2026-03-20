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

## 2b. Delivery Data (NSE MTO Archive)

```
collectors/delivery_collector.py
  → NSE MTO URL: https://nsearchives.nseindia.com/archives/equities/mto/MTO_{DDMMYYYY}.DAT
  → Parses .DAT plain text format (EQ series only)
  → DuckDB ohlcv.duckdb::_delivery (657,506 rows, 2,606 symbols)
  → Partitioned parquet: feature_store/delivery/NSE/ (6 files)
  → Features: delivery_pct, delivery_5d_avg, delivery_20d_avg, delivery_pctile
```

- NSE switched from old bhavcopy CSV (with delivery cols) to UDiFF format (no delivery data)
- MTO archive covers both 2025 and 2026 dates (old bhavcopy only to Apr 2024)
- Some non-trading days return 404 (expected)

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

| Feature | Method | Files | Columns |
|---------|--------|-------|---------|
| RSI(14) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, rsi` |
| SMA(20,50,200) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, period, sma_value` |
| ATR(14) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, atr_value, atr_period` |
| ADX(14) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, adx_plus, adx_minus, adx_value, adx_period` |
| BB(20,2) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, bb_upper, bb_middle, bb_lower, bb_period, bb_std` |
| ROC(1,3,5,10,20) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, roc_period, roc_value` |
| Supertrend(10,3) | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, close, atr_value, st_upper, st_lower, st_signal, st_period, st_multiplier` |
| Delivery % | DuckDB COPY | 6 partitioned | `symbol_id, exchange, timestamp, delivery_pct, volume, delivery_qty, delivery_5d_avg, delivery_20d_avg, delivery_pctile` |
| EMA(12,26,50,200) | Pandas fallback | 1,306 per-symbol | `symbol_id, exchange, timestamp, close, ema_12, ema_26, ema_50, ema_200` |
| MACD(12,26,9) | Pandas fallback | 1,306 per-symbol | `symbol_id, exchange, timestamp, close, macd_line, macd_signal_9, macd_histogram` |

**Total: ~792 MB** (OHLCV: 4M rows; Delivery: 657K rows)

**Why split?** DuckDB lacks `EXPONENTIAL_MOVING_AVERAGE()` function. EMA/MACD must use pandas per-symbol rolling (slow but correct). All other features use vectorized DuckDB SQL via `COPY TO PARQUET`.

## 5. Screener Pipeline

```
AIQScreener.screen()
  ├── RegimeDetector.get_market_regime()
  │     ADX median → TREND (>25) | MEAN_REV (<=25) | RANGE_BOUND (~0)
  ├── StockRanker.rank_stocks()
  │     5-factor weighted ranking (percentile scores):
  │       30% relative strength, 20% volume intensity
  │       15% trend persistence, 20% proximity to 52W highs
  │       15% delivery quality (institutional conviction)
  │     ⚠️ Top 25% filter: picks top-N from top quartile only
  │     ⚠️ ADX direction multiplier: below SMA50 → 0, below SMA20 → 0.5
  │     ⚠️ 1-year penalty: -30 pts for stocks down >30% over 1 year
  │     → DuckDB SQL on partitioned parquet + _delivery table
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

## 5b. RankBacktester

```
analytics/rank_backtester.py
  ├── load_ohlcv()         400-day lookback for ranking feature history
  ├── load_features_for_ranking()  DuckDB SQL: rel_strength + vol_intensity
  │                               + ADX trend + 52W highs + delivery_pct
  ├── rank_stocks()         Percentile rank → weighted composite score
  ├── generate_signals()    Rebalance every N days, top-N from top 25%
  ├── run_backtest()        Equal-weight basket, period-value compounding
  │                         Fees: 0.1% per trade, Sharpe/Sortino from period returns
  ├── grid_search_weights() Tests 5-factor weight combos on train set
  └── run_full_pipeline()   Train/test split → grid search → validate on test
```

**Backtest results (5-factor, Mar 2025–Mar 2026):**
- Total return: -6.33%, Annualized: -3.8%, Sharpe: -0.15, MaxDD: -24.18%
- 20 rebalance periods, 55% win rate
- Worst: Jan 30–Mar 2 2025 (-14.5%, NSE crash), Best: Mar 2–Apr 2 2025 (+11.6%)

## 6. Streamlit Dashboard

```
dashboard/app.py (Streamlit Command Center)
  ├── Overview Tab
  │     DuckDB stats (1,306 symbols, 4M rows, latest date)
  │     Sector distribution (top ranked stocks), Score histogram
  ├── Ranking Tab
  │     5-factor ranking (30/20/15/20/15 weights) → auto-refresh
  │     Top 25% filter applied, sector filter, min-score filter
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
