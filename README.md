# AI Trading System — NSE India

An AI-powered stock screening and backtesting system for the Indian NSE market, built on a three-layer modern data stack.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS & RANKING LAYER                     │
│  AIQScreener │ RegimeDetect │ MLEngine │ RiskManager            │
│  Ranker      │ Backtester  │ Visualizations                     │
├──────────────┴──────────────┴───────────────────────────────────┤
│                   FEATURE STORE & COMPUTE LAYER                  │
│  DuckDB SQL — RSI │ ADX │ SMA │ EMA │ MACD │ ATR │ BB │ ROC │ ST│
│  → data/feature_store/<feature>/NSE/<symbol>.parquet            │
├─────────────────────────────────────────────────────────────────┤
│                   DATA INGESTION & STORAGE LAYER                 │
│  DhanCollector │ DuckDB (ACID+TT) │ SQLite masterdata.db         │
└─────────────────────────────────────────────────────────────────┘
```

## File Structure

```
ai-trading-system/
├── ai_trading_system/          # Core DB utilities
│   ├── __init__.py
│   └── match_and_create_table.py   # CSV → SQLite stock_details table
├── analytics/                   # AI Analytics Package (NEW)
│   ├── __init__.py
│   ├── regime_detector.py        # ADX-based TREND/MEAN_REV classification
│   ├── ranker.py                 # Multi-factor weighted ranking (4 factors)
│   ├── ml_engine.py              # XGBoost + walk-forward validation
│   ├── risk_manager.py           # ATR position sizing, portfolio risk budget
│   ├── backtester.py             # Event-driven backtest (5 strategies)
│   ├── visualizations.py         # Plotly charts, QuantStats tearsheets
│   ├── screener.py               # AIQScreener — ties all layers
│   └── test_analytics.py
├── collectors/                   # Data ingestion
│   ├── __init__.py
│   ├── dhan_collector.py         # DhanHQ API → DuckDB OHLCV (main)
│   ├── masterdata.py
│   ├── nse_collector.py
│   └── zerodha_sector_collector.py
├── config/
│   ├── __init__.py
│   └── settings.py
├── dashboard/
│   ├── __init__.py
│   └── app.py                    # ⭐ Streamlit Command Center (4 tabs)
│       ├── Overview: market stats, sector distribution, score histogram
│       ├── Ranking: adjustable weights, DuckDB query, filters, select stock
│       ├── Chart: interactive Plotly OHLCV + RSI + MACD + MAs + Supertrend
│       └── Portfolio: ATR-based position sizing, risk budget

├── data/                         # All data — DO NOT commit to git
│   ├── masterdata.db             # SQLite — stock_details + symbols
│   ├── ohlcv.duckdb              # DuckDB — OHLCV catalog + snapshots
│   ├── feature_store/            # Feature Parquet store (150 MB)
│   │   ├── adx/NSE/<symbol>.parquet
│   │   ├── atr/NSE/<symbol>.parquet
│   │   ├── bb/NSE/<symbol>.parquet
│   │   ├── ema/NSE/<symbol>.parquet
│   │   ├── fundamental/NSE/<symbol>.parquet
│   │   ├── macd/NSE/<symbol>.parquet
│   │   ├── roc/NSE/<symbol>.parquet
│   │   ├── rsi/NSE/<symbol>.parquet
│   │   ├── sma/NSE/<symbol>.parquet
│   │   └── supertrend/NSE/<symbol>.parquet
│   ├── features/run_000001/      # Immutable OHLCV snapshots (Parquet)
│   ├── backtests/
│   ├── raw/
│   └── signals/
├── features/                     # Feature computation
│   ├── __init__.py
│   ├── feature_store.py         # DuckDB SQL feature computation (9 indicators)
│   ├── indicators.py            # Legacy pandas-based indicators
│   ├── compute_all_features.py
│   └── test_feature_store.py
├── legacy/                       # Superseded — use analytics/ instead
│   ├── ai/signal_ranker.py
│   ├── backtesting/strategy_runner.py
│   ├── execution/dhan_executor.py
│   ├── risk/risk_manager.py
│   └── signals/pattern_detector.py
├── models/                       # Saved XGBoost model files
├── reports/                       # Generated HTML reports
├── test/
│   ├── __init__.py
│   └── test_create_table.py
├── venv/                          # Python virtual environment
├── .env                           # API keys (not committed)
├── main.py
├── run_pipeline.py               # Legacy pipeline
└── requirements.txt
```

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Database (OHLCV) | DuckDB | >= 0.8.0 |
| Database (Master) | SQLite3 | built-in |
| DataFrames | pandas | >= 2.0.0 |
| ML | XGBoost | >= 2.0.0 |
| Visualization | Plotly | >= 5.17.0 |
| Dashboard | Streamlit | >= 1.28.0 |
| API | DhanHQ | python-dhanhq |
| Indicators | pandas-ta, talib-binary | |
| Backtesting | vectorbt | >= 0.25.0 |
| Stats | quantstats | |

## Data Flow

See [docs/data-flow.md](docs/data-flow.md) for the full pipeline diagrams.

### TL;DR

1. **Symbol Setup**: `all-stock-non-sme.csv` (1346 rows) → `masterdata.db::stock_details` (1306 matched NSE symbols)
2. **OHLCV Ingestion**: `DhanCollector.ingest()` → `ohlcv.duckdb::_catalog` + Parquet snapshots (342,172 rows)
3. **Feature Computation**: `FeatureStore.compute_all_features()` → 9 indicators → `feature_store/<feature>/NSE/<symbol>.parquet` (2.76M rows, 150 MB)
4. **Screener Pipeline**: `AIQScreener.screen()` → regime + rank + ML signals + risk sizing + backtest + report

## DuckDB Quirks (important for debugging)

- `ON CONFLICT` not supported — use `DELETE + INSERT` pattern
- `last_insert_rowid()` not supported — use `nextval()` before INSERT
- `QUALIFY` clause requires columns in the same CTE's SELECT/WINDOW — wrap window aggregates in subquery before QUALIFY
- DuckDB via bash hangs on `connect(db_path)` — **use PowerShell wrapper `.ps1` files**
- `NULLIF` not available — do null-safe division in Python after fetch

## Key Paths

| Asset | Path |
|---|---|
| Source CSV | `C:\Users\DIO\Opencode\all-stock-non-sme.csv` |
| Master DB | `C:\Users\DIO\Opencode\ai-trading-system\data\masterdata.db` |
| OHLCV DuckDB | `C:\Users\DIO\Opencode\ai-trading-system\data\ohlcv.duckdb` |
| Feature Store | `C:\Users\DIO\Opencode\ai-trading-system\data\feature_store\` |
| Reports | `C:\Users\DIO\Opencode\ai-trading-system\reports\` |
| Venv | `C:\Users\DIO\Opencode\ai-trading-system\venv` |

## Environment Variables

```env
DHAN_API_KEY=your_api_key
DHAN_CLIENT_ID=your_client_id
DHAN_ACCESS_TOKEN=your_access_token
```

## Usage

```powershell
# Activate venv
.\venv\Scripts\Activate.ps1

# Run full screener pipeline
python -c "from analytics.screener import AIQScreener; s = AIQScreener(); result = s.screen(top_n=20); print(result)"

# Ingest OHLCV data
python -c "from collectors.dhan_collector import DhanCollector; c = DhanCollector(); c.ingest()"

# Compute all features
python features/compute_all_features.py

# Run tests
pytest test/ -v
```

## Known State

- Market regime: **RANGE_BOUND** (0% trending) → use MEAN_REV strategy
- Currently using synthetic OHLCV data (demo mode when API unavailable)
- 1,306 symbols with full feature coverage

## Daily EOD Update

After market close (3:30 PM IST), run the daily pipeline to fetch today's candles and update features:

```powershell
# OHLCV + Features (full update)
powershell -ExecutionPolicy Bypass -File "run_daily_update.ps1"

# OHLCV only (faster, run features separately)
powershell -ExecutionPolicy Bypass -File "run_daily_update.ps1" -SymbolsOnly

# Features only (recompute all indicators)
powershell -ExecutionPolicy Bypass -File "run_daily_update.ps1" -FeaturesOnly

# Force overwrite (re-fetch all rows)
powershell -ExecutionPolicy Bypass -File "run_daily_update.ps1" -Force
```

**Stale data handling:**
- System automatically detects gaps (weekends, holidays, missed days)
- Fetches from `(last_stored_date + 1)` onwards per symbol
- Symbols with no prior data get 7-day lookback
- Summary report shows: up-to-date / stale (>1 day gap) / no-data counts
- Dhan API rate limits apply — 2 batches of 700 symbols

**Schedule automatically (Windows Task Scheduler):**
```powershell
.\setup_daily_task.ps1   # Sets Mon-Fri at 3:45 PM IST
```
- Average full pipeline runtime: ~4 seconds
