# AI Trading System — NSE India

An AI-powered stock screening and backtesting system for the Indian NSE market, built on a modern data stack with RS (Relative Strength) analysis, Telegram reporting, and Google Sheets integration.

## Architecture (Iceberg-lite)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    CHANNEL & REPORTING LAYER                                  │
│  TelegramReporter │ GoogleSheetsManager │ AIAnalyzer (OpenRouter)            │
│  Portfolio SWOT Analysis │ QuantStats Tearsheets                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                    ANALYTICS & RANKING LAYER                                 │
│  AIQScreener │ RegimeDetector │ AlphaEngine │ RiskManager                    │
│  StockRanker │ EventBacktester │ Visualizations                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                    RS FEATURE LAYER (Sector & Stock)                        │
│  compute_sector_rs.py → sector_rs.parquet (500 days × 21 sectors)            │
│  Stock vs Sector RS → stock_vs_sector.parquet (500 days × 1000)              │
│  EW Index → ew_index.parquet                                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                    FEATURE STORE (Iceberg-lite)                              │
│  DuckDB SQL — RSI │ ADX │ SMA │ ATR │ BB │ ROC │ EMA │ MACD               │
│  Partitioned Parquet: feature=xxx/year=YYYY/month=MM/*.parquet              │
│  Append-only with atomic writes (temp → rename)                              │
│  Metadata tables: _file_registry, _ingestion_status, _snapshots              │
├──────────────────────────────────────────────────────────────────────────────┤
│                    DATA INGESTION & STORAGE LAYER                            │
│  DhanCollector │ DuckDB │ SQLite masterdata.db                               │
│  TokenManager (auto-renew via TOTP base32 secret)                            │
│  yfinance as fallback for current prices                                     │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
ai-trading-system/
├── analytics/                   # AI Analytics Package
│   ├── __init__.py
│   ├── feature_reader.py        # DuckDB-backed partitioned parquet reader
│   ├── regime_detector.py       # ADX-based TREND/MEAN_REV classification
│   ├── ranker.py                # Multi-factor ranking (5 factors)
│   ├── rank_backtester.py       # Cross-sectional backtest + grid search + train/test
│   ├── ml_engine.py             # XGBoost + walk-forward validation
│   ├── risk_manager.py          # ATR position sizing, portfolio risk budget
│   ├── backtester.py            # Event-driven backtest (5 strategies)
│   ├── visualizations.py         # Plotly charts, QuantStats tearsheets
│   └── screener.py              # AIQScreener — ties all layers
├── channel/                     # Reporting & Integration
│   ├── telegram_reporter.py     # Telegram bot for sending reports
│   ├── google_sheets_manager.py # Google Sheets OAuth2 integration
│   ├── portfolio_analyzer.py    # Portfolio with SWOT analysis
│   ├── ai_analyzer.py           # AI analysis via OpenRouter (free models)
│   └── oauth_flow.py            # Google OAuth2 helper
├── collectors/                  # Data ingestion
│   ├── __init__.py
│   ├── dhan_collector.py        # DhanHQ API → DuckDB (main, with token renew)
│   ├── yfinance_collector.py    # Yahoo Finance → current prices
│   ├── delivery_collector.py     # NSE MTO archive → delivery % → DuckDB + parquet
│   ├── token_manager.py          # Auto-renew expired Dhan tokens via TOTP
│   └── ingest_full.py           # Full inception-date ingestion (all symbols)
│
│   # RS Features (new)
│   └── compute_sector_rs.py     # Compute Sector & Stock RS features
├── config/
│   ├── __init__.py
│   └── settings.py
├── dashboard/
│   ├── __init__.py
│   └── app.py                   # Streamlit Command Center (4 tabs)
│       ├── Overview: stats, sector distribution, score histogram
│       ├── Ranking: adjustable weights, sector filter, stocks ranked
│       ├── Chart: interactive Plotly OHLCV + RSI + MACD + MAs + Supertrend
│       └── Portfolio: ATR-based position sizing, risk budget
├── data/                        # All data — DO NOT commit to git
│   ├── masterdata.db            # SQLite — stock_details (1,000 stocks) + nse500
│   ├── ohlcv.duckdb             # DuckDB — OHLCV catalog + metadata tables
│   │   ├── _catalog              # ~238K OHLCV rows, 1,000 symbols
│   │   ├── _snapshots           # Version snapshots for reproducibility
│   │   ├── _feature_registry    # Feature computation tracking
│   │   ├── _file_registry       # Iceberg-lite: tracks parquet files
│   │   ├── _ingestion_status    # Iceberg-lite: per-symbol update status
│   │   └── feat_*               # Feature tables (RSI, ADX, etc.)
│   │       ├── feat_rsi         # 238K rows, 1000 symbols
│   │       ├── feat_adx         # 237K rows, 1000 symbols
│   │       ├── feat_ema         # 238K rows, 1000 symbols
│   │       ├── feat_atr         # 238K rows, 1000 symbols
│   │       ├── feat_sma         # 47K rows, 919 symbols
│   │       ├── feat_macd        # 210K rows, 979 symbols
│   │       ├── feat_bb          # 220K rows, 997 symbols
│   │       └── feat_roc         # 237K rows, 1000 symbols
│   └── feature_store/           # Iceberg-lite partitioned storage
│       └── (reserved for future partitioned parquet)
├── docs/
│   ├── data-flow.md             # Detailed pipeline diagrams
│   ├── database.md              # Database schema documentation
│   ├── architecture.md          # Iceberg-lite architecture
│   └── implementation_roadmap.md # Implementation progress
├── features/                    # Feature computation
│   ├── __init__.py
│   ├── feature_store.py          # DuckDB SQL feature computation
│   │   ├── compute_rsi()        # RSI with start_date/end_date filtering
│   │   ├── compute_adx()        # ADX with incremental support
│   │   ├── compute_sma()        # Simple Moving Average
│   │   ├── compute_ema()        # Exponential Moving Average
│   │   ├── compute_macd()       # MACD
│   │   ├── compute_atr()         # Average True Range
│   │   ├── compute_bollinger_bands() # Bollinger Bands
│   │   ├── compute_roc()        # Rate of Change
│   │   ├── compute_incremental() # Incremental feature computation
│   │   ├── store_partitioned()   # Iceberg-lite partitioned storage
│   │   └── load_partitioned()   # Load with time travel support
│   ├── indicators.py            # Legacy pandas-based indicators
│   └── compute_all_features.py
├── run/                         # Pipeline runners
│   ├── daily_pipeline.py         # Main daily pipeline
│   └── full_pipeline.py          # Full backfill pipeline
├── models/                      # Saved XGBoost model files
├── reports/                     # Generated HTML reports
├── test/
│   ├── __init__.py
│   └── test_create_table.py
├── venv/                        # Python virtual environment
├── .env                         # API keys + DHAN_TOTP (base32 secret)
├── .env.example
├── .gitignore
├── main.py
├── run_dashboard.ps1           # Start Streamlit dashboard
├── run_full_rank.ps1           # Full ranking pipeline
├── run_ingest_full.ps1          # Full OHLCV ingestion
├── run_daily_update.ps1         # Daily EOD update runner
├── setup_daily_task.ps1         # Windows Task Scheduler setup
└── requirements.txt
```

## Tech Stack

| Component | Technology |
|---|---|
| Database (OHLCV) | DuckDB >= 0.8.0 |
| Database (Master) | SQLite3 (built-in) |
| DataFrames | pandas >= 2.0.0 |
| ML | XGBoost >= 2.0.0 |
| Visualization | Plotly >= 5.17.0 |
| Dashboard | Streamlit >= 1.28.0 |
| API | DhanHQ (python-dhanhq), Yahoo Finance (yfinance) |
| Indicators | DuckDB SQL (vectorized, fast) |
| TOTP | pyotp |

## Data Flow

See [docs/data-flow.md](docs/data-flow.md) for the full pipeline diagrams.

### TL;DR

1. **Symbol Setup**: `stock_details` table (1,000 symbols) with sector mapping
2. **OHLCV Ingestion**: `DhanCollector.ingest()` → `ohlcv.duckdb::_catalog` (238K rows, 1000 symbols, Mar 2025 → Mar 2026)
3. **Feature Computation**: DuckDB SQL → `ohlcv.duckdb::feat_*` tables (1.67M total rows)
   - RSI, ADX, ATR, EMA, ROC: ~238K rows each
   - SMA, MACD, BB: ~200K rows each
4. **Incremental Updates**: `compute_incremental()` only computes new rows since last run
5. **Daily Pipeline**: `run/daily_pipeline.py` → OHLCV update + feature compute + Google Sheets
6. **Full Ranking**: Stock screener → ranked CSV output

## Iceberg-lite Architecture

### What is Iceberg-lite?

A simplified approach that replicates 90% of Apache Iceberg's benefits using:

1. **Partitioned Parquet**: `feature=xxx/year=YYYY/month=MM/*.parquet`
2. **Metadata Tables**: Track files, snapshots, ingestion status
3. **Atomic Writes**: Write to temp → rename (atomic on most systems)
4. **Append-only**: Never delete + insert, always append + deduplicate

### Key Components

```python
# 1. Metadata tables (in ohlcv.duckdb)
_file_registry       # Tracks all parquet files
_ingestion_status   # Per-symbol update status
_snapshots          # Version snapshots

# 2. Partitioned storage
data/feature_store/
  rsi/year=2025/month=03/RELIANCE.parquet
  rsi/year=2025/month=04/RELIANCE.parquet

# 3. Incremental computation
fs.compute_incremental('rsi', 'RELIANCE', 'NSE', fs.compute_rsi)
# Only computes rows after last date + lookback for rolling indicators

# 4. Time travel
fs.load_partitioned('rsi', snapshot_id=42)  # Load from specific snapshot
```

### Benefits

| Feature | Before | After |
|---------|--------|-------|
| Feature files | 8,800+ parquet | DuckDB tables |
| Updates | Full rewrite | Incremental append |
| Reproducibility | None | Snapshot-based |
| Atomic writes | No | Yes (temp→rename) |
| Query speed | Slow | Fast (DuckDB SQL) |

## DuckDB Quirks (for debugging)

- **Use PowerShell `.ps1` wrappers** — DuckDB hangs via bash/PWSH `-c`
- **ON CONFLICT is supported** — use for upsert instead of DELETE+INSERT
- **`EXPONENTIAL_MOVING_AVERAGE()` function does not exist** — use pandas fallback
- **Cannot bind DataFrame as query parameter** — use `CREATE TEMP VIEW` pattern
- **`NULLIF` not available** — do null-safe division in Python after fetch
- **`QUALIFY` clause** requires columns in the same CTE's SELECT/WINDOW — wrap in subquery first
- **Partitioned parquet**: use `read_parquet('dir/*.parquet')` with forward-slash paths

## Key Paths

| Asset | Path |
|---|---|
| Master DB | `data/masterdata.db` |
| OHLCV DuckDB | `data/ohlcv.duckdb` |
| Feature Store | `data/feature_store/` |
| Rankings | `rankings_latest.csv` |
| Reports | `reports/` |

## Environment Variables

```env
DHAN_API_KEY=your_api_key
DHAN_CLIENT_ID=your_client_id
DHAN_ACCESS_TOKEN=your_access_token
DHAN_REFRESH_TOKEN=your_refresh_token
DHAN_PIN=your_pin
DHAN_TOTP=YOUR_BASE32_TOTP_SECRET   # 32-char base32 secret for auto token renewal
```

## Usage

```powershell
# Dashboard (Streamlit)
powershell -File "run_dashboard.ps1"

# Daily pipeline (OHLCV + Features + Google Sheets)
powershell -File "run_daily_update.ps1"

# Full ranking of all stocks
& "venv/Scripts/python.exe" -c "from analytics.screener import AIQScreener; s = AIQScreener(); print(s.screen(top_n=20))"
```

## Known State (March 2026)

- **OHLCV data**: 1,000 symbols, 238,607 rows, Mar 2025 → Mar 2026
- **Feature store**: 1.67M rows across 8 technical indicators
- **Dhan API**: Returning corrupted data since March 19, 2026
- **Workaround**: Using yfinance for current prices

## Data Quality Issue (March 2026)

**Problem:** DhanHQ API returning wrong prices from March 19, 2026 onwards.

**Symptoms:**
- TCS: Actual ~₹2,400 | API returns ~₹253 (10x wrong)
- Affects ALL 1,000 stocks

**Current Workaround:**
- Using yfinance for current prices
- Historical OHLCV from DuckDB (valid up to March 18, 2026)

## Documentation

- [docs/database.md](docs/database.md) - Database schema
- [docs/architecture.md](docs/architecture.md) - Iceberg-lite architecture
- [docs/implementation_roadmap.md](docs/implementation_roadmap.md) - Implementation progress
- [docs/data-flow.md](docs/data-flow.md) - Pipeline diagrams
