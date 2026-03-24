# AI Trading System — NSE India

An AI-powered stock screening and backtesting system for the Indian NSE market, built on a modern data stack with RS (Relative Strength) analysis, Telegram reporting, and Google Sheets integration.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    CHANNEL & REPORTING LAYER                      │
│  TelegramReporter │ GoogleSheetsManager │ AIAnalyzer (OpenRouter)│
│  Portfolio SWOT Analysis │ QuantStats Tearsheets                  │
├──────────────────────────────────────────────────────────────────┤
│                    ANALYTICS & RANKING LAYER                       │
│  AIQScreener │ RegimeDetector │ AlphaEngine │ RiskManager         │
│  StockRanker │ EventBacktester │ Visualizations                  │
├──────────────────────────────────────────────────────────────────┤
│                    RS FEATURE LAYER (Sector & Stock)              │
│  compute_sector_rs.py → sector_rs.parquet (500 days × 21 sectors)│
│  Stock vs Sector RS → stock_vs_sector.parquet (500 days × 1000)  │
│  EW Index → ew_index.parquet                                      │
├──────────────────────────────────────────────────────────────────┤
│                    FEATURE STORE & COMPUTE LAYER                   │
│  DuckDB SQL — RSI │ ADX │ SMA │ ATR │ BB │ ROC │ Supertrend     │
│  (6 partitioned parquet files each)                               │
│  Pandas — EMA │ MACD (per-symbol parquet)                         │
│  → data/feature_store/<feature>/NSE/*.parquet                    │
├──────────────────────────────────────────────────────────────────┤
│                    DATA INGESTION & STORAGE LAYER                 │
│  DhanCollector │ DuckDB (ACID+TT) │ SQLite masterdata.db         │
│  TokenManager (auto-renew via TOTP base32 secret)                 │
└──────────────────────────────────────────────────────────────────┘
```

## File Structure

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
│   ├── delivery_collector.py     # NSE MTO archive → delivery % → DuckDB + parquet
│   , token_manager.py         # Auto-renew expired Dhan tokens via TOTP
│   , ingest_full.py           # Full inception-date ingestion (all 1,306 symbols)
│   , compute_features_batch.py # Fast DuckDB COPY feature computation
│   , delete_stale.py         # Remove stale 1-year data, re-ingest full
│   , run_full_rank.py        # Full ranking of all stocks → CSV
│   └── daily_update_runner.py  # CLI runner for daily EOD pipeline
│
│   # RS Features (new)
│   └── compute_sector_rs.py    # Compute Sector & Stock RS features
├── config/
│   ├── __init__.py
│   └── settings.py
├── dashboard/
│   ├── __init__.py
│   └── app.py                  # Streamlit Command Center (4 tabs)
│       ├── Overview: stats, sector distribution, score histogram
│       ├── Ranking: adjustable weights, sector filter, 614 stocks ranked
│       ├── Chart: interactive Plotly OHLCV + RSI + MACD + MAs + Supertrend
│       └── Portfolio: ATR-based position sizing, risk budget
├── data/                        # All data — DO NOT commit to git
│   ├── masterdata.db           # SQLite — stock_details (1,000 stocks) + nse500
│   ├── ohlcv.duckdb            # DuckDB — OHLCV catalog + delivery table
│   │   ├── _catalog            # ~3.17M OHLCV rows, 1,000 symbols
│   │   └── _delivery           # 657,506 delivery records, 2,606 symbols
│   ├── raw/NSE_MTO/            # Raw NSE MTO .DAT files (delivery data)
│   └── feature_store/           # Feature Parquet store
│       ├── all_symbols/         # RS features (new)
│       │   ├── sector_rs.parquet     # 500 days × 21 sectors
│       │   ├── stock_vs_sector.parquet # 500 days × 1,000 stocks
│       │   └── ew_index.parquet       # Equal-weight index
│       ├── adx/NSE/             # 6 DuckDB-partitioned files
│       ├── atr/NSE/             # 6 DuckDB-partitioned files
│       ├── bb/NSE/              # 6 DuckDB-partitioned files
│       ├── delivery/NSE/         # 6 DuckDB-partitioned files
│       ├── ema/NSE/             # 1,306 per-symbol files
│       ├── fundamental/NSE/      # 1,306 per-symbol files
│       ├── macd/NSE/            # 1,306 per-symbol files
│       ├── roc/NSE/             # 6 DuckDB-partitioned files
│       ├── rsi/NSE/             # 6 DuckDB-partitioned files
│       ├── sma/NSE/             # 6 DuckDB-partitioned files
│       └── supertrend/NSE/       # 6 DuckDB-partitioned files
├── docs/
│   └── data-flow.md             # Detailed pipeline diagrams
├── features/                    # Feature computation
│   ├── __init__.py
│   ├── feature_store.py          # DuckDB SQL feature computation
│   ├── indicators.py             # Legacy pandas-based indicators
│   ├── compute_all_features.py
│   └── test_feature_store.py
├── legacy/                      # Superseded — use analytics/ instead
│   ├── ai/signal_ranker.py
│   ├── backtesting/strategy_runner.py
│   ├── execution/dhan_executor.py
│   ├── risk/risk_manager.py
│   └── signals/pattern_detector.py
├── models/                      # Saved XGBoost model files
├── reports/                     # Generated HTML reports
├── test/
│   ├── __init__.py
│   └── test_create_table.py
├── venv/                        # Python virtual environment
├── .env                         # API keys + DHAN_TOTP (base32 secret, not committed)
├── .env.example
├── .gitignore
├── main.py
├── run_dashboard.ps1           # Start Streamlit dashboard
├── run_full_rank.ps1           # Full ranking pipeline
├── run_ingest_full.ps1         # Full OHLCV ingestion
├── run_daily_update.ps1        # Daily EOD update runner (PowerShell)
├── setup_daily_task.ps1        # Windows Task Scheduler setup
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
| API | DhanHQ (python-dhanhq) |
| Indicators | DuckDB SQL (vectorized, fast), pandas (EMA/MACD fallback) |
| TOTP | pyotp |

## Data Flow

See [docs/data-flow.md](docs/data-flow.md) for the full pipeline diagrams.

### TL;DR

1. **Symbol Setup**: `all-stock-non-sme.csv` (1,346 rows) → `masterdata.db::stock_details` (1,306 matched NSE symbols)
2. **OHLCV Ingestion**: `DhanCollector.ingest()` → `ohlcv.duckdb::_catalog` (4,029,570 rows, 1,306 symbols, inception→2026-03-18)
3. **Feature Computation**: `compute_features_batch.py` → 9 indicators → `feature_store/` (791.6 MB total)
   - DuckDB-partitioned (fast): RSI, SMA, ATR, ADX, BB, ROC, Supertrend (6 files each)
   - Per-symbol parquet (pandas fallback): EMA, MACD (1,306 files each)
4. **Screener Pipeline**: `AIQScreener.screen()` → regime + 5-factor rank (+ delivery + 1yr penalty) + ML signals + risk sizing + backtest + report
   - 5-factor: relative_strength 30%, volume_intensity 20%, trend_persistence 15%, proximity_highs 20%, delivery_pct 15%
   - Top 25% filter: picks top-N from top quartile stocks only
5. **Daily Update**: `run_daily_update.ps1` → incremental OHLCV fetch + feature recompute
6. **Full Ranking**: `run_full_rank.ps1` → 614 stocks ranked in ~13s → `rankings_latest.csv`

## DuckDB Quirks (important for debugging)

- **Use PowerShell `.ps1` wrappers** — DuckDB hangs via bash/PWSH `-c`
- **ON CONFLICT is supported** — use for upsert instead of DELETE+INSERT
- **`EXPONENTIAL_MOVING_AVERAGE()` function does not exist** — EMA/MACD use pandas fallback (slow per-symbol writes)
- **Cannot bind DataFrame as query parameter** — use `CREATE TEMP VIEW` pattern
- **`NULLIF` not available** — do null-safe division in Python after fetch
- **`QUALIFY` clause** requires columns in the same CTE's SELECT/WINDOW — wrap in subquery first
- **Partitioned parquet**: use `read_parquet('dir/*.parquet')` with forward-slash paths

## Key Paths

| Asset | Path |
|---|---|
| Source CSV | `C:\Users\DIO\Opencode\all-stock-non-sme.csv` |
| Master DB | `C:\Users\DIO\Opencode\ai-trading-system\data\masterdata.db` |
| OHLCV DuckDB | `C:\Users\DIO\Opencode\ai-trading-system\data\ohlcv.duckdb` |
| Feature Store | `C:\Users\DIO\Opencode\ai-trading-system\data\feature_store\` |
| Rankings | `C:\Users\DIO\Opencode\ai-trading-system\rankings_latest.csv` |
| Reports | `C:\Users\DIO\Opencode\ai-trading-system\reports\` |
| Venv | `C:\Users\DIO\Opencode\ai-trading-system\venv` |

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

# Full ranking of all 1000 stocks → rankings_latest.csv
powershell -File "run_full_rank.ps1"

# Full OHLCV ingestion from inception dates
powershell -File "run_ingest_full.ps1"

# Daily EOD update (after market close)
powershell -File "run_daily_update.ps1"

# Full screener pipeline
& "venv/Scripts/python.exe" -c "from analytics.screener import AIQScreener; s = AIQScreener(); print(s.screen(top_n=20))"
```

## Daily EOD Update

After market close (3:30 PM IST), run the daily pipeline to fetch today's candles and update features:

```powershell
# OHLCV + Features (full update — recommended after market close)
powershell -File "run_daily_update.ps1"

# OHLCV only (faster, run features separately after)
powershell -File "run_daily_update.ps1" -SymbolsOnly

# Features only (recompute all indicators)
powershell -File "run_daily_update.ps1" -FeaturesOnly

# Force overwrite (re-fetch all rows, ignores existing dates)
powershell -File "run_daily_update.ps1" -Force
```

**Stale data handling:**
- System reads last stored date per symbol from DuckDB automatically
- Fetches from `(last_stored_date + 1)` → today (handles weekends, holidays, gaps)
- Symbols with no prior data get inception-date lookback (2001-2004)
- Status report after each run: up-to-date / stale (>1 day gap) / no-data counts
- Dhan API rate limits apply — 2 batches of 700 symbols

**Schedule automatically (Windows Task Scheduler):**
```powershell
.\setup_daily_task.ps1   # Sets Mon-Fri at 3:45 PM IST
```

## Known State

- **OHLCV data**: 1,306 symbols, 4,029,570 rows, inception (2001-2004) → 2026-03-18
- **Delivery data**: 657,506 records from NSE MTO archive (2025-01-01 → 2026-03-19), 2,606 symbols
- **Feature store**: ~792 MB total, all indicators + delivery features computed
- **Rankings**: 614 stocks scored with 5-factor model
- **Top ranked (5-factor, Mar 2026)**: IFGLEXPOR (87.58), VERANDA (87.45), ASALCBR (84.22), AJMERA (83.92), ELECON (81.74)
- **Backtest (5-factor, Mar 2025–Mar 2026)**: -6.33% total, -3.8% annualized, Sharpe -0.15, MaxDD -24.18%, 55% win rate, 20 rebalance periods
- **ML top features**: Bollinger Bands, Supertrend, ATR (XGBoost trained on 5 symbols, 2024-2025)
- **Backtest**: BREAKOUT and MEAN_REV strategies verified with real data

## NSE Delivery Data

NSE switched from old bhavcopy CSV format (with delivery columns) to UDiFF format (no delivery). Working source: **NSE MTO Archive** at `https://nsearchives.nseindia.com/archives/equities/mto/MTO_{DDMMYYYY}.DAT`

- Plain text `.DAT` format, comma-separated
- Only `EQ` series is relevant; others (GS, TB, bonds, etc.) are filtered out
- Columns: symbol, QtyTraded (volume), DelivQty, %Deliv (delivery %)
- Covers both 2025 and 2026 dates (old bhavcopy only went to Apr 2024)

---

## TODO / Known Issues

### GitHub Actions Setup (In Progress)

**Current Problem:**
- GitHub Actions workflow runs but lacks cached feature data on first run
- Feature data (~800MB) needs to be shared between runs
- Solutions being explored: Cache vs Artifacts

**Steps to resolve:**
1. First run locally: `python run/daily_pipeline.py --force`
2. This creates local feature data in `data/feature_store/`
3. GitHub Actions can then use cache for subsequent runs

**Workaround:**
- Run workflow with `ENV=github` locally first to generate feature data
- Or manually upload initial feature data to GitHub Releases
