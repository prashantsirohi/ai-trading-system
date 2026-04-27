# Module Map

## Current runtime source of truth

### `run/`

Label:
- `current operational`

Owns:
- orchestrator entrypoints
- stage wrappers
- preflight checks
- publish retry and alert helpers

Key files:
- `ai_trading_system.pipeline.orchestrator`
- `ai_trading_system.pipeline.daily_pipeline`
- `run/stages/*.py`
- `ai_trading_system.pipeline.preflight`
- `ai_trading_system.domains.publish.delivery_manager`
- `ai_trading_system.pipeline.publish_test`

### `collectors/`

Label:
- `current operational`

Owns:
- market-data ingestion
- delivery ingestion
- master-data bootstrap
- Dhan auth helpers
- repair and reset-reingest tools

Key files:
- `ai_trading_system.domains.ingest.daily_update_runner`
- `ai_trading_system.domains.ingest.providers.nse`
- `ai_trading_system.domains.ingest.providers.dhan`
- `ai_trading_system.domains.ingest.delivery`
- `ai_trading_system.domains.ingest.masterdata`
- `ai_trading_system.domains.ingest.repair`
- `ai_trading_system.domains.ingest.reset_reingest_validate`
- `ai_trading_system.domains.ingest.token_manager`
- `ai_trading_system.domains.ingest.auth_doctor`

### `features/`

Label:
- `current operational`

Owns:
- technical indicator computation
- feature-store persistence
- sector relative-strength artifacts

Key files:
- `ai_trading_system.domains.features.feature_store`
- `features/compute_sector_rs.py`
- `features/indicators.py`

### `analytics/`

Label:
- `current operational`
- `current research`

Owns:
- ranking
- trust summarization
- DQ evaluation
- registry and control-plane access
- risk sizing
- ML dataset, training, shadow, and evaluation helpers

Operationally critical areas:
- `analytics/ranker.py`
- `analytics/data_trust.py`
- `analytics/dq/`
- `analytics/registry/`
- `analytics/risk_manager.py`

Research-heavy areas:
- `analytics/alpha/`
- `analytics/lightgbm_*`
- `analytics/training_dataset.py`
- `analytics/backtester.py`
- `analytics/rank_backtester.py`
- `analytics/patterns/`

### `execution/`

Label:
- `current operational`

Owns:
- execution adapters
- order and fill models
- execution store
- paper execution service and portfolio logic

Current limitation:
- live broker adapters exist, but the orchestrated stage uses the paper path only

Key files:
- `execution/store.py`
- `execution/service.py`
- `execution/autotrader.py`
- `execution/adapters/paper.py`
- `execution/adapters/dhan.py`

### `publishers/`

Label:
- `current operational`

Owns:
- Google Sheets, dashboard, Telegram, and QuantStats delivery adapters

Current limitation:
- several modules hardcode operational paths and should be treated as operational-only

### `channel/`

Label:
- `current operational`
- `legacy fallback`

Owns:
- ranking side outputs and publish-adjacent transforms such as breakout scan, sector dashboard, stock scan, portfolio analysis, and Telegram reporting

Current operationally used files:
- `channel/breakout_scan.py`
- `channel/stock_scan.py`
- `channel/sector_dashboard.py`
- `channel/portfolio_analyzer.py`
- `channel/telegram_reporter.py`
- `channel/google_sheets_manager.py`

Current limitation:
- `stock_scan.py` and `sector_dashboard.py` hardcode operational feature-store and master-data paths
- other script-era helpers remain in the directory but are not part of the canonical operational path

### `ui/`

Label:
- `current operational`
- `current research`

Owns:
- FastAPI execution backend
- backend data/read-model services used by the React V2 dashboard

Key areas:
- `src/ai_trading_system/ui/execution_api/`: `current operational`
- `src/ai_trading_system/ui/execution_api/services/`: `current operational`

### `web/execution-console-v2/ai-trading-dashboard-starter/`

Label:
- `current operational`

Owns:
- standalone React frontend for the FastAPI execution backend

Current limitation:
- not served by FastAPI; requires its own Vite workflow

## Research-only entrypoints

### `research/`

Label:
- `current research`

Owns:
- dataset prep
- training and evaluation entrypoints
- recipe runner
- shadow-monitor CLI flows

## Shared runtime foundation

### `core/`

Label:
- `current operational`
- `current research`

Owns:
- shared contracts
- environment loading
- bootstrap and path helpers
- logging and runtime config dataclasses

### `utils/`

Label:
- `current operational`
- `current research`

Owns:
- domain path resolution
- lower-level env and logging helpers
- small data and filesystem helpers

## Compatibility, migration, and legacy areas

### `dashboard/`

Label:
- `legacy fallback`

Role:
- compatibility wrappers that re-export current UI modules
- not a separate product surface

### `config/`

Label:
- `scaffold / incomplete`

Role:
- partial legacy configuration layer
- `config/settings.py` is not the canonical runtime configuration source

### `main.py`

Label:
- `legacy fallback`

Role:
- compatibility shim only
- exits with explicit deprecation guidance to `python -m ai_trading_system.pipeline.orchestrator`
- do not use for current operations

## Generated state directories

### `data/`

Label:
- `current operational`
- `current research`

Role:
- runtime state and local data stores
- source of truth for local run state, not for code behavior

### `reports/`

Label:
- `current operational`
- `current research`

Role:
- generated artifacts and reports

### `models/`

Label:
- `current operational`
- `current research`

Role:
- generated model artifacts

## Debugging map

If you are tracing a live operator issue, start in this order:
1. `run/`
2. `collectors/`
3. `analytics/data_trust.py` and `analytics/dq/`
4. `publishers/` and current operational files in `channel/`
5. `ui/services/` and the relevant operator surface

If you are tracing ML or offline research behavior, start in this order:
1. `research/`
2. `analytics/alpha/`
3. `analytics/registry/`
4. `src/ai_trading_system/ui/execution_api/services/ml_workbench.py`
