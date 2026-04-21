# Batch 1 Legacy Root Audit (`execution/`, `features/`, `publishers/`)

Date: 2026-04-18  
Scope: legacy-root retirement prep (behavior-preserving)

## 1) Import Usage Audit

### Static imports found

- `execution.*` imports are still used across runtime/test code, including:
  - `src/ai_trading_system/pipeline/stages/execute.py`
  - `src/ai_trading_system/interfaces/streamlit/research/app.py`
  - `src/ai_trading_system/interfaces/streamlit/research/data_access.py`
  - `src/ai_trading_system/domains/execution/*` (internal legacy-path references)
  - `tests/test_execution_foundation.py`, `tests/test_autotrader_execute_stage.py`, `tests/test_ml_workbench.py`, `tests/test_portfolio_workspace.py`, `tests/execute/*`

- `features.*` imports are still used across runtime/test code, including:
  - `collectors/daily_update_runner.py`
  - `src/ai_trading_system/domains/ingest/providers/dhan.py`
  - `src/ai_trading_system/domains/ingest/repair.py`
  - `tests/features/*`, `tests/test_feature_incremental.py`

- `publishers.*` imports are still used across runtime/test code, including:
  - `src/ai_trading_system/pipeline/stages/publish.py`
  - `src/ai_trading_system/domains/ranking/stock_scan.py`
  - `src/ai_trading_system/domains/ranking/sector_dashboard.py`
  - `run/daily_pipeline.py`, `run/publish_test.py`
  - `channel/dashboard_publisher.py`, `channel/portfolio_analyzer.py`, `channel/google_sheets_example.py`, `channel/telegram_example.py`
  - `tests/test_dashboard_publish_single_sheet.py`, `tests/test_quantstats_dashboard_publish.py`

### Dynamic/string module references

- `publishers.dashboard.*` appears in monkeypatch targets in tests:
  - `tests/test_dashboard_publish_single_sheet.py`
- `publishers.quantstats_dashboard.*` appears in monkeypatch targets in tests:
  - `tests/test_quantstats_dashboard_publish.py`

No explicit dynamic `importlib.import_module("execution...")` / `("features...")` / `("publishers...")` patterns were found in runtime code during this audit pass.

## 2) Folder File Classification

### `execution/`

- Real implementation / compatibility wrapper:
  - `execution/__init__.py` (contains `DhanExecutor` class and legacy exports)
- Shim-only modules:
  - `execution/service.py`
  - `execution/policies.py`
  - `execution/portfolio.py`
  - `execution/store.py`
  - `execution/models.py`
  - `execution/autotrader.py`
  - `execution/adapters/base.py`
  - `execution/adapters/paper.py`
  - `execution/adapters/dhan.py`
- Legacy re-export wrapper (non-shim style, but no business logic):
  - `execution/adapters/__init__.py`

### `features/`

- Real implementation:
  - `features/pattern_features.py`
- Entrypoint wrappers / scripts:
  - `features/compute_all_features.py`
  - `features/test_feature_store.py`
- Shim-only modules:
  - `features/feature_store.py`
  - `features/indicators.py`
  - `features/compute_sector_rs.py`
- Legacy re-export wrapper:
  - `features/__init__.py`

### `publishers/`

- Real implementation:
  - `publishers/dashboard.py`
- Shim-only modules:
  - `publishers/google_sheets.py`
  - `publishers/telegram.py`
  - `publishers/quantstats_dashboard.py`
- Legacy re-export wrapper:
  - `publishers/__init__.py`

## 3) Removal Safety (pre-rewrite status)

- `execution/`: **Not removable** yet (active imports + `__init__.py` compatibility class + wrappers).
- `features/`: **Not removable** yet (contains real implementation and active imports).
- `publishers/`: **Not removable** yet (contains real implementation and active imports).

## 4) Canonical Targets for Rewrite

- `execution.*` -> `ai_trading_system.domains.execution.*`
- `features.*` -> `ai_trading_system.domains.features.*`
- `publishers.*` -> `ai_trading_system.domains.publish.*` or `ai_trading_system.domains.publish.channels.*`

