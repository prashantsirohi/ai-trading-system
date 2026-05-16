# Batch 4 Legacy Root Audit (`services/`, `channel/`, `utils/`)

Date: 2026-04-18  
Scope: legacy-path retirement audit + post-rewrite verification

## 1) Direct Import Audit

### `services.*`

- Pre-change: direct runtime/test imports existed in `collectors/`, `src/ai_trading_system/domains/*`, `src/ai_trading_system/pipeline/stages/*`, and multiple `tests/*`.
- Post-change: no direct imports remain (`rg -n "\b(from|import)\s+services(\.|\b)"` returns no legacy-path usage).

### `channel.*`

- Pre-change: direct runtime/test imports existed in `run/daily_pipeline.py`, ranking/publish domain modules, tests, and `channel/portfolio_example.py`.
- Post-change: no direct imports remain (`rg -n "\b(from|import)\s+channel(\.|\b)"` returns none).

### `utils.*`

- Pre-change: direct runtime imports existed in collectors, ranking/publish modules, and `run/daily_pipeline.py`.
- Post-change: no direct imports remain (`rg -n "\b(from|import)\s+utils(\.|\b)"` returns none).

## 2) Dynamic/String Reference Audit

- Pre-change: test monkeypatch targets used `channel.*` string paths.
- Post-change: no runtime/test string references remain (`rg -n "['\"](services|channel|utils)\.[^'\"]+['\"]"` returns only this document).
- No `importlib.import_module("services.*"|"channel.*"|"utils.*")` patterns found.

## 3) Folder File Classification (Post-Change)

### `services/`

- Shim-only modules:
  - `services/execute/{candidate_builder.py,entry_policy.py,exit_policy.py}`
  - `services/features/orchestration.py`
  - `services/ingest/{orchestration.py,benchmark_ingest.py}`
  - `services/rank/{orchestration.py,dashboard_payload.py,composite.py,contracts.py,eligibility.py,factors.py,input_loader.py}`
  - `services/publish/{publish_payloads.py,signal_classification.py,telegram_summary_builder.py}`
- Wrapper/re-export modules:
  - `services/__init__.py`
  - `services/*/__init__.py`
- Real implementation remaining: **none**.

### `channel/`

- Shim/wrapper modules:
  - `channel/{breakout_scan.py,stock_scan.py,sector_dashboard.py,telegram_reporter.py,dashboard_publisher.py,google_sheets_manager.py,portfolio_analyzer.py,ai_analyzer.py,oauth_flow.py}`
- Entrypoint/example wrappers:
  - `channel/{google_sheets_example.py,portfolio_example.py,telegram_example.py}`
- Non-code artifact:
  - `channel/reports/portfolio_holdings.csv`
- Real implementation remaining: **none** (domain implementations are now under `src/ai_trading_system/domains/...`).

### `utils/`

- Shim/re-export modules:
  - `utils/{env.py,data_domains.py,logger.py,data_config.py,pyarrow_utils.py,compact_features.py,__init__.py}`
- Real implementation remaining: **none**.

## 4) Canonical Rewrite Summary

- `services.ingest.*` -> `ai_trading_system.domains.ingest.*`
- `services.features.*` -> `ai_trading_system.domains.features.*`
- `services.rank.*` -> `ai_trading_system.domains.ranking.*`
- `services.execute.*` -> `ai_trading_system.domains.execution.*`
- `services.publish.*` -> `ai_trading_system.domains.publish.*`
- `channel.*` ranking/publish modules -> `ai_trading_system.domains.ranking.*` / `ai_trading_system.domains.publish.*`
- `utils.env` / `utils.data_config` -> `ai_trading_system.platform.utils.*`

## 5) Removal Safety Decision

- `services/`: **retained for compatibility shims** (internal repo imports fully retired).
- `channel/`: **retained for compatibility shims + wrapper entrypoint examples/artifact file**.
- `utils/`: **retained for compatibility shims** (internal repo imports fully retired).

These folders are now shim/wrapper surfaces only. They are eligible for a dedicated hard-removal step once explicit policy confirms that legacy external entrypoints/import paths are no longer required.
