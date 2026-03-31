# Architecture Review

## Executive Summary

The project is now operationally useful, but its long-term maintainability is limited by three structural problems:

1. shared runtime concerns were spread across `utils/`, `run/`, and ad hoc script bootstraps
2. package boundaries were weak, especially where non-runtime layers depended on `run` internals
3. the repository still behaves more like a script collection than a packaged application

This review recommends an incremental migration, not a rewrite.

## What Is Working Well

- The staged pipeline is the right operational shape:
  - `ingest -> features -> rank -> publish`
- Run metadata, DQ, publish retries, and delivery logs create a solid control plane.
- The split between `operational` and `research` data domains is directionally correct.
- The technical ranking system has a coherent factor model and working dashboard surface.

## Critical Findings

### 1. Packaging and import discipline are weak

Symptoms:
- runtime entrypoints historically used scattered `sys.path.insert(...)` bootstraps
- the repo had no package metadata
- runtime bootstrapping was inconsistent across entrypoints

Impact:
- fragile CLI behavior
- harder testing and deployment
- higher onboarding cost

Recommendation:
- keep the current top-level packages for now
- add packaging metadata
- route shared runtime concerns through a single `core` package
- keep one centralized bootstrap helper only for compatibility scripts/examples

### 2. Cross-layer coupling is too high

Symptoms:
- `analytics` depended on `run.stages.base`
- shared contracts lived inside the runtime package

Impact:
- analytics code was not truly reusable outside the orchestrator
- boundaries between domain logic and orchestration were blurred

Recommendation:
- move shared contracts into `core.contracts`
- treat `run/` as orchestration only

### 3. `utils/` is overloaded

Symptoms:
- logging, env loading, path resolution, and data config all lived under `utils`
- `utils` mixed infrastructure concerns with generic helpers

Impact:
- unclear ownership
- hard to know what is foundational vs incidental

Recommendation:
- move foundational runtime primitives into `core/`
- keep `utils/` for compatibility and genuinely generic helpers only

### 4. Repo contains script-era layout decisions

Symptoms:
- standalone scripts mixed with runtime modules
- historical Windows/venv artifacts influenced `.gitignore`
- local data/runtime folders sit close to source packages

Impact:
- operator confusion
- accidental path assumptions
- harder packaging discipline

Recommendation:
- keep source packages cleanly separated from runtime state
- prefer package entrypoints over direct script files over time

## Incremental Target Structure

```text
ai-trading-system/
  analytics/          # market logic, ranking, research models
  channel/            # analyst-facing transforms and scan logic
  collectors/         # provider and archive ingestion
  config/             # environment-driven configuration objects
  core/               # shared runtime contracts, env, paths, logging
  ui/                 # research Streamlit UI, execution NiceGUI UI, shared UI services
  dashboard/          # compatibility wrappers for legacy UI imports/entrypoints
  features/           # technical feature computation
  publishers/         # external delivery adapters
  research/           # offline backtest/train/eval entrypoints
  run/                # orchestrator and production stage execution
  sql/                # migrations
  test/               # automated verification
  docs/               # operator and architecture docs
  data/               # local runtime state (ignored)
  reports/            # generated reports (ignored)
```

## Refactor Applied In This Phase

This phase intentionally avoids disruptive file moves and instead introduces a compatibility-safe structure:

- added `core/`
  - `core.contracts`
  - `core.env`
  - `core.runtime_config`
  - `core.bootstrap`
  - `core.paths`
  - `core.logging`
- added `publishers/`
  - dedicated external delivery boundary for Telegram, Google Sheets, and dashboard publishing
- moved shared stage contracts conceptually out of `run/`
  - `run.stages.base` is now a compatibility wrapper
- updated key runtime modules to import from `core`
- added `pyproject.toml`
- tightened `.gitignore` so `run/` is no longer broadly ignored as if it were a virtualenv

## Recommended Next Phases

### Phase 2: remove the transitional bootstrap helper entirely

- the active runtime path now uses a centralized bootstrap helper instead of scattered inline path manipulation
- remaining direct path inserts are limited to tests, examples, and legacy helpers
- next step is to eliminate even the centralized bootstrap helper by fully standardizing package entrypoints
- standardize all command execution around:
  - `python -m ...`
  - package entrypoints from `pyproject.toml`

### Phase 3: split delivery code from analyst helpers

Suggested boundary:
- `publishers/`
  - Telegram, Google Sheets, dashboard payload publishing
- `channel/`
  - scans, summaries, analysis transforms

This would make operational delivery code easier to test and reason about.

### Phase 4: isolate research as a first-class package boundary

Keep same repo, but strengthen the contract:
- production can depend on shared analytics
- production should not depend on research entrypoints
- research should never use operational mutable storage by default

### Phase 5: formalize configuration

Move from scattered `os.getenv()` access toward:
- typed runtime config objects
- one config loader per bounded context
- better defaults and validation

### Phase 6: preserve the UI split by intent

Suggested boundary:
- `ui/research/`
  - Streamlit for backtesting, factor analysis, LightGBM review, and research charts
- `ui/execution/`
  - NiceGUI for live operational monitoring, payload review, alerts, and execution workflows
- `ui/services/`
  - shared read/query layer so the two UIs do not duplicate business logic

## Operational Efficiency Recommendations

### Highest-value improvements

1. Make all read-mostly commands use artifact inputs first, DB second
2. Minimize repeated full-universe scans in dashboard paths
3. Cache sector and rank payloads aggressively at stage boundaries
4. Keep publish isolated and idempotent
5. Keep operational storage compact and rolling

### Maintainability Recommendations

1. Prefer importable modules over standalone scripts
2. Keep `core` dependency-light and stable
3. Keep `run` orchestration-only
4. Keep provider-specific logic inside `collectors`
5. Avoid introducing new cross-imports from analytics into runtime packages

## Architectural Opinion

The current project does not need a microservice split.

The best long-term design for this codebase is:
- one repo
- one packaged Python application
- strong internal package boundaries
- separate operational and research data planes
- artifact-driven runtime stages

That gives most of the maintainability benefit without adding deployment complexity.
