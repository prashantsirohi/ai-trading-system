# Maintainability and target architecture

- **Purpose:** Assess maintainability and define an incremental target architecture and upgrade policy.
- **Audience:** Technical leads and maintainers sequencing architectural work.
- **Last verified:** 2026-07-13
- **Source of truth:** Current repository structure, imports, change history, and the confirmed findings in this audit.

---

## Maintainability verdict

The repository has meaningful domain boundaries, stage contracts, migration discipline, and extensive tests, but the boundaries are not consistently enforced. Several files are subsystem-sized, configuration has multiple authorities, publishing owns upstream computation, and persistence locking is local to object instances. The appropriate target is a stricter modular monolith—not microservices.

## Maintainability heatmap

Scores are relative review judgments from static inspection and change history: 1 is low, 5 is high.

| Area | Complexity | Coupling | Change frequency | Test confidence | Operational criticality | Main concern |
|---|---:|---:|---:|---:|---:|---|
| Pipeline orchestrator/registry | 5 | 5 | 5 | 4 | 5 | attempt/artifact state and writer ownership |
| Ranking | 5 | 4 | 5 | 4 | 5 | point-in-time correctness and repeated scans |
| Features | 5 | 4 | 4 | 4 | 5 | large store module and substage contracts |
| Execution | 4 | 4 | 4 | 4 | 5 | batch risk, fill lifecycle, idempotency |
| Publish/dashboard | 5 | 5 | 5 | 3 | 4 | recomputation and 2,306-line builder |
| Ingestion/providers | 5 | 4 | 4 | 4 | 5 | provider adapters and trust boundary |
| Investigator/research | 4 | 4 | 5 | 3 | 3 | changing domain and mixed responsibilities |
| API/control center | 4 | 5 | 4 | 3 | 4 | weak schemas and shared DB writes |
| React operator UI | 4 | 3 | 4 | 2 | 3 | oversized routes and minimal tests |
| Configuration | 4 | 5 | 3 | 3 | 5 | fragmented defaults and import-time env reads |

High-risk large modules include `ranking/patterns/detectors.py` (2,780 lines), `pipeline/registry.py` (2,665), `features/feature_store.py` (2,410), `publish/dashboard.py` (2,306), `ranking/service.py` (2,205), and `pipeline/orchestrator.py` (2,046). Size alone is not a defect; here it coincides with multiple responsibilities and high change rates.

## Target dependency model

```text
operator surfaces (CLI / API / React)
                 |
        application use cases
                 |
  domain services + immutable contracts
          /                 \
  repository ports       provider/broker ports
          \                 /
       infrastructure adapters
                 |
    DATA_ROOT stores / external APIs
```

Dependencies point inward. Domain code must not import FastAPI, Google Sheets, Telegram, concrete DuckDB connections, or provider SDKs. Application services own transactions and stage transitions. Infrastructure implements ports and is the only layer that resolves physical paths.

## Proposed module boundaries

```text
ai_trading_system/
  domains/
    market_data/       contracts, trust rules, point-in-time queries
    features/          feature contracts and computation
    ranking/           factor contracts, rank application service
    portfolio/         exposure and capital-at-risk policies
    execution/         orders, fills, stops, idempotency state machine
    research/          backtest/investigation contracts
  application/
    pipeline/          DAG, attempts, retries, artifact promotion
    commands/          CLI/API use cases
    queries/           bounded read models
  ports/
    stores.py providers.py brokers.py publishers.py clock.py
  infrastructure/
    duckdb/            one writer coordinator and repositories
    providers/         Dhan/NSE/etc.
    brokers/           paper first; live adapter gated
    delivery/          Sheets/Telegram
  interfaces/
    cli/ api/
```

This is a migration direction, not a one-shot directory rewrite. Existing import paths should be preserved through temporary compatibility facades.

## Refactoring seams

| Current concentration | First extraction | Contract |
|---|---|---|
| `pipeline/registry.py` | artifact repository, attempt repository, writer coordinator | explicit attempt-scoped promotion |
| `pipeline/orchestrator.py` | DAG planner, attempt runner, DQ promoter, recovery planner | pure transition functions plus ports |
| `features/feature_store.py` | read repository, write repository, transformation services | typed feature frame metadata |
| `ranking/service.py` | input snapshot builder, factor engine, persistence | one cutoff-aware `RankInputSnapshot` |
| `publish/dashboard.py` | read-model mapper and renderer components | artifact inputs only |
| `ingest/providers/dhan.py` | transport client, normalization, retry/throttle | provider-neutral observations |
| large React pages | route shell, query hooks, focused panels | typed generated API client |

## Configuration consolidation

There are two primary configuration modules plus 89 direct environment-read sites. Consolidate into one validated settings composition root with nested sections for paths, data trust, providers, execution, API, and delivery. Resolve environment variables when the application starts—not at class definition/import time. Pass typed settings into application services. Keep secrets as references/values excluded from representations and logs.

The path helper remains the authority for physical runtime paths. Add a startup diagnostic that prints redacted resolved paths and mode, but never credential values.

## Dependency upgrade matrix

Exact latest versions were not asserted from the offline environment: the package-registry security query was blocked by network policy, and compatibility must be established in CI rather than guessed. The matrix therefore specifies the latest compatible target policy and the evidence required before pin changes.

| Dependency | Current repository pin/baseline | Latest compatible target | Main risk | Required validation |
|---|---|---|---|---|
| Python | runtime 3.12.13; project `>=3.10` | latest patched supported minor; raise floor only after operator review | syntax/wheel/platform compatibility | full matrix, CLI/API smoke, packaging install |
| DuckDB | 1.1.3 | latest stable 1.x proven against copied stores | file compatibility, locking/query plans | migration copy, concurrency and query benchmarks |
| pandas | 2.2.3 | latest stable 2.x first; 3.x separately | dtype/copy-on-write behavior | feature/rank golden parity and memory |
| NumPy | 1.26.4 | latest compatible 2.x after scientific stack | ABI and numerical changes | model load, rank parity, backtests |
| PyArrow | environment 23.0.1; contract fragmented | latest release compatible with pandas/DuckDB | Parquet schema/ABI | artifact round-trip and partition pruning |
| FastAPI | 0.115.8 | latest stable compatible with Pydantic/Starlette | validation/OpenAPI behavior | API contract snapshots and auth/CORS tests |
| Pydantic | environment 2.13.2 | latest stable 2.x | serialization and validator changes | request/response contract tests |
| Uvicorn/Starlette | 0.34.0/0.45.3 | versions selected by compatible FastAPI set | middleware/websocket behavior | control-center integration tests |
| React/Vite | build succeeds; bundle large | latest stable majors in a dedicated UI change | build plugin/config and runtime behavior | unit, API mock, Playwright, bundle budget |
| provider/broker SDKs | split across pyproject and requirements | one locked, reviewed set | API drift and credential behavior | recorded contract tests; dry-run only |

Before any upgrade, first make `pyproject.toml` plus `uv.lock` the single install contract, add the missing Telegram optional dependency/extra, and eliminate the parallel unbounded `requirements.txt` contract or generate it from the lock.

## Safe migration sequence

1. Freeze behavior with point-in-time ranking, execution-policy, artifact-promotion, and API schema tests.
2. Introduce typed contracts and adapters alongside existing implementations.
3. Centralize settings and path resolution while keeping compatibility properties.
4. Add the store writer coordinator, then move repositories one store at a time.
5. Separate rank snapshot creation from factor computation.
6. Split publish rendering from delivery and remove upstream mutations.
7. Split large UI routes and adopt generated response types.
8. Only then perform dependency upgrades in small, reversible groups.

Avoid bulk package renames, a generic repository framework, and service decomposition during correctness remediation. Those changes enlarge the blast radius without resolving the current invariants.
