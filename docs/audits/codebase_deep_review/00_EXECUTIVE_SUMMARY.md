# Deep Codebase Review — Executive Summary

- **Purpose:** Summarize the July 2026 repository-wide architecture, quality, performance, safety, and maintainability audit.
- **Audience:** Repository operators, maintainers, reviewers, and technical leads.
- **Last verified:** 2026-07-13
- **Source of truth:** The cited runtime code and verification evidence; `docs/SYSTEM_GUIDE.md` remains the current system orientation.

---

- **Audit date:** 2026-07-13
- **Scope:** Python pipeline, DuckDB/Parquet persistence, research, execution safety, publishing, FastAPI, React, configuration, dependencies, tests, and current documentation.
- **Method:** Source trace, targeted tests, package/import checks, static analysis, frontend build/tests, filesystem/path inspection, and a read-only live-store connection attempt. No live data, broker state, or application code was changed.

## Verdict and scores

The system is a capable, actively tested modular monolith with unusually strong lineage, trust/DQ, paper-execution, and operator-artifact concepts. It is **not production-ready for unattended or live trading**. The highest risks are point-in-time ranking leakage, promotion of DQ-rejected artifacts into the artifact map, DuckDB multi-process contention, non-idempotent execution retries, and risk checks that are not recomputed across a batch of buys.

| Dimension | Score / 10 | Basis |
|---|---:|---|
| Overall engineering maturity | 6.2 | Good domain layout and safety intent; inconsistent enforcement and very large modules remain. |
| Production readiness | 4.8 | Paper flow is credible; live dispatch is hard-disabled; concurrency and retry semantics are not production-safe. |
| Maintainability | 5.6 | Clear package direction, but 29 package modules exceed 800 LOC and critical orchestration modules exceed 2,000 LOC. |
| Efficiency | 5.4 | DuckDB and vectorization are used, but rank performs repeated full-history scans and the UI ships a 1.52 MB main bundle. |
| Upgradeability | 4.9 | Lock files exist, but two Python manifests disagree, several critical versions are old/pinned, and optional integrations are not modeled as extras. |
| Security | 5.8 | API-key auth, path containment, parameterized code in many critical paths, and disabled live trading are strengths; local secret permissions, OAuth token logging, permissive CORS, and unsafe SQL sites remain. |
| Test confidence | 6.0 | 1,511 test functions and strong safety-focused tests; full collection currently fails on an undeclared Telegram dependency and important concurrency/idempotency cases are absent. |

## Top ten confirmed risks

1. **AUD-001 — historical ranking uses future catalog rows (P1):** `StockRanker.rank_all(date=...)` calls loaders whose base snapshot, returns, and volume queries have no `date` cutoff.
2. **AUD-002 — DQ-rejected artifacts remain registered (P1):** the orchestrator records artifacts before DQ, while artifact lookup does not join/filter completed attempts.
3. **AUD-003 — multi-process DuckDB access is not reliable (P1):** the live `read_only=True` probe failed because another Python process held the OHLCV file; control-plane locks are per `RegistryStore` instance only.
4. **AUD-004 — portfolio heat is stale within a buy batch (P1):** heat is computed once before all actions and is not updated after accepted buys.
5. **AUD-005 — execution retry is not idempotent (P1):** correlation IDs are stored but not unique or checked; paper orders/fills use new UUIDs on every retry.
6. **AUD-006 — publish can recompute and mutate fundamentals (P1):** the default publish fallback refreshes fundamental read models when stage artifacts are absent.
7. **AUD-007 — baseline tests cannot collect (P1):** Telegram is imported as mandatory, but `python-telegram-bot` is missing from `pyproject.toml` and the environment.
8. **AUD-008 — SQL values and paths are interpolated (P2):** `analytics/feature_reader.py` directly interpolates symbols, dates, exchanges, limits, and Parquet paths.
9. **AUD-009 — secret files are mode 0644 and OAuth prints a token prefix (P2):** `.env`, `client_secret.json`, and `token.json` are untracked but group/world-readable.
10. **AUD-012 — critical modules and contracts are oversized/weakly typed (P2):** 348 broad exception handlers, 69 API routes with no response models, and high-change 2k+ LOC modules concentrate risk.

## Top ten strengths

1. Canonical `src/ai_trading_system` packaging and console entry points are present; package import and orchestrator help work.
2. `DATA_ROOT` resolves correctly to the external operational store and has path-isolation tests.
3. Run IDs, stage attempts, heartbeats, input hashes, artifact hashes, and DQ rows form a useful lineage foundation.
4. NSE source-of-record, fallback provenance, quarantine, and hard-floor DQ behaviors are explicit and heavily tested.
5. Synthetic market data is disabled in operational smoke/canary paths.
6. Execution always constructs `PaperExecutionAdapter`; the Dhan live adapter raises when `dry_run=False`.
7. Stop evaluation now receives the ranked current price and targeted stop/trailing-stop tests pass.
8. Publish delivery has content-hash dedupe, bounded retries, exponential backoff, and independent channel logs.
9. API artifact downloads use registry resolution and containment checks; traversal tests pass.
10. The React application is TypeScript-based, OpenAPI-codegen capable, builds successfully, and its unit tests pass.

## Immediate action

Freeze historical ranking/research conclusions that depend on `StockRanker.rank_all(date=...)` until AUD-001 is fixed and backfilled. Then prevent failed-stage artifacts from becoming authoritative (AUD-002), add a single-writer control-plane boundary (AUD-003), and enforce execution idempotency plus incremental risk gates (AUD-004/005). These changes provide the largest safety gain without redesigning the system.

## Explicit answers to the 25 review questions

| # | Answer |
|---:|---|
| 1 | **Mostly.** Editable/package import and the canonical CLI work, but tests add root and `src`, one script imports root `analytics`, and package resources/config are not fully isolated. |
| 2 | **No.** Core stores honor `DATA_ROOT`, but Telegram reports use `project_root/reports`, configuration retains repo fallbacks, and several scripts/docs still assume `data/`. |
| 3 | **No reliable guarantee.** A live second read-only OHLCV connection failed with a conflicting lock. |
| 4 | **No.** `RegistryStore._write_lock` is per object/process; multiple service instances and processes are not serialized by one writer. |
| 5 | **Usually, but not safely in every case.** Attempts are separate, yet failed-DQ artifacts are registered and can be resolved later. |
| 6 | **No enforced immutability.** Attempts use separate directories and hashes, but `write_json` overwrites and registry rows do not protect files from later mutation. |
| 7 | **Yes for normal rank artifacts.** Publish uses existing artifacts and delivery dedupe, but the fundamentals fallback violates this by refreshing read models. |
| 8 | **Not through the current execute stage.** It always uses paper; the Dhan adapter also hard-fails live placement. |
| 9 | **Yes.** Retrying execution can create a new UUID order/fill because correlation IDs are not enforced as idempotency keys. |
| 10 | **Current reviewed path passes the close/current price correctly.** Missing prices skip evaluation; fill-state transitions still need hardening. |
| 11 | **Risk remains.** Portfolio state is reconstructed from all fills, and heat is not recomputed after each new buy. |
| 12 | **Not all.** Many critical queries are parameterized, but confirmed interpolated value sites remain. |
| 13 | **No arbitrary shell command/path was confirmed.** Commands are argv lists and process termination validates project processes; API callers can still pass broad pipeline params under one shared key. |
| 14 | **Artificial advantage is limited for zero-weight delivery, but missing raw factors default to zero before percentile ranking and sector defaults can distort comparisons.** |
| 15 | **Not for historical dates today** because several base inputs ignore the requested date. |
| 16 | **Not proven identical.** Research has separate loaders/backtesters and no parity gate covering the full operational factor surface. |
| 17 | **No.** The historical rank base snapshot/returns/volume path has look-ahead. |
| 18 | **Partially.** OpenAPI generation exists, but 69 routes expose dictionary contracts and zero response models. |
| 19 | `pipeline/registry.py`, `pipeline/orchestrator.py`, `domains/ranking/service.py`, `domains/features/feature_store.py`, `domains/publish/dashboard.py`. |
| 20 | Add rank date cutoffs; filter artifacts by completed attempts; add one writer/idempotency boundary; recompute batch risk after fills; declare provider extras and unblock tests. |
| 21 | First failures at 10x: DuckDB lock contention, repeated rank full scans/materializations, API DataFrame/JSON payload construction, React table/bundle load, and Parquet small-file/metadata overhead. |
| 22 | DuckDB, pandas/NumPy/PyArrow coupling, FastAPI/Pydantic untyped responses, Vite/plugin deprecations, and the Git `market_intel@master` dependency. |
| 23 | `docs/_audit/current_code_truth_map.md`, `docs/architecture/data_trust_and_dq.md`, `docs/reference/execution_policy.md`, `docs/reference/api_reference.md`, and older stage docs are materially stale in places. |
| 24 | Seven orphaned root `analytics` modules, the root-path bootstrap shim, obsolete `platform/config/settings.py` defaults, and standalone publish analyzer/OAuth scripts should be migrated or deprecated after consumer verification. |
| 25 | **Yes.** A modular monolith remains the right architecture, with stricter domain/data-access boundaries and a single-writer persistence service. |
