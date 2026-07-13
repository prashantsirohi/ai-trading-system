# Confirmed Findings

- **Purpose:** Preserve evidence, impact, root cause, remediation, compatibility, tests, and estimates for confirmed audit findings.
- **Audience:** Engineers planning and reviewing remediation work.
- **Last verified:** 2026-07-13
- **Source of truth:** The cited runtime code and verification commands captured by this audit.

---

Severities reflect the current single-operator, local deployment. “P1” does not mean exploitable live trading: live Dhan placement is hard-disabled. It means the defect can invalidate research, lineage, paper risk, recovery, or an operator decision.

## AUD-001: Historical ranking is not point-in-time

**Category:** CONFIRMED DEFECT / DATA-INTEGRITY RISK  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `domains/ranking/ranker.py:97-151,265-280`; `domains/ranking/input_loader.py:61-87,244-313,315-362`  
**Affected runtime path:** rank, historical rerun, research/backtest inputs

### Evidence

`rank_all(date=...)` resolves a date but calls `load_latest_market_data(exchanges=...)` without it. That query uses `MAX(timestamp)`/`arg_max` over the whole catalog. Relative-strength returns and volume windows likewise select the whole catalog and keep the latest row. Date-aware SMA, high, delivery, stage, and benchmark inputs are then joined to this future base frame.

### Impact

Historical ranks can contain future closes, returns, and volume. Backtests and reproducibility claims using this path are invalid until rebuilt. Operational same-latest-day runs are not necessarily affected.

### Root cause

The rank service has a nominal `date` contract, but three loader methods implement “latest available” rather than “latest as of date.”

### Recommended remediation

Add required `as_of` parameters to market, return, and volume loaders; apply `timestamp <= CAST(? AS TIMESTAMP)` before windows/aggregation. Parameterize exchange lists. Reject a returned base row whose timestamp exceeds `as_of`.

### Compatibility considerations

Historical outputs will change. Increment the rank/factor contract version, rebuild affected rank/research artifacts, retain old artifacts as superseded evidence, and allow rollback by selecting the old model version—not by overwriting history.

### Tests required

Seed identical databases where one has future rows; `rank_all(date=T)` must produce byte-equivalent rank-relevant columns in both. Cover market, return, volume, sector, delivery, stage, and benchmark inputs.

### Estimated implementation complexity

M

## AUD-002: DQ-rejected artifacts are registered as authoritative candidates

**Category:** CONFIRMED DEFECT / DATA-INTEGRITY RISK  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `pipeline/orchestrator.py:938-966,1063-1097`; `pipeline/registry.py:880-930`  
**Affected runtime path:** every DQ-enabled ingest/features/rank attempt and later retry

### Evidence

The orchestrator records every stage artifact at lines 944-949, then runs DQ at 951-966. On DQ failure it marks the stage failed, but `get_artifact_map` reads every `pipeline_artifact` row for the run and does not join `pipeline_stage_run` or filter `status='completed'`.

### Impact

A rank artifact that failed hard-floor DQ can be resolved by a later isolated execute/publish retry. The registry ceases to be a trusted promotion boundary.

### Root cause

Artifact persistence and artifact authority are conflated; registration precedes validation and the schema has no promoted/valid state.

### Recommended remediation

Keep new artifacts in the in-memory `StageContext` for DQ, but register them only after DQ passes. As defense in depth, make artifact resolution join the exact completed stage attempt. Add a migration/backfill that marks or excludes artifacts from failed/interrupted attempts.

### Compatibility considerations

Diagnostic access to failed artifacts should remain available via an explicit “include failed attempts” API. Default consumers must see only promoted artifacts. Roll back by restoring the old query, not by deleting evidence.

### Tests required

Force rank DQ failure after a file is created; assert normal artifact resolution excludes it, diagnostics include it, and a later successful attempt wins.

### Estimated implementation complexity

M

## AUD-003: DuckDB writer/read concurrency is not process-safe

**Category:** HIGH-RISK DESIGN / RELIABILITY RISK  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `pipeline/registry.py:372-421`; `ui/execution_api/services/control_center.py:250-375`; `domains/execution/store.py:28-42`  
**Affected runtime path:** pipeline + API + UI background tasks + external collectors

### Evidence

`RegistryStore` creates a new per-instance `threading.RLock`; services construct many independent instances. The API starts daemon threads and a subprocess while registry heartbeats/task logs/pipeline writes open separate connections. No cross-instance or cross-process writer broker exists. During the audit, `duckdb.connect(..., read_only=True)` to the live OHLCV store failed because PID 28194 held a conflicting lock.

### Impact

UI reads, task logs, heartbeats, or parallel pipeline runs can fail under normal operator concurrency. At 10x users/tasks this is the first hard scaling limit.

### Root cause

DuckDB is treated as though connection-local locks serialize a deployment-wide control plane.

### Recommended remediation

Enforce one process owning each writable DuckDB file. Route control-plane writes through a single queue/service; keep read models in compatible read-only connections or exported snapshots. Add bounded lock-error retry only after ownership is correct. Prevent two pipeline runs from owning OHLCV writes concurrently.

### Compatibility considerations

No schema change is required initially. API task submission semantics may become queued. Provide a feature flag/rollback to the current direct writer while preserving task IDs.

### Tests required

Multi-process tests with concurrent stage heartbeat, task log, artifact registration, and read queries; assert no lock failures, ordering loss, or partial transactions.

### Estimated implementation complexity

L

## AUD-004: Portfolio heat is not recomputed after each accepted buy

**Category:** TRADING-SAFETY RISK / CONFIRMED DEFECT  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `domains/execution/autotrader.py:64-74,175-242,276-300`; `domains/execution/portfolio.py:129-175`  
**Affected runtime path:** execute batch with more than one BUY

### Evidence

`check_heat_gate` runs once against `positions_before`. The resulting Boolean is reused for every BUY. `portfolio_state` is projected after a buy for concentration checks, but the heat gate and `open_risk` are never recalculated with projected quantity/stop risk.

### Impact

Several individually acceptable buys can collectively exceed the configured portfolio heat threshold in one cycle.

### Root cause

Heat and exposure use separate state-update paths.

### Recommended remediation

Create one projected portfolio/risk state. Before each BUY, include its initial stop and quantity, evaluate all constraints, and update the state only after a confirmed fill/simulation. Re-evaluate after intervening sells.

### Compatibility considerations

Some paper trades previously accepted will become rejected. Version the execution-policy result and preserve reasons in artifacts. Rollback is a policy-version selection.

### Tests required

Two buys each below the threshold but jointly above it; only the first should fill. Add order-permutation and sell-before-buy cases.

### Estimated implementation complexity

M

## AUD-005: Execution retries do not enforce order idempotency

**Category:** TRADING-SAFETY RISK / HIGH-RISK DESIGN  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `domains/execution/autotrader.py:243-313`; `domains/execution/service.py:34-52`; `domains/execution/adapters/paper.py:71-146`; `domains/execution/store.py:42-63,135-190`  
**Affected runtime path:** repeated execute attempt or adapter retry

### Evidence

The autotrader creates a stable-looking correlation string, but `ExecutionService.submit_order` never checks it. The store has no unique constraint on correlation ID. The paper adapter always creates new order and fill UUIDs, so repeating the same action inserts another fill.

### Impact

A stage retry can double a paper position. A future live adapter would risk duplicate broker orders after timeout/retry.

### Root cause

Correlation is audit metadata, not an idempotency contract.

### Recommended remediation

Define an idempotency key including run/stage/attempt-independent decision identity, symbol, side, policy version, and effective trade date. Persist it uniquely before dispatch with a state machine (`INTENDED`, `SUBMITTED`, terminal). Reconcile ambiguous broker outcomes before resubmission.

### Compatibility considerations

Add a nullable column/index, backfill from correlation IDs where unambiguous, and retain UUID order IDs. Roll back by disabling enforcement, not dropping the key.

### Tests required

Repeat the same execute input, simulate timeout after broker acceptance, and run concurrent duplicate submissions; assert one logical order/fill.

### Estimated implementation complexity

L

## AUD-006: Publish retry can mutate upstream fundamental state

**Category:** CONFIRMED DEFECT / ARCHITECTURAL BOUNDARY RISK  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `pipeline/stages/publish.py:520-582`  
**Affected runtime path:** publish without registered fundamentals artifacts

### Evidence

With `enable_fundamental_publish_fallback` defaulting true, publish calls `refresh_fundamental_insight_readmodels`, writes the fundamentals DB and an output directory, then creates ad-hoc artifacts.

### Impact

Publish retry is not a pure replay of registered inputs. Two retries can publish different data, mutate upstream state, and break lineage/idempotency.

### Root cause

Availability fallback was implemented inside the delivery stage instead of as an explicit upstream materialization.

### Recommended remediation

Remove the refresh from publish. Resolve an existing registered fundamentals artifact or emit a typed skipped/degraded channel result. Provide a separate explicit fundamentals refresh stage/command.

### Compatibility considerations

Operators accustomed to implicit fallback need a warning/deprecation period and a UI action to refresh fundamentals before publish. Rollback with the flag for one release.

### Tests required

Run publish twice against fixed artifacts; assert no upstream DB/file mtime changes and identical dedupe keys/results.

### Estimated implementation complexity

S

## AUD-007: Dependency contract prevents full test collection

**Category:** UPGRADEABILITY RISK / TESTING GAP  
**Severity:** P1 HIGH  
**Confidence:** High  
**Affected files:** `pyproject.toml`; `requirements.txt`; `domains/publish/channels/telegram.py:16-23`  
**Affected runtime path:** install, tests, Telegram publish

### Evidence

Full `pytest -q --durations=20` stopped during collection: `ModuleNotFoundError: telegram`. The publisher raises an import error at module import time. `python-telegram-bot` exists in neither project dependencies nor the environment. `requirements.txt` and `pyproject.toml` also differ materially (Google, ML, loguru and other packages).

### Impact

CI cannot establish a green baseline from declared dependencies; deployments vary by installation method.

### Root cause

Optional provider integrations are imported eagerly and dependency ownership is split between two hand-maintained manifests.

### Recommended remediation

Make `pyproject.toml` authoritative; define extras such as `publish-google`, `publish-telegram`, `research-ml`, and `dev`. Lazy-import optional channels and return a typed unavailable result. Generate/lock with `uv.lock` and test minimal/core plus all-extras matrices.

### Compatibility considerations

Keep an `all` extra for operators. Deprecate direct `requirements.txt` installation after documenting the equivalent extra.

### Tests required

Clean-environment core install/import/test, each provider extra, and all extras; verify missing optional providers do not break unrelated collection.

### Estimated implementation complexity

M

## AUD-008: SQL values are interpolated in analytics loaders

**Category:** SECURITY RISK / DATA-INTEGRITY RISK  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** `analytics/feature_reader.py:27-57,61-88,92-131`; `domains/ranking/input_loader.py:61-80,89-134,364-381`  
**Affected runtime path:** analytics and ranking reads

### Evidence

`FeatureReader` builds SQL with interpolated Parquet patterns, symbols, exchanges, dates, and limits. Rank loader interpolates exchange lists and cutoffs. Some inputs are internal today, but `FeatureReader.read_feature/read_ohlcv/read_per_symbol` are public service methods.

### Impact

Malformed provider/user values can alter queries; quoting in symbols can break production runs. Path interpolation also risks reading unintended files if exposed.

### Root cause

Query construction mixes trusted identifiers/path fragments with values.

### Recommended remediation

Bind values and safe placeholder lists. Validate limit/window as bounded integers. Resolve and contain Parquet paths before passing them as a bound filename/list. Allow only internal enum identifiers.

### Compatibility considerations

No output contract change. Some previously accepted malformed strings will fail validation.

### Tests required

Symbols/exchanges containing quotes and SQL payloads; path escape attempts; empty and large IN lists.

### Estimated implementation complexity

S

## AUD-009: Runtime path ownership is incomplete

**Category:** MAINTAINABILITY DEBT / DATA-INTEGRITY RISK  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** `pipeline/stages/publish.py:985-987`; `domains/publish/channels/telegram.py:30-40`; `platform/config/settings.py:93-99`; scripts and legacy docs  
**Affected runtime path:** reports/models/scripts outside the canonical pipeline

### Evidence

Core database and feature paths honor `DATA_ROOT`; the live resolution was correct. Telegram is explicitly constructed with `context.project_root / "reports"`; its default is `Path("reports")`. `AIConfig.model_path` hardcodes `ai-trading-system/models`. Several scripts document repo-local data paths.

### Impact

Reports/models can split between external and repository trees; cleanup/backup/lineage becomes incomplete.

### Root cause

`DataDomainPaths` is used for primary stores but not all report/model adapters and scripts.

### Recommended remediation

Pass resolved report/model/cache roots into every adapter. Add a path-hygiene ratchet for `reports`, `logs`, databases, and model paths—not only selected `data` literals.

### Compatibility considerations

Search old roots and migrate/copy with a manifest; leave read-only compatibility lookup for one release.

### Tests required

Set all four roots to distinct temp directories and assert every generated artifact remains inside its owner root.

### Estimated implementation complexity

M

## AUD-010: Local secrets are over-permissive and OAuth logs token material

**Category:** SECURITY RISK  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** local `.env`, `client_secret.json`, `token.json`; `domains/publish/channels/oauth_flow.py:39-51`  
**Affected runtime path:** local credentials bootstrap

### Evidence

The three ignored secret files are mode `-rw-r--r--` (0644). OAuth flow prints the first 20 characters of the refresh token. `check_token` also references undefined `Request` at line 63.

### Impact

Other local users/processes can read production credentials; terminal/log history captures token material.

### Root cause

Ignore rules prevent commits but no file-permission or redaction contract is enforced.

### Recommended remediation

Require/chmod 0600, never print token substrings, write atomically with mode 0600, import the refresh request type, and add preflight permission warnings.

### Compatibility considerations

None beyond local operator setup. Do not rotate automatically; instruct the operator to rotate if shared logs may contain the printed prefix.

### Tests required

Capture stdout to assert no secret substring; verify new token files are 0600; preflight rejects/warns on 0644.

### Estimated implementation complexity

S

## AUD-011: API contract and exposure controls are weak

**Category:** SECURITY RISK / UPGRADEABILITY RISK  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** `ui/execution_api/app.py:38-77`; `ui/execution_api/schemas/requests.py:15-24`; all route modules  
**Affected runtime path:** FastAPI/React interface

### Evidence

The server binds `0.0.0.0`, allows all origins/methods/headers with credentials, and uses one shared API key. All 69 routes return dictionaries without `response_model`. `PipelineRunRequest.params` accepts arbitrary values. Process termination itself is correctly restricted to recognized project processes.

### Impact

Public-network deployment has an unnecessarily broad attack surface; backend/frontend field drift is detected late, and one key grants read, pipeline execution, research promotion, and termination capabilities.

### Root cause

The API evolved as a local operator surface without explicit roles or stable response contracts.

### Recommended remediation

Default bind to loopback, configure a strict origin allowlist, split read/operator/admin scopes, define response models and error envelopes, and replace arbitrary pipeline params with a validated allowlist per action.

### Compatibility considerations

Version response models under `/api/v1` or preserve aliases for one release. Existing local deployments can opt into `0.0.0.0`.

### Tests required

Origin/auth/scope matrix, unknown param rejection, OpenAPI snapshot compatibility, and process/pipeline action authorization.

### Estimated implementation complexity

L

## AUD-012: Complexity and broad recovery boundaries concentrate failures

**Category:** MAINTAINABILITY DEBT / OBSERVABILITY GAP  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** `pipeline/registry.py` (2,665 LOC), `pipeline/orchestrator.py` (2,046), `domains/ranking/service.py` (2,205), `domains/features/feature_store.py` (2,410), `domains/publish/dashboard.py` (2,306), and 108 other exception-heavy files  
**Affected runtime path:** system-wide

### Evidence

Twenty-nine package modules exceed 800 LOC. The five modules above combine persistence, migration, orchestration, transformation, rendering, and recovery responsibilities. There are 348 broad `except Exception` sites. Ruff reported 274 issues, including undefined names in active publish modules.

### Impact

Changes have wide blast radius, optional failures become indistinguishable from data loss, and upgrades require cross-cutting edits.

### Root cause

Incremental feature growth accumulated in facades without enforcing service/repository boundaries.

### Recommended remediation

Split by owned contract, not line count: registry migrations/run repo/artifact repo/task repo; ranking core/sidecar task coordinator/payload assembler; publish dataset resolver/channel coordinator; feature partition writer/schema registry. Replace silent broad catches with typed optional outcomes and contextual logs.

### Compatibility considerations

Keep facade classes/functions during migration and move implementations behind them. No big-bang import rewrite.

### Tests required

Contract tests around existing facades, failure-injection tests at each optional boundary, import-cycle check, and static undefined-name gate.

### Estimated implementation complexity

XL

## AUD-013: Rank and API performance repeatedly materialize full data

**Category:** PERFORMANCE BOTTLENECK  
**Severity:** P2 MEDIUM  
**Confidence:** High for query shape; live timing unavailable due lock  
**Affected files:** `domains/ranking/input_loader.py:61-460`; `ui/execution_api/services/readmodels/sector_detail.py`; `ui/execution_api/services/readmodels/rank_snapshot.py`  
**Affected runtime path:** rank and operator reads

### Evidence

Rank opens separate connections and scans the catalog for latest rows, returns, volume, SMA twice, highs, delivery and stage context; multiple queries materialize full history into pandas before keeping latest rows. API read models contain repeated `iterrows` loops. The OHLCV file is already 3.85 GB.

### Impact

History/universe growth increases CPU, memory, and lock hold time roughly with catalog size even when only one as-of snapshot is required.

### Root cause

Feature loaders optimize individual functions, not one shared point-in-time query plan.

### Recommended remediation

Build a parameterized as-of DuckDB view/query that computes reusable windows once; return a narrow Arrow/pandas snapshot. Cache only by `(as_of, catalog_version, factor_version)`. Reuse SMA results and push latest-row selection before materialization.

### Compatibility considerations

Validate column/value parity before switching. Keep legacy loader behind an engine flag for one release.

### Tests required

Query-plan scan counts, peak RSS, wall time, and exact output parity on 25, 1,000, and full universes.

### Estimated implementation complexity

L

## AUD-014: React bundle and page modules are scaling risks

**Category:** PERFORMANCE BOTTLENECK / MAINTAINABILITY DEBT  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** React application, notably `BacktestPage.tsx` (1,256 LOC), `InvestigatorPage.tsx` (869), `ResearchPage.tsx` (833), `lib/api/backtest.ts` (672)  
**Affected runtime path:** operator console startup and large tables

### Evidence

Production build succeeded but emitted one 1,518.37 kB main JS chunk (422.75 kB gzip) and Vite's >500 kB warning. The build also reported deprecated plugin `esbuild`/optimize-deps options.

### Impact

Cold load and parse costs grow; large workflow pages become hard to test and upgrade. At 10x UI functionality/users, client load and server payload materialization will dominate.

### Root cause

Routes/workflows are bundled eagerly and page-level components own API shaping plus presentation.

### Recommended remediation

Route-level lazy imports, explicit vendor/chart chunks, table virtualization, and page decomposition into query/controller/view components. Set and enforce bundle budgets.

### Compatibility considerations

No API change. Preserve route URLs and loading/error states.

### Tests required

Build-size budget, route lazy-load smoke tests, accessibility checks, and 2,500-row table performance.

### Estimated implementation complexity

M

## AUD-015: Configuration is fragmented and evaluated at import time

**Category:** MAINTAINABILITY DEBT / UPGRADEABILITY RISK  
**Severity:** P2 MEDIUM  
**Confidence:** High  
**Affected files:** `platform/config/settings.py:8-141`; `platform/utils/runtime_config.py:11-82`; `platform/utils/data_config.py`; 89 direct environment-read sites  
**Affected runtime path:** startup, tests, integrations

### Evidence

`settings.py` claims a main configuration model but is a collection of dataclasses, reads environment values into class defaults at import, hardcodes broker/model defaults, and coexists with another runtime-config module and direct `os.getenv` calls.

### Impact

Precedence is ambiguous, tests depend on import order, invalid numeric env values fail deep in startup, and secret redaction is not centralized.

### Root cause

Configuration grew per module without one validated startup boundary.

### Recommended remediation

Create one Pydantic Settings root with nested provider/execution/path/API models, explicit CLI-over-env-over-file precedence, validation, redacted repr, and lazy construction at application entrypoints.

### Compatibility considerations

Support old environment names with deprecation warnings for two releases. Do not change safe defaults during the consolidation.

### Tests required

Precedence matrix, invalid values, missing mandatory settings per enabled capability, redaction, and import-before-env tests.

### Estimated implementation complexity

L

## AUD-016: Stop state transitions assume immediate final fills

**Category:** TRADING-SAFETY RISK / HIGH-RISK DESIGN  
**Severity:** P2 MEDIUM (would become P1 before live enablement)  
**Confidence:** High  
**Affected files:** `domains/execution/service.py:34-52,136-200`; `domains/execution/autotrader.py:301-323`  
**Affected runtime path:** non-immediate/partial fill adapter

### Evidence

The service persists a BUY stop immediately after `submit_order`, using requested price/quantity, regardless of actual fills. SELL exits deactivate the stop for any status other than `REJECTED`/`ERROR`; an `OPEN` or partially filled order would deactivate protection prematurely.

### Impact

Future live/async adapters can create stops for unfilled shares or remove stops before an exit fill is confirmed.

### Root cause

Paper's immediate-fill behavior leaked into the domain state machine.

### Recommended remediation

Drive position/stop changes from fill events. Track remaining protected quantity; deactivate only after position reaches zero. Reconcile partial/rejected/expired orders.

### Compatibility considerations

Paper results remain equivalent. Add state columns/events additively and retain current stop rows during migration.

### Tests required

OPEN, partial fill, reject-after-submit, delayed fill, and partial exit scenarios.

### Estimated implementation complexity

L

## AUD-017: Attempt artifacts are not atomically written or immutable

**Category:** RELIABILITY RISK  
**Severity:** P3 LOW  
**Confidence:** High  
**Affected files:** `pipeline/contracts.py:159-174,209-215`  
**Affected runtime path:** every JSON artifact; similar direct writers elsewhere

### Evidence

`output_dir` reuses/creates the attempt directory and `write_json` opens the final path with `"w"`. There is no temporary-file + fsync + atomic replace, exclusive create, or post-registration mutation check.

### Impact

Crash/power loss can leave truncated files; accidental same-attempt writes can change content after the registered hash.

### Root cause

Attempt-number convention is treated as immutability enforcement.

### Recommended remediation

Write to a sibling temp file, fsync, atomically replace, then register; refuse overwrite once registered. Add a verification command that rehashes registered artifacts.

### Compatibility considerations

No consumer contract change. Keep diagnostics for partial temp files.

### Tests required

Failure injection mid-write, concurrent writers, overwrite refusal, and registry rehash mismatch detection.

### Estimated implementation complexity

S
