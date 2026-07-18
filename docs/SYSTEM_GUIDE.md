# AI Trading System Guide

- **Purpose:** Canonical orientation and operating contract for the current AI Trading System.
- **Audience:** Operators, developers, reviewers, and coding agents.
- **Last verified:** 2026-07-17
- **Source of truth:** Current code, primarily `src/ai_trading_system/pipeline/orchestrator.py`, `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/pipeline/registry.py`, `src/ai_trading_system/domains/execution/store.py`, and `pyproject.toml`.

---

This is the single starting point for understanding the system. Code is authoritative for runtime behavior; this guide is the canonical human-readable summary. Follow its links instead of searching the repository or relying on older summaries.

## System purpose and boundaries

The repository contains a single-operator, NSE-focused trading and research system. The operational domain ingests trusted market data, computes features, ranks opportunities, prepares and tracks candidates, optionally enriches them, dispatches paper or explicitly authorized live orders, and publishes operator views. The research domain runs isolated backtests, optimizations, model training, and performance tracking.

The main surfaces are:

- The Python pipeline and domain packages under `src/ai_trading_system/`.
- The FastAPI operator backend under `src/ai_trading_system/ui/execution_api/`.
- The React operator console under `web/execution-console-v2/ai-trading-dashboard-starter/`.
- External runtime storage resolved from `.env`, normally through `DATA_ROOT`.

The canonical, persistence-free vocabulary for future opportunity management is
owned by `src/ai_trading_system/domains/opportunities/`. It keeps ranking
opportunity, Investigator evidence, candidate lifecycle, and stock/sector
structural stage as separate axes; its policy guards are pure and are not wired
to execution. See [opportunity lifecycle contracts](architecture/opportunity_lifecycle_contracts.md).

Phase 1 is [Canonical Opportunity Contracts](architecture/opportunity_lifecycle_contracts.md).
Phase 2 [Persistent Candidate Registry](architecture/opportunity_registry.md) adds an
append-oriented, historically reconstructable control-plane store. The optional
Phase 3A [Shadow Lifecycle Orchestration](architecture/opportunity_shadow_orchestration.md)
writes that history when explicitly enabled; execution does not consume it. The
existing candidate tracker remains the current pipeline's operational lifecycle store.
Phase 3B adds optional full-universe [weekly structural coverage](stages/weekly_stage.md)
and [shadow scan routing](stages/scan_router.md). Phase 4A adds an isolated,
strictly read-only `/api/v1` service over governed Phase 3 state; development is
allowed while production deployment remains blocked by the published Phase
3C-5 limitations. Phase 4A-1 completes operator projections from registered
Phase 3C artifacts, conflict aggregation, semantic lineage/freshness, and
low-cardinality API telemetry without adding routes or writes. See the
[Phase 4A runbook](runbooks/phase4a_read_only_api.md).
Phase 4B adds the [read-only operator dashboard](runbooks/phase4b_operator_dashboard.md)
in the existing React/Vite console. Its production bundle consumes only Phase
4A `/api/v1` GET endpoints, keeps credentials in page memory, renders API-owned
freshness, lineage, and limitations, and exposes no mutation controls.
Phase 4A permits credential-header CORS preflight only for configured origins;
the subsequent GET remains authenticated, and local development should prefer
the dashboard's same-origin Vite proxy.
Protected Phase 4A requests are process-limited by direct client address before
credential validation and by credential after successful authentication;
deployment remains responsible for proxy-aware distributed limiting.
Phase 3C-1 adds append-only [sector-membership and stage-correction
governance](stages/weekly_stage.md) without changing execution, publishing, or the
Phase 3B history payloads. Phase 3C-1A hardens that governance with explicit
correction-authority precedence, supersession-cycle rejection, and quarantined
legacy correction-impact link statuses; the operator store may still contain no
real Phase 3B weekly-stage rows until Phase 3B has run. Phase 3C-2 hardens scan
routing with policy-v2 reason-to-tier precedence, row-level validation,
structural new-long blocks separated from active-position structural risk, and
fail-closed provisional early-entry sector checks; execution and publish remain
unchanged.
Phase 3C-3 makes fill-derived active-position coverage explicit: a position is
fully monitored only when it has one validated `POSITION_MONITOR` route, a
valid cycle identity, and complete current market/structural data. Missing or
stale data opens a deduplicated critical control-plane incident and suppresses
positive shadow action evidence. Position-to-episode attachment now requires
setup/lifecycle timing compatibility. Position-only recovery defaults to
`report_only`, records deterministic proposals, never fabricates pre-entry
history, and does not affect execution or broker state.

Phase 3C-4 adds advisory performance and operational instrumentation around the
Phase 3B–3C shadow path. It records monotonic stage/operation timings, normalized
process peak RSS, row/symbol throughput, database work, and artifact size/hash
metadata under policy `phase3c4-performance-policy-v1`. Functional and
performance status remain separate; threshold failures do not block the pipeline
by default. See the [performance benchmark runbook](runbooks/phase3c4_performance_benchmark.md).

Phase 3C-5 adds an offline, immutable calibration-eligibility and Phase 4
readiness evidence layer under policy `phase3c5-calibration-policy-v1`. It
fails closed on post-decision inputs, unresolved stage or correction
governance, non-point-in-time membership, incomplete outcome windows,
survivorship gaps, recovered-position-only history, identity defects, and
sample-quality failures. Eligible, excluded, quarantined, and pending samples
remain separately auditable. It does not calibrate thresholds, alter scoring or
routing, write operator databases, or implement Phase 4. See the
[calibration and readiness runbook](runbooks/phase3c5_calibration_and_readiness.md).
Copied-realistic builds derive migration status from the copied schema and real
Phase 3B history from completed weekly-stage lineage. They also project
point-in-time membership, correction governance, decision/admission policy
snapshot IDs, completed-week sector-gate cohorts, and structured evaluate-all
admission records into calibration rows. The immutable manifest records policy
snapshot coverage, and the Phase 4 API exposes snapshot and primary-admission
coverage alongside readiness, health, and response limitations.

ADR-0007 R0 is a separate research-only calibration harness for the proposed
four-lane pattern evidence classifier. `ai-trading-pattern-r0-calibrate` reads
`_catalog` and `weekly_stage_snapshot` point-in-time through read-only DuckDB
connections, dispatches the exact lane/history-band detector allowlist before
detector execution, and writes only to an explicit immutable research output
directory. It never writes `pattern_scan.csv`, pattern cache, pipeline
artifacts, operator databases, router evidence, lifecycle state, candidates,
opportunities, or execution state. Policy and dataset hashes make an exact
replay verifiable; wall-clock telemetry is observational and excluded from
equality hashes. See the [rank contract](stages/rank.md#offline-r0-pattern-lane-calibration).

## Safety and operating invariants

- Resolve live data through the existing path helpers and `$DATA_ROOT`; never hardcode a repo-local `data/...` path in application code.
- The local operator setting is `DATA_ROOT=/Volumes/MacData/Trading/data`. If `DATA_ROOT` is unset, code retains a legacy repo-local fallback; operational work must load `.env` and use the configured external root.
- NSE bhavcopy is the operational OHLC source of record. Historical collection
  prefers the standard NSE equity bhavcopy ZIP before the security-full report;
  collector-owned canonical caches are isolated from legacy generic archives so
  a stale fallback file cannot override that order. Provider fallback and
  quarantine behavior are defined in [data sources](reference/data_sources.md)
  and [trust and DQ](architecture/data_trust_and_dq.md).
- Synthetic smoke data is disabled. Canary runs use a reduced real symbol universe.
- Critical trust or DQ failures block downstream execution.
- Historical OHLCV repair and research-to-operational backfill project candidate
  rows together with adjacent retained observations before writing. A proposed
  batch with at least 10 symbols showing simultaneous raw-close gaps of 30% or
  more is rejected before delete, upsert, or insert. The corresponding ingest
  DQ result records the affected dates and a JSON symbol sample.
- Daily raw-catalog refresh completes before split/bonus normalization. Refreshed
  symbols with active split/bonus history have adjusted OHLC reapplied even when
  no action definition changed, so incremental tail rewrites cannot silently
  reset valid adjustment factors without forcing a full-catalog rewrite.
- Historical ranking is point-in-time: market, return, volume, delivery, sector,
  stage, benchmark, and persisted feature inputs cannot read observations after
  the requested run date. One immutable `RankInputSnapshot` owns that cutoff and
  caches repeated factor reads for the decision.
- Default artifact resolution promotes only outputs whose exact producing stage
  attempt completed. Failed-attempt files remain immutable forensic evidence but
  cannot feed retries, execution, or publishing. Registered artifacts advance
  through `written` → `dq_passed` → `promoted` lifecycle states.
- Phase 3 policy content is version-bound: each policy label is registered with
  a canonical content hash in `policy_version_registry` (migration 037), and a
  label reappearing with different thresholds or constants fails that optional
  shadow stage with `POLICY_VERSION_CONTENT_MISMATCH` before any stage-owned
  write. Changing a policy value requires a successor version label. See
  [ADR-0006](decisions/ADR-0006-entry-model-and-stage-policy-freeze.md).
- Provisional stock S1→S2 triggers use the latest governed completed-week
  locked sector snapshot known at the decision timestamp. Current-week sector
  structure is monitoring evidence only; missing/untrusted membership,
  insufficient coverage, missing locks, and non-Stage-2 prior locks remain
  separately measurable fail-closed cohorts under `lifecycle-policy-v1.1`.
  Current mapping reads `masterdata.symbols` first and fills only missing or
  placeholder NSE mappings from `stock_details`; conflicting or ambiguous
  sources remain visible and fail closed rather than silently overwriting the
  primary master.
- Paper execution is the safe default. Do not enable live broker placement without explicit operator authorization, and do not describe the live path as production-certified.
- New buys are checked against projected cumulative portfolio heat before
  submission. Risk reserved by earlier accepted buys in the same execution
  batch counts toward the threshold, including orders not yet represented as
  open fills. A store-scoped inter-process batch lock serializes competing
  decision batches against the same execution ledger.
- Execution submissions carrying a non-empty correlation ID are idempotent:
  an identical retry returns the original order and fills without dispatching
  again, while reuse of that key for a different order payload is rejected.
  The durable intent is reserved before dispatch; unknown broker outcomes require
  reconciliation and are never blindly resubmitted.
- Position stops follow confirmed cumulative fills. Open/unfilled buys create no
  stop, partial fills protect only filled quantity, and exits deactivate
  protection only after the net position reaches zero.
- Preview, diagnostics, documentation checks, and tests must not mutate broker state or live DuckDB files.

## Operational design and stages

<!-- system-guide-logical-stages: ingest,features,rank,weekly_stage,scan_router,investigator,opportunities,fundamentals,candidates,candidate_tracker,events,execute,insight,narrative,publish,perf_tracker -->

```text
ingest -> features -> rank -> weekly_stage* -> scan_router* -> investigator -> opportunities* -> fundamentals* -> candidates
       -> candidate_tracker -> events -> execute -> insight -> narrative
       -> publish -> perf_tracker
```

`PIPELINE_ORDER` contains all 16 logical stages above. The current CLI default omits `weekly_stage`, `scan_router`, `opportunities`, and `narrative`, so its normal stage list remains `ingest,features,rank,investigator,fundamentals,candidates,candidate_tracker,events,execute,insight,publish,perf_tracker`. Phase 3B `compare` or `shadow` mode inserts `weekly_stage,scan_router` after `rank`; registry shadow mode inserts `opportunities` after `investigator`. Canary mode replaces the unchanged default with `ingest,features,rank`.

When rank is skipped because its inputs are unchanged, downstream stages that require rank evidence—including `scan_router`—hydrate the latest promoted artifacts from a completed run. A failed rank attempt is never eligible for this reuse.

`fundamentals` is optional in the orchestrator's implicit-stage contract, but the CLI's default stage string names it explicitly. To omit it from a CLI run, pass an explicit `--stages` list without `fundamentals`; the current `--no-enable-fundamentals` flag does not remove it from that default string. `candidate_tracker` is enabled by default and `--no-enable-candidate-tracker` removes it from the default CLI list. Any other explicit `--stages` list runs only the requested stages after expanding the `features` alias.

| Stage | Responsibility | Primary handoff | Detailed contract |
|---|---|---|---|
| `ingest` | Refresh, validate, provenance-tag, and quarantine operational OHLCV/delivery data. | Trusted catalog and ingest artifacts | [ingest](stages/ingest.md) |
| `features` | Compute technical, sector, valuation, earnings, and derived feature snapshots. | Feature Parquet and snapshot metadata | [features](stages/features.md) |
| `rank` | Score the universe and materialize ranking, breakout, pattern, stock, sector, and Stage 1 evidence. | Rank artifact family | [rank](stages/rank.md) |
| `weekly_stage` | Classify full-universe stock and sector structure and run light Stage 1 discovery. | Universal stage history and coverage artifacts | [weekly stage](stages/weekly_stage.md) |
| `scan_router` | Resolve rank, stage, candidate, active-position, and recent-exit coverage. | Routing and comparison artifacts | [scan router](stages/scan_router.md) |
| `investigator` | Build a non-executable operator investigation queue from post-rank evidence. | Investigator artifacts and control-plane history | [investigator](stages/investigator.md) |
| `opportunities` | Optionally reconcile canonical candidate episodes in non-authoritative shadow mode. | Opportunity registry and audit artifacts | [opportunities](stages/opportunities.md) |
| `fundamentals` | Optionally import and score fundamental evidence. | Fundamental scores and watchlists | [fundamentals](stages/fundamentals.md) |
| `candidates` | Deterministically select the operator/execution shortlist. | `final_candidates.csv` | [candidates](stages/candidates.md) |
| `candidate_tracker` | Maintain durable lifecycle episodes, reviews, alerts, and current candidate state. | Tracker DB and tracker artifacts | [candidate tracker](stages/candidate_tracker.md) |
| `events` | Collect and enrich catalyst/event evidence. | Event packet and enriched rank data | [events](stages/events.md) |
| `execute` | Apply trust, policy, portfolio, and risk gates before paper or authorized live dispatch. | Actions, orders, fills, positions | [execute](stages/execute.md) |
| `insight` | Build the structured analyst brief from upstream evidence. | `market_insight.json` | [insight](stages/insight.md) |
| `narrative` | Render the configured market narrative. | `market_report.json` | [narrative](stages/narrative.md) |
| `publish` | Deliver already-materialized outputs to configured channels. | Delivery records and publish summary | [publish](stages/publish.md) |
| `perf_tracker` | Mature forward-return cohorts in the research domain. | Research performance rows | [performance tracker](stages/perf_tracker.md) |

### Feature substages

The `features` CLI alias expands in this exact order:

<!-- system-guide-feature-substages: features_technical,features_sector_rs,features_valuation,features_stock_valuation_bands,features_sector_earnings,features_phase1,features_snapshot -->

```text
features_technical
-> features_sector_rs
-> features_valuation
-> features_stock_valuation_bands
-> features_sector_earnings
-> features_phase1
-> features_snapshot
```

Each substage receives its own run/stage/attempt record. See [operational data flow](architecture/operational_data_flow.md) for inputs, artifacts, preflight, DQ, failure, and retry behavior.

## Persistence and lineage

Load `.env` before operating the system:

```bash
set -a
source .env
set +a
```

Canonical operational paths are resolved beneath `$DATA_ROOT`:

| Store or tree | Responsibility |
|---|---|
| `$DATA_ROOT/ohlcv.duckdb` | Operational OHLCV, delivery, trust/provenance, quarantine, registries, and feature metadata. |
| `$DATA_ROOT/control_plane.duckdb` | Pipeline runs, stage attempts, artifacts, DQ, alerts, models, operator state, durable decision history, canonical opportunity-registry history, and Phase 3B/3C structural governance history. |
| `$DATA_ROOT/execution.duckdb` | Orders, fills, positions, and execution ledger state. |
| `$DATA_ROOT/candidate_tracker.duckdb` | Candidate episodes, snapshots, reviews, alerts, and current lifecycle state. |
| `$DATA_ROOT/masterdata.db` | Shared instrument/master data. |
| `$DATA_ROOT/fundamentals/` | Fundamental snapshots and stores. |
| `$DATA_ROOT/raw/` | Provider-native raw inputs. |
| `$DATA_ROOT/feature_store/<symbol_id>/` | Per-symbol feature Parquet snapshots. |
| `$DATA_ROOT/stage_store/` | Stage-owned durable materializations. |
| `$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/` | Immutable-attempt CSV/JSON/HTML artifacts. |
| `$DATA_ROOT/cache/` and `$DATA_ROOT/exports/` | Runtime caches and explicit exports. |

For `DATA_DOMAIN=research`, domain-owned stores are re-rooted under `$DATA_ROOT/research/`; master data remains shared. `MODELS_ROOT`, `REPORTS_ROOT`, and `LOGS_ROOT` can independently relocate their trees.

The control plane records one `pipeline_run`, one `pipeline_stage_run` per stage attempt, and a content-hashed `pipeline_artifact` row per registered output. Discover lineage through the registry rather than assuming a filesystem listing is complete. See [storage and lineage](architecture/storage_and_lineage.md), [database schema](reference/database_schema.md), and [artifacts](reference/artifacts.md).

## Operator quick start

Run commands from the repository root after loading `.env`. Use `PYTHONPATH=src` when the package has not been installed editable.

Bootstrap runtime directories:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
```

Run the current default operational pipeline:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --data-domain operational
```

Run a reduced real-data canary without network publishing:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --canary --symbol-limit 25 --local-publish
```

Run a read-only ADR-0007 R0 replay into a new research bundle:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.research.pattern_lane_calibration.cli \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD --cadence weekly \
  --output-dir /path/to/new/pattern-r0-bundle
```

This command does not authorize R1, alter rank output, or admit lane evidence
to an operational consumer. `--verify-against <manifest>` recomputes the same
dates and compares policy, source, dataset, and row-count hashes. Long runs
report live throughput and ETA and resume compatible completed-date checkpoints
from `<output-dir>.checkpoints`.

Pipeline startup is verify-only for the control-plane schema. It proceeds when
the schema is current and otherwise fails without opening a migration writer.
Apply migrations separately after taking and verifying an operator-store
backup; the command also requires the live control plane to match its copied
backup byte-for-byte:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.migrate_control_plane \
  --backup-dir "$DATA_ROOT/backups/<timestamp>" \
  --from-migration 033 --to-migration 041 --apply
```

`--apply-control-plane-migrations` is an explicit startup override for
controlled bootstrap contexts. Routine operator runs should use the separate,
backup-gated command above so migrations and pipeline execution remain distinct.

Run Phase 3B comparison without changing registry, execution, or published payloads:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-scan-routing-mode compare --local-publish
```

Run Phase 3A plus Phase 3B shadow persistence:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-registry-mode shadow \
  --opportunity-scan-routing-mode shadow \
  --local-publish
```

The command above uses the configured runtime stores. When validation must not mutate live stores, follow the [copied-data canary](runbooks/copied_data_canary.md) maintenance-window procedure instead.

Preview or annotate legacy Phase 3B rows only in an explicitly copied control plane:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.annotate_phase3c1_governance \
  --copied-control-plane /path/to/copied-control_plane.duckdb
```

The command refuses the configured operator control plane. Applying annotations
also requires `--apply --confirm-copied-store`; follow the
[Phase 3B/3C copied-store runbook](runbooks/phase3b_shadow_verification.md).

Retry one stage for an existing run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages publish
```

Start the API and React console in separate terminals:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.ui.execution_api.app --port 8090
```

Start the separate Phase 4A read-only API against deterministic fixtures:

```bash
PHASE4_API_AUTH_ENABLED=false PHASE4_API_LOCAL_DEV_MODE=true \
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api \
  --fixture-profile small_fixture --host 127.0.0.1 --port 8765
```

It never applies migrations, triggers a pipeline, imports a broker adapter, or
exposes business mutation methods. See the [API runbook](runbooks/phase4a_read_only_api.md).

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
VITE_PHASE4_API_BASE_URL=http://127.0.0.1:8765 npm run dev -- --host 127.0.0.1
```

Enter the runtime Phase 4 API key in the session-only login view. See the
[dashboard runbook](runbooks/phase4b_operator_dashboard.md) for routes,
configuration, fixture smoke testing, and the public-bundle credential caveat.

Run safe diagnostics:

```bash
curl http://localhost:8090/api/execution/health
duckdb "$DATA_ROOT/control_plane.duckdb" -cmd \
  "SELECT run_id, status, started_at FROM pipeline_run ORDER BY started_at DESC LIMIT 1"
duckdb "$DATA_ROOT/ohlcv.duckdb" -cmd \
  "SELECT MIN(date), MAX(date), COUNT(*) FROM _catalog"
```

Before a repair or migration, follow [backup and restore](runbooks/backup_and_restore.md). The exhaustive command and flag inventory is [commands](reference/commands.md); isolated production-shaped validation is in [copied-data canary](runbooks/copied_data_canary.md), and recovery starts with [troubleshooting](runbooks/troubleshooting.md).

## Where to go deeper

| Question | Read next |
|---|---|
| How does a complete run move data? | [Operational data flow](architecture/operational_data_flow.md) |
| Where is data persisted and how is lineage resolved? | [Storage and lineage](architecture/storage_and_lineage.md) |
| How are canonical candidate episodes reconstructed? | [Opportunity registry](architecture/opportunity_registry.md) |
| Why was a run degraded or blocked? | [Data trust and DQ](architecture/data_trust_and_dq.md) and [DQ response](runbooks/dq_failure_response.md) |
| What does one stage read, write, and retry? | The relevant document under [stages](INDEX.md#stages-13) |
| Which configuration, schema, artifact, or CLI contract applies? | [Reference documents](INDEX.md#reference) |
| How does the operator UI work? | [UI architecture](architecture/ui_architecture.md) and [API reference](reference/api_reference.md) |
| How is research isolated? | [Research domain](domains/research_domain.md) |
| What is planned rather than implemented? | [Target architecture](architecture/target_architecture.md) |
| Why was a major design chosen? | [Architecture decisions](INDEX.md#decisions-adrs) |

## Maintenance contract

Update this guide in the same commit whenever a change affects its system-level contract. Update the linked detailed document at the same time.

| Change | Code authority | Required detailed update |
|---|---|---|
| Pipeline order, aliases, optional/default stages, retry semantics | `pipeline/orchestrator.py`, `pipeline/stages/` | `architecture/operational_data_flow.md` and affected stage docs |
| Runtime roots, stores, lineage, or migrations | `platform/db/paths.py`, `pipeline/registry.py`, execution/tracker stores, `pipeline/migrations/` | `architecture/storage_and_lineage.md` and schema/artifact references as applicable |
| Trust, DQ, execution safety, or broker defaults | Ingest trust, DQ engine, execution policy/adapters | Trust/DQ or execution-policy reference and affected stage doc |
| Console scripts, common flags, or operator startup | `pyproject.toml` and CLI parsers | `reference/commands.md` and configuration references |
| API/UI system boundaries | FastAPI app/routers and React application | UI architecture and API reference |

After documentation changes, run:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/check_docs.py
```
