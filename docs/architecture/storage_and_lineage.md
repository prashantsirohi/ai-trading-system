# Storage and Lineage

- **Purpose:** Detailed contract for runtime roots, persistent stores, artifacts, and run lineage.
- **Audience:** Operators recovering runs, engineers adding persistence, and reviewers tracing data.
- **Last verified:** 2026-07-15
- **Source of truth:** `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/pipeline/registry.py`, `src/ai_trading_system/domains/execution/store.py`, `src/ai_trading_system/domains/opportunities/registry/`, `src/ai_trading_system/pipeline/stages/candidate_tracker.py`, and `src/ai_trading_system/pipeline/migrations/`.

---

Start with the [System Guide](../SYSTEM_GUIDE.md). This document owns detailed persistence and lineage behavior.

## Root resolution

`get_domain_paths()` loads the project environment and resolves `DATA_ROOT`, `REPORTS_ROOT`, `LOGS_ROOT`, and `MODELS_ROOT`. With the operator's `.env`, operational runtime data lives on external storage. When `DATA_ROOT` is set but unavailable, guarded pipeline paths must fail instead of silently recreating the mount path.

Code retains a compatibility fallback to `<repo>/data` when `DATA_ROOT` is unset. That fallback is not the operational deployment contract and must not be hardcoded into application code or documentation commands.

## Operational stores

| Store | Canonical path | Primary owner | Purpose |
|---|---|---|---|
| OHLCV | `$DATA_ROOT/ohlcv.duckdb` | Ingest, trust, features | Price/volume, delivery, provenance, quarantine, source freshness, and feature metadata. |
| Control plane | `$DATA_ROOT/control_plane.duckdb` | Orchestrator and `RegistryStore` | Runs, attempts, artifacts, DQ, alerts, models, operator state, pattern/cache metadata, decision history, canonical opportunity-registry history, Phase 3B universal stage/routing history, and Phase 3C-1 membership/correction overlays. |
| Execution ledger | `$DATA_ROOT/execution.duckdb` | `ExecutionStore` | Orders, fills, positions, stops, and broker/paper execution state supported by the active code. |
| Candidate tracker | `$DATA_ROOT/candidate_tracker.duckdb` | Candidate tracker domain | Candidate episodes, transitions, snapshots, fundamental reviews, alerts, and current lifecycle state. |
| Master data | `$DATA_ROOT/masterdata.db` | Ingest/master-data services | Shared instrument and symbol identity data. |
| Fundamentals | `$DATA_ROOT/fundamentals/` | Fundamentals domain | Imported source snapshots and fundamental read models. |

Do not infer that execution or legacy candidate-tracker tables live in the control plane merely because their artifacts are registered there. The canonical opportunity registry is a distinct control-plane model and does not migrate or synchronize the existing tracker.

## Runtime trees

| Tree | Layout and use |
|---|---|
| Raw inputs | `$DATA_ROOT/raw/` for provider-native downloads and source snapshots. |
| Feature store | `$DATA_ROOT/feature_store/<symbol_id>/features_<start>_<end>.parquet`. |
| Stage store | `$DATA_ROOT/stage_store/` for stage-owned durable materializations. |
| Pipeline attempts | `$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/`. |
| Training datasets | `$DATA_ROOT/training_datasets/`. |
| Cache and exports | `$DATA_ROOT/cache/` and `$DATA_ROOT/exports/`. |
| Models, reports, logs | Resolved independently through `MODELS_ROOT`, `REPORTS_ROOT`, and `LOGS_ROOT`, falling back to repository roots when unset. |

## Research-domain isolation

With `DATA_DOMAIN=research`, `get_domain_paths()` re-roots domain-owned data under `$DATA_ROOT/research/`:

```text
$DATA_ROOT/research/research_ohlcv.duckdb
$DATA_ROOT/research/feature_store/
$DATA_ROOT/research/pipeline_runs/
$DATA_ROOT/research/training_datasets/
$DATA_ROOT/research/optuna/
```

Research model, report, and log roots are similarly namespaced beneath their configured roots. `masterdata.db` remains shared at `$DATA_ROOT/masterdata.db`. Operational stages must not write research results into operational OHLCV or feature stores.

## Attempt artifacts

Every executed stage gets an attempt directory:

```text
$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/<artifact>
```

Examples:

```text
$DATA_ROOT/pipeline_runs/<run_id>/ingest/attempt_1/ohlc.csv
$DATA_ROOT/pipeline_runs/<run_id>/features_snapshot/attempt_1/feature_snapshot.json
$DATA_ROOT/pipeline_runs/<run_id>/rank/attempt_1/ranked_signals.csv
$DATA_ROOT/pipeline_runs/<run_id>/candidate_tracker/attempt_1/candidate_tracker_current.csv
$DATA_ROOT/pipeline_runs/<run_id>/execute/attempt_2/executed_orders.csv
```

The exact artifact registry is documented in [artifacts](../reference/artifacts.md).
Partial files and registered artifact rows can remain after a failed attempt;
their presence does not make them authoritative. Default artifact maps and
latest-artifact reads join the exact `(run_id, stage_name, attempt_number)`
producer, require `pipeline_stage_run.status = 'completed'`, and require the
artifact lifecycle to be `promoted`.

Artifact rows begin as `written`. After applicable DQ succeeds they become
`dq_passed`; completing the exact stage attempt promotes them in the same
registry transaction that records stage completion. A crash or failure before
promotion leaves diagnostic evidence but no downstream authority.

Failed-attempt evidence remains available explicitly through
`RegistryStore.get_attempt_artifacts(run_id, stage_name, attempt_number)`. This
diagnostic path does not promote the files for downstream consumption.

## Control-plane lineage

- `pipeline_run` stores the logical run identity, date, domain, status, timing, and metadata.
- `pipeline_stage_run` stores each `(run, stage, attempt)` lifecycle.
- `pipeline_artifact` stores registered output URIs, content hashes, producer identity, and optional schema/version metadata. Authority is derived from the matching completed `pipeline_stage_run`; it is not inferred from the artifact row alone.
- `dq_result` stores rule outcomes per run/stage/attempt.
- Publisher delivery rows and alerts record downstream operational outcomes.

Use the registry's completed-attempt resolution to discover authoritative
outputs. Filesystem search is a fallback only when no control-plane database is
available; when the control plane exists, publish-only resolution must not fall
back to a failed attempt merely because its file is newer.

## Durable decision state versus attempt snapshots

CSV and JSON artifacts are immutable-attempt evidence and publish/debug inputs. Durable current or historical decision facts live in control-plane tables owned by their read/write models. The current pipeline's mutable candidate lifecycle facts remain in `candidate_tracker.duckdb`; canonical episode history written through the opportunity-registry API lives in `control_plane.duckdb`. The optional Phase 3A/3B shadow stages write canonical and universal structural history, but no synchronization or execution dependency exists between the stores. Orders and fills live in `execution.duckdb` and are read without mutation for Phase 3B monitoring.

Write modes that distinguish live updates, replay/backfill, and current-state rebuild must preserve their domain's current-state contract. Do not reconstruct or replace current state merely because an older artifact exists.

## Phase 3C-1 structural governance

Migration `034_opportunity_phase3c1_governance.sql` leaves all Phase 3B rows,
payload JSON, hashes, and identities untouched. It adds:

- `sector_membership_history` for effective-dated, recorded-at membership observations;
- `stage_observation_governance` for original, correction, withdrawal, and legacy-annotation events;
- `stage_observation_dependency` for sector-to-stock and sector-to-membership lineage;
- `stage_correction_impact` for review-required links to candidate episodes, snapshots, decisions, and attributions.

Membership trust is explicit: `POINT_IN_TIME_VERIFIED`, `OBSERVED_AT_RUN`, or
`LATEST_ONLY_BACKFILL`. Latest-only rows are excluded from canonical stage reads
and sector aggregation by default. An observed latest-master snapshot records
what the weekly run saw on that session; it does not claim historical
point-in-time validity.

Canonical stock and sector readers apply both the effective stage cutoff and a
separate recorded-availability cutoff. A correction becomes visible only after
its governance event was recorded, and a superseded observation remains valid
for earlier reconstructions. Corrections append review impacts; they do not
rewrite candidate lifecycle, attribution, execution, or published artifacts.

## Backup and mutation safety

At minimum, back up OHLCV, control-plane, execution, candidate-tracker, master-data, fundamentals, and feature-store state before migrations or repairs. Treat `pipeline_runs/` as audit evidence even where upstream stores can reproduce some artifacts.

Never run repair or migration commands against live stores without explicit task scope and a verified backup. Follow [backup and restore](../runbooks/backup_and_restore.md).

The execution ledger also stores durable submission intents before adapter
dispatch. A reserved intent without a linked order represents an unknown outcome
that must be reconciled; retries do not create another order. Execution batches
and submissions use store-adjacent lock files to serialize competing processes
for this ledger without changing broker state.
