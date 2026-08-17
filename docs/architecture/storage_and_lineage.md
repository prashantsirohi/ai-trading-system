# Storage and Lineage

- **Purpose:** Detailed contract for runtime roots, persistent stores, artifacts, and run lineage.
- **Audience:** Operators recovering runs, engineers adding persistence, and reviewers tracing data.
- **Last verified:** 2026-08-14
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
| Control plane | `$DATA_ROOT/control_plane.duckdb` | Orchestrator and `RegistryStore` | Runs, attempts, artifacts, DQ, lifecycle-aware alert incidents, models, operator state, decision history, canonical opportunity-registry history, immutable Investigator performance events and derived horizons, Phase 3B universal stage/routing history, Phase 3C-1 governance, and Phase 3C-3 recovery proposals/actions. |
| Execution ledger | `$DATA_ROOT/execution.duckdb` | `ExecutionStore` | Normalized decisions, orders, fills, positions, stops, and broker/paper execution state supported by the active code. |
| Actual Trading Journal | `$DATA_ROOT/trade_journal.duckdb` | `TradeJournalStore` | Append-oriented broker import provenance, identity evidence, fills/orders, actual-position reconstruction, snapshots/reconciliations, episodes, analysis and annotations. It is isolated from the execution ledger and daily pipeline. |
| Candidate tracker | `$DATA_ROOT/candidate_tracker.duckdb` | Candidate tracker domain | Candidate episodes, transitions, snapshots, fundamental reviews, alerts, and current lifecycle state. |
| Master data | `$DATA_ROOT/masterdata.db` | Ingest/master-data services | Shared instrument and symbol identity data. |
| Fundamentals | `$DATA_ROOT/fundamentals/` | Fundamentals domain | Imported source snapshots and fundamental read models. |
| Research screener | `$DATA_ROOT/research_screener/control_plane.duckdb` | Persistent screener domain | Single-writer, append-oriented provenance, identity, immutable screen inputs/decisions/DQ, versioned research-document/evidence history, and qualitative claim/review policy history. It has no pipeline or execution consumer. |

The Screener SQLite store under `$DATA_ROOT/fundamentals/` records the detected
statement basis on financial facts and derived market valuations. Financial
facts use `(symbol, period_type, report_date, statement_basis, metric_id,
available_at)` identity; valuation rows use `(symbol, date, statement_basis,
source)`. A legacy financial table whose column exists but whose primary key
omits `statement_basis` is rebuilt to the same basis-aware identity before any
consolidated write. `screener_sync_result` retains requested-versus-detected basis and
export-path evidence for each attempted symbol. `screener_sync_batch` records
the missing-results mode, expected report date, retry cooldown, and separate
succeeded/skipped/failed counts. Those fields let selection suppress a recent
same-quarter skip without hiding it from audit. Existing valuation stores with
either legacy key refuse implicit upgrade: the sync requires an explicit migration
backup directory, creates a consistent SQLite backup plus SHA-256 sidecar, then
performs and row-count-verifies the transactional table rebuild.

The active analytical policy is `preferred_available`. The
`screener_statement_basis_resolution` view chooses exactly one physical basis
per symbol: consolidated must be at least as current as standalone and retain
enough quarterly and annual history for the active comparisons; otherwise the
symbol resolves to standalone. `screener_financials_resolved` and
`company_growth_features_resolved` carry that choice and its reason downstream.
Financial and valuation joins use the same selected basis, so a symbol cannot
mix standalone facts with consolidated valuations. The physical tables remain
unchanged and both explicit bases stay queryable for audit and replay.
Legacy `fundamental_period_facts` and `company_growth_features` keys that omit
`statement_basis` fail closed. Their explicit transactional migration requires
a verified DuckDB backup, preserves every existing row as its recorded basis
(legacy rows default to standalone), verifies counts, and only then swaps to
the basis-aware keys.

Do not infer that execution or legacy candidate-tracker tables live in the control plane merely because their artifacts are registered there. The canonical opportunity registry is a distinct control-plane model and does not migrate or synchronize the existing tracker.

The isolated research-screener control plane treats acquisition attempts and
content artifacts as separate identities. `ingestion_run` is version/run scoped;
`source_artifact` is content-and-locator addressed and may therefore be reused;
`ingestion_artifact` is the many-to-many bridge that proves which acquisition
attempt attached each artifact to a screen run. `source_artifact.ingestion_run_id`
retains the first acquisition only as a compatibility pointer. Complete run
lineage must use the bridge or the artifact IDs frozen in `dataset_snapshot`.

## Runtime trees

| Tree | Layout and use |
|---|---|
| Raw inputs | `$DATA_ROOT/raw/` for provider-native downloads and source snapshots. Canonical official cash bhavcopy caches are exchange-isolated under `NSE_EQ/` and `BSE_EQ/`; BSE cache filenames use the requested trade date and never reuse a lower-priority generic archive. |
| Feature store | `$DATA_ROOT/feature_store/<symbol_id>/features_<start>_<end>.parquet`. |
| Stage store | `$DATA_ROOT/stage_store/` for stage-owned durable materializations. |
| Pipeline attempts | `$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/`. |
| Training datasets | `$DATA_ROOT/training_datasets/`. |
| Cache and exports | `$DATA_ROOT/cache/` and `$DATA_ROOT/exports/`. |
| Models, reports, logs | Resolved independently through `MODELS_ROOT`, `REPORTS_ROOT`, and `LOGS_ROOT`, falling back to repository roots when unset. |

## Research-domain isolation

The persistent screener is separately isolated beneath
`$DATA_ROOT/research_screener/`. Completed output packs live at
`runs/<run_id>/`; raw official responses, normalized Parquet/CSV records, and a
checksum manifest are immutable. The store is not the operational
`control_plane.duckdb` and its migration never touches pipeline, execution,
candidate-tracker, master-data, OHLCV, or fundamentals stores. See the
[screener architecture and rollback note](../research_screener/architecture_and_schema.md).
Migration 008 adds qualitative claims, independent agent reviews with token and
batch usage, and deterministic policy decisions to this isolated store. The
tables have no operational consumer; see the
[qualitative claim contract](../research_screener/qualitative_claim_contract.md).

Annual-report research output is separately immutable under
`research_runs/<research_run_id>/`, with resumable source checkpoints under
`checkpoints/annual_reports/<parent_filing_run>/<semantic_version>/`. The
`research_discovery_run` row names the immutable parent filing run and snapshot
hash. `research_document` and `research_evidence` payloads retain the research
run, company, source artifact, cutoff, page, confidence, and review state.
`source_artifact` stores both valid reports and failed/truncated exchange bytes.

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

CSV and JSON artifacts are immutable-attempt evidence and publish/debug inputs. Durable current or historical decision facts live in control-plane tables owned by their read/write models. The current pipeline's mutable candidate lifecycle facts remain in `candidate_tracker.duckdb`; canonical episode history written through the opportunity-registry API lives in `control_plane.duckdb`. The optional Phase 3A/3B shadow stages write canonical and universal structural history, but no synchronization or execution dependency exists between the stores. Orders and fills live in `execution.duckdb` and are read without mutation for Phase 3B/3C monitoring. Migration 036 adds lifecycle-aware alert incidents and deterministic position-recovery proposals/actions only to the control plane; it does not alter execution tables or broker state.

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
for earlier reconstructions. Competing terminal corrections are resolved only
through the versioned authority order `reviewed_operator_correction` >
`data_repair_pipeline` > `classifier_version_migration` >
`original_observation`; equal-authority terminal competition raises an explicit
governance conflict rather than falling back to insertion or hash ordering.
Supersession cycles are rejected at write time and surfaced as conflicts if
malformed imported data is encountered during reads. Corrections append review
impacts; they do not rewrite candidate lifecycle, attribution, execution, or
published artifacts.

Phase 3C-1A adds additive metadata columns for authority reference/time,
governance policy version, correction-impact match counts/evidence, and
calibration quarantine. Legacy impact links use `linked`,
`unresolved_legacy_no_match`, or `unresolved_legacy_ambiguous`; both unresolved
states are review-required and excluded from authoritative calibration by
default. A copied operator store with no `weekly_stage` history is classified as
`EMPTY_PRE_PHASE3B`, not as a governance defect.

## Policy snapshot enforcement (ADR-0006 A3)

Every semantic Phase 3 policy value — runtime-suppliable thresholds and
single-sourced code constants alike — is fingerprinted per version label at
stage start by `domains/opportunities/policy_snapshot.py`. Migration 037's
`policy_version_registry` binds each label to exactly one canonical content
hash. The `weekly_stage`, `scan_router`, and `opportunities` stages
register-or-verify all labels in one transaction **before any stage-owned
write**.

A known label with different content raises
`POLICY_VERSION_CONTENT_MISMATCH`, naming the changed fields and requiring a
successor version label. Per the operator-approved interpretation of ADR-0006
"startup fails": the mismatch fails **only the affected optional Phase 3
shadow stage**, fail-closed and before it writes; the overall pipeline
continues because Phase 3 never feeds execution or publish.

The composite `policy_snapshot_id` is persisted in `pipeline_run`
metadata (`policy_snapshot` audit event with per-label hashes), in shadow
summary artifacts, in admission identity, and in dedicated nullable columns on
`candidate_episode` (open and close), `candidate_transition`, and
`candidate_decision_context`. The columns sit outside semantic payload JSON
and idempotency identities, so legacy rows and pre-037 replay hashes are
unchanged.

Phase 3C-5 copied-realistic builds project the decision-context snapshot as
`policy_snapshot_id` and the episode-open snapshot as
`admission_policy_snapshot_id` into every calibration artifact row. They also
carry the episode's canonical structured admission JSON and A2 sector-gate
cohort fields. The immutable manifest lists distinct decision-time snapshot
IDs, while Phase 4 read models expose snapshot and primary-admission coverage.
Copies predating migrations 037/041 remain readable with null provenance and a
fail-closed migration-readiness limitation.

Migration `042_investigator_performance_evaluation.sql` materializes complete
point-in-time Investigator attribution on append-only candidate snapshots and
adds immutable discovery/entry events plus mutable per-horizon outcomes.
Decision context is never updated during maturation. The legacy
`investigator_cohort_performance` table is a lossy symbol/date compatibility
projection: canonical discovery rows may be inserted when absent, while only
forward-return outcome columns may subsequently change.

Historical attribution reconstruction must use registered artifacts from the
same run whose artifact timestamp is no later than the decision timestamp.
Later evidence can be retained only as `RETROSPECTIVE_ENRICHED` and is excluded
from primary metrics. The reconstruction CLI refuses the configured live control
plane and symlinks and writes only to an explicit regular-file copy.

Migration `043_investigator_attribution_policy_validation.sql` adds the frozen
Investigator review-policy evidence columns, structured artifact lineage and
evaluation states, executable shadow-fill anchors, append-only evaluation
transitions, per-horizon stop-touch timing, and daily coverage receipts. A
coverage replay appends a content-addressed receipt; readers deterministically
select the latest receipt for each date and metric. The deterministic shadow
fill is analytical evidence only and never touches the execution ledger or a
broker adapter.

Phase 3.5C needs no new migration. Successor
`investigator-attribution-policy-v2` changes source ownership and sampling
admission: full Investigator scores own attribution, while routed artifacts add
lineage and routing diagnostics only. New sampling and source-fidelity files are
immutable attempt artifacts; canonical snapshots, events, horizons, evaluation
transitions, and coverage receipts remain in the migration-043 tables.

Migration 038 extends `candidate_decision_context` with nullable completed-week
sector-gate evidence and taxonomy columns. These columns are also outside the
semantic payload and idempotency identity; the actual decision blockers remain
inside the existing immutable decision payload. The governed locked-sector
reader resolves by decision-time availability, so a later correction cannot
rewrite or reinterpret an earlier gate record.

Migration 039 extends `candidate_snapshot` with nullable `last_progress_at`
and `last_retention_counted_session` columns and exposes both through
`candidate_current_state`. They are operational counter lineage outside
snapshot JSON, semantic hashes, and idempotency keys, preserving pre-039
replay identity. The existing `days_in_state` and `days_without_progress`
columns now mean observed trading-session counts under
`opportunity-retention-v1.1`.

Migration 040 adds append-only `candidate_episode_relation` lineage for
ADR-0006 A1. The relation binds a momentum predecessor to its breakout
successor with deterministic identity, setup-family rule version, run, and
source-artifact hash. Predecessor close, successor open, relation append, and
successor observations share one registry transaction.

Migration 041 adds nullable `candidate_episode` JSON columns for all satisfied
admission rules and the seven structured rule evaluations. They do not alter
episode identity or duplicate the canonical primary fields: `opening_reason`
and `setup_family` remain the primary admission reason and family.

## Backup and mutation safety

At minimum, back up OHLCV, control-plane, execution, candidate-tracker, master-data, fundamentals, and feature-store state before migrations or repairs. Treat `pipeline_runs/` as audit evidence even where upstream stores can reproduce some artifacts.

Never run repair or migration commands against live stores without explicit task scope and a verified backup. Follow [backup and restore](../runbooks/backup_and_restore.md).

The BSE-only bhavcopy backfill resolves `masterdata.db` and `ohlcv.duckdb`
through the operational path helpers. Preview mode writes only canonical raw
source caches. Apply mode first checkpoints and copies the complete OHLCV store
plus the targeted pre-write rows, then appends provenance and upserts only the
requested `(symbol_id, BSE, timestamp)` keys. Legacy and UDiFF source rows share
the canonical provider label `bse_bhavcopy`; missing weekday files and sessions
with no target trades remain separately visible in the run report.

The new-symbol onboarding workflow composes that backfill rather than owning a
second OHLCV writer. Before its other mutations it creates
`$DATA_ROOT/backups/symbol-onboarding-<UTC>/`, using SQLite's backup API for
`masterdata.db` and the canonical Screener store and copying only pre-existing
target feature Parquet files. Official BSE profile classifications are current
master state in `symbols.sector`/`symbols.industry`; their source URL, observed
taxonomy, and payload hash are retained in `symbol_classification`. Apply-mode
reports are written beneath `$REPORTS_ROOT/symbol_onboarding/`. Preview opens
the canonical stores read-only and creates neither backups nor reports.
Pre-master `--discover-missing` preview reads only `masterdata.db` plus official
BSE active-equity and company-profile responses; it does not open OHLCV, insert
candidate rows, create a checkpoint, or write a report artifact.
Combining discovery with `--promote-discovered --apply` changes that contract
only after the complete explicit scope passes identity, collision, company-ISIN,
and classification checks. It then checkpoints `masterdata.db`, inserts all
approved rows in one SQLite transaction without replacement semantics, persists
the official classification lineage, and continues through the same OHLCV,
feature, Phase 1, fundamentals, and verification path. A discovery gap produces
no partial master insertion.

Daily operational ingest reuses the same canonical BSE cache and identity
normalizer without invoking the historical command's full-store backup. It
starts each mastered symbol after its latest stored BSE date, uses the target
session only for a symbol with no existing BSE history, and performs the same
pre-write OHLC validation, provenance recording, targeted upsert, and anomaly
quarantine. BSE missing dates are promoted into the shared unresolved-session
contract only when the NSE primary path confirms the date was an exchange
session, preventing ordinary weekday holidays from creating false blockers.

The normal daily pipeline passes `feature_exchanges=[NSE,BSE]` to technical and
Phase 1 feature computation and `rank_exchanges=[NSE,BSE]` to ranking. Technical
Parquet remains exchange-partitioned below
`$DATA_ROOT/feature_store/<family>/<exchange>/<symbol>.parquet`; Phase 1 and rank
inputs use `(symbol_id, exchange)` as their analytical identity. Canonical rank,
breakout, pattern, candidate, and publish artifacts may therefore contain both
exchanges. The execution candidate boundary independently defaults to
`execution_exchanges=[NSE]`; BSE analytics do not authorize BSE order placement.

`PipelineOrchestrator` does not implicitly migrate `control_plane.duckdb`.
Its default `RegistryStore` opens in schema-verification mode and fails before a
run is created when required tables or columns are absent. Operator migrations
use `interfaces.cli.migrate_control_plane`, which runs outside pipeline
execution, requires `--apply`, verifies the copied control-plane checksum from
`SHA256SUMS.txt`, and confirms that the live pre-migration file still matches
the backup. The pipeline CLI exposes `--apply-control-plane-migrations` only as
an explicit bootstrap override; it is not the routine operator migration path.

## Phase 4A read-only access

The Phase 4A API opens DuckDB with `read_only=True` and never constructs
`RegistryStore`, `ExecutionStore`, or a schema initializer. Source precedence
is governed rows, immutable promoted artifacts, then summaries. Missing tables
are `SOURCE_NOT_MIGRATED`, empty tables are `SOURCE_EMPTY`, and missing optional
evidence is explicit rather than fabricated. Freshness never uses file mtime.

`small_fixture` is in memory. `copied_store` requires a regular-file copy and
rejects symlinks and the configured operator store. `operator_read_only`
resolves the operational root without creating it. No profile writes a cache
or response snapshot beneath `DATA_ROOT`.

## Phase 3C-4 performance artifacts

Performance evidence is artifact-backed; Phase 3C-4 adds no database migration.
Each instrumented run writes five registered files under
`$DATA_ROOT/pipeline_runs/<run_id>/performance/attempt_1/`. Benchmark runs write
the same files only beneath their explicit temporary `--output-root`. The
benchmark opens a supplied copied control plane read-only, rejects the configured
operator store, and rejects symlinked benchmark targets. Runtime metrics are
observations, not canonical trading inputs, and are excluded from stage input
hashes.

The execution ledger also stores durable submission intents before adapter
dispatch. A reserved intent without a linked order represents an unknown outcome
that must be reconciled; retries do not create another order. Execution batches
and submissions use store-adjacent lock files to serialize competing processes
for this ledger without changing broker state. Every planned action is
additionally normalized into `execution_decision` as `EXECUTED`, `REJECTED`,
`SUPPRESSED`, `PREVIEW`, or `ERROR`. Pre-submission heat and portfolio-policy
blocks are suppressions and have no order row.

## Phase 3C-5 calibration evidence

Phase 3C-5 adds no database migration and never writes operator stores.
Calibration artifacts live only beneath the explicit `--output-root`. A
`copied_realistic` build requires an explicitly supplied regular-file copy of
`control_plane.duckdb`, opens it read-only, rejects the configured operator
store, and rejects symlinked inputs or output roots.

The manifest binds policy and builder versions, the as-of boundary, source
database hashes, schema versions, sample IDs, configuration, and the eligible
dataset hash. Exact replay must reproduce both manifest identity and eligible
dataset content hash. A matching manifest identity with a different dataset
hash is an integrity failure. Excluded, quarantined, and pending rows remain
separate lineage evidence and are never silently promoted into calibration.
## Phase 4A read-only projection boundary

The Phase 4A API reads canonical governed DuckDB rows first, then promoted
immutable artifacts registered to completed stage attempts, then canonical
summary artifacts. Missing evidence produces a typed partial response; the API
does not manufacture rows or repair sources. Artifact discovery is constrained
to configured `DATA_ROOT`, copied-store roots, or the explicit
`PHASE4_API_ARTIFACT_ROOT`; symlinks and paths outside those roots are rejected.

Each governed response carries common source, run, content/schema/policy,
semantic as-of, and availability lineage plus primary/supporting consistency.
Freshness uses run, manifest, session, and availability timestamps. Filesystem
modification time is not a freshness input. Different run IDs or semantic as-of
values yield `SOURCE_VERSION_MISMATCH` and a partial response.
