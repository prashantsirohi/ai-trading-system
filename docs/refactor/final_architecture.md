# Final Architecture (Post Refactor Phases 0-8)

This document captures the implemented end state after the refactor plan in
`docs/refactor/CODEX_REFACTOR_PLAN.md` Phases 0 through 8.

It is intentionally implementation-based, not aspirational.

## 1. Runtime Structure

The canonical pipeline remains:
1. `ingest`
2. `features`
3. `rank`
4. `execute`
5. `publish`

Orchestration and stage contracts:
- `run/orchestrator.py`
- `run/stages/base.py`
- `run/stages/ingest.py`
- `run/stages/features.py`
- `run/stages/rank.py`
- `run/stages/execute.py`
- `run/stages/publish.py`

The stage wrappers are now thin and defer domain work to service modules.

## 2. Service Layer End State

Domain services now provide the primary orchestration logic per stage:
- Ingest: `services/ingest/orchestration.py`
- Features: `services/features/orchestration.py`
- Rank: `services/rank/*`
  - contracts: `contracts.py`
  - data loading: `input_loader.py`
  - factors/composite: `factors.py`, `composite.py`
  - payload assembly: `dashboard_payload.py`
  - orchestration entry: `orchestration.py`
- Execute: `services/execute/candidate_builder.py`
- Publish: `services/publish/*`
  - payload assembly: `publish_payloads.py`
  - telegram rendering: `telegram_summary_builder.py`

## 3. Data Integrity and Trust Guarantees

### Write-boundary validation

Critical ingest writes now validate rows before persistence:
- shared validators: `collectors/ingest_validation.py`
- catalog write enforcement:
  - `collectors/dhan_collector.py` (`_upsert_ohlcv`)
  - `collectors/ingest_full.py` (`write_dfs_to_duckdb`)
- delivery write enforcement:
  - `collectors/delivery_collector.py` (`_upsert_delivery`)

Validation failures are fail-closed and observable (exceptions/logged errors),
instead of silently normalizing malformed writes.

### Explicit repair path

Swapped `symbol_id`/`exchange` schema drift is now explicitly repairable via:
- `scripts/repair_ingest_schema.py`

This replaces reliance on ad hoc read-time repair for core ingest safety.

### Trust lineage and quarantine

Trust and quarantine behavior remains intact:
- lineage columns in `_catalog` / `_catalog_history`
- `_catalog_provenance` recording
- `_catalog_quarantine` active/observed handling

No refactor phase removed trust lineage or quarantine enforcement.

## 4. Publish Runtime End State

Publish delivery behavior remains retry-safe and idempotent through:
- `run/publisher.py` (`PublisherDeliveryManager`)

Rendering and payload composition were separated from delivery attempts:
- `services/publish/publish_payloads.py`
- `services/publish/telegram_summary_builder.py`

`run/stages/publish.py` remains the delivery coordinator and artifact publisher.

## 5. Compatibility Decisions

The refactor prioritized runtime compatibility over broad breaking removals.

Compatibility retained:
- `run.daily_pipeline` remains a compatibility wrapper into orchestrator flow.
- `PublishStage._build_telegram_tearsheet(...)` remains as a thin compatibility
  method delegating to `services.publish.build_telegram_summary(...)`.
- Existing channel identifiers and publish dedupe keys are unchanged.

Compatibility tightened:
- Core ingest writes now fail on malformed rows rather than relying on read-time
  correction.
- Rank input loading reduced redundant read-time swap normalization where queries
  are already constrained to canonical exchange values.

## 6. Removed / Reduced Shim Notes

Removed/reduced shim behavior in this refactor sequence:
- Reduced reliance on read-time symbol/exchange swap repair in rank loaders
  for return, volume, SMA, and highs query paths.
- Publish message rendering is no longer embedded in stage internals; stage now
  delegates to publish services.

Not removed (by design):
- Legacy compatibility modules noted in architecture docs (`dashboard/`,
  script-era entrypoints) remain present and should continue to be treated as
  non-canonical surfaces until explicitly retired in a dedicated migration.

## 7. Maintainer Migration Notes

When adding or changing stage logic:
- prefer adding behavior in `services/<domain>/` and keep `run/stages/*` thin
- preserve stage artifact schemas and names unless a migration is documented
- preserve publish channel names and dedupe semantics unless compatibility logic
  is added

When changing ingest schemas or contracts:
- enforce at write boundaries in `collectors/ingest_validation.py`
- provide explicit repair/migration tooling under `scripts/`
- keep failures observable and avoid silent repair-on-read for critical paths

When updating docs:
- update `docs/architecture/*` for runtime behavior
- update `docs/reference/*` for commands/artifacts
- keep this file current when compatibility assumptions or shim posture changes
