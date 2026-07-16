# Weekly Stage

- **Purpose:** Define Phase 3B full-universe stock/sector structural coverage and light discovery.
- **Audience:** Operators and engineers validating structural visibility.
- **Last verified:** 2026-07-16
- **Source of truth:** `domains/opportunities/coverage.py` and `pipeline/stages/weekly_stage.py`.

---

Start with the [System Guide](../SYSTEM_GUIDE.md).

## Purpose

`weekly_stage` is optional and runs only when `--opportunity-scan-routing-mode` is `compare` or `shadow`, or when explicitly named. It reads all eligible as-of NSE cash-equity OHLCV without a rank cap. The current incomplete week is provisional. The final scheduled trading session locks the week; a missed holiday-shortened lock is caught up on the next run.

The stage writes `weekly_stock_stage_universe`, `weekly_sector_stage_universe`, `weekly_stage_exclusions`, `light_pattern_scan`, `stage_promotion_candidates`, and `weekly_stage_summary`. Append-only stock and sector observations live in `control_plane.duckdb`; the legacy mutable `ohlcv.weekly_stage_snapshot` is unchanged.

Missing sector mapping excludes a stock from sector breadth but not stock
classification. Current mapping uses `masterdata.symbols` as primary and the
NSE `stock_details` industry group only for symbols absent from the primary or
carrying a placeholder sector. Primary/fallback conflicts are reported without
overwriting `symbols`; duplicate fallback conflicts are omitted as ambiguous.
Sector structure uses constituent breadth and coverage, never sector rank.
Historical membership cannot be reconstructed from latest-only master data.
Phase 3C-1 records each new master-data mapping as `OBSERVED_AT_RUN` for that
session and prefers an effective `POINT_IN_TIME_VERIFIED` interval when one
exists. `LATEST_ONLY_BACKFILL` membership is quarantined from authoritative
sector aggregation.

## Entrypoints

`WeeklyStageCoverageStage.run` is registered as logical stage `weekly_stage`.

## Input data

As-of `_catalog` OHLCV, latest `masterdata.db` symbol sectors and NSE holidays, and typed stage/routing configuration.

## Output artifacts

The registered universal stock, sector, exclusion, light-pattern, promotion, and summary artifacts listed above.

## Main modules

`domains/opportunities/coverage.py`, `domains/opportunities/routing.py`, and `pipeline/stages/weekly_stage.py`.

## Process flow

Load uncapped eligible OHLCV, resolve or observe effective-date sector membership,
determine provisional/locked timing, classify stocks, aggregate trusted
constituents, run light discovery, append histories and governance dependencies,
then register attempt artifacts.

## Correction and as-of behavior

`weekly_stock_stage_history` and `weekly_sector_stage_history` remain immutable.
An exact replay writes nothing new. A changed normalized source hash appends a new
observation and a `CORRECTION` event that names the superseded terminal
observation. Sector hashes include constituent stock hashes and membership IDs,
so a membership or constituent correction appends a recalculated sector row and
its dependency set even when headline breadth values happen to be unchanged.

`read_stock_stage_as_of` and `read_sector_stage_as_of` select terminal,
non-superseded observations known by the requested availability time. A later
correction cannot leak into an earlier reconstruction. Provisional and locked
rows continue to coexist; locked wins only for the same source-week endpoint.
When multiple terminal corrections exist for that same endpoint, the resolver
uses the Phase 3C-1A authority policy
`reviewed_operator_correction > data_repair_pipeline >
classifier_version_migration > original_observation`. If that policy does not
produce a unique winner, the reader raises a governance conflict. Supersession
cycles are rejected before insert and reported as malformed-governance conflicts
if imported data already contains a cycle.

`read_locked_sector_stage_prior_completed_week` applies the same availability,
supersession, quarantine, and authority rules after restricting candidates to
`stage_status = locked`. The opportunities early-entry gate uses this reader in
one bulk call for all routed sectors. Consequently, an incomplete current-week
provisional sector row can never displace the latest completed-week lock, and a
later correction cannot repaint an earlier gate decision.

## DQ

Invalid OHLCV, insufficient history, illiquidity, and missing sector mappings are explicit exclusions. Low sector constituent coverage returns `UNKNOWN`.
`weekly_stage_summary` reports `sector_mapping_symbols`,
`sector_mapping_missing`, `sector_mapping_coverage_ratio`, and source warnings
so mapping regressions are directly observable.

## Failure modes

Missing `_catalog` or persistence failures fail only this optional stage. No execution or broker state is mutated.

## Retry behavior

Artifacts are attempt-scoped. Universal history and governance are idempotent by normalized identities.

## Downstream consumers

`scan_router` and Phase 3B opportunity reconciliation. Existing rank, execution, and publish consumers do not read these outputs.

## Commands

Use `--opportunity-scan-routing-mode compare` for artifact-only validation or `shadow` with registry shadow mode for reconciliation.

Legacy Phase 3B annotation must run against a copied control plane. See
[Phase 3B Shadow Verification](../runbooks/phase3b_shadow_verification.md).

## Performance instrumentation

Phase 3C-4 times price-history loading, stock-stage computation, sector
aggregation, light-pattern evaluation, history persistence, artifact writes, and
the stage total. It records row/symbol throughput, persistence counts/time, and
artifact sizes/hashes. Sector aggregation has its own advisory threshold. These
observations cannot alter classifications, exclusions, locks, or persisted stage
identities.
