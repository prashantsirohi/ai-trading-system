# Weekly Stage

- **Purpose:** Define Phase 3B full-universe stock/sector structural coverage and light discovery.
- **Audience:** Operators and engineers validating structural visibility.
- **Last verified:** 2026-07-14
- **Source of truth:** `domains/opportunities/coverage.py` and `pipeline/stages/weekly_stage.py`.

---

Start with the [System Guide](../SYSTEM_GUIDE.md).

## Purpose

`weekly_stage` is optional and runs only when `--opportunity-scan-routing-mode` is `compare` or `shadow`, or when explicitly named. It reads all eligible as-of NSE cash-equity OHLCV without a rank cap. The current incomplete week is provisional. The final scheduled trading session locks the week; a missed holiday-shortened lock is caught up on the next run.

The stage writes `weekly_stock_stage_universe`, `weekly_sector_stage_universe`, `weekly_stage_exclusions`, `light_pattern_scan`, `stage_promotion_candidates`, and `weekly_stage_summary`. Append-only stock and sector observations live in `control_plane.duckdb`; the legacy mutable `ohlcv.weekly_stage_snapshot` is unchanged.

Missing sector mapping excludes a stock from sector breadth but not stock classification. Sector structure uses constituent breadth and coverage, never sector rank. Membership is latest-only before Phase 3B; each new observation preserves the mapping seen at classification time.

## Entrypoints

`WeeklyStageCoverageStage.run` is registered as logical stage `weekly_stage`.

## Input data

As-of `_catalog` OHLCV, latest `masterdata.db` symbol sectors and NSE holidays, and typed stage/routing configuration.

## Output artifacts

The registered universal stock, sector, exclusion, light-pattern, promotion, and summary artifacts listed above.

## Main modules

`domains/opportunities/coverage.py`, `domains/opportunities/routing.py`, and `pipeline/stages/weekly_stage.py`.

## Process flow

Load uncapped eligible OHLCV, determine provisional/locked timing, classify stocks, aggregate sectors, run light discovery, append histories, then register attempt artifacts.

## DQ

Invalid OHLCV, insufficient history, illiquidity, and missing sector mappings are explicit exclusions. Low sector constituent coverage returns `UNKNOWN`.

## Failure modes

Missing `_catalog` or persistence failures fail only this optional stage. No execution or broker state is mutated.

## Retry behavior

Artifacts are attempt-scoped. Universal history is idempotent by entity/week/status/version/source hash.

## Downstream consumers

`scan_router` and Phase 3B opportunity reconciliation. Existing rank, execution, and publish consumers do not read these outputs.

## Commands

Use `--opportunity-scan-routing-mode compare` for artifact-only validation or `shadow` with registry shadow mode for reconciliation.
