# Scan Router

- **Purpose:** Define Phase 3B/3C deterministic shadow scan allocation, position coverage, and routing-governance validation.
- **Audience:** Operators and engineers validating analysis coverage.
- **Last verified:** 2026-07-15
- **Source of truth:** `domains/opportunities/routing.py` and `pipeline/stages/scan_router.py`.

---

Start with the [System Guide](../SYSTEM_GUIDE.md).

## Purpose

The router consumes rank, universal stage, promotion, opportunity-registry lifecycle, and fill-derived position state. Phase 3C-2 uses `scan-routing-policy-v2`: each selection reason has a canonical minimum tier, the effective tier is the highest required tier, and the winning reason is deterministic by tier then tie-break order. All reasons remain on the decision even though only the effective tier is emitted.

Active positions and recently exited positions receive `POSITION_MONITOR` without broker calls. Active positions, triggered candidates, and pending-follow-through candidates are uncapped and cannot be manually downgraded below their required tier. An active position is fully monitored only when its route and cycle identity are valid and its latest close/session, weekly stock stage, structural price relation, relative-strength input, source week, and symbol mapping are complete. Freshness is measured in stored trading sessions, with zero stale sessions allowed by default. Missing data remains routed but is not fully monitored.

`compare` writes artifacts only. `shadow` also lets the opportunities stage consume routed evidence and recover position-only episodes. Neither mode changes execution, candidates, publish, Sheets, Telegram, or UI payloads.

## Entrypoints

`ScanRouterStage.run` is registered as logical stage `scan_router`.

## Input data

Registered rank and weekly-stage artifacts, opportunity-registry current lifecycle, and read-only execution fills/stops.

## Output artifacts

Routing, discovery, deep-scan, position-monitor, conflict, coverage-summary, old-versus-new comparison, `active_position_coverage.csv`, `active_position_missing_data.csv`, and `position_monitor_reconciliation.csv` artifacts. Existing columns are preserved; Phase 3C-2 appends `effective_scan_tier`, `winning_reason`, all selection reasons/details, structural new-long block fields, active-position structural-risk fields, and routing hash/decision identifiers.

## Main modules

`domains/opportunities/routing.py`, `domains/execution/store.py`, and `pipeline/stages/scan_router.py`.

## Process flow

Resolve each selection source, apply caps only to rank/stage allocations, validate the reason-to-tier policy, collapse to the highest tier while preserving reasons, reconcile active coverage, persist validated routing history, and register artifacts.

## DQ

Every active fill-derived position must receive `POSITION_MONITOR`. Unknown reasons/tiers, reason-tier mismatches, too-low effective tiers, invalid winning reasons, and unsafe manual downgrades emit routing conflicts and are excluded from trusted route artifacts. Missing market data remains routed, emits a `CRITICAL` `active_position_missing_market_data` alert, and opens a deterministic incident keyed by position cycle, missing-field signature, and expected market session. Identical open incidents dedupe, restored data resolves them, and recurrence after resolution emits again. Telegram fanout follows the existing alert threshold and is disabled by default.

## Failure modes

An omitted active position fails this optional stage. Invalid row-level routing decisions degrade the optional stage and are excluded from trusted artifacts; systemic construction failures fail the stage. Missing optional lifecycle or execution stores produce empty optional inputs rather than broker calls.

## Retry behavior

Artifacts are attempt-scoped and routing history is idempotent per routing decision ID. The existing routing-history table is unchanged; Phase 3C-2 stores v2 lineage in `decision_json`.

## Downstream consumers

Routed Investigator sidecars and Phase 3B/3C opportunity reconciliation only. Both consumers validate routing artifacts before trusting them. Execution and publish remain unchanged.

## Commands

Use `--opportunity-scan-routing-mode compare` or combine `shadow` with `--opportunity-registry-mode shadow`.

## Performance instrumentation

Phase 3C-4 times input loading, deterministic route resolution, route validation,
active-position coverage/alert reconciliation, artifact writes, routing-history
persistence, and the stage total. Routing input hashes, decision IDs, selected
tiers, reasons, conflicts, and alert behavior remain functional outputs and are
compared exactly by the replay benchmark; timing and memory fields are ignored.
