# Scan Router

- **Purpose:** Define Phase 3B deterministic shadow scan allocation and position coverage.
- **Audience:** Operators and engineers validating analysis coverage.
- **Last verified:** 2026-07-14
- **Source of truth:** `domains/opportunities/routing.py` and `pipeline/stages/scan_router.py`.

---

Start with the [System Guide](../SYSTEM_GUIDE.md).

## Purpose

The router consumes rank, universal stage, promotion, opportunity-registry lifecycle, and fill-derived position state. Precedence is active position, recent exit, triggered/pending follow-through, stage promotion, rank selection, then structural coverage. All reasons remain on the decision even though only the highest tier is emitted.

Active positions and recently exited positions receive `POSITION_MONITOR` without broker calls. Active positions, triggered candidates, and pending-follow-through candidates are uncapped. A missing-market-data position remains routed and creates a high-severity `routing_conflicts` row.

`compare` writes artifacts only. `shadow` also lets the opportunities stage consume routed evidence and recover position-only episodes. Neither mode changes execution, candidates, publish, Sheets, Telegram, or UI payloads.

## Entrypoints

`ScanRouterStage.run` is registered as logical stage `scan_router`.

## Input data

Registered rank and weekly-stage artifacts, opportunity-registry current lifecycle, and read-only execution fills/stops.

## Output artifacts

Routing, discovery, deep-scan, position-monitor, conflict, coverage-summary, and old-versus-new comparison artifacts.

## Main modules

`domains/opportunities/routing.py`, `domains/execution/store.py`, and `pipeline/stages/scan_router.py`.

## Process flow

Resolve each selection source, apply caps only to rank/stage allocations, collapse to the highest tier while preserving reasons, reconcile active coverage, persist routing history, and register artifacts.

## DQ

Every active fill-derived position must receive `POSITION_MONITOR`. Missing market data remains routed and emits a high-severity conflict row.

## Failure modes

An omitted active position fails this optional stage. Missing optional lifecycle or execution stores produce empty optional inputs rather than broker calls.

## Retry behavior

Artifacts are attempt-scoped and routing history is idempotent per run attempt, symbol, policy, and source hash.

## Downstream consumers

Routed Investigator sidecars and Phase 3B opportunity reconciliation only. Execution and publish remain unchanged.

## Commands

Use `--opportunity-scan-routing-mode compare` or combine `shadow` with `--opportunity-registry-mode shadow`.
