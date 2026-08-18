# Opportunities Stage

- **Purpose:** Operate the optional canonical opportunity-registry shadow stage.
- **Audience:** Operators and engineers debugging opportunity reconciliation.
- **Last verified:** 2026-08-15
- **Source of truth:** `src/ai_trading_system/pipeline/stages/opportunities.py`.

---

## Purpose

The optional `opportunities` stage follows `investigator` in `PIPELINE_ORDER`. It is absent from the default CLI string and is inserted after Investigator when `--opportunity-registry-mode shadow` is used with that default.

## Entrypoints

`OpportunityStage.run` is called by the canonical pipeline orchestrator. Mode and dry-run behavior come from the orchestrator CLI flags documented below.

## Input data

Required input is registered `rank/ranked_signals`. Optional inputs are full Investigator scores and Stage-1 state plus rank breakout, pattern, stock-scan, sector-dashboard, weekly-stage, and routing artifacts. In shadow routing mode full `investigator_scores` remains authoritative; routed scores are diagnostic-only. Rank and routed pattern evidence are unioned. Weekly sector structure and rank sector RS/quadrant are complementary and neither replaces the other. Missing optional inputs become audit warnings. The stage reads the weekly stage snapshot store only to enrich the registered stock-stage row with source-week and creation metadata.

## Output artifacts

Writes are append-oriented canonical observations in `$DATA_ROOT/control_plane.duckdb` through `OpportunityRegistryService`. `--opportunity-registry-dry-run` disables those writes while retaining audit files. The stage never writes execution or candidate-tracker stores.

The attempt directory contains `opportunity_shadow_summary.json` and the admission, update, transition, closure, reconciliation, warning, rejection, conflict, current-state, compatibility, recovery-proposal/action, and position-monitor reconciliation CSVs listed in the [artifact reference](../reference/artifacts.md).

When both `--fundamental-discovery-mode shadow` and registry shadow are enabled, the stage also consumes `fundamental_thesis_universe`, emits `candidate_fundamental_observations.csv`, and persists `candidate_fundamental_observation`. `FUNDAMENTAL_THESIS` uses a separate `fundamental_thesis` setup family and a duplicate typed source bundle, so it neither replaces nor attaches to technical or Investigator-primary episodes. Compare mode never supplies this registry input.

## Main modules

- `domains/opportunities/adapters/` converts registered rows without persistence.
- `domains/opportunities/orchestration/` owns admission, matching, assembly, lifecycle, progress, retention, and coordination.
- `domains/opportunities/registry/` owns all canonical DuckDB reads and writes.
- `pipeline/stages/opportunities.py` owns pipeline context and audit artifact materialization.

## Process flow

The stage loads registered sources, adapts and reconciles by exchange/symbol, checks active-position episode compatibility before any attachment, matches or admits episodes, evaluates one transition, persists canonical observations, evaluates retention/closure, and writes the reconciliation view.

## DQ

Semantic identity conflicts, cross-episode inconsistencies, invalid timestamps, invalid stage locks, and incompatible setup matching are explicit conflicts or rejections. Missing optional evidence and unavailable sector structure are warnings and never become negative evidence.

## Failure modes

A required-source failure is non-blocking for the main pipeline: the stage attempt is failed, downstream stages continue, and the run ends `completed_with_opportunity_errors`. Use the same run ID for an isolated retry:

## Retry behavior

Exact same-run source replay is detected before writes and leaves current history unchanged. Changed source hashes follow the Phase 2 idempotency/conflict contract. A retry uses registered artifacts from the supplied run ID.

## Downstream consumers

Phase 3A through Phase 3C-3 have no execution, publish, candidate-tracker, API, or UI consumer. Registry query callers and audit review are the only consumers. In shadow mode, routing lineage is added to reconciliation; routed Investigator sidecars cannot replace attribution. Qualifying weekly-momentum onsets use an independent `investigator_primary` shadow family so structural dimensions remain attribution-only. Incomplete active-position evidence records `evidence_complete=false`, suppresses positive shadow actions, and keeps legacy execution unchanged. Recovery defaults to report-only proposals; reviewed recovery requires reviewer, timestamp, and notes, while automatic recovery remains disabled unless explicitly configured.

## Commands

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages opportunities --opportunity-registry-mode shadow
```

See [opportunity shadow orchestration](../architecture/opportunity_shadow_orchestration.md) for rules and artifact details.

## Performance instrumentation

Phase 3C-4 records the existing adapter/matching and registry-persistence
durations, audit-artifact writes, and the stage total. It also records artifact
row counts, sizes, and hashes. Performance status is advisory and separate from
the stage's existing completed/degraded/failed functional status. Candidate
admission, setup matching, lifecycle, recovery, execution, and publish contracts
are unchanged.
