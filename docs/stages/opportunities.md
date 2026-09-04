# Opportunities Stage

- **Purpose:** Operate the optional canonical opportunity-registry shadow stage.
- **Audience:** Operators and engineers debugging opportunity reconciliation.
- **Last verified:** 2026-09-04
- **Source of truth:** `src/ai_trading_system/pipeline/stages/opportunities.py`.

---

## Purpose

The optional `opportunities` stage follows `investigator` in `PIPELINE_ORDER`. It is absent from the default CLI string and is inserted after Investigator when `--opportunity-registry-mode shadow` is used with that default.

## Entrypoints

`OpportunityStage.run` is called by the canonical pipeline orchestrator. Mode and dry-run behavior come from the orchestrator CLI flags documented below.

## Input data

Required input is registered `rank/ranked_signals`. Optional inputs are full Investigator scores and intake receipts, Stage-1 state, the normalized pattern-lane assessment artifact, plus rank breakout, pattern, stock-scan, sector-dashboard, weekly-stage, and routing artifacts. In shadow routing mode full `investigator_scores` remains authoritative; routed scores are diagnostic-only. Rank and routed pattern evidence are unioned. Investigator rows outside the rank shortlist receive rank context only when an explicit rank score is present; otherwise they remain evidence-only, and `final_score` is never substituted for rank score. Investigator sector names fill missing rank-sector names, and Investigator `sector_rs_value` or sector-percentile fields fill missing weekly sector-relative-strength context. Weekly sector structure remains preferred when available. Completed pattern scans persist `KNOWN` or `NONE`; intentionally excluded and capacity-limited rows remain distinguishable from scanner errors or unexplained absence. Missing optional inputs become audit warnings. The stage reads the weekly stage snapshot store only to enrich the registered stock-stage row with source-week and creation metadata.

## Output artifacts

Writes are append-oriented canonical observations in `$DATA_ROOT/control_plane.duckdb` through `OpportunityRegistryService`. `--opportunity-registry-dry-run` disables those writes while retaining audit files. The stage never writes execution or candidate-tracker stores.

The attempt directory contains `opportunity_shadow_summary.json`, the convergence
snapshot and performance artifact family, `technical_evidence_labels.csv`,
`technical_evidence_cohorts.csv`, `opportunity_source_reconciliation.csv`,
`opportunity_integrity_receipt.csv`, `opportunity_registry_freshness.csv`, and
the admission, update, transition, closure, reconciliation, warning, rejection,
conflict, current-state, compatibility, recovery-proposal/action, and
position-monitor reconciliation CSVs listed in the
[artifact reference](../reference/artifacts.md).

When both `--fundamental-discovery-mode shadow` and registry shadow are enabled, the stage also consumes `fundamental_thesis_universe`, emits `candidate_fundamental_observations.csv`, and persists `candidate_fundamental_observation`. `FUNDAMENTAL_THESIS` uses a separate `fundamental_thesis` setup family and a duplicate typed source bundle, so it neither replaces nor attaches to technical or Investigator-primary episodes. Compare mode never supplies this registry input.

Under `near-high-20dma-shadow-v1`, the stage derives one neutral point-in-time technical observation per exchange/symbol/session. Adjusted OHLCV owns the decision-session price, 20-session SMA, and 252-session closing high; the authoritative full Investigator context independently owns the `WEEKLY_GAINER` label. The observation retains within-10%-of-52-week-high, above-SMA20, combined-entry, and first-close-below-SMA20 labels. Fundamental and Investigator episodes reference the same observation ID without sharing admission authority. A fundamental duplicate continues to clear `investigator_context`; its technical labels remain available through the neutral observation instead. Missing source values remain explicit `UNKNOWN`, while an unrelated lane is `NOT_APPLICABLE`.

## Main modules

- `domains/opportunities/adapters/` converts registered rows without persistence.
- `domains/opportunities/orchestration/` owns admission, matching, assembly, lifecycle, progress, retention, and coordination.
- `domains/opportunities/registry/` owns all canonical DuckDB reads and writes.
- `pipeline/stages/opportunities.py` owns pipeline context and audit artifact materialization.

## Process flow

The stage loads registered sources, adapts and reconciles by exchange/symbol, creates and persists the neutral symbol technical observation, checks active-position episode compatibility before any attachment, matches or admits episodes, evaluates one transition, persists canonical observations, evaluates retention/closure, and writes the reconciliation view.

## DQ

Semantic identity conflicts, cross-episode inconsistencies, invalid timestamps, invalid stage locks, and incompatible setup matching are explicit conflicts or rejections. Missing optional evidence and unavailable sector structure are warnings and never become negative evidence. Scan receipts distinguish `MISSING`, `SUCCESS_ZERO_ROWS`, and `SUCCESS_ROWS`; zero rows therefore do not imply a producer failure. Daily v3 coverage uses the latest snapshot per candidate/setup so retries cannot inflate the denominator, and pattern known-or-none is measured only among pattern-evaluable rows while `UNKNOWN` and `NOT_EVALUATED` remain failures. Under `investigator-attribution-policy-v3` and its v4 successor, a matured discovery without an ordered pending-follow-through transition closes as `INELIGIBLE_LIFECYCLE_SEQUENCE`. Legacy v1/v2 events retain their original frozen eligibility behavior. V4 expands weekly tracking to every five-session return strictly above 5%, without allowing an earlier daily spike to suppress the weekly observation. Primary eligibility uses the immutable `WEEKLY_GAINER` trigger source even when contextual classification produces another move tag.

`setup-family-v1.3` scopes matching and ambiguity to the incoming lane. Parallel
fundamental-thesis and Investigator-primary episodes do not block technical
progression; duplicate or incompatible episodes within the same lane still fail
closed. `registry_conflicts.csv` carries a stable `reason_code`. Opportunity
summary counters separately report Investigator rank-context fallbacks,
evidence-only rows, and evidence-only weekly gainers. The integrity receipt
requires zero adapter rejections.

P1.5 established `opportunity_convergence_view.csv` under immutable
`opportunity-convergence-v1.1`; P2 emits the current additive sector-lineage
shape under successor `opportunity-convergence-v1.2`. It contains one row per exchange, symbol,
observed session, and policy snapshot across the union of available lane
sources. Investigator membership means a fresh tracked weekly gainer with
`final_score >= 65`; fundamental membership means fresh admission eligibility;
pattern membership means fresh normalized pattern evidence. The row preserves
each lane's source-specific state, freshness, artifact and evidence hashes, and
explicit `KNOWN`, `NONE`, `NOT_ELIGIBLE`, `NOT_EVALUATED`, `ERROR`, or
`UNKNOWN` evaluation state. Membership maps deterministically to one of
`I_ONLY`, `F_ONLY`, `P_ONLY`, `I_F`, `I_P`, `F_P`, `I_F_P`, or `NONE`.
Future-dated, duplicate, malformed, and unexplained evidence cannot enter a
cohort. Cardinality, key uniqueness, source availability, state completeness,
hash coverage, active freshness, cohort exclusivity, unknowns, and source
errors are appended to the existing Investigator readiness artifact.

P2 persists those rows as immutable `opportunity_convergence_observation`
records. It appends discovery-close and versioned discovery-next-open-fill
anchors from trusted OHLCV, and links confirmation-close and executable-fill
anchors only when canonical performance events prove the transition. Each
anchor receives explicit 3/5/10/20-session rows. An incomplete horizon remains
`PENDING`, carries its observed-session count and optional partial diagnostic
return, and is excluded from performance aggregates. Matured outcomes include
return, MFE, MAE, +2%/+5%/stop timing, NIFTY 50-relative return, and governed
sector-relative return. Output separates mutually exclusive cohorts,
overlapping lane marginals, insufficient-sample research cohorts, and
non-overlapping ten-session windows. Confidence is exploratory below 30,
provisional at 30–59, moderate at 60–119, and policy-eligible at 120 or more.
Readiness remains pending until the discovery-session, 20-session maturation,
120-sample, and three positive weekly-momentum window gates pass.

Technical evidence requires positive price, SMA20, and 52-week-high values for a known combined-entry label. Exactly 90% of the 52-week high and equality with SMA20 both pass. The SMA20-break label is `NOT_APPLICABLE` without a prior session and becomes `MET` only on a known above-to-below close transition. These labels are research-only and cannot alter ranking, admission, lifecycle, candidates, or execution. Publish may project them unchanged into operator-facing shadow tabs, without granting decision authority.

P0 integrity receipts compare declared source rows with rows read, bundle outcomes
with bundles assembled, and created snapshot/transition counts with actual
run-scoped DuckDB deltas. Transition CSV rows are post-persistence facts in shadow
mode and `PREVIEW` plans in dry run. The registry freshness receipt requires a
canonical snapshot for the resolved market session, reports current-run and
current-session snapshot/transition counts separately, and lists intervening OHLCV
sessions with no canonical snapshots. Gaps are never backfilled. Any mismatch or
continuity gap degrades this optional stage and fails its readiness inputs while
remaining isolated from execution.

Matured `CANDIDATE_DISCOVERED` events are joined through their source snapshot to the neutral observation and summarized in mutually exclusive `FUNDAMENTAL_ONLY`, `TECHNICAL_ONLY`, and `FUNDAMENTAL_AND_TECHNICAL` cohorts at 5/10/20/60 sessions. Samples are deduplicated by cohort, exchange, symbol, decision session, and horizon; returns use the existing deterministic next-open entry basis.

Sector-relative performance uses `investigator-sector-index-taxonomy-v1.1`.
Governed healthcare, mining, industrial, electrical-equipment, logistics, and
power variants resolve only to existing primary Pharma, Metals,
Infrastructure, or Energy indices. Ambiguous Consumer, chemicals, textiles,
and broad-service sectors remain unmapped rather than falling back to the
market benchmark.

## Failure modes

A required-source failure is non-blocking for the main pipeline: the stage attempt is failed, downstream stages continue, and the run ends `completed_with_opportunity_errors`. Use the same run ID for an isolated retry:

## Retry behavior

Exact same-run source replay is detected before writes and leaves current history unchanged. Changed source hashes follow the Phase 2 idempotency/conflict contract. A retry uses registered artifacts from the supplied run ID.

## Downstream consumers

Phase 3A through Phase 3C-3 have no execution, candidate-tracker, API, or decision-making UI consumer. Registry query callers and audit review remain the authoritative consumers; publish additionally mirrors the two neutral technical-evidence CSVs into non-authoritative Google Sheets shadow views. In shadow mode, routing lineage is added to reconciliation; routed Investigator sidecars cannot replace attribution. Qualifying weekly-momentum onsets use an independent `investigator_primary` shadow family so structural dimensions remain attribution-only. Incomplete active-position evidence records `evidence_complete=false`, suppresses positive shadow actions, and keeps legacy execution unchanged. Recovery defaults to report-only proposals; reviewed recovery requires reviewer, timestamp, and notes, while automatic recovery remains disabled unless explicitly configured.

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
