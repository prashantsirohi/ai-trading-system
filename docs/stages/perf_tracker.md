# Stage: perf_tracker

- **Purpose:** Record provenance-backed rank cohorts and adjusted forward returns without blocking the operational pipeline.
- **Audience:** Operators, developers, and research reviewers.
- **Last verified:** 2026-09-12
- **Source of truth:** `pipeline/stages/perf_tracker.py` and `research/perf_tracker/{backfill,forward_returns,quality,schema,historical_backfill}.py`.

## Purpose

The optional measurement stage runs after publish. Backfill, quality reports,
health, ranking feedback, and artifact writes share a non-blocking exception
boundary. Failure produces `status=failed`, `error_class`, `error`, and
`failed_component`. If the summary itself cannot be written, the stage returns
that metadata without artifacts; registry stage metadata still records degradation.

## Entrypoints

`PerfTrackerStage.run` invokes `run_backfill` after publish. Historical research
uses `run_historical_backfill` with an explicit research OHLCV source.

## Input data

The operational backfill reads the domain control-plane DB read-only. It accepts
only promoted `ranked_signals` and `watchlist_buckets` artifacts whose producing
attempt completed. Parent runs must have completed (including completed with
DQ/opportunity warnings), except the explicitly supplied current running run.
For each run date it chooses the latest registered rank run/attempt; optional
buckets come from a completed promoted publish attempt in that same run, even
when rank and publish attempt numbers differ. Modification times and unregistered
files have no authority. Missing files or SHA-256 mismatches fail the backfill.

Each written row retains `source_run_id`, `source_artifact_path`, and
`source_lineage_json` containing exact rank/publish paths, attempts, and hashes.
Bucket joins preserve exchange identity; exchange-less bucket rows attach only
when the ranked symbol resolves unambiguously to one exchange.

## Return calculations

`adjusted_exchange_sessions_v1` computes percentage close-to-close returns at
5/10/20/60 exchange sessions. Session dates are the distinct dates observed
across the exchange's OHLCV catalog, including special weekend sessions. This
is not a separately verified exchange calendar: a date missing from the entire
exchange catalog cannot be inferred by the calculator.

Both endpoints must have a positive finite `adjusted_close`. There is no raw
price fallback. Duplicate symbol/date rows, missing entry prices, and missing
matured exit prices produce null returns and quarantine reasons. A missing
symbol bar cannot shift the target to a later date. Horizons beyond the observed
calendar remain pending. Existing anomaly guards still apply after calculation;
quality annotation preserves pre-existing quarantine reasons/status.

## Persistence and trusted reads

Paths resolve through `get_domain_paths`; operational performance lives in
`$DATA_ROOT/research.duckdb`, separate from research OHLCV under
`$DATA_ROOT/research/research_ohlcv.duckdb`.

`rank_cohort_performance` is keyed by `(run_date, symbol_id, exchange)`. Schema
initialization adds nullable `return_policy_version` and `source_lineage_json`.
The trusted view requires the current return policy plus existing quality and
anomaly checks. Legacy rows remain in the raw table and are excluded, not silently
relabeled. Both backfill paths archive replaced rows to
`rank_cohort_performance_history` with `archived_at`, then replace the selected
date cohorts in one transaction. Current row counts remain idempotent; the
archive retains each replacement's prior evidence.

## Output artifacts

The attempt emits `perf_tracker_summary.json`, `tracker_health.json`, quality
and ranking-feedback summaries, and rank-bucket, sector, repeated-symbol, and
excluded-row CSV reports. An unsuccessful attempt returns an explicit failure
summary when writable. Artifacts are registered through the normal orchestrator.

## Retry behavior

Back up the live research store before applying schema changes or regeneration.
Existing cohorts require recomputation from verified promoted inputs and adjusted
OHLCV. Raw-price historical results are not certified by installing the code.
No market-data or feature rebuild is performed by this stage; the earlier
technical-indicator correction separately requires a full technical rebuild,
followed by snapshot/DQ and rank refresh before generating new rank evidence.

A rerun rebuilds the selected operational cohorts and re-matures pending outcomes.
Historical research backfill uses the same return calculation with its explicit
research OHLCV path. Failed/degraded measurement does not authorize changes to
strategy weights or execution.

## Main modules

`backfill.py` owns promoted-artifact selection and operational writes;
`historical_backfill.py` owns research cohort construction; `forward_returns.py`
owns adjusted session returns; `quality.py` preserves exclusions; `schema.py`
owns the current table and trusted view. The stage wrapper owns failure isolation.

## Process flow

Resolve and verify producer receipts, build cohort rows, compute returns,
preserve quality exclusions, archive/replace cohorts transactionally, and emit
health/quality/feedback artifacts.

## DQ

Missing adjusted endpoints and return anomalies exclude rows from trusted
analytics. Producer failure, missing promotion, and mismatched artifact hashes
cannot establish trusted lineage. These measurement checks do not relax core
pipeline DQ or execution gates.

## Failure modes

A component failure is visible in summary/registry metadata and stops this
measurement attempt while allowing the operational pipeline to complete.
A missing registry yields no eligible artifact cohorts. Invalid/missing hashes
fail rather than falling back to arbitrary files. Unmatured horizons remain null.

## Downstream consumers

Performance API diagnostics, digests, MCP reads, and optimization consumers use
the trusted view. Installing this code does not certify existing runtime data;
consumers see no legacy rows through the updated trusted view until recomputation.

## Commands

The daily pipeline includes this stage. A separately authorized operational
backfill can call `run_backfill(project_root=..., only_dates=[...])`; the pipeline
supplies `current_run_id` so today's completed rank/publish attempts are eligible
while the parent run is still running. The weekly digest is a separate consumer.

See [storage and lineage](../architecture/storage_and_lineage.md),
[database schema](../reference/database_schema.md), and [rank](rank.md).
