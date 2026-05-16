# ADR-0001: Staged Pipeline

- **Purpose:** Record the decision to organize the system as a staged pipeline with per-stage materialized artifacts.
- **Audience:** Developer, future agents.
- **Last verified:** 2026-05-16
- **Source of truth:** Code paths cited inline (file references in the Decision section) + [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md).
- **Status:** Accepted (historical — predates this docs cleanup; documented retroactively from code structure).

---

## Context

This is an NSE-focused trading/research system that needs to: ingest market data, compute features, rank signals, generate execution candidates, optionally enrich with fundamentals and corporate-action events, generate paper orders, build operator narratives, and publish results to external channels.

Alternatives considered (implicitly, by code evolution):

1. **Monolithic daily script** — one Python entrypoint runs everything sequentially with in-memory passes. Simple but: hard to retry a partial failure; no per-step lineage; hard to test stages in isolation; UI can't introspect mid-run state.
2. **Event-driven / streaming** — Kafka or similar. Massive infrastructure cost for a system that runs once a day on a Mac mini.
3. **Staged pipeline with materialized artifacts** — discrete stages, each producing a versioned artifact on disk. Stages can be re-run individually; the UI reads artifacts; DQ runs between stages.

## Decision

Use **option 3**. The pipeline is a fixed ordered list of stages in [`pipeline/orchestrator.py:41`](../../src/ai_trading_system/pipeline/orchestrator.py) `PIPELINE_ORDER`:

```
ingest → features → rank → fundamentals(opt) → candidates → events → execute → insight → narrative → publish → perf_tracker
```

Each stage writes its outputs to `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`. A control-plane DuckDB tracks runs, stages, attempts, and artifact registry rows.

## Consequences

**Positive:**
- Stages can be retried independently (`ai-trading-pipeline --run-id <id> --stages publish`).
- DQ runs between stages; bad data is caught before it corrupts downstream artifacts.
- The UI reads materialized artifacts, so it doesn't need to re-run the pipeline.
- Every run is reproducible from `data/pipeline_runs/<run_id>/` + the input DuckDBs.
- New stages can be added without changing existing ones (see `perf_tracker` and `fundamentals` added later).

**Negative:**
- More disk usage than streaming.
- Adding a stage requires more ceremony than adding a function to a monolith.
- `data/pipeline_runs/` grows without bound — backup-and-restore runbook calls out that this dir is reproducible and can be pruned aggressively.

## See also

- [`docs/architecture/operational_data_flow.md`](../architecture/operational_data_flow.md)
- [`docs/architecture/storage_and_lineage.md`](../architecture/storage_and_lineage.md)
- [`docs/development/adding_new_stage.md`](../development/adding_new_stage.md)
