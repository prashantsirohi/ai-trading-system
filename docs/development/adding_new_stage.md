# Adding a New Pipeline Stage

- **Purpose:** Checklist for adding a new stage to the 11-stage pipeline.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** Existing stage wrappers under `src/ai_trading_system/pipeline/stages/`; `pipeline/orchestrator.py:41` `PIPELINE_ORDER`.

---

## When to add a stage

Add a stage when you have a discrete unit of work that:

- Reads from prior stage artifacts (not from the next stage's intermediate state)
- Produces a materialized artifact under `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`
- Has a well-defined DQ / failure contract

If the work fits inside an existing stage's responsibility, extend the existing stage instead.

## Checklist

### Code

- [ ] Create `src/ai_trading_system/pipeline/stages/<new_stage>.py` with a `class <Name>Stage` exposing `name = "<new_stage>"` and `run(self, context: StageContext) -> StageResult` — see `pipeline/stages/perf_tracker.py` for a minimal example.
- [ ] Add `"<new_stage>"` to `pipeline/orchestrator.py::PIPELINE_ORDER` in the correct position.
- [ ] Implement the worker logic in `src/ai_trading_system/domains/<domain>/` (a service class, not in the stage wrapper).
- [ ] Use `context.write_json(...)` / `context.write_dataframe(...)` for artifacts so they land in the canonical path.
- [ ] If the stage is non-blocking, broad-except in the wrapper and document it. Otherwise let exceptions propagate.

### DQ

- [ ] Add DQ rules under `src/ai_trading_system/pipeline/dq/` if the stage produces artifacts that need quality gating.
- [ ] Decide severity: critical (blocks downstream) vs high/medium/low (logs + non-blocking).

### Registry / contracts

- [ ] Update `pipeline/contracts.py` only if you need a new `StageArtifact` shape; usually existing shapes suffice.
- [ ] If the stage writes to DuckDB, add a migration under `pipeline/migrations/`.

### CLI

- [ ] If the stage takes flags, add them to `pipeline/orchestrator.py`'s argparse parser.
- [ ] If the stage should be opt-in (like `fundamentals`), wire skip logic into the orchestrator.

### Tests

- [ ] Unit test the worker service in `tests/<domain>/`.
- [ ] Integration test the stage wrapper end-to-end in `tests/integration/`.
- [ ] Smoke test that includes the stage in `tests/smoke/`.
- [ ] Verify the canary passes: `ai-trading-pipeline --canary --skip-preflight --stages ingest,features,rank,<new_stage>`.

### Docs

- [ ] Write `docs/stages/<new_stage>.md` using the stage doc template (see existing stages for format).
- [ ] Add `<new_stage>` to `docs/architecture/operational_data_flow.md` diagram.
- [ ] Add row to `docs/reference/artifacts.md` listing artifacts the stage produces.
- [ ] Add to `docs/INDEX.md`.
- [ ] If the stage is part of a new domain, add `docs/domains/<new_domain>.md`.

### Update the architecture doc

Update `docs/architecture/operational_data_flow.md` to reflect the new 12th (or more) stage and update the Mermaid diagram.

## See also

- [`adding_new_factor.md`](adding_new_factor.md), [`adding_new_publisher.md`](adding_new_publisher.md), [`adding_new_api_endpoint.md`](adding_new_api_endpoint.md)
- [`docs_update_checklist.md`](docs_update_checklist.md)
