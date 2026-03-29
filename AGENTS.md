# AGENTS.md

## Scope
This file applies to the entire repository unless a deeper `AGENTS.md` overrides it.

## Mission
Keep the trading system reliable while moving toward a production-hardened pipeline with explicit stages:
1. `ingest`
2. `features`
3. `rank`
4. `publish`

## Working Rules
- Preserve business behavior unless you are replacing it with an equivalent staged behavior.
- Favor reversible, incremental refactors over broad rewrites.
- Treat DuckDB metadata as authoritative for orchestration, lineage, and DQ outcomes.
- Publish logic must stay isolated from upstream data preparation so retries do not trigger re-ingest or re-feature work.
- Prefer idempotent writes keyed by `run_id`, `stage_name`, and `attempt_number`.

## Required Reading Before Major Changes
1. `docs/architecture_target.md`
2. `docs/dq_rules.md`
3. `docs/ops_runbook.md`

## Schema and Migration Expectations
- New governance or control-plane tables must ship with SQL in `sql/migrations/`.
- Rollback steps must be documented alongside the change.
- Do not silently mutate historical artifacts in place.

## Pipeline Conventions
- Every orchestrated run must write:
  - `pipeline_run`
  - `pipeline_stage_run`
  - `pipeline_artifact`
- DQ outcomes must be persisted per run and per stage.
- Stages communicate through recorded artifacts, not hidden globals.
- Critical DQ failures stop downstream stages immediately.
- Publish failures are retryable and should not invalidate upstream artifacts.

## Model Governance
- Trained models must register:
  - artifact URI
  - feature schema hash
  - training snapshot reference
  - evaluation metadata
- Deployments must reference a registered model row.

## Verification
- Add or update fast tests or executable smoke checks for:
  - stage boundaries
  - registry persistence
  - DQ blocking
  - publish retry isolation
- Document exact verification commands in the final summary or PR notes.
