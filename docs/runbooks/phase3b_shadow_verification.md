# Phase 3B Shadow Verification

- **Purpose:** Validate 5–10 trading sessions of Phase 3B without changing execution or publishing.
- **Audience:** Operator.
- **Last verified:** 2026-07-15
- **Source of truth:** `weekly_stage`, `scan_router`, routed Investigator sidecars, and opportunity reconciliation.

---

Follow [copied-data canary](copied_data_canary.md) and [backup and restore](backup_and_restore.md). Back up `control_plane.duckdb` before migration 033 reaches an operator store. Point `DATA_ROOT` at the copy.

Old routing only:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --local-publish
```

Phase 3A only:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-registry-mode shadow --local-publish
```

Phase 3B comparison and shadow:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-date <session> --new-run \
  --opportunity-scan-routing-mode compare --local-publish

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-date <session> --new-run \
  --opportunity-registry-mode shadow \
  --opportunity-scan-routing-mode shadow --local-publish
```

Repeat for 5–10 stored sessions spanning a completed NSE week. For same-run replay, repeat the shadow command with its `--run-id` plus `--force-rerun --stages weekly_stage,scan_router,investigator,opportunities`.

Same-run replay comparison is valid only within one policy snapshot. A
gate-affected pre-A2 decision replayed under `lifecycle-policy-v1.1` is expected
to fail closed if its blockers change under the same idempotency key; use a new
run for cross-policy comparison. Gate-untouched records remain duplicate-safe.

Review the weekly/routing summaries, comparison, conflicts, active-position
coverage/missing-data artifacts, compatibility, recovery proposals/actions, and
opportunity reconciliation. A healthy run has fully monitored equal to active
total; a merely routed position with incomplete data is unhealthy. Confirm
critical missing-data incidents dedupe on replay, resolve after restored data,
and recur after a later gap. Confirm incompatible/ambiguous episodes never
receive position evidence and report-only recovery creates no episode. Also
confirm provisional and locked observations coexist, sector coverage guards
fire, stage promotions outside the rank cap appear, and replay creates no
semantic duplicates.

## Phase 3C-1 copied-store migration and annotation

Do not point this workflow at the configured operator store. Back up the operator
control plane first, create a separate copy, and set the command argument to that
copy. Initializing `RegistryStore` against the copy applies additive migration
034; the command refuses the configured `$DATA_ROOT/control_plane.duckdb`.

Preview without applying migration or annotations:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.annotate_phase3c1_governance \
  --copied-control-plane /path/to/copied-control_plane.duckdb
```

Apply deterministic overlays after reviewing the preview:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.annotate_phase3c1_governance \
  --copied-control-plane /path/to/copied-control_plane.duckdb \
  --run-id phase3c1-copied-validation \
  --apply --confirm-copied-store
```

Run the apply command again and confirm `applied.total` is zero. Verify that
Phase 3B observation IDs, JSON payloads, and source hashes are byte-identical
before and after annotation. Then replay at least one provisional session, its
completed week, and the same final run. Confirm:

- `OBSERVED_AT_RUN` membership is retained without being described as verified history;
- latest-only backfills do not enter sector aggregation;
- corrected stock and sector rows form terminal supersession chains;
- earlier availability cutoffs return the pre-correction observation;
- sector dependencies name stock and membership observations;
- correction impacts are review-only and execution/publish artifacts are unchanged.

Stop after copied-store validation. Applying migration 034 or annotations to an
operator store requires a separate approval and a verified backup.

Apply migration 036 only by initializing `RegistryStore` against a temporary or
copied control plane. Do not apply migrations 034, 035, or 036 to the operator
control plane during this verification. Phase 3C-3 performs no broker calls,
order/stop writes, execution eligibility changes, ranking changes, or publish/UI
payload changes.
