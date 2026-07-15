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

Review the weekly/routing summaries, comparison, conflicts, and opportunity reconciliation. Confirm active monitored equals active total, provisional and locked observations coexist, sector coverage guards fire, stage promotions outside the rank cap appear, and replay creates no semantic duplicates.

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
