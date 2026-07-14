# Phase 3B Shadow Verification

- **Purpose:** Validate 5–10 trading sessions of Phase 3B without changing execution or publishing.
- **Audience:** Operator.
- **Last verified:** 2026-07-14
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
