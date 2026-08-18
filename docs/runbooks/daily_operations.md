# Daily Operations

- **Purpose:** Redirect operators to the maintained daily commands while this detailed checklist remains incomplete.
- **Audience:** Operator.
- **Last verified:** 2026-08-15
- **Source of truth:** [`docs/SYSTEM_GUIDE.md`](../SYSTEM_GUIDE.md), [`docs/reference/commands.md`](../reference/commands.md), and current runtime code.


---

This detailed checklist has not yet been populated. Do not infer operational steps from the empty historical headings.

Use the [System Guide](../SYSTEM_GUIDE.md#operator-commands) for the maintained run, canary, retry, UI, and diagnostic commands. Use [data trust and DQ](../architecture/data_trust_and_dq.md) for trust interpretation and [troubleshooting](troubleshooting.md) for failures.

## Fundamental discovery cadence

Schedule `ai-trading-fundamentals-sync --missing-current-results --allow-download` before the evening pipeline. Run it daily from 10 Jan–20 Feb, 10 Apr–15 Jun, 10 Jul–20 Aug, and 10 Oct–20 Nov; run it weekly otherwise. The command's default 72-hour per-symbol cooldown prevents a still-unpublished quarter from being downloaded every day; use `--missing-results-retry-cooldown-hours` only when a different freshness/traffic tradeoff is intentional. A failed sync is degraded evidence: investigate its receipt in `$DATA_ROOT/fundamentals.duckdb`, but do not delete or replace the last trusted snapshot.

The evening shadow run uses `--fundamental-discovery-mode shadow --opportunity-registry-mode shadow`. The pipeline must not be granted provider credentials for this purpose and never invokes the sync command. Use `compare` instead of `shadow` when artifacts are wanted without opportunity-registry writes. The default `off` mode preserves the existing pipeline behavior.
