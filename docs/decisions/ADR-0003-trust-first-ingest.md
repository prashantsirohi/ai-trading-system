# ADR-0003: Trust-First Ingest

- **Purpose:** Record the decision to use provenance + quarantine + trust gating rather than silently accepting missing or degraded data.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** Code paths cited inline (file references in the Decision section) + [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md).
- **Status:** Accepted.

---

## Context

Market data ingest is the most failure-prone part of the system: NSE bhavcopy occasionally has missing dates, malformed rows, or symbol-level outages; Dhan and yfinance have their own gaps and rate limits.

Previous behavior (legacy `collectors/`): if a source failed, ingest would silently fall back to the next provider; if all failed, the row was just absent. Downstream stages had no way to know that a symbol's data was stale.

This caused two failure modes:

1. **Silent staleness** — yesterday's price used as today's; features and ranks computed on bad data; trades placed.
2. **Selective blindness** — some symbols missing while others present; sector RS calculations corrupted; no operator visibility.

## Decision

Ingest now tracks **provenance** (which provider supplied each row, freshness, quarantine state) and a **trust envelope** that downstream stages read via the StageContext. The trust statuses are: `trusted`, `degraded`, `blocked`, `legacy`, `missing`. The quarantine states are: `active`, `observed`, `resolved`.

Downstream stages can:

- Refuse to run on `blocked` trust (default for execute)
- Run with a warning on `degraded` (default for rank)
- Run with explicit operator override (`allow_untrusted_rank`, `allow_untrusted_execution`, `block_degraded_execution`)

DQ rules between stages further gate based on row counts, NaN ratios, and coverage. See [`docs/architecture/data_trust_and_dq.md`](../architecture/data_trust_and_dq.md).

## Consequences

**Positive:**
- Silent staleness becomes loud failure. Operator sees a trust-block in `ingest_summary.json` and the pipeline stops before publish.
- Quarantine state lets the operator inspect bad data and decide: reingest, ignore, or accept degraded.
- Trust envelope is visible in the UI per-stage.

**Negative:**
- More moving parts. Trust decisions are scattered across `ingest/trust.py`, stage wrappers, and DQ rules. Operators need to learn what `degraded` vs `blocked` means.
- The orchestrator preflight is *stricter* than the default ingest path (requires Dhan creds the ingest doesn't need). Documented in [`docs/reference/configuration.md`](../reference/configuration.md).
- Override flags exist for operator convenience, which is also an over-ride footgun.

## See also

- [`docs/architecture/data_trust_and_dq.md`](../architecture/data_trust_and_dq.md)
- [`docs/runbooks/dq_failure_response.md`](../runbooks/dq_failure_response.md)
- [`docs/runbooks/data_repair.md`](../runbooks/data_repair.md)
