# ADR-0004: Artifact-Driven Publish

- **Purpose:** Record the decision to make the publish stage read only materialized artifacts (not in-memory pipeline state), so it can be retried independently.
- **Audience:** Developer, operator.
- **Last verified:** 2026-05-16
- **Source of truth:** Code paths cited inline (file references in the Decision section) + [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md).
- **Status:** Accepted.

---

## Context

The system publishes to external channels (Google Sheets, Telegram, QuantStats PDF). These channels fail more often than internal compute does: network glitches, rate limits, OAuth token expiry, sheet permission changes.

If publish were tightly coupled to the rest of the pipeline — e.g. compute + publish in one process — then any publish failure would force re-computation of everything, wasting time and producing slightly different results (timestamps shift, data may have updated, etc.).

## Decision

The publish stage reads **only materialized artifacts** from `data/pipeline_runs/<run_id>/<stage>/attempt_<n>/`. It does not depend on any in-memory state from prior stages.

Consequences for the retry path:

```bash
# A run completed all stages except publish failed.
ai-trading-pipeline --run-id <id> --stages publish
```

This retries publish using the **same artifacts** the original attempt would have used. The result is byte-equivalent to a successful first attempt (modulo the channel state — e.g. a Telegram retry might send the same message twice if dedupe is misconfigured).

Per-channel roles (see [`docs/reference/publish_contracts.md`](../reference/publish_contracts.md)):

- `publish_of_record` / `publish_auxiliary` (blocking) — failure fails the stage
- `publish_optional` / `diagnostic` (non-blocking) — failure logs and continues
- `informational` (blocking) — failure fails the stage

Dedupe key is `run_id + channel + artifact_hash`. Re-sending the same payload is a no-op.

## Consequences

**Positive:**
- Publish failures don't cost compute time.
- The artifact set is the contract between compute and publish. Channels can be added without touching compute.
- Re-running publish is safe by default.
- A `--local-publish` mode writes only a local summary, useful for offline verification.

**Negative:**
- Channels that legitimately need to re-fetch live data (e.g. portfolio P&L) have a tension: artifacts are stale-by-design, but the portfolio sheet wants now-prices. Current resolution: portfolio channel is `publish_optional` and tolerates fetching at publish time.
- The artifact filesystem layout becomes a public contract. Renaming a column in `ranked_signals.csv` may break publish channels silently. Mitigation: integration tests cover the channel → artifact contract.

## See also

- [`docs/stages/publish.md`](../stages/publish.md)
- [`docs/reference/publish_contracts.md`](../reference/publish_contracts.md)
- [`docs/runbooks/publish_retry.md`](../runbooks/publish_retry.md)
