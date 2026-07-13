# Security and execution safety

- **Purpose:** Document security posture, execution-path safety, confirmed gaps, and live-readiness gates.
- **Audience:** Operators, security reviewers, and execution/risk maintainers.
- **Last verified:** 2026-07-13
- **Source of truth:** The cited API, execution, persistence, configuration, and local file-mode evidence.

---

## Verdict

Paper execution is effectively enforced in the pipeline today and preview paths do not place broker orders. That is a strong safety baseline. The system is not ready to enable live placement: batch portfolio heat can be undercounted, submissions are not idempotent, stop state assumes immediate fills, API exposure is permissive, and local credential files are world-readable by default.

## Threat and control matrix

| Surface | Current control | Confirmed gap | Required action |
|---|---|---|---|
| Live broker placement | execute stage constructs `PaperExecutionAdapter`; Dhan adapter rejects non-dry-run | future wiring could bypass a centralized live gate | one explicit, audited live-mode capability requiring operator enablement and startup interlock |
| Preview/diagnostic | preview avoids state-changing stop updates and uses paper path | contracts are behavioral rather than type-enforced | read-only adapter/interface plus mutation-denial tests |
| SQL | many paths parameterize values | `analytics/feature_reader.py` interpolates filters; other internal loaders build SQL strings | bind all market/user values; whitelist identifiers only |
| API auth/network | optional API key middleware | binds `0.0.0.0`; permissive CORS with credentials; no roles | localhost default, explicit origins, mandatory auth for mutations, role/audit model |
| Process control | termination validates recognized project process | authorization is coarse | privileged operator role, immutable audit event, tests against PID substitution |
| Credentials | ignored by Git | `.env`, OAuth client, and token files are mode 0644; OAuth flow prints token prefix | require 0600, redact all token material, document key rotation |
| Secrets in config | environment based | 89 direct env reads and representations can drift | centralized secret-safe settings |
| Artifacts/downloads | traversal protection is tested | non-atomic writes and mutable registered paths | atomic write/promote and content-hash verification |
| External delivery | retry/dedupe manager | publish can recompute and write upstream state | make delivery consume registered immutable artifacts only |

## SQL injection review

No claim is made that the whole repository is injectable. The confirmed violation is that `analytics/feature_reader.py` constructs `WHERE` clauses from exchange, symbol, date, pattern, and limit values. Ranking loaders also interpolate internal exchange/date values. Even when callers are currently trusted, this conflicts with the repository SQL contract and makes later exposure unsafe.

Remediation pattern:

```python
placeholders = ", ".join("?" for _ in symbols)
sql = f"SELECT ... WHERE symbol IN ({placeholders}) AND trade_date <= ?"
params = [*symbols, cutoff]
conn.execute(sql, params)
```

Only table/column identifiers selected from closed internal enums may be interpolated. Add static tests that reject f-string/format SQL in repository query modules, with narrow suppressions for whitelisted identifiers.

## Full execution path trace

```text
ranked candidates
  -> execute stage builds current price map and portfolio state
  -> Autotrader applies eligibility, concentration and heat gates
  -> ExecutionService submits through adapter
  -> PaperExecutionAdapter produces simulated order/fill
  -> execution store persists order, fill, decision, and stop state
  -> later execute run evaluates/trails active stops
```

### What is safe now

- The pipeline execute stage explicitly uses `PaperExecutionAdapter`.
- The Dhan execution adapter defaults to dry-run and rejects live placement in the inspected path.
- Current-price stop evaluation uses the ranked close map, not the stop value as market price.
- Trailing-stop updates exist and are skipped in preview.
- Concentration is recalculated against the evolving in-memory portfolio during a batch.

### What blocks live mode

1. Portfolio heat/open risk is calculated once from `positions_before` and reused for every buy. Several individually permitted orders can jointly exceed the portfolio limit.
2. `correlation_id` is stored but not uniquely constrained or checked before adapter submission. A retry can create a new paper/broker order and fill.
3. A BUY stop can be activated immediately after submission rather than after a confirmed fill. Conversely, a submitted OPEN/partial sell can deactivate an existing stop because only rejected/error outcomes retain it.
4. Order, fill, stop, and decision transitions are not modeled as one durable idempotent state machine with reconciliation.
5. There is no centralized live-mode interlock covering CLI, API, scheduled pipeline, and direct adapter construction.

## Required execution state model

Use durable identities and explicit transitions:

```text
decision -> submission_intent -> submitted -> acknowledged
        -> partially_filled -> filled
        -> rejected | cancelled | expired | reconciliation_required
```

`submission_intent` must be committed with a unique idempotency key before contacting the broker. Adapter retries reuse that key. Broker responses and reconciliation advance state monotonically. Position and stop changes derive from confirmed cumulative fills, not from submission success. Unknown outcomes block resubmission until reconciliation.

## Portfolio and stop policy requirements

For every candidate, recompute projected gross/net/sector/single-name exposure and capital at risk using all earlier accepted orders and expected fill quantity. Reserve risk at submission, then reconcile it to actual fills. Concurrent workers must acquire one portfolio decision lock or use optimistic versioning.

Stop evaluation should use an explicit price-source record: symbol, value, market timestamp, ingestion timestamp, trust status, and freshness. Stale/untrusted prices must not silently trigger or suppress live orders. Gaps through stops need a documented order policy. Trailing anchors update only from trusted market observations; stop activation/deactivation follows fills.

## API and operator hardening

- Bind to loopback by default; require an explicit setting for remote bind.
- Replace wildcard CORS with configured origins and do not combine wildcard origins with credentials.
- Require authentication for every mutating route and stronger authorization for process termination, execution, repair, and configuration changes.
- Add response models and bounded request schemas; replace generic `dict[str, Any]` stage parameters with discriminated typed structures.
- Emit append-only audit events for operator, action, target, before/after state, correlation ID, and result.
- Rate-limit authentication and expensive task creation endpoints.
- Never expose environment, filesystem roots, command lines containing secrets, or raw exception traces in API responses.

## Credential handling

Change local secret files to owner read/write only and add a startup warning or refusal for permissive modes. Remove refresh-token prefix printing from the OAuth flow. Store Google refresh tokens in the OS keychain or an operator-approved secret store where feasible. Keep `.env` ignored, add secret scanning to CI, and document rotation for every provider.

## Go-live gate

Live placement must remain disabled until all of these pass:

- unique idempotency and crash-recovery tests around broker submission;
- multi-order and concurrent portfolio-risk tests;
- partial-fill, cancel, reject, timeout, and unknown-outcome reconciliation tests;
- stop activation/deactivation tests driven by fill quantities;
- mandatory authenticated operator interlock and immutable audit log;
- paper shadow run against production-shaped inputs for an operator-defined period;
- rollback/runbook exercise with no live-store mutation during diagnostics.
