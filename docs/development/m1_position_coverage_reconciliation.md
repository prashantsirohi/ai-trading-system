# M1 Position Coverage Reconciliation

- **Purpose:** Explain the September 7 run's 5/9/4 position counts and define a bounded repair plan.
- **Audience:** Operator, opportunity-monitoring and execution implementers.
- **Last verified:** 2026-09-09
- **Source of truth:** Registered artifacts for `shadow-2026-09-07-193455`, read-only execution fills/orders, and source modules linked below.
- **Status:** Investigation complete for this run. R1 and the R2 reporting/API/UI separation are implemented in the working tree; R3–R5 remain planned. No lifecycle recovery or broker action was performed.

## Result

There were **five distinct active paper position cycles before execution**, not nine. All five passed scan-router route/market-data coverage. None could attach to a compatible canonical episode. Opportunity orchestration processed four cycles a second time through fundamental-lane bundles, inflating its denominator and recovery/compatibility artifact rows to nine.

The correct cycle-grained reading of the existing evidence is therefore:

- Router market/routing coverage: **5/5**.
- Canonical episode attachment: **0/5**.
- Opportunity summary as actually written: **0/9**, counting bundles rather than distinct cycles.
- Post-execution open paper positions: **4**, after three exits and two new entries.

Correcting duplicate counting would not make attachment coverage pass. The zero numerator is supported by the recorded compatibility reasons. These are shadow lifecycle gaps; this finding does not mean there were nine unmonitored broker positions or that market data was absent for all positions.

## Scope, time, and provenance

Inspection used checkout `8b10f58` on `main`. Operational roots were resolved with `.env` and `get_domain_paths(data_domain="operational")`; DuckDB connections were read-only. Source artifacts were joined to their exact completed stage attempts and checked against their registered hashes. No runtime service that initializes schemas was called.

The run is named for decision session **2026-09-07**, but these stage attempts were recorded on **2026-09-08**: scan router 15:18–15:18, opportunities 15:26–15:29, and execute 15:29–15:29 in control-plane timestamps. This distinction matters: a source-session label is not an execution observation timestamp. Fill/order membership was additionally verified through correlation IDs prefixed by the exact run ID, avoiding an assumption that naive timestamps across stores share the same clock convention.

Signed-fill aggregation before the first fill attributed to this execution run yielded exactly the five identities in the table below, all `paper`/`NSE`. The immutable pre-execution routing artifact corroborates that set. No paper balances were combined with the actual journal. Prices, quantities, account identifiers, and credentials are intentionally not copied into this document.

## Per-cycle explanation

All five rows have a valid `POSITION_MONITOR` route and no missing router market-data fields. The cycle IDs and reasons below come from the registered opportunity compatibility/reconciliation artifacts.

| Symbol (NSE, paper) | Position cycle ID | Opportunity rows | Recorded attachment failure | Subsequent execution in this run |
|---|---|---:|---|---|
| ENTERO | `position-cycle-bed1ba0761b37acce5917e74` | 2 | Technical `early_accumulation` and `fundamental_thesis` episodes both at `setup_forming`; neither lifecycle is position-compatible. Reported as `ambiguous_multiple_episodes` | Retained |
| MANINDS | `position-cycle-dd4ac0bb4e1b0bde87d2943d` | 2 | Technical `early_accumulation` and `fundamental_thesis` episodes both at `setup_forming`; neither lifecycle is position-compatible. Reported as `ambiguous_multiple_episodes` | Filled SELL, `rebalance_out` |
| RATNAVEER | `position-cycle-bab7c5080a02dedeaa4c9ba1` | 1 | `momentum_leader` episode at `investigating`; `insufficient_evidence` because lifecycle is not position-compatible | Filled SELL, `rebalance_out` |
| RAYMOND | `position-cycle-34c93d2588d0190dee6b23ed` | 2 | Fundamental episode exists but trigger timing is unavailable; `insufficient_evidence`. The inspected episode had no snapshot/transition available at the decision cutoff | Retained |
| WELCORP | `position-cycle-d9011d9145abce9bf7f592e9` | 2 | Evaluated momentum episode is `investigating`; fundamental episode is `weakening` but its last transition is outside the compatibility window. Reported as `ambiguous_multiple_episodes` | Filled SELL, `rebalance_out` |

The repeated rows have the **same cycle IDs and recovery proposal IDs**, not separate fills or positions. Each repeat adds `fundamental_discovery` to its source lineage. RATNAVEER has only the original bundle. All proposals remain `report_only`; no recovery actions were present in the run.

The artifact reports three ambiguous **cycles** and two insufficient-evidence **cycles**. Bundle counting expands those to six and three respectively. Multiple open episodes do not establish that multiple episodes actually qualify: none of the five had a valid attachment in the recorded evaluation. Do not relax compatibility merely to eliminate the warning.

## Exact causes in current code

### Duplicate position accounting follows discovery-lane expansion

[Opportunity orchestration](../../src/ai_trading_system/domains/opportunities/orchestration/service.py) attaches routing/position fields, then `_attach_fundamental_thesis_bundles` uses `replace(base, ...)` to append an independent fundamental bundle. It preserves `active_position`, cycle identity, routing ID, and market completeness. The main loop increments `active_positions_total` and performs compatibility/recovery for **each active bundle**.

This is the source of 5 unique cycles becoming 9 counts. It also duplicates compatibility and recovery artifact rows. The durable proposal identity remains cycle-based, so duplicate artifact rows alone do not demonstrate duplicate database proposals.

### Router coverage and opportunity coverage mean different things

[`ScanRouterStage._position_coverage`](../../src/ai_trading_system/pipeline/stages/scan_router.py) checks cycle identity, validated route, current market session, weekly structure, relative strength, and symbol mapping. Its `FULLY_MONITORED` records have `episode_match_status=not_evaluated`.

The opportunity loop requires compatible episode attachment before incrementing its fully-monitored counter. Its compatible branch uses market completeness and routing-ID presence; Investigator evidence completeness is a separate counter. Consequently, the same label currently expresses different predicates. The run's separate 8/9 Investigator-evidence counter is also bundle-grained and must not be advertised as eight distinct covered positions.

### Attachment policy is stricter than the execution entry path

[`evaluate_position_episode_compatibility`](../../src/ai_trading_system/domains/opportunities/position_monitoring.py) requires transition timing and a lifecycle in `triggered`, `pending_followthrough`, `confirmed`, `advancing`, or `weakening`, except for a matching explicit recovery identity. It reports ambiguity whenever more than one open episode exists, even when none qualifies. Its parameter named `trigger_alignment_sessions` currently measures calendar-day differences from `last_transition_at`, not market sessions or a dedicated entry-trigger timestamp.

Execution independently consumes rank and risk/portfolio policy, so a legitimate paper fill does not imply an existing canonical confirmation. This explains the current missing attachments without proving a broker or ranking defect. A repair must preserve that separation and avoid inventing pre-entry history.

## Why execution reports four positions

The registered `executed_fills` artifact and correlated execution ledger agree:

| Change | Symbols | Evidence |
|---|---|---|
| Three pre-existing positions closed | WELCORP, MANINDS, RATNAVEER | SELL fills; execution decisions `EXECUTED` / `ORDER_ACCEPTED` |
| Two positions opened | KRN, MSTCLTD | BUY fills; execution decisions `EXECUTED` / `ORDER_ACCEPTED` |
| Two positions retained | ENTERO, RAYMOND | Remain in `positions_after` |
| Proposed entry did not become a position | BLISSGVS | `SUPPRESSED` / `PORTFOLIO_CONSTRAINT_LIMIT`; no corresponding run order/fill |

Thus **5 − 3 + 2 = 4**. The positions artifact contains ENTERO, RAYMOND, KRN, and MSTCLTD. It is generated from `result["positions_after"]` by [ExecuteStage](../../src/ai_trading_system/pipeline/stages/execute.py). Pre-execution monitoring and post-execution holdings must not be compared as the same population. The new KRN/MSTCLTD cycles require a later monitoring observation; this audit does not claim the September 7 pre-execution route covered them.

## Bounded repair plan

| Step | Owner and change | Required verification / acceptance |
|---|---|---|
| R1: Reconcile once per cycle | Opportunity orchestration: perform position compatibility/recovery and position counters once per `(exchange, symbol, position_cycle_id)` before or independently of discovery-lane expansion. Lane snapshots may reference the result without repeating position writes/counters | Fixture with 5 active cycles and 4 fundamental duplicates must produce 5 reconciliation records and preserve all intended lane observations. This recorded case must remain 0/5 attached, with 3 ambiguous and 2 insufficient-evidence cycles under unchanged policy. Retry must not multiply proposals |
| R2: Separate coverage predicates | Router/opportunity artifact and API owners: expose route/data coverage, episode attachment, and evidence completeness as separate facts; define any aggregate explicitly with its observation time and population | Reconcile unique-cycle denominators across artifacts and UI. Duplicate lanes cannot improve or worsen coverage. Missing data/route/cycle stays fail-closed. Version schemas/policies where semantics change; preserve historical artifacts |
| R3: Clarify compatibility policy | Opportunity policy owner: distinguish zero compatible episodes from multiple compatible claims; define lane ownership, the correct trigger event, and calendar-day versus trading-session alignment before changing matching | Versioned successor if behavior changes. Tests for technical/fundamental coexistence, multiple valid claims, no valid claim, holidays, missing trigger, post-entry episode, and recovered-cycle identity. Never match on symbol alone. This case provides no evidence to attach any of the five automatically |
| R4: Keep recovery explicit | Registry/operator: retain report-only proposals; prepare position-only recovery only for cycles still open at the intended recovery cutoff, using the documented reviewed-recovery process | Backed-up/copied-store validation before any live migration. No fabricated discovery/confirmation events. Already exited historical cycles are not blindly recovered. Broker and execution ledger unchanged |
| R5: Align observation boundaries | Execution/monitoring/read-model owners: record the fill cutoff or input snapshot used by monitoring and label pre/post-execution populations. Existing `list_position_cycles()` reads all available fills without an as-of argument | Historical/copy replay excludes later fills by an explicit boundary; same snapshot produces same cycle set. Demonstrate 5 pre-execution and 4 post-execution for this run. Do not silently change pipeline ordering |

**Recommended next implementation:** R1, followed by R2. They correct accounting and presentation while preserving attachment eligibility. R3–R5 require their explicit contracts; they are not prerequisites for accurately reporting 0/5 today. Applying reviewed recovery or transferring lifecycle authority is outside this audit task.

## Validation and evidence references

Artifacts are under configured `$DATA_ROOT/pipeline_runs/shadow-2026-09-07-193455/<stage>/attempt_1/`. Actual bytes matched registered SHA-256 values:

| Stage / artifact | SHA-256 |
|---|---|
| scan_router / active_position_coverage | `967a519f003d2fbb09913d42216553d7ba43945ed1600a75560d5e78a0e7a4f4` |
| opportunities / position_monitor_reconciliation | `bd965dcf8bf3bf33aed271577265e35ea4f4a3f99f1193de9f08aa7660619711` |
| opportunities / position_recovery_proposals | `d504dadae908df3319bc815395dc77d08e2c664df857a0e47c6ae52b781b1445` |
| execute / executed_fills | `223ba69fe8d9e9693692e1c82bdea04dc192083f744e57f8687de8a9de49b27f` |
| execute / execution_decisions | `a072cbe5aa6b6efddb646370685590b981f9bef36bb495e0529a9c919ba08544` |
| execute / positions | `b9476c45311e0ecda10e38dd90a027981b50366b4964d2cdca51459515f2221c` |

The opportunity compatibility, router reconciliation, and trade-actions artifact bytes were also hash-verified. Ledger queries bound run/symbol parameters and read only explicit required columns. Current database records were not frozen into a full copy; immutable artifacts own the reported historical compatibility outcomes.

All 9 existing Phase 3C-3 compatibility and position-monitoring tests passed, separately from the live evidence reads. Their passing status validates their covered contracts, not the proposed repairs. Documentation validation passed for 126 current documents, and `git diff --check` passed. No application code, live database, or broker state changed.

## First operator-baseline review

The [M1 baseline form](m1_decision_ownership_audit.md#five-session-baseline-protocol) remains at **0/5 measured sessions**. For the first actual review:

1. Record review date, source market session/run, and the surfaces actually opened. Use current completed evidence for the operational review; this September 7 case is a historical reconciliation.
2. Start active review timing; review source health and position exceptions first, then discoveries/changes, continuing watches, and entry-review evidence.
3. Record unique listings reviewed, unchanged alert repeats, unresolved evidence gaps, and listings requiring investigation. Stop timing after the review and exclude interruptions.
4. Supply those observations to fill session 1; keep unmeasured fields blank. The engineering time spent on this audit is not an operator-review baseline.

No review-session measurement or daily-surface preference was inferred from the instruction to perform this reconciliation.

## Implementation follow-up — 2026-09-09

Cycle deduplication and separate reporting fields are implemented, including nullable API fields and distinct dashboard labels. Duplicate active-position lanes retain evidence/reference rows without reapplying attachment/recovery. No compatibility policy was relaxed, no historical artifact was replaced, and no live recovery was applied. The implementation uses the first routing-owned position bundle; non-position fundamental episodes retain their independent processing. Validation and remaining limitations are recorded in the consolidation plan.

## Copied real-data validation — 2026-09-09

**R1/R2 validation passed.** A bounded replay used the five recorded position listings and their original upstream rows, including four fundamental lane copies. Both attempts produced:

| Predicate | Result |
|---|---:|
| Unique exchange/symbol/cycle reconciliations | 5 |
| Additional lane references | 4 |
| Route and market data covered | 5/5 |
| Investigator evidence complete | 4/5 |
| Canonical episode attached | 0/5 |
| Ambiguous / insufficient-evidence cycles | 3 / 2 |
| Recovery proposals emitted per attempt | 5 unique |
| Additional durable proposals / recovery actions | 0 / 0 |

The source control plane was opened read-only, with no WAL present, and held open while copying to a regular disposable file. Its SHA-256 matched the copy before writes and remained `0c9dc6f904bd8e4a340de7f5f34daec75b12342f4f64333ec6d5c7e77e7178ed` after replay. All 202 promoted artifacts from completed attempts of the source run were copied and checked against registered hashes. Replay inputs filtered symbol-bearing CSVs to the five real listings while retaining sector/context inputs; no market rows were generated. Writable roots were isolated below the disposable directory.

The copied registry contains completed historical work, not a reconstructed pre-run database. For the selected positions, all available snapshot timestamps preceded September 7, and the replay's cycle/status pairs exactly matched the immutable compatibility artifact. Existing deterministic proposals were retained on both attempts. This validates position accounting and retry behavior on the recorded case; it does not establish equivalence for unrelated lifecycle or performance calculations.

The copied-store API first read the original artifacts and returned unknown attachment for all five rows. After registering replay artifacts only in the disposable database, it returned route/data coverage true, attachment false, and reconciliation session September 7 for every cycle. API reads left the copied database hash unchanged. Headless Chromium rendered those API responses: the overview showed five router-covered positions and zero attachments; all five table rows showed route/data **Yes** and attachment **No**. Original-artifact responses showed attachment **Unavailable**. Browser requests were GET-only; unrelated dashboard endpoints used static responses for this bounded check.

Evidence and replay helpers are retained temporarily at `/private/tmp/m1-position-replay-fdbvge86/`: `manifest.json`, structured `attempt-2.json`, original/bounded artifacts, `api-legacy.json`, `api-replay.json`, `dashboard-check.json`, and overview/position screenshots. This directory is disposable and is not a committed runtime dataset.

Final checks: 144 backend tests, 46 frontend tests, TypeScript checking, production build, OpenAPI snapshot verification, documentation validation, and whitespace checks passed. The build retains its existing large-chunk advisory. This was a service-level artifact replay, not a full pipeline canary: ingest, feature/rank recomputation, OHLCV enrichment, execution, and publishing were not run. `--local-publish` was therefore not invoked. No feature rebuild is required. R3–R5 and the unmeasured M1 operator baseline remain open.
