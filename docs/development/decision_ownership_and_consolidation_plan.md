# Decision Ownership and Consolidation Plan

- **Purpose:** Define the proposed decision owners, delivery sequence, and acceptance gates for consolidating Phase 3.5 into a coherent operator workflow.
- **Audience:** Operator, implementing engineers, and reviewers.
- **Last verified:** 2026-09-13
- **Source of truth:** Current boundaries in [System Guide](../SYSTEM_GUIDE.md), [opportunity registry](../architecture/opportunity_registry.md), [candidate tracker](../stages/candidate_tracker.md), and [decision read-model migration](../architecture/decision_read_model_migration.md). This document owns the proposed consolidation backlog only.
- **Status:** M1 repository audit prepared; G1 remains open. This document does not change runtime authority, deployment readiness, or execution permissions.

## Objective and scope

Make the daily system answer: what deserves investigation, what changed, what is awaiting confirmation, and which positions require attention. Preserve separate evidence, lifecycle, admission, and execution decisions so each answer is traceable to its owner.

The destination is a research-backed operator decision system with one lifecycle authority per responsibility and a controlled execution service downstream. Delivery follows four gates: decision ownership, trustworthy measurement, a unified operator view, and incremental authority migration. This is a delivery plan, not another current system overview; the System Guide retains that role.

The operator owns workflow priorities, evaluation acceptance criteria, and eventual authority-transfer acceptance. Implementing engineers own contracts, code, verification, and rollback evidence. Each implementation change must name its responsible engineer and reviewer; no additional staffing is assumed.

## Current boundaries and proposed ownership

Module paths below are relative to `src/ai_trading_system/`. Proposed ownership is conditional on the gates in this plan.

| Decision or fact | Current owner / boundary | Proposed destination |
|---|---|---|
| Relative opportunity rank | `domains/ranking/`; full-universe history is analytical evidence and `ranked_signals` is the actionable rank shortlist | Retain ranking ownership. A lane observation does not manufacture rank eligibility. |
| Discovery evidence | `domains/investigator/`, `fundamental_discovery` stage, and `pattern_lane_scan` stage retain source-specific evidence | Retain independent I/F/P evidence and exclusions. Convergence describes overlap; it does not admit a trade. |
| Operational candidate selection | `domains/candidates/` and the `candidates` stage produce `final_candidates`; execution also uses `ExecutionCandidateBuilder` | Retain until entry-selection interfaces and consumers are explicitly reconciled. Do not assume the operator shortlist and execution selection are identical. |
| Watchlist episode and lifecycle | `domains/candidate_tracker/service.py` owns the current operational tracker; `domains/opportunities/registry/` owns separate canonical shadow history | Transfer individually mapped lifecycle responsibilities to `OpportunityRegistryService` only after M4. Do not treat tracker reviews and canonical transitions as equivalent without a mapping. |
| Shadow admission and confirmation | `domains/opportunities/orchestration/` evaluates canonical shadow policy | Retain as shadow evidence throughout this program. Any operational admission-policy transfer requires a separately reviewed interface and policy decision. |
| Actual positions, orders, fills, and stops | `domains/execution/` owns the execution ledger; `domains/trade_journal/` owns the separate actual-trading journal | Preserve both scopes. Pin account, source, and paper/actual mode on every position projection; never combine their balances implicitly. |
| Permission to submit an order | `pipeline/stages/execute.py`, `ExecutionService`, and risk/portfolio gates | Retain execution authority and paper default. The consolidated view cannot grant broker permission. |
| Performance facts | Investigator and convergence evaluators, technical cohorts, research performance, and the journal answer different questions | Share calculation contracts and reusable primitives while retaining separate discovery, strategy, and actual-trade populations. |
| Operator presentation | Existing API/read models, React console, and publish consumers | Google Sheets is the primary daily workspace; Telegram supplies summaries; the dashboard supports mostly weekend/research use (operator confirmed 2026-09-12). Consolidate governed projections around these roles. |

## M1 — Freeze the operating decision contract

The [M1 decision ownership audit](m1_decision_ownership_audit.md) records the verified consumer matrix, real-case evidence, decision contracts, ownership gaps, and blank five-session operator baseline. Missing canonical confirmation/closure examples and operator acceptance remain explicit G1 requirements.

**Dependency:** None. **Implementation owner:** Opportunity orchestration interfaces and existing operator read-model owners. **Acceptance owner:** Operator.

1. Walk through the current daily review using recorded real examples: a new discovery, a continuing watch, a confirmation, an invalidation, an open position, and incomplete evidence.
2. Enumerate the exact producer, consumer, source, and decision owner for each step. Include API, Sheets, PDF, alerts, and execution inputs; use the existing read-model migration matrix rather than duplicating its source-selection rules.
3. Specify each decision's required inputs, freshness/cutoff, policy version, reasons, allowed output, and missing-data behavior. Distinguish an operator attention queue from a canonical lifecycle state and an executable instruction.
4. Define proposed attention queues: **Investigate**, **Watch**, **Entry review**, and **Position attention**. Shadow entry eligibility must be visibly identified as shadow. Displayed conditions must come from recorded policy evidence, not new UI rules.
5. Freeze baseline workflow measurements over five completed operator sessions: review time, unique securities reviewed, repeated unchanged alerts, unresolved evidence gaps, and number requiring investigation. Treat these as usability baselines, not trading results.

**Deliverables:** The completed ownership/consumer matrix, decision contracts, representative walkthroughs, and operator baseline. Update the relevant detailed contracts when implementation changes them.

**Gate G1:** Every decision has one named authority; every consumer has a declared source and fallback; all six walkthrough categories have an explainable outcome. Operator records acceptance of queue meanings and the daily workflow. Unresolved ownership is a blocker to dependent migration, not a reason to guess.

## M2 — Establish comparable performance evidence

**Dependency:** G1 for the evaluation questions. Correctness repair and inventory can begin immediately. **Implementation owner:** Existing performance evaluators and research measurement modules.

Create one versioned measurement contract covering:

- Market-session calendar, exchange coverage, absent/suspended symbols, and invalid or quarantined observations.
- Discovery-close, confirmation-close, next-open shadow fill, and actual-fill anchors, including entry-session excursions and stop timing.
- Adjusted price basis, costs/slippage, benchmark and sector alignment, and mapping provenance.
- Effective time versus recording time; historical reconstruction versus contemporaneously captured evidence.
- Explicit pending, invalid, excluded, and matured states; correction handling without repainting frozen evidence.
- Observation grain and repeated-symbol/episode overlap. Raw row count is not an independent sample count.
- Policy snapshots: cross-version pooling must be declared and justified, never automatic.

First reconcile a fixed set of identical observations across existing calculators. Extract shared pure calculation primitives where behavior is genuinely identical; do not create a new performance store or force unrelated populations into one evaluator.

The 2026-09-08 review identified five repair candidates to recheck against current code before patching: stale pattern freshness overriding session age; symbol-row counts replacing market-session horizons; omitted entry-session risk; null-price anchors that cannot recover; and incomplete windows counted toward stability. Preserve versioned policy semantics when changing calculations.

Freeze the evaluation design before inspecting comparative results:

| Question | Comparison | Required controls |
|---|---|---|
| Does convergence improve selection? | Mutually exclusive I/F/P cohorts and separately labelled lane marginals | Common date/universe/anchor, missing-data coverage, sector/regime mix, repeated-symbol dependence |
| Does ranking discriminate within a lane? | Predeclared rank bands within the same lane population | Unranked evidence remains a separate group; never assign it an invented rank |
| Does confirmation help? | Paired discovery, confirmation, and fill results where available | Report confirmation conversion and non-confirmers; avoid evaluating only successful survivors |
| Does the system help daily review? | Operator baseline versus consolidated-view trial | Comparable session workload; retain counts of hidden or deferred items |

Choose one primary comparison and material improvement threshold with the operator before comparative evaluation. Freeze confidence method, minimum independent sample requirement, observation period, window completion rules, and stopping rule. Existing Phase 3.5 counts/readiness checks remain current policy; this plan does not silently replace them or assert that passing them proves economic value.

**Deliverables:** Measurement contract, reconciled fixtures, correction/version policy, frozen evaluation specification, and a forward shadow collection log. Record insufficient evidence as such; no positive result is required to complete engineering verification.

**Gate G2:** Matching inputs produce matching outputs within documented precision across applicable consumers. All five review scenarios have verified dispositions and regression coverage where relevant. Missing sessions, policy changes, and incomplete windows cannot silently become accepted samples. Forward reports disclose maturity, coverage, overlap, and uncertainty.

## M3 — Consolidate the operator view

**Dependency:** G1 for design; G2 before performance comparisons are presented as validated. **Implementation owner:** Existing publish/Google Sheets and Telegram channel owners, with API/read-model and React console owners for shared evidence and research views.

Prioritize the existing Google Sheets daily workflow and Telegram summary, following the operator's September 12 clarification. Reuse governed projections through existing publish/read-model boundaries; map the actual workbook tabs before choosing layout changes. Retain the dashboard for weekend/research detail. For dashboard/API changes, follow the [Phase 4A](../runbooks/phase4a_read_only_api.md) and [Phase 4B](../runbooks/phase4b_operator_dashboard.md) restrictions; passing this milestone does not lift their production-readiness limitations.

Each security has one visible listing identity with separate episodes/theses underneath. Show source-specific I/F/P states, rank context when available, lifecycle authority, eligibility/blockers, changes since the previous session, source dates, policy versions, and evidence links. Missing data and disagreement must remain visible. Distinguish structural new-long blocks from monitoring an already open position.

Queue membership is presentation over authoritative facts. It creates no lifecycle transitions, new admission policy, order intents, or persistent parallel watchlist. Multiple applicable queues may reference the same episode; show unique-security counts and explain overlap.

Pilot the Sheets-led daily workflow and Telegram summary for five completed operator sessions; record dashboard research use separately when applicable. Before the trial, set a review-time/workload target using the M1 baseline. Check every mapped open position remains discoverable, every queue item has a traceable reason, missing evidence is visible, and no task requires manual reconciliation between conflicting lifecycle authorities. Keep immutable artifact downloads and specialist research views accessible.

**Deliverables:** Read-only workspace, source/authority labels, walkthrough evidence, and trial results against the frozen usability target.

**Gate G3:** Operator can complete the defined daily review from the workspace; all sampled rows reconcile to their declared sources; position coverage has no unexplained omissions; the usability target passes or is explicitly revised with evidence. No broker mutation or shadow-to-operational authority change is introduced.

## M4 — Transfer lifecycle responsibilities and retire duplication

**Dependency:** G1–G3. **Implementation owner:** Candidate tracker and canonical registry owners, with each affected consumer owner.

Prepare one migration record per responsibility: episode identity/open-close state, snapshots, reviews, alerts, and downstream current-state reads. Each record must contain the old writer/source, proposed writer/source, exact consumers, semantic mapping, parity evidence, cutover boundary, rollback method, and retirement criteria.

1. Inventory all direct and indirect tracker consumers, including scheduled commands and dynamic artifact readers. Mark each **retain**, **migrate**, or **retire** with a reason.
2. Compare both paths on copied stores and identical recorded inputs, with at least ten completed sessions plus targeted retry, closure, re-entry, multi-lane, correction, and missing-data cases. All differences must be classified; safety, identity, position coverage, and time-cutoff differences require resolution before cutover.
3. Switch read-only consumers individually after parity is established. Preserve explicit source labels and the last verified fallback. A consumer reading canonical shadow history must still disclose that authority status.
4. For each operational lifecycle transfer, make a versioned boundary and record operator acceptance. New writes have one operational owner for that responsibility; legacy comparison output remains explicitly non-authoritative.
5. Rehearse rollback on a copy. Returning reads alone is insufficient after writers change: prove the old path can recover intervening inputs without losing episodes, duplicating alerts, or overwriting history. If recovery is unproven, suspend the affected authority transfer.
6. Retire old writers only after ten additional completed sessions without unexplained divergence and a successful rollback rehearsal. Archive old history read-only; remove obsolete code only after consumer checks and documented retention decisions.

Do not fabricate pre-cutover lifecycle events. Use an explicit legacy reference or bounded initial state with provenance where semantic history cannot be reconstructed. Live-store changes require the repository's backup and migration procedures and a separately authorized implementation task.

**Deliverables:** Responsibility migration records, parity results, cutover/rollback runbook, and an audited retirement list.

**Gate G4:** Each transferred responsibility has exactly one operational writer, every affected consumer uses its declared authority, rollback has been demonstrated, and no unresolved parity defect remains. Execution selection, sizing, stops, broker placement, and the actual journal retain their existing ownership.

## Delivery order and evidence of completion

M1 audit work and M2 correctness implementation are recorded below; M3/M4 remain **NOT STARTED**. No acceptance gate has passed.

| Work item | Dependency | Completion evidence |
|---|---|---|
| D1: Operator walkthrough and consumer inventory | None | G1 ownership matrix and source/consumer map |
| D2: Freeze decision and evaluation contracts | D1 | Accepted workflow; frozen comparison/threshold specification |
| D3: Reconcile measurements and repair defects | Inventory can start now; final contract from D2 | G2 calculation and coverage evidence |
| D4: Build read-only workspace | D2; validated performance depends on D3 | Source reconciliation and G3 operator trial |
| D5: Prepare responsibility migrations | D1; cutover depends on G1–G3 | Copied-store parity and rollback records |
| D6: Transfer consumers/writers and retire | D5 and per-responsibility acceptance | G4 cutover, observation, and retirement evidence |

Planning envelope: week 1 for D1–D2; weeks 2–3 for D3; weeks 3–4 for D4 if G2 permits. D5 preparation can overlap. D6 is gate-driven, not date-driven. Forward maturation needs actual market sessions; existing trustworthy history can support evaluation, but implementation time and reconstructed data cannot substitute for missing forward evidence.

Record progress here using date, work item, implementation reference, verification result, unresolved blockers, and acceptance decision. A failed economic comparison is a valid finding and should guide simplification; it must not be hidden by adding signals or changing thresholds after evaluation.

| Date | Work item | Evidence and status | Acceptance |
|---|---|---|---|
| 2026-09-09 | D1 / M1; decision portion of D2 drafted | [Read-only source and recorded-case audit](m1_decision_ownership_audit.md); 39 policy/matching tests and documentation/whitespace checks passed. Consumer matrix, decision contracts, and baseline form prepared; candidates-stage consumer documentation corrected. Real canonical confirmation/closure, external consumer inventory, and five human review sessions remain open | G1 open; operator workflow acceptance pending. No runtime authority transferred |

## Deferred scope and exit criteria

M1 follow-up on 2026-09-09: [Position coverage reconciliation](m1_position_coverage_reconciliation.md) resolves the 5/9/4 denominator discrepancy and records a bounded R1–R5 repair sequence. Five cycles were counted as nine lane bundles; three paper exits and two entries explain four post-execution positions. No repair or recovery was applied. The five-session operator baseline remains 0/5 measured.

Defer new discovery lanes, a combined I/F/P super-score, automatic strategy promotion, new stores for operator projections, portfolio optimization, and live broker enablement. Research screener/J-curve work remains isolated unless a separate integration contract is accepted. Existing independent work is not disabled by this plan.

Completion means an accepted daily workflow, comparable evidence, one primary operator workspace, and resolved lifecycle ownership for the explicitly migrated responsibilities. It does not mean demonstrated profitability or production certification. Retain components with distinct responsibilities or demonstrated operator/research value; retire duplication only with verified replacements and preserved history.

## Position repair implementation — 2026-09-09

R1 cycle-scoped reconciliation and R2 separate coverage/attachment reporting are implemented through artifacts, the read-only Phase 4 API, and dashboard labels. The initial historical audit and original 0/9 artifact remain unchanged. Compatibility policy revision, reviewed live recovery, and fill-cutoff alignment (R3–R5) remain deferred. G1 is still open; implementation does not manufacture missing real confirmation/closure examples or operator-baseline measurements.

Validation: 144 backend tests passed across opportunity orchestration, stage integration, Phase 3C-3 compatibility/monitoring, and Phase 4 API. The final API evidence-projection adjustment passed its 2 targeted tests. All 46 frontend tests, TypeScript checks, production build, OpenAPI snapshot check, documentation validation, and whitespace checks passed. Vite retained a non-blocking large-chunk warning. No live pipeline/canary or recovery was run; temporary-store tests cover five cycles with four fundamental copies, persistent and dry-run recovery, retries, and missing market data. No feature rebuild is required because feature/ranking calculations are unchanged.

Copied-real-data follow-up: [bounded replay and API/browser evidence](m1_position_coverage_reconciliation.md#copied-real-data-validation--2026-09-09) passed on September 9. Two attempts reproduced five unique cycles, four lane references, 5/5 route/data coverage, and 0/5 attachments with unchanged compatibility outcomes. Retries added no durable proposals or recovery actions; the source database checksum remained unchanged. The final complete backend suite passed all 144 tests. This validates R1/R2; G1 and the remaining milestones stay open.

## M2 correctness implementation — 2026-09-09

[Measurement contract and acceptance record](m2_measurement_contract.md) documents
successors `opportunity-convergence-v1.3` and `opportunity-convergence-performance-v2`.
All five reviewed convergence defects were confirmed and repaired with regression
coverage. The corrected path separates policy-snapshot/exchange strata; legacy
convergence history stays stored and is excluded from successor maturation and
samples. The legacy Investigator calculator is unchanged, with common complete
close-anchor parity checked and its divergent cases explicitly retained as M2
follow-up. No live migration, repair, execution, or feature rebuild occurred.

Validation: the broad opportunity-domain, stage, API and wrapper suite passed
409 tests. After tightening window boundaries to include observations lacking
anchors, the targeted performance/service suite passed 47 tests. Retained real
September 7 artifacts rebuilt 1,626 interpreted observations in a temporary store;
two retries retained cardinality and, without market prices, created no anchors
or accepted performance samples. Source hashes were verified. Evidence is in
`/private/tmp/m2-real-artifact-2v0pj1p4/result.json` (disposable retrospective check).
A real-market replay remains unperformed: the live OHLCV store was locked by
another writer on both read-only checks; that process was left running.

**G2 remains open.** Primary comparative question/threshold, independent-sample
and stopping design, forward collection, broader adjustment/quarantine/calendar
and calculator reconciliation, and the copied-market replay remain required.
No comparative economic result or forward maturity is claimed.

Final M2 verification: 61 targeted convergence/source/policy-snapshot tests passed after the last calendar regression; Ruff, whitespace checks, and documentation validation (127 current documents) passed. Earlier wrapper changes remain preserved in the uncommitted worktree.

## M1 surface clarification — 2026-09-12

Operator confirmed Google Sheets for practical daily work, Telegram for summaries,
and the dashboard mostly for weekends/research. The [M1 audit](m1_decision_ownership_audit.md#operator-surface-usage--confirmed-2026-09-12)
records these roles. M3 now prioritizes that workflow rather than assuming daily
React-console use. Exact Sheets tabs, review-session measurements, queue-sequence
acceptance, and external schedules remain outstanding. This documentation update
changes no publisher, API, workbook, or runtime authority.

## Proposed stage universe and final review list — 2026-09-12

The [stage universe and final review ranking plan](stage_universe_final_review_plan.md)
defines U1–U5 as a bounded M2/M3 workstream: freeze Stage 2/late Stage 1
eligibility, independent P-or-F qualification and review ordering; build and
validate local shadow artifacts; pilot a Sheets tab; then evaluate adoption.
U1 pure policy implementation is recorded in the
[review contract](u1_review_policy_contract.md). U2 source integration is implemented;
[U3 copied-real validation](u2_u3_final_review_validation.md) passed engineering
checks while real-case acceptance remains open for calendar and source coverage. This proposal preserves upstream rank,
operational selection and lifecycle ownership; G1/G2 remain open.
