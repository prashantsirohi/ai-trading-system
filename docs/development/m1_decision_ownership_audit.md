# M1 Decision Ownership Audit

- **Purpose:** Record verified decision ownership, consumer paths, real-case walkthroughs, and the remaining G1 acceptance work.
- **Audience:** Operator and consolidation implementers.
- **Last verified:** 2026-09-19
- **Source of truth:** Source modules linked below and read-only observations of configured operational stores on 2026-09-09; governed by the [consolidation plan](decision_ownership_and_consolidation_plan.md).
- **Status:** Repository audit, queue meanings, and risk-first daily workflow accepted by the operator on 2026-09-19. G1 remains open: recorded confirmation/terminal lifecycle evidence and five operator-session measurements are incomplete.

## Scope and conclusion

**Position follow-up:** The [per-cycle reconciliation](m1_position_coverage_reconciliation.md) explains the apparent 0/9 coverage: five distinct pre-execution cycles were processed as nine lane bundles. Router route/data coverage was 5/5; canonical attachment was 0/5. Subsequent paper exits/entries explain the four post-execution positions. Historical summary values below remain quoted as recorded, not silently rewritten.

Audited checkout `8b10f58` on `main`, preserving the existing uncommitted consolidation documentation. Read operational stores with `duckdb.connect(..., read_only=True)` after loading `.env` and resolving `get_domain_paths(data_domain="operational")`. No schema initializers, pipeline stages, broker calls, or live-store mutations were invoked.

The current system has two selection paths and two lifecycle vocabularies. Execution selects from rank directly; `final_candidates` seeds the operational tracker. The canonical registry is a separate shadow lifecycle, already exposed through the read-only Phase 4 API and market MCP. Consolidation must make these authorities visible before transferring any responsibility.

The consumer inventory below covers direct source references in `src/ai_trading_system/`, related report builders, and repository `scripts/`/`tools/`. Host-level scheduled jobs and deployed UI behavior were not observed. Operator surface usage was subsequently supplied on 2026-09-12 as recorded below; schedules and detailed tab/task mapping remain inventory items; this is not an assertion that every external consumer has been found.

## Verified ownership and consumer inventory

Source links resolve beneath `src/ai_trading_system/`. **Retain/migrate/retire** are proposed dispositions, not executed changes. Existing governed rank/stage/pattern read selection remains defined by [decision read-model migration](../architecture/decision_read_model_migration.md).

| ID | Producer / source | Actual consumer and source/fallback behavior | Proposed disposition |
|---|---|---|---|
| C1 | `rank` / `ranked_signals` | [candidate builder](../../src/ai_trading_system/domains/candidates/builder.py) computes `final_candidates`; absent optional evidence changes enrichment, not source authority | **Retain** operator-selection role; name it separately from execution selection |
| C2 | `rank` / `ranked_signals`, dashboard, optional breakout/stock context | [ExecutionCandidateBuilder.build](../../src/ai_trading_system/domains/execution/candidate_builder.py) filters exchanges, trust, breakout linkage, and Stage 2 before prioritizing. [ExecuteStage](../../src/ai_trading_system/pipeline/stages/execute.py) requires rank. Neither path reads `final_candidates` | **Retain** execution selection and gates. No tracker/registry cutover can replace this input implicitly |
| C3 | `candidates` / `final_candidates`, fundamental watchlist/bucket inputs | [CandidateTrackerStage](../../src/ai_trading_system/pipeline/stages/candidate_tracker.py) requires final candidates; [tracker service](../../src/ai_trading_system/domains/candidate_tracker/service.py) unions the supplied candidate sources and persists episodes, reviews, snapshots, status alerts | **Migrate later**, responsibility by responsibility. Preserve fundamental review semantics |
| C4 | Tracker current artifact | [publish payload resolver](../../src/ai_trading_system/domains/publish/publish_payloads.py), [PublishStage](../../src/ai_trading_system/pipeline/stages/publish.py), dashboard and decision-bundle builders. Missing tracker input becomes an empty frame; rank/stage/pattern governed DB overlays have separate explicit fallbacks | **Migrate** tracker projection after parity; retain rank/stage/pattern source contract |
| C5 | Tracker current → publish decision bundle | [decision_bundle.py](../../src/ai_trading_system/domains/publish/decision_bundle.py) shapes watchlist and Telegram digest; dashboard/Sheets receive tracker context through publish. Display alerts may be derived from current status rather than the persisted tracker alert ledger | **Migrate** shared projection; define one alert identity before deduplication or retirement |
| C6 | Tracker current in supplied datasets | [weekly PDF data loader](../../src/ai_trading_system/domains/publish/channels/weekly_pdf/data_loader.py), builder, metrics, charts; absent dataset yields empty tracker coverage | **Migrate** projection; **retain** PDF as an output channel, not an authority |
| C7 | Same-run tracker CSV | [latest_operational_snapshot](../../src/ai_trading_system/ui/execution_api/services/readmodels/latest_operational_snapshot.py) chooses same-run tracker attempts by filesystem mtime; unreadable files are skipped, then empty frame. [rank_snapshot](../../src/ai_trading_system/ui/execution_api/services/readmodels/rank_snapshot.py) uses this context for ranking views | **Migrate** to declared governed resolution; do not claim this fallback already enforces completed-attempt precedence |
| C8 | Explicit tracker DB in fundamental-opportunity report | [data_loader._read_tracker_current](../../src/ai_trading_system/domains/publish/channels/fundamental_opportunities/data_loader.py) reads active `tracked_candidates`; missing path/table returns empty, read errors add warnings | **Migrate** read projection after parity; keep report classification separate |
| C9 | Fundamental-opportunity CLI with `update_tracker=True` | [report builder](../../src/ai_trading_system/domains/publish/channels/fundamental_opportunities/builder.py) calls the tracker writer with a fundamental bucket shortlist; explicit tracker path or output-directory fallback. Default is no update | **Inventory and migrate** this additional write entrypoint before retiring the pipeline tracker |
| C10 | Tracker alert table/artifact | Tracker persists `candidate_tracker_alerts`; PublishStage recognizes its artifact name, but the inspected payload assembler does not materialize a dedicated tracker-alert dataset | **Retain audit evidence**; verify delivery requirements. Registered artifact presence is not proof of alert delivery |
| C11 | Canonical episodes/snapshots and children | [Phase 4 service](../../src/ai_trading_system/interfaces/api/services/phase4.py) serves `/api/v1/candidates` and detail/snapshots/decisions/outcomes from control-plane tables; missing evidence follows typed source-state handling, not tracker substitution | **Retain**, explicitly labelled canonical shadow; connect the existing React Phase 4 workspace through these contracts |
| C12 | Canonical lifecycle tables | [market MCP lifecycle tools](../../src/ai_trading_system/interfaces/mcp/tools/lifecycle.py) expose candidate status/history, Investigator evidence, and opportunity episodes point-in-time; no legacy tracker fallback | **Retain** read-only scope; disclose which lifecycle is being returned |
| C13 | Scan-router/opportunity position artifacts | Phase 4 service reads coverage, missing-data, compatibility, recovery proposals, and actions from registered artifacts. The two stages have distinct reconciliation outputs and observation times | **Retain** source-stage/cycle identity; reconcile coverage before presenting one position-attention queue |
| C14 | Execution fills/stops versus actual journal | [execution store](../../src/ai_trading_system/domains/execution/store.py) and execution API own paper/broker ledger scope; actual journal remains a separate account-scoped domain | **Retain separate** source/account/mode; never interpret a shadow episode as a position |
| C15 | Rank outputs and publish/read models | [insight](../../src/ai_trading_system/pipeline/stages/insight.py) reads rank. The source search found no direct `final_candidates` reader in insight/narrative/publish or execution; tracker is the direct downstream reader | **Correct documentation contract**, then retain each actual source; do not migrate based on the older claimed pipeline |
| C16 | Pipeline/CLI scheduling | [orchestrator](../../src/ai_trading_system/pipeline/orchestrator.py) and [daily_pipeline](../../src/ai_trading_system/pipeline/daily_pipeline.py) configure tracker stages/parameters; report CLI supplies another writer entrypoint | **Retain** until all invoked writer paths are mapped. Deployed host schedules remain unverified |

No implementation is approved for deletion by this audit. Retirement candidates are duplicated presentation transformations and obsolete tracker writer entrypoints **after** replacement parity, external-consumer inventory, and rollback verification.

## Operator surface usage — confirmed 2026-09-12

The operator reports Google Sheets for all practical daily work, Telegram for
summaries, and the dashboard mostly on weekends or for research.

| Surface | Confirmed role | M1 baseline treatment |
|---|---|---|
| Google Sheets | Primary daily operational workspace | Record the workbook tabs actually reviewed and time spent examining their evidence |
| Telegram | Summary channel | Record summaries/alerts actually read and unchanged repeats encountered |
| Dashboard | Mostly weekend or research use | Record only when used; identify research/weekend sessions separately from routine daily reviews |

The baseline should observe this existing workflow. Daily dashboard use is not a
requirement, and reading a Sheets row plus its Telegram summary counts as one
reviewed listing. A Telegram summary is not automatically a repeated alert:
count a repeat only when its entity, reason, and underlying evidence are unchanged
from an alert already reviewed. No exact workbook/tab names, review durations,
or session counts were supplied by this clarification.

This closes the surface-preference question. On 2026-09-19 the operator also
accepted the queue meanings and risk-first sequence below. M3 planning prioritizes
the Sheets workflow and Telegram summary, with the dashboard supporting research
and detail. Acceptance does not complete G1 without the human baseline and missing
recorded lifecycle examples.

## Decision contracts and recommended workflow

The accepted daily sequence is **source health and position exceptions → new/changed opportunities → continuing watches → entry review**. These are attention queues, not new lifecycle states. Paper/actual scope and source session must be visible before interpreting any position or candidate.

The accepted queue meanings are:

- **Investigate:** evidence is incomplete, conflicting, newly discovered, or requires research; it grants no entry authority.
- **Watch:** an existing operational or shadow thesis remains observable, with its owning source and next review condition visible; it is not a canonical transition.
- **Entry review:** a shadow setup has sufficient recorded trigger, invalidation, freshness, and policy evidence for operator review; execution gates still own order permission.
- **Position attention:** an existing paper/actual position has a monitoring, coverage, deterioration, or episode-linkage issue; it is not an automatic sell or recovery action.

## Operator decision workflow — confirmed 2026-09-19

The operator described the actual daily path in a structured interview rather
than reconstructing old timing metrics:

1. Open Google Sheets and inspect market breadth first.
2. Review Top Ranked, then the Investigator tab, then Shadow Setups.
3. Research promising listings through custom ChatGPT prompts, Screener.in, and
   charts in TradingView or Chartink.
4. Add a promising listing to the operator's Zerodha watchlist for manual
   monitoring.
5. Consider entry only when chart pattern or technical evidence, valuation
   comfort, and forward-EPS expectations align.
6. Reject or remove a listing when price action fails to sustain or breaks down.
7. When breadth resembles the 2024 market top—described as an elevated high/low
   ratio together with index P/E near its upper historical ceiling—use a cautious
   posture: trim existing exposure and avoid adding new positions.

These are operator decisions and qualitative heuristics. Zerodha watchlist
membership is not a canonical system lifecycle state, confirmation, or order
permission. The breadth/valuation condition has no accepted numeric threshold
and must not be automated from this interview. M3 should make the research
handoff easy by carrying symbol, thesis, pattern/setup, trigger, invalidation,
valuation context, forward-EPS context, evidence gaps, and source dates. Existing
execution and portfolio-risk owners retain order, sizing, and broker authority.

| Decision | Authority and required evidence | Time/version and missing-input behavior | Allowed output |
|---|---|---|---|
| Investigate a discovery | Investigator/fundamental/pattern producer evidence; rank context only when it exists; convergence supplies I/F/P overlap | Retain source session, artifact hash and source policy; canonical observations also carry policy snapshot. Stale, unknown, and error remain distinct. Current pattern freshness override is an M2 defect candidate, not a guaranteed safeguard | Inspect evidence or add to an attention queue; no entry authority |
| Continue watching | Operational tracker status/reasons and episode dates; show canonical shadow state separately | Tracker `snapshot_date` and run parameters identify its calculation. It does not expose the same per-episode immutable policy-snapshot contract as the canonical registry. Empty candidate input returns empty current output; it does not prove all prior watches closed | Watch/review with source label; no canonical transition inferred from `IMPROVING`/`STABLE` |
| Review confirmation | Canonical transition plus snapshot and policy, owned by [transitions.py](../../src/ai_trading_system/domains/opportunities/orchestration/transitions.py) and persisted by opportunity orchestration | Triggered or pending-follow-through plus confirmed follow-through can produce canonical `confirmed`; other guards still apply. Raw pattern `confirmed` is insufficient. Missing canonical transition means no proven canonical confirmation | Shadow entry-review evidence only |
| React to invalidation/deterioration | Tracker `classify_candidate` for `RESULT_FAILURE`/`TECHNICAL_FAILURE`/removal; canonical transition/retention for shadow `failed`, `weakening`, or closure | Preserve source and reasons. A tracker result failure can remain active. Structural deterioration of an open position is distinct from rejecting a new long | Investigate/review/retain the owning lifecycle result; do not emit a sell or close another domain's episode |
| Monitor a position | Execution fill-derived position in a named mode/account; shadow position-cycle coverage and compatibility | Compare ledger time with coverage-run time. Route coverage, market completeness, and episode compatibility are separate requirements. Missing/ambiguous attachment remains recovery/attention evidence | Position attention; report-only recovery remains report-only |
| Permit order submission | `ExecutionCandidateBuilder`, execution risk/portfolio policies, `ExecutionService` | Required rank plus configured exchange/trust/linkage/stage gates; paper adapter in the pipeline. Record effective request and execution decision. Preserve current gate/override semantics rather than claiming unknown trust always blocks | Existing paper execution path; no new broker permission from M1 |

For every proposed queue item require: listing identity, episode/cycle identity if available, authority label, source session and recording time, reason or blocker, policy/config reference, source link, and next review condition. A missing review condition must display as unspecified rather than being invented by the UI. Store no new queue state in M1.

## Recorded walkthroughs

Evidence was inspected on 2026-09-09. Most recent completed shadow run observed: `shadow-2026-09-07-193455`, decision session 2026-09-07. Examples demonstrate recorded behavior, not investment recommendations or current-session freshness.

### W1 — New discovery: APLAPOLLO, NSE

`candidate_episode` records `candidate_0154bc9cc5b623c5a4be2d98fb2e59e954aef05f48781d1a92a438c6c5a1823d`, opened 2026-09-07 with `setup_family=momentum_leader`, `opening_reason=rank_threshold`, and the run above. Snapshot `snapshot_158479980f69739a5683a42458726c04e9b62daa3b05f80f91b195132073195d` records `investigating`, action `watch`, eligibility `unknown`.

Decision: present a new **shadow discovery** for investigation. Rank-threshold admission did not produce an executable instruction. Recorded policy snapshot: `8e56823cc00366e7cbcea3cc632ac74cf7e4a2a185e7e0c6f92365dfacd8fc72`.

### W2 — Continuing watch: AEROFLEX

Tracker episode `AEROFLEX-2026-06-24-1` has snapshots on September 3, 4, and 7 with `IMPROVING`, reason `health >= 65`; it remains active with latest seen date September 7.

Decision: continuing operational **Watch**, with an unchanged-status indication. Do not recast this as canonical follow-through confirmation or count every repeated snapshot as a new discovery. The tracker identity is symbol-based; exchange-aware migration needs an explicit identity mapping.

### W3 — Confirmation: recorded case unavailable

The inspected control plane contained 354 Investigator performance events, all `CANDIDATE_DISCOVERED`; no `ENTRY_CONFIRMED` or executable events were present. Canonical snapshot/transition states were limited to `investigating`, `early_accumulation`, `setup_forming`, and `weakening`.

The code path for triggered/pending follow-through to `confirmed` was inspected. Existing policy/matching tests passed, including `test_confirmed_followthrough_collapses_missing_pending_observation`. This is contract evidence, not a real recorded confirmation. **The real-case requirement remains open**; do not manufacture a transition or treat a pattern label as a substitute.

### W4 — Invalidation review: MANGLMCEM; terminal canonical case unavailable

Tracker episode `MANGLMCEM-2026-09-07-1` records `RESULT_FAILURE`, reason `result failure`. Alert `MANGLMCEM-2026-09-07-1-2026-09-07-RESULT_FAILURE` records `WATCH_CAREFULLY → RESULT_FAILURE`, severity `high` on September 7. This is a real adverse-thesis review example.

Decision: surface a review exception from the tracker, not a broker exit. No closed canonical episodes were present in the inspected control plane. Consequently, **canonical terminal invalidation/closure still needs a recorded walkthrough**; this tracker example does not establish closure parity.

### W5 — Open position: ENTERO, NSE, paper

Read-only signed-fill aggregation in `execution.duckdb:execution_fill` showed an open paper position, with latest fill September 4. The September 7 opportunity artifact records cycle `position-cycle-bed1ba0761b37acce5917e74`, `compatibility_status=ambiguous_multiple_episodes`, and `outcome=POSITION_RECOVERY_REQUIRED`. Compatibility evidence lists two episodes whose lifecycles are not position-compatible; recovery proposal `position-recovery-79698765f2db5ac98a4958d9` exists.

Decision: **Position attention** for unresolved episode attachment. The fill ledger owns the position; neither a recovery proposal nor a watch episode can override it. No quantity, price, or account identifiers are copied into this audit.

### W6 — Incomplete evidence: ACMESOLAR, NSE

Snapshot `snapshot_4dc789ad0691005794fd4b2ac13feb9c332ff4788d15d25e1a3cade9be1c1911` in the September 7 run records `setup_forming` and missing Investigator-context fields: `pattern_family`, `pattern_state`, `setup_quality_bucket`, `breakout_type`, `candidate_tier`, `qualified_breakout`.

Decision: display the recorded state with the missing evidence explicitly attached. Absence of pattern/breakout context is not a negative classification and does not justify hiding the opportunity. Fresh registry persistence alone does not prove evidence completeness.

### Run-level reconciliation and artifact provenance

The September 7 pipeline and opportunities stage attempts have registry status `completed`; the opportunity summary reports semantic status `degraded`, 40 registry conflicts, 9 active positions, 9 position-monitor routes, 8 positions with complete evidence, and **0 fully monitored positions**. Convergence-source status is `PASS`, convergence-performance status is `FAIL`. These are separate statuses, not interchangeable success labels.

The registry-freshness receipt reports 232 current-session snapshots, 76 transitions, and no intervening missing registry sessions. Its `PASS` is relative to the September 7 decision session, not September 9 wall-clock freshness.

Artifacts were selected by run/stage/attempt joined to a completed `pipeline_stage_run`, then their actual bytes were checked against registered SHA-256 hashes. Relative paths below are under configured `$DATA_ROOT/pipeline_runs/shadow-2026-09-07-193455/opportunities/attempt_1/`:

| Artifact | Verified SHA-256 |
|---|---|
| `opportunity_shadow_summary.json` | `a777a09d1e63fa30220d13c7745523355ea78b0f0e962b3fa1c6b9afb91ac941` |
| `position_monitor_reconciliation.csv` | `bd965dcf8bf3bf33aed271577265e35ea4f4a3f99f1193de9f08aa7660619711` |
| `opportunity_registry_freshness.csv` | `6bc1fb5caabe02db21c1901a3e3433c6048e49f1649445ea8b7d599d5110f00b` |

`position_episode_compatibility` and `registry_conflicts` artifact hashes also matched. DB examples use the exact keys above; the live databases were not copied or frozen, so later queries may return additional history. Run IDs, dates, and record IDs distinguish this inspection from a reproducible immutable database snapshot.

## Ownership gaps and disposition

| Gap | Evidence / implication | Required next step and owner |
|---|---|---|
| G1-A: Operator selection is not execution selection | Execution reads rank directly. The previous candidates-stage consumer claims were corrected in this M1 documentation change | Candidate/execution owners compare selected populations. Retain the split until an explicit policy decision; no automatic wiring change |
| G1-B: Lifecycle vocabulary is not interchangeable | Tracker health statuses versus canonical setup progression; real adverse tracker evidence but no canonical closed/confirmed history | Tracker/registry owners map each responsibility and preserve unmapped states; obtain missing real walkthroughs before authority transfer |
| G1-C: Tracker has another writer entrypoint | Fundamental-opportunity report can update tracker outside the candidate-tracker stage | Include report CLI configuration and deployed schedules in cutover inventory |
| G1-D: Alert persistence is not alert delivery | Ledger artifacts and status-derived Telegram/display alerts follow different paths | Publish owner defines delivered-event identity, unchanged-repeat rule, and channel coverage; do not retire the alert ledger based on a dashboard |
| G1-E: UI fallback differs from governed artifact resolution | Tracker loader sorts attempts by file mtime | Read-model owner changes this under a separate verified patch; M3 must expose current limitations until then |
| G1-F: Position coverage has multiple denominators | [Reconciled](m1_position_coverage_reconciliation.md): 5 unique pre-execution cycles, 9 duplicated lane bundles, 4 post-execution positions after 3 exits and 2 entries; 0/5 canonical attachments | Cycle-level accounting and separate artifact/API/UI coverage fields implemented in the working tree. Historical artifacts remain unchanged; attachment rules and coverage gates are not relaxed |
| G1-G: Pipeline completion is not semantic readiness | Completed attempt with degraded opportunity summary and failed performance readiness | UI/publish owners show execution, semantic data health, and shadow readiness separately |
| G1-H: Human measurements and external consumers incomplete | Operator confirmed Sheets for daily work, Telegram summaries, and dashboard for weekends/research on September 12 | Map actual Sheets tabs and schedules; record five-session baseline. Surface preference is resolved; timing and detailed consumer verification remain open |

These gaps block declaring G1 passed. They do not require changes to scoring, broker mode, or live stores during M1.

## Five-session baseline protocol

**Protocol version:** `m1-operator-baseline-v1` (documentation-only; not a registered trading policy). **Collection status checked 2026-09-19: 0/5 sessions measured.** Six run-specific forms exist for evidence sessions 2026-09-09, 10, 11, 16, 17, and 18; all remain `NOT RECORDED` with blank human observations. Pipeline runs are not substituted for reviews. The following rows are a blank collection form, not observations.

`./scripts/run_daily_shadow.sh` now creates a blank run-specific Markdown form
and prints its path after each run. Fill that form after your review; retries
preserve existing notes. Collection status here changes only when actual
observations have been reconciled into the table. See
[commands](../reference/commands.md#operational-pipeline) for output behavior.

The operator prefers a short structured interview over filling the form. After
each prospective review, collect these questions conversationally and then write
the answers into the run-specific record:

1. Was the breadth → Top Ranked → Investigator → Shadow Setups sequence completed,
   and did breadth change the exposure stance?
2. Which listings moved to external research, and which source was used?
3. Which listings were added to the Zerodha watchlist, and what technical,
   valuation, or forward-EPS evidence supported that choice?
4. Which listings were rejected or removed because price action failed to
   sustain or broke down?
5. Approximately how many unique listings were reviewed, repeated unchanged,
   left with evidence gaps, or required further investigation? Active review
   minutes may be supplied when remembered; they are never inferred.

The generated form remains the durable session record; the operator does not
need to edit it directly. The assistant may populate it only from explicit
answers and must leave unknown fields blank.

Use five completed daily reviews on distinct market sessions. Record the actual review date, evidence session/run, and surfaces used. Start timing when opening the daily review; stop when the defined queues have been reviewed. Subtract interruptions and record them separately. A missed review is missing, never zero minutes. Do not reconstruct human time from pipeline timestamps.

| Session | Review date / evidence session / run | Surfaces used | Active review minutes | Unique listings reviewed | Unchanged alert repeats | Unresolved evidence gaps at end | Unique listings requiring investigation |
|---|---|---|---|---|---|---|---|
| 1 | Not recorded | — | — | — | — | — | — |
| 2 | Not recorded | — | — | — | — | — | — |
| 3 | Not recorded | — | — | — | — | — | — |
| 4 | Not recorded | — | — | — | — | — | — |
| 5 | Not recorded | — | — | — | — | — | — |

Definitions: unique listings use `(exchange, symbol)` rather than display rows; an unchanged repeat is a delivered alert with the same entity, reason, and underlying evidence as one already reviewed (include duplicate channels, exclude real changes); an evidence gap is a distinct unresolved `(entity, missing field/source)` at review end; requiring investigation means the operator marked further evidence work necessary. A watch-only item is not automatically an investigation. Keep uncertain classifications in notes instead of guessing.

After five sessions, report all observations plus median review time, review/investigation counts, total unchanged repeats, and remaining gaps. Freeze the M3 usability target before its trial; this baseline itself sets no trading-performance threshold.

## G1 acceptance record

| Requirement | Status updated 2026-09-19 |
|---|---|
| Source-grounded repository owner/consumer matrix | Prepared; external scheduling and runtime UI checks remain open |
| Decision inputs, authority, cutoff, missingness, permitted outputs | Prepared for operator acceptance; current gaps explicitly retained |
| Six real walkthrough categories | Discovery/watch/position/missing-evidence inspected; adverse tracker case inspected; canonical confirmation and terminal closure unavailable |
| Surface roles | Confirmed: Sheets primary daily, Telegram summary, dashboard mostly weekends/research |
| Queue/workflow acceptance | **Accepted 2026-09-19:** risk-first sequence and Investigate/Watch/Entry review/Position attention meanings above |
| Actual operator decision path | **Confirmed 2026-09-19:** breadth → ranked/investigator/setup review → external research → manual Zerodha watchlist → multi-factor entry consideration or price-action rejection |
| Five-session baseline | Protocol ready; 0/5 measured |
| Gate G1 | **OPEN — not passed** |

Recommended acceptance scope: adopt the proposed risk-first review sequence and separate operational tracker, canonical shadow, and paper/actual position labels. This accepts the workflow definition only; it does not waive missing evidence or authorize lifecycle migration.

Validation: 39 existing opportunity policy/matching tests passed; no generated market data was used to substitute for a recorded walkthrough. `scripts/check_docs.py` validated 125 current documents without errors; existing verification-date advisories remain. `git diff --check` passed. No production UI, publishing delivery, scheduled job, or broker behavior was exercised.
