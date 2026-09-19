# Stage universe and final review ranking plan

- **Purpose:** Plan a Stage 2 plus late Stage 1 review universe, independent pattern/fundamental qualification, and one ordered daily review list.
- **Audience:** Operator, ranking/opportunity engineers, and publish maintainers.
- **Last verified:** 2026-09-19
- **Source of truth:** This document owns the proposed delivery backlog only. Current behavior remains in the [System Guide](../SYSTEM_GUIDE.md), [pattern lane contract](../stages/pattern_lane_scan.md), and [fundamental discovery contract](../stages/fundamental_discovery.md).
- **Status:** U1/U2 implemented; U3 accepted for an NSE shadow pilot; U4 local workbook prepared and external pilot pending; U5 not started. No ranking, lifecycle or execution authority transferred.

## Intended result and scope

Produce one explainable, ordered opportunity-review list in Google Sheets from a Stage 2 or late Stage 1 universe. Telegram summarizes material changes; the dashboard provides weekend/research detail. Each listing shows why it qualifies, its setup readiness, independent lane evidence, and blockers.

```text
Trusted broad market → existing cross-sectional features and rank context
                    → governed Stage 2 / late Stage 1 eligibility
                    → independent pattern and fundamental qualification
                    → readiness grouping and deterministic review ordering
                    → Google Sheets daily list → Telegram change summary
```

This is an analytical review universe, not a replacement master universe or ingestion filter. Keep broad-market coverage for relative strength, sector comparisons, excluded-row auditing, and position monitoring. Actual positions remain discoverable even after leaving the opportunity universe.

The first release is a shadow list alongside existing outputs. Existing `ranked_signals`, `final_candidates`, tracker state, admission rules, and execution inputs keep their current meaning. No claim of superior selection or returns follows from producing a coherent list.

## Proposed decision contract

### 1. Universe eligibility

The new review policy owns one explicit eligibility result per listing and decision session. It consumes governed structural facts and existing trust/liquidity evidence; a pattern scanner's lane label is not itself the universe authority.

| Evidence | Proposed treatment |
|---|---|
| Fresh governed Stage 2 | Include if required trust, identity, liquidity, and structural inputs pass; classify extension/readiness separately |
| Fresh governed Stage 1 with late-base structure | Include only when all frozen late Stage 1 checks pass |
| Fresh S1→S2 transition | Preserve both current stage and transition; resolve using current stage and explicit transition rules, with no duplicate row |
| Early Stage 1, Stage 3, Stage 4 | Exclude from new-opportunity list with recorded reasons |
| Missing, stale, conflicting or future stage evidence | Evidence exception; cannot qualify through an optimistic fallback |
| Young/IPO listing without sufficient structural history | Explicit out-of-scope/insufficient-history result in v1; existing research lanes continue independently |
| Open position in any stage | Retain separate position attention coverage; new-opportunity exclusion cannot hide it |

Use existing `Stage1StructurePolicy` as a starting candidate for late Stage 1, not an already accepted definition. It checks a 65-bar base, depth no greater than 35%, 20-bar contraction at most 0.90, pivot distance at most 10%, non-deteriorating short/long RS, volume dry-up at most 0.90, and moving-average conditions. Its surrounding classifier uses at least 180 bars and a separate liquidity gate. U1 must freeze exact formulas, units, freshness, history requirements and boundary cases; reuse source primitives where possible without changing the existing frozen policy.

The current scanner's `stage2_continuation` uses its own technical score/validity conditions, while governed weekly Stage 2 is a different fact. U1 must explicitly reconcile disagreement. Proposed precedence is governed stage for universe membership, technical setup evidence for readiness, and a visible conflict reason when they disagree. A technical lane must never upgrade governed S3/S4 into this universe.

Retain exchange in identity and declare exchange-specific coverage. BSE analytical rows may qualify only when required evidence is available; do not substitute NSE calendars or imply execution eligibility. Current M2 convergence performance lacks an approved BSE calendar.

### 2. Lane qualification

Proposed v1 selection is **eligible universe AND (P qualifies OR F qualifies)**. Both lanes evaluate independently; neither requires the other's success. Investigator remains contextual evidence, not a third mandatory gate.

| Lane | Qualifying evidence | Non-qualifying states to retain |
|---|---|---|
| Pattern (P) | Fresh normalized pattern assessment with an allowed positive setup under the frozen review policy | Successful no-pattern scan, suppression-only pattern, not eligible, missing, stale, error |
| Fundamental (F) | At least one qualifying accounting thesis with valid identity, basis, availability and freshness, plus required daily context | Evaluated but no thesis, missing/stale accounting data, unsupported model, unresolved basis/context |

`KNOWN` alone is not sufficient proof of a positive pattern. U1 maps signal families, suppression evidence and quality gates to an explicit qualification result. Fundamental qualification must distinguish accounting-thesis success from daily projection/admission status; do not equate an existing registry episode with today's success or rewrite the accounting cache around the new stage filter.

Label rows **P+F**, **P-only**, or **F-only**, and retain the other lane's exact state. P-qualified/F-missing is distinct from P-qualified/F-evaluated-none. Global source failure marks the list degraded and discloses the affected population; it must not masquerade as a complete single-lane market. Identity/trust failures still block the affected listing even if one lane qualifies.

Evaluate P AND F as a comparator, not the initial default. Do not give P+F an automatic ranking bonus before testing its incremental value.

### 3. Final review ordering

The proposed ordering is **eligibility → readiness group → existing rank score → stable listing identity**. Readiness is a review priority, not canonical confirmation or permission to trade.

| Proposed order | Meaning |
|---|---|
| 1. Setup review | Qualified listing with sufficient fresh setup, trigger, invalidation and extension evidence under the frozen policy |
| 2. Developing watch | Qualified listing whose setup is still forming, including late Stage 1 and fundamental theses awaiting technical readiness |
| 3. Extended / defer | Qualified listing with a recorded extension or other deferral reason |
| Separate evidence exceptions | Insufficient/conflicting evidence that prevents reliable readiness classification or universe qualification |

Within each group, use the upstream broad-universe rank score as an initial, explicitly provisional tie-breaker. Missing scores sort after scored rows within the same group and remain missing; use exchange/symbol as the final deterministic tie-break. Keep Stage 1/Stage 2 rank-band diagnostics because the present score favors established strength. Do not average raw P/F scores, double count legacy and lane patterns, or quietly change factor weights.

U1 must define readiness predicates and precedence from authoritative input fields. Incomplete trigger/risk evidence cannot be displayed as setup-ready. F-only rows can reach setup review only through independently available technical evidence; no P success is manufactured. Existing admission/sector/risk blockers remain visible and never become broker permission through sorting.

Keep the full qualified list and separate exclusion/exception evidence. A top-25 display is only a view over qualified rows: never fill empty slots with outside-universe or unqualified listings. Distinguish the review list from the position-attention queue.

## Ownership, dependencies and proposed output

`domains/ranking/` retains broad rank ownership. A pure review-policy module under `domains/opportunities/` is the proposed owner of universe decisions, lane-state normalization and review ordering. The opportunity stage is the proposed materialization owner after both lane stages finish; U1 verifies this placement against current consumers before implementation. Publish formats already-materialized decisions and introduces no independent eligibility or sorting logic.

Fundamental discovery currently requires promoted `ranked_universe`, so moving or replacing the upstream rank would create dependency problems. Keep that input intact. The existing pattern contract permits convergence presentation only; adding this consumer requires an explicit shadow review-projection contract update, with no automatic operational adoption.

Proposed attempt artifacts (names to freeze in U1):

- `final_review_universe.csv`: every assessed listing with eligibility/reasons and required-source states.
- `final_review_list.csv`: one ordered row per qualified exchange/symbol/session, with linked multi-thesis/setup detail.
- `final_review_summary.json`: coverage, exclusions, exceptions, missing lanes, truncation, input lineage and degraded/complete status.

Use the existing artifact registry and configured roots; add no parallel watchlist database. Only completed, promoted producer attempts may supply inputs. Bind decision cutoff, source sessions, producer run/attempt/hash, policy snapshot and code version. Reused rank context must satisfy a declared age contract and retain its original date; never relabel older evidence as current. Exact retries reproduce decision content; changed inputs create a new auditable attempt.

Sheets columns: review priority, exchange/symbol, stage/transition, readiness, P/F combination, pattern state/setup, fundamental state/thesis, upstream rank score, extension, blocker/next check, evidence dates and links. Add the policy/run identifier and coverage warning at tab level. Keep detailed exceptions reachable. Telegram reports new/changed/removed items and meaningful exceptions from the same artifact, with unchanged-alert suppression consistent with M1.

The 2026-09-19 operator interview adds a research-handoff requirement to U4.
The Sheets row should support the path from setup review into custom ChatGPT
research, Screener.in fundamentals, and TradingView/Chartink chart review, then
record the operator's manual Zerodha-watchlist disposition. Watchlist membership
must remain an operator note rather than a system lifecycle state. Entry remains
a discretionary combination of technical setup, valuation comfort, and forward
EPS; failed price action/breakdown remains a review/removal reason. Breadth-driven
trimming and avoidance of new entries must be displayed as operator posture until
numeric thresholds receive a separate policy contract.

## Delivery sequence and acceptance gates

U1 and U2 are implemented. U3 copied-real validation is **ACCEPTED FOR THE NSE SHADOW PILOT** after official-source calendar repair and a complete same-run rebuild; BSE calendar support remains explicit and unsupported. U4 is **IN PROGRESS**: the 52-row local pilot workbook is prepared, while the external Sheets tab awaits authorization. U5 remains **NOT STARTED**. See the [U2/U3 evidence record](u2_u3_final_review_validation.md). U-prefix steps are subordinate to the existing M1–M4 program, not replacement milestones.

| Step | Work and responsible module | Completion evidence |
|---|---|---|
| U1 — Freeze review contract | Opportunity/ranking owners map current fields, define stage/liquidity/history predicates, lane qualification, readiness, conflict handling, sort keys and consumer boundaries | Versioned contract; real examples covering S2, late/early S1, extended, P-only, F-only, both, missing evidence and stage conflict; all constants and unresolved choices recorded |
| U2 — Build local shadow projection | Opportunity owner adds pure evaluator and attempt artifacts after both lanes; updates stage/artifact/policy documentation | Targeted eligibility, missingness, cutoff, identity, ordering and retry tests; complete qualification/exclusion counts; existing operational consumer outputs unchanged on identical inputs |
| U3 — Validate copied real runs | Opportunity and measurement owners replay retained real inputs on isolated copies with local publishing only | Source/hash reconciliation; broad rank and DQ artifacts checked; full denominator accounting; exclusion and position-coverage walkthrough; no future inputs or unexplained differences |
| U4 — Pilot Sheets workflow | Publish owner maps actual workbook tabs, prepares local tab/Telegram previews, then adds the shadow tab for an authorized publishing trial | Five completed operator reviews compared with M1 baseline; no outside-universe fill; same materialized decisions across surfaces; changes, exceptions and positions discoverable |
| U5 — Evaluate and decide adoption | Measurement owner applies accepted M2 design; operator decides whether this becomes the primary review list | Coverage, workload, matured cohort results and uncertainty; documented retain/revise/reject decision; tested publisher rollback before any default change |

U1/U2 may proceed while the human M1 baseline and M2 correctness evidence remain open. U3 engineering checks do not close G2. U4 needs accepted queue meanings and a usable baseline for comparison; no performance claim before G2. U5 primary-review adoption requires the relevant M1/M2/M3 acceptance evidence. M4 lifecycle transfer remains a separate task; this plan does not depend on completing it to build a shadow list.

At implementation time, read storage/lineage contracts and recheck current code before modifying artifacts. Use temporary stores and copied real market evidence for canaries. `--local-publish` suppresses external delivery but is not by itself database isolation. No live DB repair, broker mutation, or external summary send is part of creating this plan.

## Evaluation design to freeze before comparative results

Proposed primary question: **Does this staged P-or-F review list improve selection and review efficiency relative to the current daily list?** Treat selection quality and usability as separate outcomes. This is a proposed direction for M2, not an accepted threshold or completed study.

Compare the full current daily list with the proposed list at the same decision cutoff and display capacity. Also compare ordering within their common eligible universe to separate effects of the universe filter from effects of sorting. Record all omissions and additions. Within the proposed universe, report mutually exclusive P+F, P-only and F-only cohorts, the stricter AND comparator, and explicit missing-data groups. Stage, sector, regime and exchange mix must remain visible.

Before evaluating comparative outcomes, freeze primary anchor/horizon, material-improvement and risk thresholds, costs, sample unit, dependence-aware confidence method, minimum sample, observation window and stopping rule in the [M2 contract](m2_measurement_contract.md). Repeated listings and overlapping horizons are not independent trials. Historical replays are retrospective; five usability sessions cannot demonstrate economic benefit. Pending, invalid, unsupported-exchange and incomplete-source observations remain separately counted.

Engineering acceptance requires zero unexplained duplicates, eligibility leaks, post-cutoff inputs, or position omissions; every included and excluded row must reconcile to source evidence. Usability acceptance uses the M1 measurements of review time, unique listings, repeated unchanged alerts, evidence gaps and further investigation. Economic acceptance requires the frozen M2 criteria and matured evidence; insufficient evidence means continue shadow collection, not promotion.

## Rollout, rollback and rebuild impact

Local artifacts come first, followed by a shadow Sheets tab, followed by an explicit primary-review decision. Retain the existing daily output during the trial. Rollback disables the new projection/presentation consumer and restores the prior tab routing; verify that neither path changes trackers, fills, positions or execution selection. Preserve trial artifacts for audit.

No feature rebuild is required for this documentation change. The proposed first implementation should reuse existing features and scores; U1 must identify any absent inputs. If new formulas or factor weights become necessary, record their rebuild scope separately before implementation. Do not claim a full rebuild is unnecessary for an as-yet-undefined ranking change.

The next work is the authorized U4 external Sheets pilot after the M1 baseline is usable. No runtime default, lifecycle authority, execution permission, or Telegram delivery has changed.

## U1 implementation — 2026-09-13

The [U1 review policy contract](u1_review_policy_contract.md) records the pure
evaluator, frozen initial thresholds, source mapping, test scenarios and
hash-verified September 11 source walkthroughs. Stage/transition, P/F
qualification, readiness and deterministic ordering now have executable
contracts. The evaluator has no pipeline consumers. Full daily late-base
metrics, setup selection and extension coverage require the U2 adapters; U1
real-case acceptance is not yet complete. Existing M1/M2 gates stay open.
