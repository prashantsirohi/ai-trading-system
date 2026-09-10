# M2 measurement contract and correctness record

- **Purpose:** Define the corrected convergence measurement semantics, calculator boundaries, and outstanding M2 acceptance evidence.
- **Audience:** Operator, performance-evaluator maintainers, and reviewers.
- **Last verified:** 2026-09-09
- **Source of truth:** `src/ai_trading_system/domains/opportunities/convergence_performance.py`, `src/ai_trading_system/domains/opportunities/orchestration/convergence.py`, `src/ai_trading_system/domains/opportunities/policy_snapshot.py`, and the linked regression tests.

## Status and scope

M2 correctness repairs are implemented for the unified convergence path. **G2 remains open.** The five findings have regression coverage, but a complete copied-market replay, broader calculator/source reconciliation, operator-selected comparison, and forward collection remain outstanding. This document implements part of [M2](decision_ownership_and_consolidation_plan.md#m2--establish-comparable-performance-evidence); it does not transfer lifecycle or execution authority.

The active successors are `opportunity-convergence-v1.3` for source interpretation and `opportunity-convergence-performance-v2` for measurement. `policy_snapshot.py` fingerprints their semantics. Existing policy labels are not reused. No table migration, live-store repair, historical backfill, or feature rebuild is required by this patch.

## Current convergence measurement contract

| Topic | Current rule |
|---|---|
| Observation identity | Exchange, symbol, evidence session, composite policy snapshot. Producer artifacts and hashes remain frozen. Duplicate retries cannot replace interpreted evidence |
| Pattern freshness | Computed stale, future, or unknown source-session status cannot be overridden by a producer's `FRESH` label. A current-session date also cannot upgrade an explicitly stale producer label |
| Calendar | NSE uses the observed NIFTY 50 index-session series from `_index_catalog`. A missing calendar, duplicate calendar dates, or absent anchor session blocks maturation. No stock-row fallback. BSE has no approved calendar in this implementation and remains insufficient-data |
| Horizon | Target is the 3rd, 5th, 10th, or 20th reference session strictly after the anchor session. Missing or duplicate stock bars cannot move that target to a later session. `observed_sessions` counts elapsed reference sessions, not usable stock rows |
| Absent/suspended stocks | Required missing stock sessions produce `INSUFFICIENT_PRICE_DATA`; no forward-fill, suspension-return assumption, or synthetic bar. Restored data may advance nonterminal outcomes |
| Price source | Stock open/high/low/close from configured operational `_catalog`; index open/close from `_index_catalog`. This patch does not introduce another adjustment algorithm or silently mix `adjusted_close` with differently based OHLC. Corporate-action basis and quarantine propagation require the separate source audit below |
| Discovery close | Append only after a positive finite close exists on a unique decision-session bar. Missing prices remain an observation with failed anchor coverage, not a permanently null immutable anchor |
| Discovery next-open fill | Exact next reference-session open plus the existing 5 bps entry slippage, rounded to six decimals. Missing next-session stock open defers the anchor; no later-bar substitution |
| Canonical anchors | Confirmation and executable anchors require linked immutable Investigator performance events with positive finite prices. Candidate linkage starts from same listing/session discovery events. They are shadow evidence, not broker fills |
| Entry-session risk | Next-open and executable shadow-fill anchors include the anchor session's high/low in MFE, MAE, target-touch, and stop-touch calculations. Touch on that day is day 0; close anchors start on the next session at day 1. The terminal horizon date is still anchor plus N sessions |
| OHLC validity | Required bars must have finite positive prices and ordered low/open/close/high bounds. Invalid or incomplete risk bars cannot yield an accepted return |
| Return and risk | Percentage change to horizon close; MFE/MAE use maximum high/minimum low over the holding window. Stop touch is an observation, not a simulated stop execution. Daily bars cannot resolve ordering when stop and profit levels touch in the same session |
| Benchmark/sector | Same anchor and target sessions; open basis for open-fill anchors, close basis otherwise. Unique endpoint rows required. Sector aliases resolve through the existing primary mapping. Missing relative evidence is explicit `PARTIAL_MATURED`, not zero; window stability requires benchmark coverage for every sample |
| Costs | Discovery next-open applies the existing 5 bps entry slippage only. Brokerage, taxes, exit costs, liquidity/impact, and actual execution latency are not modelled; results are not net strategy P&L |
| Maturity | Not enough future reference sessions: `PENDING`, with optional partial diagnostic return. Elapsed horizon but missing/invalid required data: `INSUFFICIENT_PRICE_DATA`. Usable stock outcome with missing relative data: `PARTIAL_MATURED`. Complete evidence: `MATURED` |
| Corrections | Pending/insufficient outcomes may advance. Existing anchors and terminal horizons are frozen; later corrected prices do not repaint them. Re-evaluation under corrected source evidence requires a separately governed version/reconstruction, not an in-place repair |
| Aggregation | Separate policy snapshot and exchange before mutually exclusive I/F/P cohorts, lane marginals, windows, and readiness. No implicit cross-version or cross-snapshot pooling |
| Sample dependence | Retain raw, unique-listing, observation, episode, and overlap counts. Existing confidence bands are descriptive sample-count labels; they are not statistical confidence intervals or proof of independent samples |
| Stability windows | Fixed groups of ten reference-market discovery sessions per policy snapshot/exchange; pending samples and observations without anchors participate in boundaries. Require all ten sessions represented in the scope, every required discovery anchor present, every included sample matured, and complete benchmark coverage before positive means can mark a window stable |
| Window overlap | Discovery-date groups do not overlap; their 20-session holding periods can overlap. They must not be treated as three independent economic trials |
| Recording time | Observation session and recorded observation time remain separate. Market outcomes use data available when evaluation runs; this is not a point-in-time historical data-vintage reconstruction. Copied historical replays must be labelled retrospective |

## Legacy and consumer boundaries

The v2 evaluator accepts v1.3 observations only. It reads/matures/reports that version from the existing tables. Older observations, anchors (including legacy null anchors), and pending/terminal horizons remain stored unchanged and are excluded from v2 aggregates. Readiness includes `OPPORTUNITY_CONVERGENCE_LEGACY_HISTORY_EXCLUDED` with the excluded observation count. Restarting collection under a successor policy therefore does not inherit legacy sample eligibility.

| Calculator / consumer | Reconciliation disposition |
|---|---|
| Convergence discovery-close vs `performance_evaluation._mature_horizon` | An identical complete-price fixture matches target date, return, MFE/MAE, touch timing, and benchmark/sector-relative returns to six-decimal output precision |
| Legacy Investigator event evaluator | Unchanged. It still uses its existing symbol-row horizons and event-risk semantics. Incomplete-session and open-fill calculations are **not** interchangeable with v2; successor migration remains outstanding |
| Rank-cohort research tracker / backtests | Different selection populations, costs, and portfolio semantics. No pooling or claim of full parity. Source, calendar, adjustment and recording-time reconciliation remains outstanding |
| Actual trading journal / execution ledger | Own actual or paper fills and account/mode scope. V2 does not import actual fills or use them as shadow anchors |
| Opportunity stage / publish projections | Retain shadow-only status. Artifact rows carry policy and snapshot strata; display/report consumers must preserve those dimensions rather than sum incompatible cohorts |

There was no extraction of a shared calculator merely to reduce duplication: the verified common close-anchor subset agrees, while the remaining lifecycle, fill and population semantics differ. Broader reuse needs the remaining consumer audit.

## Five finding dispositions and validation

Regression evidence is in `tests/domains/opportunities/test_convergence_performance.py`, `tests/domains/opportunities/orchestration/test_convergence.py`, and the service integration tests.

| Finding | Disposition / regression |
|---|---|
| Producer FRESH overrides stale pattern date | Fixed; stale and absent dates exclude membership |
| Stock-row horizon hides missing sessions | Fixed against reference calendar; missing/duplicate/invalid bars cannot shift maturity, and restored bars can complete the original target |
| Entry-day risk omitted | Fixed for open-fill anchors; entry-day stop and profit touch reported at day 0 |
| Null immutable discovery anchor cannot recover | Fixed for new observations by deferring anchor creation; retry after real price arrival appends once. Legacy null anchors are preserved and excluded |
| Partial window counted stable | Fixed; 10/10/1 sessions yields only two complete stable windows, and pending rows or another policy snapshot cannot complete the first window |

Additional tests cover nonfinite/negative/missing OHLC, policy-stratum separation, unsupported exchanges, terminal-result preservation, and common-input parity. Unit fixtures are isolated test data and are not forward market evidence.

## Planned comparative evaluation — not yet frozen

No comparative performance result has been inspected to choose thresholds. The primary question was requested from the operator and is pending. Before comparative evaluation, record and accept all of:

| Design field | Status |
|---|---|
| Primary question: convergence selection, within-lane rank discrimination, or confirmation benefit | Operator selection pending |
| Primary anchor, horizon, eligible universe and control cohort | Pending selected question |
| Material improvement threshold and cost assumptions | Pending operator agreement; do not infer from sample outcomes |
| Independent-sample unit and confidence method | Pending; must handle repeated listings and overlapping holding periods |
| Minimum independent sample, observation period and stopping rule | Pending; existing 30/120/three-window gates are not substitutes |
| Sector/regime stratification, unranked group, non-confirmers and missingness | Required in the frozen specification |
| Point-in-time data-vintage and corporate-action/quarantine audit | Required before economic acceptance |

## Forward collection and G2 evidence log

| Date | Evidence | Status |
|---|---|---|
| 2026-09-09 | Five defect regressions and complete-input calculator parity | Implemented; validation results recorded in the consolidation plan |
| 2026-09-09 | Real OHLCV read for copied-market validation | Blocked by an existing Python writer lock (PID 14226); no process terminated and no live store changed |
| 2026-09-09 | Retained September 7 source artifacts, 1,626 observations, two retries in a temporary store | Passed cardinality/hash and missing-price exclusion checks; no market-price or economic result claimed |
| 2026-09-09 | Forward v1.3/v2 sessions accepted for comparative evaluation | None claimed; historical artifact checks are retrospective |

G2 stays open until the market replay and remaining source/calculator checks pass and the comparative design is accepted. G1's five operator reviews can continue independently. No profitability, production readiness, or automatic promotion follows from these engineering repairs.

The reference-calendar contract assumes the NIFTY 50 session series is complete for the interval. It does not independently certify an exchange holiday calendar or detect a session missing from both index and stock sources. That source-completeness audit remains a G2 requirement; benchmark-row availability alone is not production certification.
