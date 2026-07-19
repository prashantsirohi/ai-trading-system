# ADR-0007: Multi-Lane Pattern Evidence Scan over the Executable Universe

- **Purpose:** Define a governed four-lane pattern evidence scan and its
  research-first calibration path without changing current rank consumers.
- **Audience:** Operator, developers, reviewers, future agents.
- **Last verified:** 2026-07-19
- **Source of truth:** Current behavior is owned by
  `domains/ranking/patterns/`, `domains/ranking/service.py`,
  `pipeline/stages/weekly_stage.py`, and `pipeline/stages/scan_router.py`.
- **R0 implementation:** `research/pattern_lane_calibration/`.
- **R1a implementation:** `pipeline/stages/pattern_lane_scan.py`,
  `research/pattern_lane_calibration/shadow.py`.
- **R1a evidence:** A/B/C safety proof at
  `docs/evidence/adr-0007/r1a-safety-proof/2026-07-17-7d5f03a/`; daily gate
  `pipeline/session_gate.py`; parity policy `platform/parity/comparison_policy.py`.
  See "R1a shadow" under D7.
- **Status:** **Proposed; R0 pilot executed 2026-07-18.** The R0 harness
  passed (reproducible, immutable, no production impact). Calibration evidence
  is partial/inconclusive: the operator authorized a narrowly scoped R1a
  shadow integration and R0.1 measurement repair, and declined to authorize
  R2 routing and R3 admission. See "R0 pilot outcome" under D7.

---

## Context

The legacy rank pattern scan is coupled to the filtered rank frame. Stage-2
context is left-joined from that frame; absent symbols receive an effective
zero score and are removed by the Stage-2 prescreen. Disabling
`pattern_stage2_only` removes one prescreen but does not reconstruct complete,
rank-independent structural context.

The legacy `pattern_scan.csv` is also capped at 150 highest-ranked rows, not
150 unique symbols. Integrated stock scan, candidate construction, and the
existing Stage-1 lifecycle consume legacy pattern evidence. Investigator runs
its own scan and routed Investigator work is limited to `deep_scan_universe`.
No new evidence source may bypass the existing flow:

```text
weekly_stage -> scan_router -> routed Investigator -> opportunity reconciliation
```

Weekly stage already provides uncapped mature structural coverage, light
pattern evidence, and governed routing tiers. `LIGHT_PATTERN` does not enter
Investigator deep scanning; only `FULL_INVESTIGATOR` and `POSITION_MONITOR` do.
ADR-0007 therefore adds evidence, not a parallel lifecycle.

Detector history is not uniform. `ipo_base` can run at 35 bars. From 35 through
119 valid bars it is the only eligible family; the other families require at
least 120 bars. The standard liquidity gate requires 50 bars, so 35–49-bar IPOs
need a distinct observational policy.

The detector emits `three_weeks_tight`, while `TIER_1_PATTERNS` currently
contains the non-canonical alias `3wt`. That tier defect is outside this ADR
and requires its own focused change.

## Decision

The proposed scan evaluates the executable universe through one dedicated,
point-in-time structural context and assigns exactly one lane per symbol and
as-of date:

```text
stage2_continuation
stage1_base
young_listing_base
ipo_early_base
no_lane
```

Evidence production, router admission, opportunity consumption, candidate
scoring, lifecycle mutation, publishing, and execution admission remain
separately versioned decisions.

## D1 — Executability and liquidity

For at least 50 bars, `pattern-standard-liquidity-policy-v1` requires:

- at least 50 valid bars;
- close at least INR 20;
- current cross-sectional turnover percentile at least 0.20;
- no active dated DQ or corporate-action exclusion.

For 35–49 bars, `ipo-early-liquidity-policy-v1` independently requires:

- 35 through 49 bars;
- missing-session ratio at most 20% and continuity at least 80%;
- median available-history turnover at least INR 5,000,000;
- median available-history volume at least 50,000;
- close at least INR 20 and at least 20 estimation sessions;
- NSE exchange and an observation on the latest exchange session;
- 100% valid OHLCV relationships;
- no active dated DQ or corporate-action exclusion.

The early-IPO gate grants observational eligibility only. Undated exclusion
lists are rejected because applying future knowledge to earlier dates is not
point-in-time safe.

## D2 — Point-in-time structural context

`pattern_structure_context` is reconstructed before rank eligibility,
minimum-score, top-N, or rank pattern prescreening. It does not read `ranked` or
`ranked_universe`. Its minimum contract includes symbol/exchange/date, bar
count, close, liquidity policy and result, SMA50/150/200, SMA200 slope,
52-week-high distance, Stage-2 score/label/validity, latest governed weekly
stage and age, and a content-derived `structure_observation_id`.

Every market and weekly-stage input is bounded inclusively to the historical
as-of date. Later market rows are available only to outcome evaluation.

## D3 — Deterministic lane policy

`pattern-lane-r0-policy-v1` applies this precedence:

1. 35–49 bars: `ipo_early_base` only when the early-IPO gate passes.
2. 50–179 bars: `young_listing_base` only when the standard gate passes.
3. At least 180 bars: evaluate mature Stage-2 before Stage-1.
4. Otherwise: `no_lane`.

`pattern-stage2-validity-policy-v1` requires at least 200 complete bars, valid
non-imputed SMA150/SMA200 and SMA200 slope inputs, the production structural
Stage-2 predicate, and score at least 70. A young stock cannot enter Stage-2
from an incomplete score.

`weekly-stage-freshness-policy-v1` treats a weekly observation as fresh for at
most 10 exchange trading sessions. `pattern-stage1-structure-policy-v1`
requires a fresh S1/S1-to-S2 observation plus all of:

- close within 15% of SMA150 and absolute 20-day SMA150 slope at most 2%;
- 65-bar base depth at most 35%;
- latest 20-bar median range at most 90% of the prior 20-bar median;
- price within 10% of the base pivot;
- 20-versus-60-bar return trend delta at least zero;
- latest 20-bar median volume at most 90% of the prior window;
- close at least 85% of SMA200 and SMA200 slope at least -1%.

## D4 — Exact detector-family policy

Family filtering occurs before detector execution under
`pattern-family-policy-v1`. `A` means allowed, `X` excluded, `S`
suppression-only, and `N` not applicable due to history.

| Family | IPO 35–49 | Young 50–119 | Young 120–179 | Stage 1 180+ | Stage 2 180+ |
|---|---:|---:|---:|---:|---:|
| `cup_handle` | N | N | A | A | A |
| `round_bottom` | N | N | A | A | A |
| `double_bottom` | N | N | A | A | A |
| `flag` | N | N | X | X | A |
| `high_tight_flag` | N | N | X | X | A |
| `ascending_triangle` | N | N | A | A | A |
| `symmetrical_triangle` | N | N | A | A | A |
| `ascending_base` | N | N | A | A | A |
| `vcp` | N | N | A | A | A |
| `flat_base` | N | N | A | A | A |
| `stage2_reclaim` | N | N | X | X | A |
| `darvas_box` | N | N | A | A | A |
| `pocket_pivot` | N | N | X | X | A |
| `ipo_base` | A | A | A | A | N |
| `inside_week_breakout` | N | N | A | A | A |
| `three_weeks_tight` | N | N | X | X | A |
| `inside_day` | N | N | A | A | A |
| `head_shoulders` | N | N | S | S | S |

The complete matrix is validated in code. A missing family or lane/history row
fails policy construction.

## D5 — R0 evidence and outcomes

R0 runs offline through `ai-trading-pattern-r0-calibrate`. It opens the OHLCV
store read-only and writes a new explicit immutable directory containing:

- structural context and lane reasons;
- exact detector invocation counts and untruncated signals;
- 5/10/20-session returns, benchmark-relative returns against the
  equal-weight liquid-1000 universe index (labelled `UNIV_TOP1000_EW`;
  stored as `universe_index_daily` universe `UNIV_TOP1000_MCAP` with
  `index_type = equal_weight` — the stored id names the top-1000-by-market-cap
  membership, not the weighting), MFE, MAE, confirmation, failure,
  invalidation, and sessions-to-breakout;
- deterministic same-date/lane/history-band nearest-liquidity controls;
- lane/family/history/state/origin/regime/liquidity metrics with 95% Wilson
  intervals and a 30-observation minimum-sample flag;
- winner-window recall isolated from precision populations;
- serialized policies, source hashes, byte hashes, row counts, summary, and
  replay manifest;
- observational runtime diagnostics excluded from equality hashes.

The R0 CLI must report live date and symbol progress, rate, ETA, signal counts,
and checkpoint commits. It processes eligible symbols in parallel and commits
each completed date under a policy/source-bound checkpoint so interruption does
not discard the entire replay.

The manifest binds `pattern-r0-reconstruction-policy-v1`,
`pattern-r0-outcome-policy-v2`, and every policy above. An exact verification
rerun compares policy, source, dataset, and row-count hashes.
`pattern-r0-outcome-policy-v1` used the `NIFTY50` benchmark symbol, which is
absent from the OHLCV store; v2 supersedes it with the broad equal-weight
liquid-1000 index (`UNIV_TOP1000_EW`) loaded from `universe_index_daily`.

## D6 — Production boundary

R0 does not write or replace:

- `pattern_scan.csv` or any registered rank artifact;
- pattern cache or control-plane rows;
- weekly-stage, router, Investigator, or opportunity state;
- Stage-1 lifecycle or watchlist state;
- candidate, publish, execution, order, fill, or position state.

Known winner windows are recall-only. Broader historical signals and matched
controls form the precision population. R0 output cannot authorize consumer
admission.

## D7 — Later rollout gates

### R0

Operator approval is required before running the pre-registered replay. The
run must preserve policy/source hashes and report incomplete outcome windows
separately.

#### R0 pilot outcome (2026-07-18)

Pilot replay: 81 weekly as-of dates 2025-01-03 through 2026-07-16, output at
`pattern_lane_r0/2026-07-17-pilot` (immutable, `REPRODUCIBLE`), post-hoc
analysis at `pattern_lane_r0/2026-07-17-pilot-analysis`. Operator decisions:

| Stage | Decision | Reason |
|---|---|---|
| R0 harness | Pass | Reproducible, immutable, no production impact |
| R0 calibration evidence | Partial/inconclusive | Dead Stage-1 lane; incomplete measurement channels |
| R1 shadow plumbing | Conditionally authorized (R1a scope) | Read-only, ~5–6 min per date, changes no decisions |
| R2 router integration | Not authorized | No demonstrated timeliness or reliable promotion threshold |
| R3 candidate/opportunity admission | Not authorized | Most lanes underperform the broad benchmark |

Key evidence: `stage1_base` produced 11 assignments because weekly-stage
history does not exist before April 2026 (`weekly_stage_snapshot` holds four
week-ends, 2026-04-10 through 2026-05-01; the governed
`weekly_stock_stage_history` begins full-universe writes 2026-07-10), so the
lane was starved of input, not weakly tuned. Benchmark-relative 20-session
medians (equal-weight liquid-1000): `stage2_continuation` −0.5%,
`young_listing_base` −2.1%, `ipo_early_base` −3.7%; `flat_base` is the only
sizeable family above baseline (+0.5%, 52.9% beat rate, n=2,149). Winner
recall (post-hoc): 29/31 winners signalled, but only 9/30 before
`first_guard_pass`. R0.1 measurement repair (control outcomes, episode
deduplication, regime join, stricter confirmation, independent invalidation,
official winner-recall rerun) plus a Stage-1-only replay after weekly-stage
backfill are required before R2 gates can be evaluated.

The weekly-stage history gap was closed 2026-07-18 by a point-in-time
backfill (`stage_backfill.py`, policy `weekly-stage-v2`, 92,143 append-only
observations over 84 completed weeks 2024-12-05 through 2026-07-10, frozen
OHLCV snapshot bound in `weekly-stage-backfill-2026-07-18/backfill_manifest.json`,
zero conflicts against live governed rows). Weekly-stage coverage is
evaluated against the population eligible under the governed weekly-stage
classifier: symbols admitted by the R0 liquidity gate but excluded by the
weekly-stage classifier remain visible as cross-policy exclusions
(`cross_policy_exclusions.csv`, reason-coded, zero unexplained) and are not
counted as missing stage observations. Calibration replays read the
backfill through `weekly_stage_source_mode = frozen_backfill`; live
consumers use `governed_current` precedence.

R0.1 executed 2026-07-18 via `research/pattern_lane_calibration/r0_analysis.py`
(bundle `pattern_lane_r0/2026-07-17-pilot-analysis-v2`, manifest bound to the
pilot manifest hash). Episode-level signal-minus-control shows a small
positive selection edge (median +0.24%, 50.9% beat rate, n=10,083 at 20
sessions) that does not yet clear the R2 benchmark-relative gate; `flat_base`
and `vcp` are the only families positive after deduplication and control
comparison, while raw `high_tight_flag`/`three_weeks_tight` positives were
pseudo-replication artifacts. See the bundle `REPORT.md`.

#### Stage-1-only replay (2026-07-18, post-backfill)

Bundle `pattern_lane_r0/2026-07-18-stage1-replay(-analysis)`, `frozen_backfill`
source mode, lane `stage1_base`, `weekly-stage-v2`. The backfill lifted fresh
weekly-stage coverage of stage-1 candidates from 5.2% to **94.9%** and
stage1_base assignments from 11 to **80** (58 symbols, 48 complete 20-session
outcomes). Stage-1 signals are positive and control-beating: episode-level
benchmark-relative median **+2.6%** (57.5% beat), signal-minus-control median
**+7.6%** (64% beat control, n=28) — the strongest control-relative edge of any
lane, though small-sample. Offline gate comparison (stock-level forward
benchmark-relative, entry=as_of): 8-of-8 is benchmark-neutral (−0.2%, 49.3%)
while every relaxation is monotonically worse (7-of-8 −1.1%, 6-of-8 −1.1%,
hard+scored −1.1%/−1.3%, all-admissible −1.7%); the incremental stocks each
relaxation adds underperform. **Decision: keep the 8-of-8
`pattern-stage1-structure-policy-v1` baseline unchanged** — its low yield is
precision, not over-restriction. 0/31 funnel-autopsy winners received a
stage1_base signal (those winners were already advancing when captured); the
lane contributes high-precision, low-recall basing evidence. Carry stage1_base
forward as observational alongside `flat_base` and `vcp`; sample size and thin
flow (~4–5 assignments/month) keep it short of an R2 admission mandate. See the
bundle `REPORT.md`.

### R1

R1 may add an independent shadow scan only after the ADR and lane artifact
contract are approved. Legacy execution remains unchanged. Decision consumers,
including the existing Stage-1 lifecycle builder, must remain row-equivalent.

#### R1a shadow — safety proof

The row-equivalence claim above is proven, not asserted. Enabling
`--pattern-lane-scan-mode shadow` was validated with an A/B/C controlled
comparison (A=off, B=shadow, C=off control) on isolated APFS copy-on-write
clones for run date 2026-07-17 at commit `7d5f03a`: every legacy decision
artifact was byte-identical A vs B (`rank/pattern_scan.csv` exact SHA-256,
`final_candidates`, `positions`, `trade_actions`, candidate/stage-1 lifecycle,
scan-router tier/reason decisions, opportunity admissions/transitions). The only
flag-caused output is the new stage's own additive `performance/*` telemetry.
Every residual difference was reproduced in the same-mode A~C control (generated
ids, timestamps, float-summation jitter, run-scoped hashes, rank tie jitter) —
ordinary nondeterminism, catalogued in the parity policy.

The durable, independently-reviewable evidence is the committed audit record at
`docs/evidence/adr-0007/r1a-safety-proof/2026-07-17-7d5f03a/` (bound to the full
signed bundle by `bundle.sha256` and the GitHub Release asset in
`bundle_reference.json`). The procedure is
[shadow_stage_ab_parity](../runbooks/shadow_stage_ab_parity.md); the field
classification is `platform/parity/comparison_policy.py`
(`shadow-parity-policy-v1`). Day 1 counts; the full three-run test does not
repeat daily.

#### Daily operation from Day 2

Run the normal production pipeline with the three shadow flags — no A/B/C:

```
ai-trading-pipeline --run-date <session> \
  --opportunity-registry-mode shadow \
  --opportunity-scan-routing-mode shadow \
  --pattern-lane-scan-mode shadow --local-publish
```

A session counts toward the 20-session clock when all eight hold (evaluated by
`ai-trading-shadow-session-gate --run-id <id>`, module
`pipeline/session_gate.py`, `r1a-session-gate-v1`): the lane stage completed; all
seven lane artifacts are registered; runtime passes (p95 ≤ 10 min, none > 15
min); policy diagnostics pass; source diagnostics pass; no stale evidence is
admitted as fresh; no malformed signal rows; the registry and routing shadows
complete (a non-blocking `opportunities` degradation is tolerated); and no
operational consumer changed (`operational_side_effects == false`).

During the shadow period, run the offline cross-shadow reconciliation
(`ai-trading-cross-shadow`, read-only, `research/pattern_lane_calibration/
cross_shadow.py`) to compare lane evidence against the opportunity-registry
shadow without writing pattern evidence into the registry: pattern-before /
same-day / pattern-after / pattern-only / registry-only / suppression-conflict /
possible-duplicate. This measures whether the lane scan adds genuine early
discovery or merely confirms rank/Investigator finds.

#### R1a review points

**After five successful sessions**, review operational health: runtime median
and p95; snapshot-fallback rate (the Day-1 value of ~22.5% is not a blocker —
no stale evidence was admitted — but each fallback's reason must stay visible);
governed-stage freshness; lane and family signal volume; fresh vs carry-forward
ratio; registry conflicts and duplicate episodes; and pattern-to-registry
lead/lag examples from the cross-shadow report.

**After twenty successful sessions**, make three *separate* decisions: (1)
whether the opportunity registry may become the authoritative lifecycle store;
(2) whether selected pattern evidence may enter the registry as
**non-actionable observational** events; (3) whether pattern reasons may move
into scan-router **compare** mode. Do not move directly from R1a to active
Investigator routing.

Course: Day-1 safety proof (complete) → Days 2–5 operational monitoring → Days
1–20 independent dual shadows → cross-shadow reconciliation during the period →
Day-20 registry and pattern reviews → next possible mode: observational bridge /
router compare.

### R2

Router integration requires approved `scan-routing-policy-v3` and
`pattern-promotion-policy-v1`. Initial source reasons are
`pattern_stage1_base_detected`, `pattern_young_listing_base_detected`,
`pattern_ipo_early_base_detected`, and `pattern_base_promoted`. Early-IPO
evidence remains observational and cannot promote itself.

### R3

Candidate, opportunity, lifecycle, publish, and execution admission each
require a separately versioned calibrated policy, replay tests, shadow
comparison, rollback, and operator approval.

The independent legacy scan may be removed only after row, order, schema,
dtype, lifecycle, cache, duplicate, null/default, serialization, and top-150
projection parity passes over diverse historical dates.

## Consequences

- R0 is reproducible and point-in-time without changing operational behavior.
- Exact policies can be reviewed before results are known.
- Early IPOs are observable without being treated as executable candidates.
- Full detector cost is attributable by lane and family.
- R1 still requires a separate production implementation and authorization.
- Correcting the `3wt` tier alias remains a separate change.
