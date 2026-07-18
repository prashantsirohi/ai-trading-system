# ADR-0007: Multi-Lane Pattern Evidence Scan over the Executable Universe

- **Purpose:** Define a governed four-lane pattern evidence scan and its
  research-first calibration path without changing current rank consumers.
- **Audience:** Operator, developers, reviewers, future agents.
- **Last verified:** 2026-07-17
- **Source of truth:** Current behavior is owned by
  `domains/ranking/patterns/`, `domains/ranking/service.py`,
  `pipeline/stages/weekly_stage.py`, and `pipeline/stages/scan_router.py`.
- **R0 implementation:** `research/pattern_lane_calibration/`.
- **Status:** **Proposed.** The R0 harness exists, but R0 execution and each
  later rollout stage still require separate operator authorization.

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
  equal-weight `UNIV_TOP1000_MCAP` universe index, MFE, MAE,
  confirmation, failure, invalidation, and sessions-to-breakout;
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
`UNIV_TOP1000_MCAP` index loaded from `universe_index_daily`.

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

### R1

R1 may add an independent shadow scan only after the ADR and lane artifact
contract are approved. Legacy execution remains unchanged. Decision consumers,
including the existing Stage-1 lifecycle builder, must remain row-equivalent.

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
