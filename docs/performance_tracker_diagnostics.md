# Performance Tracker — diagnostics reference

The Research page is purely observational. It does not change ranking weights,
paper-trading rules, bucket thresholds, or any production config. It reads
`data/research.duckdb` through `rank_cohort_performance_trusted`: quarantined
rows and persisted horizon-specific forward-return anomalies remain in the raw table for review
but are excluded from strategy metrics.

## 1. Tracker health
Date range and trusted row count, plus raw and excluded row counts. If
`rows == 0`, the rank backfill has not run yet or every row is quarantined.

## 2. Three-number monitor
Top-10 avg_20d, top-200 avg_20d, and `top-200 − 201+` Δ on the 20-day horizon.
This is the quickest read on whether the ranking is doing anything at all.

## 3. Rank concentration diagnostic
Per-rank-band returns + deltas, with an interpretation badge:

- **weak** — `top-10 avg_20d − top-200 avg_20d < 0.50` pp. Treat top-200 as the
  eligible universe; top-10 isn't earning its concentration risk.
- **strong** — gap ≥ 1.50 pp. Top-10 concentration is justified.
- **mixed** — between thresholds.

## 4. Cohort forward returns
Top-N / 51-200 / 201+ averages and hit rates. If top-10 ≈ 201+, the model isn't
discriminating regardless of bucketing.

## 5. Bucket coverage
Per-bucket first/last date, dates, rows, distinct symbols, share of all rows,
and the share of rows whose `fwd_5d` / `fwd_20d` have matured. Use this to
distinguish "bad bucket" from "bucket only existed for two weeks".

## 6. Bucket attribution
Plain bucket-level forward returns. Buckets where `avg_5d < 0` **and**
`hitrate_5d < 40%` are highlighted red.

## 7. Same-date bucket attribution
Compares each bucket only against rows from dates where any bucket was
assigned. A bucket row is flagged `Small — directional only` when
`trading_days < 10` **or** `n < 500`. Always check this before drawing
conclusions about CORE_MOMENTUM / EARLY_STAGE2 — many "negative" buckets
disappear once the date sample is matched.

## 8. Bucket composition
Average factor state at assignment time per bucket. Only columns that exist
on the table are queried — missing slots display as `—`. The column appears
in the "Missing columns" note at the top of the panel. Use this to tell
whether bad buckets are late-stage / weak-trend / overextended rather than
mislabelled.

## 9. Conditional factor IC
Spearman IC inside three slices: `full_universe`, `top_200_only`,
`rank_201_plus_only`, across 5/10/20-day forward horizons. **Top-200 ic_20d
is the most important diagnostic.** If it is near zero, no reweighting of
ranking factors can rescue top-10 selection.

## 10. Factor coverage
Per-factor `status`:

- `not_wired` — 0% coverage. (Highlighted red.) `momentum_accel` shows up here
  if the pipeline is not populating it.
- `poor_coverage` — < 50%. (Highlighted red.)
- `partial_coverage` — 50–80%. (Highlighted amber.) Excluded from drift alerts
  by being downgraded to `unreliable_coverage`.
- `ok` — ≥ 80%.

## 11. Drift watch
Recent IC vs baseline IC with sample-size guardrails:

| `recent_n` band       | Maximum status                       |
| --------------------- | ------------------------------------ |
| `< 1500`              | `insufficient_sample` (no alert)     |
| `1500 ≤ … < 3000`     | `watch` only (no warning/critical)   |
| `≥ 3000`              | `warning` / `critical` allowed       |

A `critical` requires `delta_ic ≤ -0.03` **and** `|baseline_ic| ≥ 0.05`. Factors
with coverage_pct < 80 are tagged `unreliable_coverage` regardless of delta.

## 12. Latest digest viewer
Lists markdown digests under `data/research/perf_digests/` and renders the
selected one inline. Digests include the same interpretation paragraphs
emitted by the UI (top-200 edge, concentration verdict, factors missing
coverage, drift suppression count).

---

### How to answer the sprint's six questions from this page

1. **Are CORE_MOMENTUM / EARLY_STAGE2 genuinely bad?** Read the same-date
   attribution panel. Ignore any bucket with the `Small — directional only`
   pill.
2. **Are bucket labels joined correctly?** Bucket coverage shows whether each
   bucket has the expected date range, symbol count, and forward-return
   maturity.
3. **Do any ranking factors have predictive IC inside top-200?** Conditional
   IC, `top200 ic_20d` column.
4. **Which factors are missing or under-populated?** Factor coverage, status
   column.
5. **Are drift warnings being suppressed when sample size is too small?** Drift
   watch — look for `Insufficient sample`, `Watch`, `Unreliable coverage`.
6. **Is top-10 concentration justified?** Rank concentration diagnostic badge.
