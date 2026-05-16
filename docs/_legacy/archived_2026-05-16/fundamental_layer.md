# Fundamental Snapshot Layer

## Purpose

The fundamental snapshot layer adds a slow-moving quality and risk gate on top of the existing technical-first ranking pipeline. It imports manually downloaded Screener.in CSV exports, normalizes the data, scores fundamentals, flags risks, and enriches watchlist candidates after rank artifacts are generated.

It does not scrape Screener, call a Screener API, change ranker weights, or modify `ranked_signals.csv`.

## Screener CSV Export Workflow

1. Export a CSV from Screener.in with the configured fundamental columns.
2. Save it under `data/raw/screener/`, for example `data/raw/screener/screener_fundamentals_20260507.csv`.
3. Import and score:

```bash
python -m ai_trading_system.domains.fundamentals.import_screener \
  --file data/raw/screener/screener_fundamentals_20260507.csv \
  --snapshot-date 2026-05-07
```

4. Run the normal technical pipeline. If `data/fundamentals/fundamental_scores_latest.csv`
   exists, the orchestrator auto-runs the optional fundamentals stage after rank.
   You can still force it explicitly with `--enable-fundamentals`.

```bash
python -m ai_trading_system.pipeline.orchestrator
```

5. Optional manual enrichment for a specific rank attempt:

```bash
python -m ai_trading_system.domains.fundamentals.enrich_rank \
  --rank-dir data/pipeline_runs/<run_id>/rank/attempt_1 \
  --fundamental-scores data/fundamentals/fundamental_scores_latest.csv \
  --run-id <run_id>
```

## Column Mapping

The import maps Screener's `NSE Code` to `symbol`, strips whitespace, uppercases symbols, coerces financial values to floats, and converts `Is not SME` into a boolean-ish 1/0 value. Unknown extra Screener columns are preserved in the normalized DuckDB snapshot but are not used by v1 scoring.

Required input columns are `Name`, `NSE Code`, `Industry Group`, and `Industry`.

## Scoring Model

The total score is:

```text
0.35 * quality_score
+ 0.25 * growth_score
+ 0.20 * balance_sheet_score
+ 0.10 * valuation_score
+ 0.10 * ownership_score
```

Quality uses ROCE, ROE, OPM, OPM stability, and Piotroski score. Growth uses 3-year and 5-year sales/profit growth plus quarterly profit growth. Balance sheet uses debt-to-equity, cash from operations, and free cash flow. Valuation is mostly sector-relative so high-quality growth is not over-penalized by absolute multiples. Ownership uses pledge, promoter, DII, and FII holdings.

## Red Flags

Hard red flags include SME status, pledge above 10%, debt-to-equity above 2 for non-financial companies, weak ROCE/ROE, negative sales and profit growth, negative CFO and FCF, and Piotroski below 4.

Debt-to-equity hard flags are not applied to financial sectors such as banks, NBFCs, insurance, housing finance, asset management, capital markets, and financial services.

Minor warnings include moderate pledge, moderate debt for non-financial companies, OPM decline, negative quarterly profit growth, negative FCF, and negative CFO.

## Tier Interpretation

- `A`: score >= 70 with no red flags.
- `B`: score >= 55 with no red flags.
- `C`: score >= 40 with weaker score or minor warnings.
- `Reject`: score < 40 or any hard red flag.

## Enriched Watchlist Outputs

Latest output:

```text
data/fundamentals/watchlist_candidates_latest.csv
```

Optional per-run output:

```text
data/pipeline_runs/<run_id>/fundamentals/watchlist_candidates.csv
```

The final watchlist score is:

```text
0.70 * composite_score
+ 0.15 * breakout_pattern_score
+ 0.15 * fundamental_score
```

Buckets are sorted in this priority: `ADD_TO_WATCHLIST`, `STUDY_ONLY`, `TECHNICAL_ONLY_RISK`, `AVOID_RED_FLAG`, and `IGNORE_FOR_NOW`.

## Pipeline Freshness Policy

Fundamental data is quarterly and slow-moving, so the pipeline treats it as a
context/gating layer rather than a primary technical signal. The default
pipeline auto-runs fundamentals only when the latest scores CSV exists:

```text
data/fundamentals/fundamental_scores_latest.csv
```

Freshness is based on `screener_snapshot_date` or `snapshot_date` from that
file:

- Fresh: `<= 100` days old.
- Aging but usable: `101-135` days old.
- Stale warning: `> 135` days old.

Stale or missing fundamentals never block ingest, features, rank, or publish.
If the fundamentals stage is explicitly run with no snapshot, it writes a
`skipped_missing_snapshot` summary with warnings.

## Historical Trends

Each import compares the new Screener snapshot with the previous available snapshot in `data/fundamentals.duckdb`. The importer writes:

```text
data/fundamentals/fundamental_trends_latest.csv
```

It also persists a `fundamental_trends` DuckDB table with score, quality, growth, balance sheet, valuation, ownership, ROCE, ROE, OPM, debt, pledge, sales growth, and profit growth deltas.

Trend labels include `IMPROVING`, `STABLE_GOOD`, `DETERIORATING`, `TURNAROUND`, `VALUE_TRAP_RISK`, and `INSUFFICIENT_HISTORY`. The watchlist enrichment includes `fundamental_score_delta`, `fundamental_trend_label`, and `trend_reason`. Improving or deteriorating fundamentals are mentioned in `watchlist_reason`; value-trap risk is penalized.

## Known Limitation

Trend history depends on repeated manual Screener imports. It cannot infer older fundamental changes unless earlier exports have been imported.

## Industry Fundamentals Extension

Industry-level fundamental context is layered on top of the existing stock-level
fundamentals using a manually exported Screener "Industries Overview" CSV.
This is a context/gating/explanation layer applied **after** rank — it does not
modify ranker weights, `ranked_signals.csv`, or the existing
`final_watchlist_score` formula.

### Workflow

1. Manually export the Screener Industries Overview to e.g.
   `data/raw/screener/screener_industries_2026-05-08.csv`.
2. Run the importer:

   ```bash
   python -m ai_trading_system.domains.fundamentals.import_screener_industries \
     --file data/raw/screener/screener_industries_2026-05-08.csv \
     --snapshot-date 2026-05-08
   ```

3. Run the orchestrator with the fundamentals stage enabled:

   ```bash
   python -m ai_trading_system.pipeline.orchestrator --enable-fundamentals
   ```

### Output files

- `data/fundamentals/industry_fundamental_scores_latest.csv`
- `data/pipeline_runs/<run_id>/fundamentals/industry_fundamental_scores.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_1/sector_dashboard_enriched.csv`
  (when `sector_dashboard.csv` exists)
- `data/fundamentals.duckdb` tables: `industry_fundamental_snapshot`,
  `industry_fundamental_scores`

### Scoring model

For each industry row we winsorize numeric inputs and compute:

- `industry_growth_score` — percentile rank of `sales_growth_wavg`.
- `industry_quality_score` — `0.50 * percentile(roce_wavg) + 0.50 * percentile(opm_wavg)`.
- `industry_valuation_score` — inverse percentile of `median_pe`. Non-positive
  P/E is forced to a neutral 50.
- `industry_momentum_score` — percentile rank of `median_1y_return`.
- `industry_fundamental_score = 0.30*growth + 0.30*quality + 0.20*valuation + 0.20*momentum`.

All scores are clipped to [0, 100] and rounded to two decimals.
Winsorization bounds are: sales growth `[-50, 150]`, OPM `[-50, 80]`,
ROCE `[-30, 80]`, momentum `[-80, 200]`, P/E `[0, 150]`.

### Labels

`QUALITY_GROWTH_LEADER`, `EXPENSIVE_MOMENTUM`, `VALUE_ROTATION_CANDIDATE`,
`CYCLICAL_RECOVERY`, `WEAK_FUNDAMENTALS`, `DISTORTED_DATA`, fallback `BALANCED`.
Warnings: `low_company_count`, `expensive_sector`, `negative_opm`, `weak_roce`,
`extreme_sales_growth`, `distorted_operating_margin`, `missing_key_metrics`.

### Effect on the watchlist

Industry context is appended/applied as follows (no change to
`final_watchlist_score`):

- `QUALITY_GROWTH_LEADER` + `ADD_TO_WATCHLIST` → reason gains
  "industry backdrop supportive".
- `EXPENSIVE_MOMENTUM` → reason gains
  "sector expensive, avoid chasing extended entries".
- `VALUE_ROTATION_CANDIDATE` → reason gains "value rotation sector candidate".
- `WEAK_FUNDAMENTALS` + `ADD_TO_WATCHLIST` → bucket downgraded to `STUDY_ONLY`.
- `DISTORTED_DATA` → reason gains
  "industry data distorted, verify manually".
- `low_company_count` warning → reason gains "low company count industry".

Unmatched watchlist rows (no Screener industry match) get a neutral 50 score and
label `UNKNOWN`. Missing industry CSV never fails the pipeline.

### Known limitation

Joining is performed on a normalized `industry_key` derived from the granular
`industry` column on the watchlist side and the Screener industry overview
column. Screener may use broader industry buckets than the universe symbol's
`industry` value (which comes from `industry_group`-style sources), so the
match rate is partial in practice. Unmatched rows are preserved with neutral
values rather than dropped.
