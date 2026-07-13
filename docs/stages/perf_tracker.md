# Stage: perf_tracker

- **Purpose:** Observability stage. Appends today's rank cohort to `rank_cohort_performance` and recomputes forward 5/10/20/60-day returns for rows whose horizons just matured. **Non-blocking** — failures never fail the pipeline.
- **Audience:** Operator, developer, research.
- **Last verified:** 2026-05-16
- **Source of truth:**
  - [`src/ai_trading_system/pipeline/stages/perf_tracker.py`](../../src/ai_trading_system/pipeline/stages/perf_tracker.py)
  - [`src/ai_trading_system/research/perf_tracker/backfill.py`](../../src/ai_trading_system/research/perf_tracker/backfill.py)
  - [`src/ai_trading_system/research/perf_tracker/forward_returns.py`](../../src/ai_trading_system/research/perf_tracker/forward_returns.py)
  - [`src/ai_trading_system/research/perf_tracker/schema.py`](../../src/ai_trading_system/research/perf_tracker/schema.py)
  - [`src/ai_trading_system/research/perf_tracker/digest.py`](../../src/ai_trading_system/research/perf_tracker/digest.py)

---

## Purpose

Phase 0 of the rank feedback loop. After `publish` succeeds, this stage:

1. Picks up today's `ranked_signals.csv` and joins with `watchlist_buckets.csv` (from publish).
2. Computes forward 5/10/20/60-day returns from OHLCV close-on-close.
3. Upserts into `rank_cohort_performance` (DELETE+INSERT keyed on `run_date+symbol_id+exchange`).
4. Re-matures any historical rows whose forward horizons hit today.
5. Writes a small `perf_tracker_summary.json` artifact.

This stage is **observability**, not a hard dependency. Any failure is a tracking gap, not a pipeline blocker — see the docstring in `perf_tracker.py`: *"Failures here must NOT block the pipeline — measurement is observability, not a hard dependency."*

## Entrypoints

- **Stage wrapper:** [`pipeline/stages/perf_tracker.py`](../../src/ai_trading_system/pipeline/stages/perf_tracker.py) — class `PerfTrackerStage`, `name = "perf_tracker"`.
- **Worker:** `research.perf_tracker.backfill.run_backfill(project_root=...)` — no date filter; processes everything available.

## Input data

- All historical `data/pipeline_runs/<run_id>/rank/attempt_*/ranked_signals.csv` files (most-recent attempt per calendar date wins — see `backfill._latest_attempt_per_date`).
- Same-run `data/pipeline_runs/<run_id>/publish/attempt_*/watchlist_buckets.csv` for bucket attribution.
- `_catalog` table in `data/ohlcv.duckdb` for close-on-close return math (read-only).

## Output artifacts

| Artifact | Path | Notes |
|---|---|---|
| `perf_tracker_summary` | `data/pipeline_runs/<run_id>/perf_tracker/attempt_<n>/perf_tracker_summary.json` | Status + `dates_processed` + `rows_upserted`. On failure: status=`failed` with error message. |
| `tracker_health` | `data/pipeline_runs/<run_id>/perf_tracker/attempt_<n>/tracker_health.json` | Raw/trusted/excluded rows, fixture and duplicate checks, artifact lag, and recent cohort-size regression warning. |

**DuckDB writes:**

- Database: `data/research.duckdb` (resolved by [`schema.py::research_db_path`](../../src/ai_trading_system/research/perf_tracker/schema.py) as `operational paths.root_dir / "research.duckdb"`). **This is NOT `data/research_ohlcv.duckdb`** (that's the OHLCV isolation store).
- Table: `rank_cohort_performance` — DDL in `schema.py::RANK_COHORT_DDL`. Primary key `(run_date, symbol_id, exchange)`. Columns: rank_position, composite_score (+adjusted), rank_mode, watchlist_bucket, config_id, `fwd_<N>d_return`/`fwd_<N>d_matured_at` for N∈{5,10,20,60}, factor scores (`factor_rs`, `factor_vol`, `factor_trend`, `factor_prox`, `factor_deliv`, `factor_sector`, `factor_momentum_accel`), sector_name, inserted_at.
- Trusted analytics view: `rank_cohort_performance_trusted`. API diagnostics, digests, and the optimizer fast path read this view so rows with `data_quality_status != 'trusted'` or a persisted forward-return anomaly remain inspectable but do not influence strategy metrics.
- Provenance columns: `source_type`, `source_run_id`, and `source_artifact_path`. New writes identify whether rows came from operational pipeline artifacts or historical research scoring.
- Index: `idx_rank_cohort_date` on `run_date`.

Schema is idempotent (`CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`), ensured on every connection via `open_research_db()`.

## Main modules

| Module | Role |
|---|---|
| `pipeline/stages/perf_tracker.py` | Stage wrapper; broad `except Exception` to keep pipeline alive. |
| `research/perf_tracker/backfill.py::run_backfill` | Discovers ranked_signals + watchlist_buckets per date, computes returns, upserts. |
| `research/perf_tracker/forward_returns.py::compute_forward_returns` | Joins (symbol, run_date) against `_catalog`, computes `fwd_<N>d_return` for horizons `(5,10,20,60)`. Pending horizons return NaN. |
| `research/perf_tracker/schema.py` | DDL + `open_research_db()` context manager. |
| `research/perf_tracker/digest.py` | Weekly markdown digest. Run separately, not part of the pipeline stage. |

## Process flow

1. `PerfTrackerStage.run(context)` calls `run_backfill(project_root=context.project_root)`.
2. `_latest_attempt_per_date` walks `data/pipeline_runs/*/rank/attempt_*/ranked_signals.csv`, picks the freshest-mtime attempt per calendar date, pairs with `publish/attempt_*/watchlist_buckets.csv`.
3. Columns mapped from ranked_signals → tracker via `RANKED_TO_TRACKER` (e.g. `rel_strength_score` → `factor_rs`). Unknown columns silently dropped.
4. `compute_forward_returns` opens `data/ohlcv.duckdb` read-only, builds per-symbol row indices, self-joins to read `close_at_run_date + N` for each horizon.
5. Rows upserted via DELETE+INSERT on `(run_date, symbol_id, exchange)`.
6. Stage writes `perf_tracker_summary.json` with `dates_processed` and `rows_upserted`.

## DQ / trust gates

None — by design. See "Purpose" above.

## Failure modes

| Symptom | Likely cause |
|---|---|
| `status: failed` in summary, pipeline still green | Any exception during backfill: missing OHLCV, locked DuckDB, malformed ranked_signals CSV. Error captured in metadata. |
| `rows_upserted: 0` | No new dates to process, or no `_catalog` data for any symbol. |
| Forward-return columns all NaN | Horizon hasn't matured yet (expected for latest run_date). |
| Tracker missing a new factor column | `RANKED_TO_TRACKER` not updated after rank stage added it. |
| Raw row count exceeds trusted row count | Rows were quarantined or excluded by the persisted 5-day anomaly guardrail. Inspect the raw table before changing weights. |

## Retry behavior

Fully idempotent — DELETE+INSERT keyed on `(run_date, symbol_id, exchange)`. Safe to re-run. Re-running on the same `run_id` creates `attempt_<n+1>/perf_tracker_summary.json` but leaves no duplicate database rows.

## Downstream consumers

- **Weekly digest** — `research/perf_tracker/digest.py::build_digest` queries `rank_cohort_performance` for cohort returns, bucket attribution, factor IC (rolling 30/90-day), and drift flags (IC drop >30% vs 6-month baseline). Output: `data/research/perf_digests/digest_<YYYY-WW>.md`. Currently out-of-pipeline; wire into weekly runbook if desired.
- **Research / ML training** — `rank_cohort_performance` is the labelled dataset for factor IC analysis and any future supervised model predicting forward returns from rank-stage factors.
- **API** — `ui/execution_api/routes/perf_tracker.py` reads this table. See [`docs/reference/api_reference.md`](../reference/api_reference.md).

## Commands

```bash
# Part of the canonical pipeline
ai-trading-pipeline

# Backfill from scratch (e.g., after schema change)
python -c "from ai_trading_system.research.perf_tracker.backfill import run_backfill; print(run_backfill())"

# Weekly digest manually
python -c "from ai_trading_system.research.perf_tracker.digest import build_digest; print(build_digest().output_path)"
```

`ai-trading-daily` (legacy 5-stage wrapper) does **not** include this stage.

## See also

- [`docs/stages/rank.md`](rank.md) — produces `ranked_signals.csv`.
- [`docs/stages/publish.md`](publish.md) — produces `watchlist_buckets.csv`.
- [`docs/architecture/storage_and_lineage.md`](../architecture/storage_and_lineage.md)
- [`docs/reference/database_schema.md`](../reference/database_schema.md) — `rank_cohort_performance` schema.
