# Weekly Operations

- **Purpose:** Weekly maintenance — backups, weekly PDF, perf_tracker digest, optimization recipes.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/stages/perf_tracker.md`](../stages/perf_tracker.md), [`docs/stages/publish.md`](../stages/publish.md), [`docs/reference/commands.md`](../reference/commands.md), `src/ai_trading_system/domains/publish/channels/weekly_pdf/`, `src/ai_trading_system/research/perf_tracker/`.

---

## Cadence

Run on a weekend or before market open Monday, after the last daily run of the week has completed.

## 1. Backup operational stores

See [backup_and_restore.md](./backup_and_restore.md) for details. Minimum weekly backup:

```bash
mkdir -p backups/$(date +%Y-%m-%d)
cp data/ohlcv.duckdb         backups/$(date +%Y-%m-%d)/
cp data/control_plane.duckdb backups/$(date +%Y-%m-%d)/
cp data/execution.duckdb     backups/$(date +%Y-%m-%d)/
cp data/research.duckdb      backups/$(date +%Y-%m-%d)/
cp -R data/feature_store     backups/$(date +%Y-%m-%d)/
```

`data/pipeline_runs/` is reproducible — back it up only if you need an audit trail.

## 2. Weekly PDF report

The weekly PDF is a publish channel under `domains/publish/channels/weekly_pdf/`. It is emitted whenever the publish stage runs with the channel enabled. There is no separate weekly-only CLI entrypoint in `pyproject.toml`.

To produce the weekly PDF as part of a normal publish run:

```bash
ai-trading-pipeline --stages ingest,features,rank,candidates,events,insight,narrative,publish
```

Then inspect the artifact under `data/pipeline_runs/<run_id>/publish/attempt_<n>/` and the delivery row in `publisher_delivery_log` (`control_plane.duckdb`).

> Current code status: a standalone weekly-pdf-only CLI is not documented — verify before relying on a non-pipeline path.

## 3. perf_tracker digest

The perf_tracker stage runs as part of the daily pipeline and writes `rank_cohort_performance` in `data/research.duckdb` (see [`docs/stages/perf_tracker.md`](../stages/perf_tracker.md)). The weekly digest is built by `src/ai_trading_system/research/perf_tracker/digest.py`.

To regenerate digest data outside the pipeline:

```bash
python -m ai_trading_system.pipeline.orchestrator --stages perf_tracker
```

Inspect:

```bash
duckdb data/research.duckdb "SELECT run_date, COUNT(*) FROM rank_cohort_performance GROUP BY 1 ORDER BY 1 DESC LIMIT 10;"
```

> Current code status: a dedicated CLI entrypoint for emitting the digest as a standalone report is not in `pyproject.toml` — verify before relying on this.

## 4. Research / optimization recipes

```bash
ai-trading-research-recipe --recipe <recipe_name>
ai-trading-research-recipe --bundle <bundle_name>
```

Recipes live under `config/research_recipes.toml`. Research uses the isolated research data domain — set `DATA_DOMAIN=research` if you want it pointed at `data/research_ohlcv.duckdb`.

## 5. Shadow monitor (ML overlay)

```bash
python -m ai_trading_system.research.shadow_monitor
python -m ai_trading_system.research.shadow_monitor --backfill-days 30
```

## 6. Disk and log housekeeping

1. List old run dirs older than retention window:
   ```bash
   find data/pipeline_runs -maxdepth 1 -type d -mtime +30 -print
   ```
   Confirm before deleting.
2. Rotate logs as configured by your deployment (see [deployment_mac_mini.md](./deployment_mac_mini.md)).

## 7. Verification

- `backups/<date>/` contains the four DuckDB files and `feature_store/`.
- Weekly PDF artifact present in the latest publish attempt dir.
- `rank_cohort_performance` has rows for the past week.
