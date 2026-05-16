# Data Repair

- **Purpose:** Repair OHLCV gaps, inspect quarantine, and re-ingest a date window safely.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/stages/ingest.md`](../stages/ingest.md), [`docs/reference/commands.md`](../reference/commands.md), `src/ai_trading_system/domains/ingest/reset_reingest_validate.py`, `src/ai_trading_system/domains/ingest/trust.py`.

---

## When to use this runbook

- A rank or execute run was blocked by trust (see [troubleshooting.md](./troubleshooting.md)).
- `ingest_summary.json` shows `unresolved_dates` or persistent `quarantined_row_count`.
- A bhavcopy-validation gate failed (`DataQualityCriticalError` from `run_bhavcopy_validation`).
- You suspect bad rows in `_catalog` for a known date range.

## 1. Inspect catalog and quarantine

```bash
duckdb data/ohlcv.duckdb "SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM _catalog;"
duckdb data/ohlcv.duckdb "SELECT trade_date, COUNT(*) FROM _catalog_quarantine WHERE trade_date >= current_date - 14 GROUP BY 1 ORDER BY 1;"
duckdb data/ohlcv.duckdb "SELECT symbol_id, trade_date, reason FROM _catalog_quarantine WHERE trade_date='YYYY-MM-DD' LIMIT 50;"
```

## 2. Inspect the offending ingest run

```bash
ls data/pipeline_runs/<run_id>/ingest/attempt_*/
cat data/pipeline_runs/<run_id>/ingest/attempt_*/ingest_summary.json | jq '.unresolved_dates, .quarantined_row_count, .bhavcopy_validation'
```

## 3. Dry-run reset + re-ingest + validate

This does **not** delete or modify catalog rows. It produces a report so you can review the scope.

```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD \
  --to-date YYYY-MM-DD
```

Report location: `reports/data_repairs/reset_reingest_<from>_to_<to>_<timestamp>/reset_reingest_report.json`.

## 4. Apply the repair

```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD \
  --to-date YYYY-MM-DD \
  --apply
```

Notes from the legacy runbook (`docs/_legacy/archived_2026-05-16/ohlcv_reset_reingest_runbook.md`):

- A pre-delete window backup (`catalog_window_backup.parquet`) is written by default. Do not pass `--skip-backup` unless you accept loss of rollback safety.
- Final validation is a critical gate; failure aborts the apply step.

### Stricter validation thresholds (optional)

```bash
python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD \
  --to-date YYYY-MM-DD \
  --apply \
  --validation-source auto \
  --min-coverage 0.95 \
  --max-mismatch-ratio 0.02 \
  --close-tolerance-pct 0.005
```

> `--validation-source auto` and the threshold flags above are documented in the legacy runbook. Current code status: re-verify each flag against `reset_reingest_validate.py` before relying on them in unfamiliar windows.

## 5. Rebuild downstream after repair

After the apply step succeeds, rerun the affected stages:

```bash
python -m ai_trading_system.pipeline.orchestrator --stages ingest,features,rank
```

If the prior run was otherwise publishable, follow up with a publish retry for that run_id — see [publish_retry.md](./publish_retry.md).

## 6. Feature rebuild after repair

A successful repair changes `_catalog` rows for the repaired window. The features stage reads `updated_symbols` from the latest `ingest_summary.json` for incremental compute. To force a fresh features attempt:

```bash
python -m ai_trading_system.pipeline.orchestrator --stages features
```

> Current code status: a documented "force full feature rebuild for a symbol set" CLI flag was not verified. Run the standard features stage and rely on `updated_symbols` from the repaired ingest.

## 7. Verify

1. `data/ohlcv.duckdb::_catalog` row counts match the report's `post-reingest window counts`.
2. `_catalog_quarantine` has no active rows for the repaired window.
3. A fresh `ingest` run reports `freshness_status: "fresh"` and empty `unresolved_dates`.
4. `rank` runs without trust block.
