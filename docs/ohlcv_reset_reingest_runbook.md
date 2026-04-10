# OHLCV Reset + Re-ingest Runbook

This runbook is for a safe operational reset of a corrupted date window.

## 1. Dry-run first (no deletion)

```bash
PYTHONPATH=. ./.venv/bin/python -m collectors.reset_reingest_validate \
  --from-date 2025-04-01 \
  --to-date 2026-04-07
```

## 2. Apply deletion + re-ingestion + final validation

```bash
PYTHONPATH=. ./.venv/bin/python -m collectors.reset_reingest_validate \
  --from-date 2025-04-01 \
  --to-date 2026-04-07 \
  --apply \
  --validation-source auto
```

`--validation-source auto` means:
- try NSE bhavcopy first
- if bhavcopy is unavailable/empty, fallback to yfinance

## 3. Optional stricter validation thresholds

```bash
PYTHONPATH=. ./.venv/bin/python -m collectors.reset_reingest_validate \
  --from-date 2025-04-01 \
  --to-date 2026-04-07 \
  --apply \
  --validation-source auto \
  --min-coverage 0.95 \
  --max-mismatch-ratio 0.02 \
  --close-tolerance-pct 0.005
```

## 4. Where reports are written

- `reports/data_repairs/reset_reingest_<from>_to_<to>_<timestamp>/reset_reingest_report.json`
- Includes:
  - pre-delete window counts
  - deleted row count
  - repair report summary
  - final validation status and metrics
  - post-reingest window counts

## Notes

- Backup is written by default before delete (`catalog_window_backup.parquet` in report dir).
- Use `--skip-backup` only if you intentionally want speed over rollback safety.
- Final validation blocks on failure (critical gate).
