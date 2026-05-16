# DQ Failure Response

- **Purpose:** Triage DQ rule failures by severity, inspect `dq_result`, and rerun safely.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/architecture/data_trust_and_dq.md`](../architecture/data_trust_and_dq.md), [`docs/stages/ingest.md`](../stages/ingest.md), `src/ai_trading_system/pipeline/dq/engine.py`.

---

## Severity ladder

The DQ engine classifies failures into severity bands. Reference: `src/ai_trading_system/pipeline/dq/engine.py`.

| Severity | Behavior |
|---|---|
| **critical (hard-floor)** | Never relaxed by `dq_mode=relaxed`. Stage aborts with `DataQualityCriticalError`. Examples: `ingest_catalog_not_empty`, `ingest_required_fields_not_null`, `ingest_ohlc_consistency`, `ingest_duplicate_ohlcv_key`, `features_snapshot_created`, `features_registry_not_empty`. |
| **critical (repairable)** | In `dq_mode=strict` aborts; in `dq_mode=relaxed` (default) downgraded to amber and the stage proceeds. |
| **high** | Surfaces as a warning; stage continues. Investigate before next run. |
| **medium / low** | Logged; no immediate action required, but trend watch. |

## 1. Inspect the failing rule

```bash
duckdb data/control_plane.duckdb "
  SELECT rule_name, severity, status, details
  FROM dq_result
  WHERE run_id='<run_id>'
  ORDER BY severity DESC, rule_name;
"
```

The `details` column carries the JSON payload with offending counts, sample rows, and thresholds.

## 2. Inspect offending source rows

Ingest rules target `_catalog`; features rules target the feature store; rank rules target rank outputs. Examples:

```bash
# OHLC consistency violations
duckdb data/ohlcv.duckdb "
  SELECT * FROM _catalog
  WHERE high < open OR high < close OR low > open OR low > close OR high < low
  ORDER BY timestamp DESC LIMIT 50;
"

# Duplicate keys
duckdb data/ohlcv.duckdb "
  SELECT symbol_id, exchange, timestamp, COUNT(*) c
  FROM _catalog
  GROUP BY 1,2,3 HAVING c > 1
  ORDER BY c DESC LIMIT 50;
"

# Unresolved dates / quarantine
duckdb data/ohlcv.duckdb "SELECT trade_date, COUNT(*) FROM _catalog_quarantine GROUP BY 1 ORDER BY 1 DESC LIMIT 14;"
```

## 3. Triage decision

| Symptom | Action |
|---|---|
| Critical hard-floor (ingest_*) | Repair source data. See [data_repair.md](./data_repair.md). Do NOT bypass with `--allow-untrusted-*`. |
| Critical repairable, `dq_mode=relaxed` already in effect | Amber warning is already raised; verify root cause and address before strict-mode runs. |
| `ingest_provider_coverage_low` | Investigate provider outage; rerun ingest after recovery. |
| `ingest_recent_universe_price_jump_anomaly` | Confirm against bhavcopy. If real (corporate action), allow and document; if spurious, repair window. |
| `ingest_segment_distribution_drift` | Check series-policy / universe changes upstream. |
| `ingest_latest_trade_date_quarantine_clear` | Repair quarantined rows for the latest trade date — see [data_repair.md](./data_repair.md). |
| `features_*` critical | See [troubleshooting.md](./troubleshooting.md) "features: snapshot or registry empty". |
| High severity | Note in operator log; continue. |

## 4. Safe rerun

After the underlying data is repaired:

```bash
# Re-evaluate affected stages with a fresh attempt
python -m ai_trading_system.pipeline.orchestrator --stages <stage>[,<stage>]
```

If you need to retry the same run_id (e.g., to preserve downstream lineage):

```bash
python -m ai_trading_system.pipeline.orchestrator --run-id <run_id> --stages <stage>
```

## 5. Strict vs relaxed mode

- `dq_mode=relaxed` (default) downgrades repairable critical failures to amber.
- `dq_mode=strict` aborts on all critical failures, hard-floor or repairable.
- Set via stage params on the orchestrator (`--params`) or pipeline config.

## 6. Verify

1. New `dq_result` rows for the rerun show `status='passed'` (or `amber` where expected).
2. Downstream stages complete without trust block.
3. No regression on adjacent rules (compare to the prior good run).
