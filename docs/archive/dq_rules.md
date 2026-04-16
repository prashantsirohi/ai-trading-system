> ARCHIVED - superseded by the canonical docs in /docs. Do not use this file as the current source of truth.

# Data Quality Rules

## Severity Policy
- `critical`: hard fail, block downstream stages
- `high`: continue run, but persist and alert
- `medium`: warning for monitoring and trend review
- `low`: informational

## Implemented Rule Catalog

### `ingest`
1. `ingest_duplicate_ohlcv_key`
   Severity: `critical`
   Contract: raw OHLCV key must be unique on `symbol_id`, `exchange`, `timestamp`.
2. `ingest_catalog_not_empty`
   Severity: `critical`
   Contract: `_catalog` must contain at least one row after ingest.
3. `ingest_required_fields_not_null`
   Severity: `critical`
   Contract: `symbol_id`, `exchange`, `timestamp`, `open`, `high`, `low`, `close`, and `volume` must all be populated.
4. `ingest_ohlc_consistency`
   Severity: `critical`
   Contract: `high >= max(open, close)`, `low <= min(open, close)`, and `high >= low`.
5. `ingest_negative_volume`
   Severity: `high`
   Contract: volume should not be negative.

### `features`
6. `features_snapshot_created`
   Severity: `critical`
   Contract: features stage must emit a `snapshot_id`.
7. `features_registry_not_empty`
   Severity: `critical`
   Contract: features stage must report non-zero computed rows.
8. `features_catalog_freshness`
   Severity: `high`
   Contract: source `_catalog` data should be recent relative to the logical run date.

### `rank`
9. `rank_artifact_not_empty`
   Severity: `critical`
   Contract: `ranked_signals` must contain at least one row.
10. `rank_required_columns_present`
   Severity: `high`
   Contract: `ranked_signals` must include `symbol_id` and `composite_score`.
11. `rank_duplicate_symbols`
   Severity: `medium`
   Contract: `ranked_signals` should not contain duplicate `symbol_id` rows.
12. `rank_symbol_coverage_low`
   Severity: `high`
   Contract: ranked output should meet the configured minimum symbol coverage threshold.

## Persistence
Each evaluation writes a `dq_result` row with:
- `run_id`
- `stage_name`
- `rule_id`
- `severity`
- `status`
- `failed_count`
- `message`
- `sample_uri`

Each configured rule also persists in `dq_rule` with:
- `severity`
- `rule_sql`
- `enabled`
- ownership metadata

## Execution Semantics
- DQ checks run after `ingest`, `features`, and `rank`.
- Critical failures raise a stage error and stop downstream execution.
- Publish is not a DQ-gated stage because it is non-authoritative delivery.
- High and medium failures are persisted and available for alerting or operator review.

## Rule Lifecycle
New rules should be added to `dq_rule` with:
- owner
- severity
- description
- rollout date

When adding a new critical rule:
1. document it here
2. add migration or seeding logic if needed
3. add a smoke test or unit test covering the failure mode
