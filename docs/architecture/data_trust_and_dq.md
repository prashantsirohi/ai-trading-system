# Data Trust and Data Quality

- **Purpose:** Explain how ingest assigns a per-run trust status, how quarantine works, how the DQ engine classifies rule failures, and what blocks the pipeline vs what is recorded and ignored.
- **Audience:** Operators triaging a failed run; engineers writing or relaxing a DQ rule.
- **Last verified:** 2026-07-14
- **Source of truth:** `src/ai_trading_system/domains/ingest/trust.py` (lines 868–1095, 424–500, 750–860), `src/ai_trading_system/pipeline/contracts.py` (`TrustConfidenceEnvelope` from line 12), `src/ai_trading_system/pipeline/dq/engine.py`, `src/ai_trading_system/pipeline/stages/narrative.py:174-176`, `src/ai_trading_system/pipeline/orchestrator.py:1249-1290`.

## Trust statuses

Computed by `load_data_trust_summary` (`domains/ingest/trust.py:868-1095`) and wrapped by `TrustConfidenceEnvelope` (`pipeline/contracts.py:12`).

| Status | Set when (`trust.py` reference) |
|---|---|
| `missing` | DuckDB file absent or `_catalog` table empty (lines 878-894, 900-915). |
| `legacy` | `_catalog` table exists but has no `validation_status` column (line 1047). |
| `trusted` | `validation_status` present and no fallback / quarantine ratios cross thresholds (line 1047, default branch). |
| `degraded` | Fallback ratio over `fallback_warn_threshold` (default 0.25), or active quarantine present but under blocking thresholds (lines 1059-1063). |
| `blocked` | Active quarantine exceeds `blocked_quarantine_symbol_threshold` (default 10) **or** `blocked_quarantine_ratio_threshold` (default 0.01) for critical-universe symbols (lines 1055-1057). |

The status is propagated as `data_trust_status` through stage payloads (`stages/execute.py:268`, `stages/insight.py:123`, `stages/publish.py:378/627`).

## Quarantine states

Stored in `_catalog_quarantine` (`domains/ingest/trust.py:255-272`). The status column has three values observed in code:

| Quarantine status | Lifecycle | Source |
|---|---|---|
| `active` | Newly inserted; counted in trust-summary degradation/blocking ratios. | `trust.py:776, 803-815` (insert path), `:798` (active key for delete). |
| `resolved` | Flipped when a later ingest replaces the row with validated data. | `trust.py:850-857` (`UPDATE ... SET status = 'resolved'`). |
| `permanently_unavailable` | Stale-sweep promotion after `stale_days` (`sweep_stale_quarantine`). | `trust.py:472-486` (`UPDATE ... SET status = 'permanently_unavailable'`). |

Earlier docs mentioned an `observed` state; that token does not appear in the current quarantine code path. Treat any older reference to `observed` as stale.

## Where trust blocks the pipeline

Trust degradation is informational at the stage boundary — it does not by itself raise an exception. It influences downstream behaviour at two enforced points:

1. **Narrative validation** (`pipeline/stages/narrative.py:174-176`) — when `data_trust.status` is `degraded` or `blocked`, the narrative text must contain the trust word; if not, a `degraded_trust_must_show_warning` issue is flagged. Whether that issue blocks publish depends on the narrative validation handling downstream.
2. **DQ-driven blocking** is what actually aborts a run. The orchestrator logs `status=blocked_by_dq` (`orchestrator.py:1249, 1266, 1284`) when a `DataQualityCriticalError` propagates — typically a `red_block` failure or an unrelaxed `red_repairable` failure with `dq_mode=strict`.

If you need a hard trust gate, add a stage-level DQ rule that reads the trust summary and emits a `red_block` failure; do not rely on the trust status to abort runs on its own.

## DQ rule bands

`DataQualityEngine.evaluate` (`pipeline/dq/engine.py:68-100`) classifies every rule outcome into a band:

| Band | When | Effect |
|---|---|---|
| `green` | `failed_count == 0` | Pass. |
| `amber` | Non-critical severity, or `red_repairable` downgraded by `dq_mode=relaxed` | Recorded; never blocks. |
| `red_repairable` | Critical severity, *not* in `HARD_FLOOR_RULES` | Raises `DataQualityRepairableError` unless `dq_mode=relaxed` (then downgraded to `amber` with `relaxed_from='red_repairable'`). |
| `red_block` | Rule is in `HARD_FLOOR_RULES` | Raises `DataQualityCriticalError` regardless of `dq_mode`. Aborts the run. |

Hard-floor rule set (`dq/engine.py:27-38`):
`ingest_catalog_not_empty`, `ingest_required_fields_not_null`, `ingest_ohlc_consistency`, `ingest_duplicate_ohlcv_key`, `features_snapshot_created`, `features_registry_not_empty`, `rank_artifact_not_empty`.

### Severity vs band

Severity is the rule author's tag (`severity` column on `dq_rule`); band is the runtime classification. The mapping in `_apply_relaxation` (`engine.py:113-118`) is:
- `severity == 'critical'` → default band `red_repairable` (unless on the hard-floor list, then `red_block`).
- non-critical severity → default band `amber`.

Beyond `critical`, the engine treats severity as opaque metadata. Severity labels seen in code include `critical`; other tiers (`high`, `medium`, `low`) are not enforced by the engine itself and should be considered author-defined hints. Migration `013_events_enrichment_log.sql:28` documents an events-specific `severity` column with values `low-info | medium | high`, but that is independent of the DQ engine's banding.

## Raw-price basis continuity

`ingest_bulk_raw_price_basis_shift` scans NSE equity raw closes for dates where
at least 10 distinct symbols move by at least 30% from their preceding stored
observation. Both thresholds remain configurable through
`dq_bulk_raw_gap_symbol_count` and `dq_bulk_raw_gap_pct`. A failure stores a JSON
sample URI in `dq_result`; the sample lists each suspicious date, symbol count,
symbols, median absolute move, and maximum absolute move.

The same default thresholds protect historical write tools before mutation:

- OHLCV repair combines fetched candidate rows with the immediately preceding
  and following retained observation for every rewritten symbol. Validation
  runs before backup, delete, provenance write, or upsert.
- Research-to-operational backfill builds the projected target series from
  missing candidates, retained rows in the selected chunk, and the adjacent
  retained observations. Only gaps involving a candidate row are considered.
  Validation runs before insert.

A broad failure rejects the entire proposed write with the affected dates and
counts. Do not bypass the gate by splitting one unsafe batch into smaller symbol
batches. Investigate provider basis, corporate actions, and source archives;
then retry from a verified consistent source.

## dq_mode

Driven by `context.params['dq_mode']` (`engine.py:69`), default `relaxed`. CLI plumbing is in the orchestrator entrypoint. The only effect today: `relaxed` mode downgrades non-hard-floor `red_repairable` failures to `amber`.

## Inspecting failures

Every rule evaluation persists a row to `dq_result` (`engine.py:75-86`) with:

`run_id, stage_name, rule_id, severity, status, failed_count, message, sample_uri, band, relaxed_from`

Queries:
- "Which rules failed for this run?" — `SELECT rule_id, band, status, message FROM dq_result WHERE run_id = ? ORDER BY stage_name`.
- "What was relaxed?" — `SELECT * FROM dq_result WHERE relaxed_from IS NOT NULL`.
- "What blocked the run?" — `SELECT * FROM dq_result WHERE band = 'red_block' AND status = 'failed'`.
- "Which symbols caused a bulk basis failure?" — read the JSON file referenced
  by `sample_uri` for `ingest_bulk_raw_price_basis_shift`.

The control plane store is `data/control_plane.duckdb`.

## Recovery workflow

See [../runbooks/dq_failure_response.md](../runbooks/dq_failure_response.md) for the operator playbook. The short version:

1. Read `dq_result` for the failed run; identify the offending rule and `band`.
2. For `red_block`: fix the upstream data (re-ingest, repair quarantine rows); the run cannot succeed without it.
3. For `red_repairable` in `strict` mode: re-run with `dq_mode=relaxed` if the failure is known-acceptable, otherwise fix upstream.
4. For trust-only degradation surfacing as `narrative` warnings: ensure the rendered narrative explicitly mentions the trust state.

Related: [storage_and_lineage.md](./storage_and_lineage.md) for where `dq_result` lives; [operational_data_flow.md](./operational_data_flow.md) for the run/attempt model.
