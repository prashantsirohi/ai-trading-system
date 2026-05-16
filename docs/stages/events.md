# Stage: events

- **Purpose:** Join rank-stage triggers (breakout, volume shock, bulk-deal) with the `market_intel` corporate-events store, apply a noise-filter chain, and emit per-trigger enriched signals plus a market-wide event snapshot.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:**
  - `src/ai_trading_system/pipeline/stages/events.py` (`EventsStage`, `EventsStageConfig`)
  - `src/ai_trading_system/domains/events/{trigger_collector,event_packet_builder,event_llm_router,noise_filter,enrichment_service,event_materiality,triggers,analyst_brief_builder,payload_builder}.py`
  - `src/ai_trading_system/integrations/market_intel_client.py`
  - `src/ai_trading_system/domains/catalysts/collector.py`

> Note: the truth map references `domains/events/service.py`, but no such file exists; the orchestration logic lives directly in `pipeline/stages/events.py`. The shared `EventQueryService` (consumed read-only) is provided by the vendored `market_intel` package via `integrations/market_intel_client.py`.

---

## Purpose

`events` sits between `rank` (and `candidates`, depending on `PIPELINE_ORDER`) and `execute`. It produces:

1. A consolidated list of operationally interesting triggers (`events_triggers.csv`).
2. A market-wide "important events" snapshot for the day (`market_events_snapshot.json`).
3. Per-trigger enriched signals (`events_enrichment.json`) with materiality, severity, top category, suppress reasons.
4. A roll-up summary (`events_summary.json`).

The stage is feature-flagged via `params.events_enabled` (default `True` at stage level; `events.py:62`).

## Entrypoints

- Stage wrapper: `src/ai_trading_system/pipeline/stages/events.py::EventsStage.run`.
- Invoked by orchestrator as part of `PIPELINE_ORDER` (`pipeline/orchestrator.py:41`).
- `EventsStage.__init__` accepts injectable `config`, `query_service_factory`, and `noise_filter` for tests.

## Input data

- Rank artifacts:
  - `rank.breakout_scan` (`breakout_scan.csv`) — primary trigger source, filtered to `EventsStageConfig.breakout_tiers` (default `("A", "B")`).
  - `rank.volume_shockers` (`volume_shockers.csv`) — optional trigger source; best-effort parsed in `_read_volume_shocker_csv` (`events.py:391`).
- `market_intel` DuckDB (read-only) via `EventQueryService`:
  - `get_important_events(...)` — market-wide snapshot.
  - `get_bulk_deals(...)` — bulk-deal triggers (`collect_bulk_deal_triggers`).
  - `get_events_for_symbol(...)` — per-trigger event lookup during enrichment.
  - `get_collector_health()` — heartbeat used to label `market_intel_status` as `ok | stale | missing | degraded` (`events.py:210`–`225`).
  - `get_market_caps([...])` — optional, feeds the materiality filter.
- `control_plane.duckdb` (via `context.registry.connection`) — read by the per-symbol dedup filter; also receives best-effort writes to `events_enrichment_log` (`events.py:601`).

## Output artifacts

Under `data/pipeline_runs/<run_id>/events/attempt_<n>/`:

| Artifact type | File | Notes |
|---|---|---|
| `market_events_snapshot` | `market_events_snapshot.json` | `{run_id, run_date, market_intel_status, events: [...]}` rows sorted by materiality, importance, date (`events.py:200`). |
| `events_triggers` | `events_triggers.csv` | Header `symbol,trigger_type,as_of_date,trigger_strength,trigger_metadata_json` (`events.py:531`). |
| `events_enrichment` | `events_enrichment.json` | `{signals: [EnrichedSignal.to_dict(), ...]}`. |
| `events_summary` | `events_summary.json` | `trigger_count`, `event_count`, `snapshot_event_count`, `suppressed_count`, breakdowns by trigger type / severity / top category / materiality, `market_intel_status`. |

When triggers are empty, the stage still writes all four files (with empty CSV header and zero-value summary) via `_emit_empty` (`events.py:477`).

When `events_enabled=False`, the stage returns immediately with `metadata={"events_enabled": False, "skipped": True}` and no artifacts (`events.py:96`–`102`).

## Main modules

- `pipeline/stages/events.py` — orchestration, config merge, artifact emission, best-effort persistence to `events_enrichment_log`.
- `domains/events/trigger_collector.py` — `collect_breakout_triggers`, `collect_bulk_deal_triggers`, `merge_triggers`. Default bulk-deal lookback 3 days, min value 5 Cr (`EventsStageConfig` defaults).
- `domains/events/triggers.py` — `Trigger` dataclass.
- `domains/events/enrichment_service.py::EnrichmentService.enrich` — per-trigger event-list build + noise-filter application; `summarize(signals)` is used for `events_summary.json`. Defaults: `DEFAULT_LOOKBACK_DAYS=30`, `DEFAULT_PER_TRIGGER_EVENT_LIMIT=10`, `DEFAULT_MIN_TRUST=80.0`.
- `domains/events/noise_filter.py::build_default_filter_chain` — composes seven filters (per module docstring): category whitelist, trust gate, materiality gate, time decay, per-symbol dedup (against `events_enrichment_log`), corroboration annotation, universe filter (pass-through). Each filter may set `_materiality_label ∈ {low, medium, high, critical}` and `_material_pct` on events.
- `domains/events/event_materiality.py::score_event_materiality` — materiality scoring helper used by the packet builder.
- `domains/events/event_packet_builder.py::build_event_packet` — compact packet for the `insight` stage (downstream).
- `domains/events/event_llm_router.py` — LLM model routing + deterministic fallback for the narrative report (downstream of events).
- `domains/events/analyst_brief_builder.py::build_analyst_brief` — used by the `insight` stage.
- `domains/events/payload_builder.py` — formats per-trigger payloads.
- `integrations/market_intel_client.py::get_event_query_service` — read-only handle (`resolve_db_path` returns the market_intel DuckDB path).
- `domains/catalysts/collector.py` — catalyst evidence loader (used by the `catalysts` domain and by `event_packet_builder` for context); not invoked directly by `EventsStage.run`.

## Process flow

1. Merge config with `context.params` (`_merge_config`, `events.py:115`); short-circuit when `events_enabled=False`.
2. Collect market snapshot (`_collect_market_snapshot`, `events.py:172`):
   - Resolve `EventQueryService` (status `missing` if not reachable, `degraded` on query exceptions).
   - Compute collector heartbeat status.
   - Query important events in the lookback window for `SNAPSHOT_CATEGORIES` (16 categories, `events.py:40`) at tiers A/B.
   - Apply the noise-filter chain to keep only events that pass for a synthetic `breakout` trigger.
   - Sort rows by `(materiality_label, importance_score, event_date)` descending.
3. Collect triggers (`_collect_triggers`, `events.py:313`):
   - Breakout from `breakout_scan.csv` (Tier A/B by default).
   - Volume-shock from `volume_shockers.csv` if present (best-effort parser).
   - Bulk-deal from `market_intel`; if `EventQueryService` is unreachable the stage proceeds with the rank-only triggers.
   - Triggers are merged via `merge_triggers` (deterministic order).
4. If no triggers survive, emit empty artifacts and return with `metadata.trigger_count=0` (`events.py:106`).
5. Enrich (`_enrich`, `events.py:431`):
   - Build a `_NullQuerier` if market_intel is unreachable (event lists become empty).
   - Construct `EnrichmentService(query_service, noise_filter, lookback_days, per_trigger_event_limit, min_trust)` and call `enrich(triggers)`.
6. Emit (`_emit`, `events.py:518`):
   - Write triggers CSV + snapshot JSON + enrichment JSON.
   - Compute summary via `summarize(signals)`; add `snapshot_event_count` and `market_intel_status`.
   - Best-effort `INSERT OR REPLACE INTO events_enrichment_log` for every signal (migration 013); failures are logged and swallowed (`events.py:601`).
7. Register four `StageArtifact` rows and return aggregate metadata.

## DQ / trust gates

- **`market_intel_status`** propagates through every output file:
  - `missing` — `market_intel` DuckDB not found; snapshot is empty.
  - `degraded` — query exception during snapshot or enrichment.
  - `stale` — `get_collector_health().last_heartbeat` older than 15 minutes (`events.py:225`).
  - `ok` — heartbeat fresh.
- **Trust gate** (filter 2 of the noise chain): events with `trust_score < min_trust` (default 80.0) are dropped.
- **Materiality gate** (filter 3): drops low-materiality events for value-bearing categories using `score_event_materiality` + market-cap provider.
- **Per-symbol dedup** (filter 5): reads `events_enrichment_log` to suppress repeats of `(symbol, top_category)` within the recency window.
- The stage degrades open: any single failure path (missing market_intel, broken filter input, log-write failure) results in warnings/metadata rather than a stage failure.

## Failure modes

- `EventQueryService` constructor raises `FileNotFoundError` → snapshot empty, `market_intel_status="missing"`, bulk-deal triggers skipped, enrichment uses `_NullQuerier` (empty event lists).
- `EventQueryService` query exception → `market_intel_status="degraded"` and warning logged; stage continues.
- `events_enrichment_log` insert failure (e.g., table absent on older deployments) → swallowed at debug level (`events.py:647`).
- Malformed `volume_shockers.csv` → exception caught, returns empty list (`events.py:425`).
- Required input `breakout_scan` missing → `_sibling_stage_dir` falls back to the conventional path; if the file is also absent, `collect_breakout_triggers` returns an empty list and the stage emits empty artifacts.

## Retry behavior

- Stateless aside from the best-effort `events_enrichment_log` upsert (which uses `INSERT OR REPLACE` keyed by signal, so retries are idempotent).
- A retry re-queries `market_intel` and re-builds all four artifacts.
- No per-task `task_status.json` resumability inside this stage.

## Downstream consumers

- `insight` — reads `events_enrichment.json` / `market_events_snapshot.json` and assembles the analyst brief via `analyst_brief_builder.build_analyst_brief`.
- `narrative` — consumes the insight packet and produces `market_report.json` via `event_llm_router.py` (model selection from `config/llm_brain.yaml`, override `LLM_BRAIN_CONFIG`).
- `publish` — surfaces summary counts in Telegram / Sheets / PDF channels.
- The FastAPI `insight` and `sectors` routers read the snapshot / enrichment files for the React console.

## Commands

```bash
# Run events as part of the full pipeline.
ai-trading-pipeline --run-date <yyyy-mm-dd>

# Re-run only the events stage (requires an existing rank attempt with breakout_scan.csv).
ai-trading-pipeline --run-date <yyyy-mm-dd> --stages events

# Disable the stage for a run via pipeline params (orchestrator-level toggling).
# (Set events_enabled=False in the run params or via the orchestrator config.)
```

Tunable params (read in `_merge_config`, `events.py:115`):

| Param | Default | Effect |
|---|---|---|
| `events_enabled` | `True` | When `False`, stage short-circuits. |
| `events_bulk_lookback_days` | `3` | Bulk-deal lookback. |
| `events_bulk_min_value_cr` | `5.0` | Minimum bulk-deal value (Cr). |
| `events_lookback_days` | `30` | Snapshot + enrichment lookback. |
| `events_per_trigger_limit` | `10` | Max events per trigger. |
| `events_min_trust` | `80.0` | Trust-gate threshold. |
| `events_snapshot_limit` | `500` | Snapshot row cap. |
| `events_min_importance` | `0.0` | Snapshot importance floor. |

> **Unverified:** the precise filter ordering and behavior of the dedup window are documented in `domains/events/noise_filter.py` (module docstring) but specific recency thresholds were not re-grepped for this writeup — consult `build_default_filter_chain` before quoting numbers.
