# Catalyst Intelligence Domain

- **Purpose:** Corporate-action and event awareness — NSE announcements, market_intel feed, LLM-assisted materiality classification, and enrichment into rank artifacts.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/domains/catalysts/`](../../src/ai_trading_system/domains/catalysts/), [`src/ai_trading_system/domains/events/`](../../src/ai_trading_system/domains/events/), [`src/ai_trading_system/integrations/market_intel_client.py`](../../src/ai_trading_system/integrations/market_intel_client.py)

---

## Responsibility

Be the **event/catalyst layer** that wraps technical ranking with context. Three logical pieces:

1. **Catalyst collection** — pull corporate announcements from NSE + market_intel.
2. **Event packet construction** — assemble per-symbol event context.
3. **LLM-assisted classification** — route ambiguous events through an LLM (OpenRouter) for materiality.

## Package / module ownership

### `domains/catalysts/`
- `collector.py` — NSE corporate actions / market_intel sweeper.

### `domains/events/`
| Module | Role |
|---|---|
| `service.py::EventsOrchestrationService` | Events stage orchestration. |
| `trigger_collector.py` | Trigger events from catalyst data. |
| `event_packet_builder.py` | Assemble per-symbol event context. |
| `event_llm_router.py` | LLM-based materiality classification (uses `config/llm_brain.yaml`). |
| `noise_filter.py` | Filter low-signal events deterministically before any LLM call. |
| `enrichment_service.py` | Merge events into rank artifacts. |
| `analyst_brief_builder.py` | Used by `insight` stage to build LLM context. |

### `integrations/`
- `market_intel_client.py` — outbound client to the always-on `market_intel` runner described in [`docs/operations/market_intel_runner.md`](../_legacy/archived_2026-05-16/operations_market_intel_runner.md).

## Public contracts

Stage artifacts:

- `data/pipeline_runs/<run_id>/events/attempt_<n>/event_packet.json` — rich event data structure.
- `event_enriched_rank.csv` — rank data + event flags.
- `data/pipeline_runs/<run_id>/insight/attempt_<n>/market_insight.json` — analyst brief.
- `data/pipeline_runs/<run_id>/narrative/attempt_<n>/market_report.json` — LLM-generated narrative.

## Storage ownership

- Stage artifacts (events, insight, narrative).
- Catalyst storage in DuckDB — **verify exact table when writing `reference/database_schema.md`.**

## Dependencies

- External: NSE corporate actions endpoint, market_intel service, OpenRouter LLM API.
- Env vars: `OPENROUTER_KEY` / `OPENROUTER_API_KEY`, `LLM_BRAIN_CONFIG`.
- Config: `config/llm_brain.yaml`, `config/events_filters.json`.

## Extension points

- New catalyst source: extend `catalysts/collector.py`.
- New event filter: add a deterministic rule to `noise_filter.py` **before** considering LLM routing.
- New LLM model: change `config/llm_brain.yaml`; router will pick it up.

## Known gaps

- LLM cost controls — `noise_filter.py` is the first gate; track LLM-routed event count carefully.
- Materiality classification is opinionated; document the heuristic when writing `reference/...`.

## See also

- [`docs/stages/events.md`](../stages/events.md)
- [`docs/stages/insight.md`](../stages/insight.md)
- [`docs/stages/narrative.md`](../stages/narrative.md)
- [`docs/operations/market_intel_runner.md`](../_legacy/archived_2026-05-16/operations_market_intel_runner.md) (to be migrated to runbooks)
