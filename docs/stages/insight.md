# Stage: insight

- **Purpose:** Build deterministic technical + event intelligence packets and the analyst brief consumed by the narrative LLM stage.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/pipeline/stages/insight.py`](../../src/ai_trading_system/pipeline/stages/insight.py), [`src/ai_trading_system/domains/events/analyst_brief_builder.py`](../../src/ai_trading_system/domains/events/analyst_brief_builder.py), [`src/ai_trading_system/domains/events/event_packet_builder.py`](../../src/ai_trading_system/domains/events/event_packet_builder.py)

---

## Purpose

Deterministic enrichment stage. Combines rank/breakout/pattern/sector/dashboard artifacts with event intelligence and open positions into a single `combined_insight_packet.json` and reason-card-style `analyst_brief.json`. Kept separate from `narrative` so that an LLM provider outage cannot block downstream packet generation.

## Entrypoints

- Stage wrapper: [`src/ai_trading_system/pipeline/stages/insight.py::InsightStage`](../../src/ai_trading_system/pipeline/stages/insight.py)
- Runs after `execute`, before `narrative` (`PIPELINE_ORDER` in [`pipeline/orchestrator.py:41`](../../src/ai_trading_system/pipeline/orchestrator.py))
- Invoked by `ai-trading-pipeline`

## Input data

Read via `context.artifact_for(...)` ([`insight.py:32-37`](../../src/ai_trading_system/pipeline/stages/insight.py)):

| Upstream stage | Artifact type | Required |
|---|---|---|
| `rank` | `ranked_signals` | yes (best-effort read) |
| `rank` | `breakout_scan` | optional |
| `rank` | `pattern_scan` | optional |
| `rank` | `sector_dashboard` | optional |
| `rank` | `dashboard_payload` (JSON) | optional |
| `execute` | `positions` | optional — drives portfolio-symbol set |

Parameters:
- `insight_report_type` — `"daily"` (default) or `"weekly"`
- `watchlist_symbols` — comma-separated string

## Output artifacts

Under `data/pipeline_runs/<run_id>/insight/attempt_<n>/`:

| Artifact | File |
|---|---|
| `technical_packet` | `technical_packet.json` |
| `event_packet` | `event_packet.json` |
| `analyst_brief` | `analyst_brief.json` |
| `combined_insight_packet` | `combined_insight_packet.json` |
| `event_confluence` | `event_confluence.csv` |
| `event_features` | `event_features.csv` |

## Main modules

- [`domains/events/event_packet_builder.py::build_event_packet`](../../src/ai_trading_system/domains/events/event_packet_builder.py) — event intel snapshot, portfolio/watchlist overlays, confluence frame
- [`domains/events/analyst_brief_builder.py::build_analyst_brief`](../../src/ai_trading_system/domains/events/analyst_brief_builder.py) — symbol-level reason cards merging rank, breakout, pattern, events; computes event_confluence/risk/recency scores and event-vs-price alignment label
- [`domains/events/analyst_brief_builder.py::build_event_features_frame`](../../src/ai_trading_system/domains/events/analyst_brief_builder.py) — flattens analyst brief to per-symbol features
- DQ summary helper: reads `data_quality_result` from the pipeline registry connection ([`insight.py:134-163`](../../src/ai_trading_system/pipeline/stages/insight.py))

## Process flow

1. Read upstream rank + execute artifacts (missing artifacts return empty frames — non-fatal).
2. Derive `portfolio_symbols` from `positions.csv` and `watchlist_symbols` from params.
3. `build_event_packet(...)` → event packet + confluence frame.
4. `_build_technical_packet(...)` → market regime, sector strength, top-50 rank/breakout/pattern, positions, DQ summary.
5. Merge into `combined_packet`, then `build_analyst_brief(combined_packet)` → symbol cards + sector cards.
6. Persist all five JSON artifacts and two CSVs; return `report_type`, `event_count`, `confluence_count` metadata.

## DQ / trust gates

- Pulls last 50 `data_quality_result` rows for the current `run_id` into the technical packet (best-effort; swallows registry errors).
- Surfaces `data_trust_status` from `dashboard_payload.summary`.
- `market_intel_status` from event packet propagates to narrative validation.

## Failure modes

- Any individual artifact read failure returns an empty `DataFrame`/`{}` — stage continues.
- Registry unavailable → DQ summary is `{}` (no raise).
- The stage does not raise on degraded inputs; downstream `narrative` validation enforces trust warnings.

## Retry behavior

Pure transform. Re-running on the same `run_id` regenerates artifacts in a new `attempt_<n>` directory. No external side effects.

## Downstream consumers

- [`narrative` stage](narrative.md): requires `combined_insight_packet` and `analyst_brief`.
- [`publish` stage](publish.md): reads `event_confluence` and (via narrative) the daily/weekly insight JSON for the dashboard overlay.

## Commands

```bash
# Daily report
ai-trading-pipeline

# Weekly report (sets insight_report_type=weekly)
ai-trading-pipeline --insight-report-type weekly

# Inspect produced packet
jq '.symbol_cards[0]' data/pipeline_runs/<run_id>/insight/attempt_1/analyst_brief.json
```
