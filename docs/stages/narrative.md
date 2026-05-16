# Stage: narrative

- **Purpose:** LLM synthesis of the daily/weekly market report from the deterministic insight packet, with validation and deterministic fallback.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/pipeline/stages/narrative.py`](../../src/ai_trading_system/pipeline/stages/narrative.py), [`src/ai_trading_system/domains/events/event_llm_router.py`](../../src/ai_trading_system/domains/events/event_llm_router.py), [`config/llm_brain.yaml`](../../config/llm_brain.yaml)

---

## Purpose

Runs the market-report LLM against the combined insight packet and analyst brief, validates the markdown output, and falls back to a deterministic synthesis when validation fails or no API key is set. All LLM-shaped artifacts live here so an LLM provider outage cannot block the deterministic [`insight` stage](insight.md).

## Entrypoints

- Stage wrapper: [`src/ai_trading_system/pipeline/stages/narrative.py::NarrativeStage`](../../src/ai_trading_system/pipeline/stages/narrative.py)
- Runs after `insight`, before `publish` (`PIPELINE_ORDER` in [`pipeline/orchestrator.py:41`](../../src/ai_trading_system/pipeline/orchestrator.py))

## Input data

- **Required artifacts** ([`narrative.py:32-33`](../../src/ai_trading_system/pipeline/stages/narrative.py)):
  - `insight.combined_insight_packet`
  - `insight.analyst_brief`
- **Params:** `insight_report_type` — `"daily"` (default) or `"weekly"`
- **Config file:** `config/llm_brain.yaml` (override path via `LLM_BRAIN_CONFIG` env var, [`event_llm_router.py:108`](../../src/ai_trading_system/domains/events/event_llm_router.py)). Defines route per report type with `primary` / `fallback` model, `max_output_tokens`, `temperature`. Defaults in `event_llm_router.DEFAULT_ROUTES` ([`event_llm_router.py:16-29`](../../src/ai_trading_system/domains/events/event_llm_router.py)).
- **Env vars:**
  - `OPENROUTER_KEY` or `OPENROUTER_API_KEY` — OpenRouter API key ([`event_llm_router.py:57`](../../src/ai_trading_system/domains/events/event_llm_router.py)). When unset, the LLM call is skipped and a deterministic synthesis is used (`status: skipped_no_api_key`).
  - `LLM_BRAIN_CONFIG` — optional override of `config/llm_brain.yaml` path.

## Output artifacts

Under `data/pipeline_runs/<run_id>/narrative/attempt_<n>/`:

| Artifact | File |
|---|---|
| `llm_synthesis` | `llm_synthesis.json` |
| `llm_synthesis_raw` | `llm_synthesis_raw.json` |
| `daily_insight_json` / `weekly_insight_json` | `<report_type>_insight.json` |
| `daily_insight_markdown` / `weekly_insight_markdown` | `<report_type>_insight.md` |
| `telegram_summary` | `telegram_summary.txt` |
| `model_usage` | `model_usage.json` |
| `validation_report` | `validation_report.json` |

## Main modules

- [`domains/events/event_llm_router.py::build_market_synthesis`](../../src/ai_trading_system/domains/events/event_llm_router.py) — route selection, OpenRouter call, JSON synthesis normalization + validation
- [`event_llm_router.build_deterministic_synthesis`](../../src/ai_trading_system/domains/events/event_llm_router.py) — pure-Python fallback synthesis from analyst brief
- [`event_llm_router.render_market_report_markdown`](../../src/ai_trading_system/domains/events/event_llm_router.py) — render synthesis JSON to markdown
- [`narrative.validate_report`](../../src/ai_trading_system/pipeline/stages/narrative.py) — output guardrails (no code fences, no truncation, no buy/sell guarantee phrases, no invented symbols, event claims must be cited, degraded-trust/market-intel warning surfacing)
- [`narrative._build_telegram_summary`](../../src/ai_trading_system/pipeline/stages/narrative.py) — short summary appended for Telegram channel

## Process flow

1. Load `combined_insight_packet` and `analyst_brief`.
2. `build_market_synthesis(packet, report_type=...)`:
   - Pick `weekly_market_report` or `daily_market_report` route from `llm_brain.yaml` (or default).
   - If no API key → return `build_deterministic_synthesis(analyst_brief)` with `status: skipped_no_api_key`.
   - Otherwise call OpenRouter; if response fails synthesis JSON validation → fallback to deterministic synthesis with `status: validation_fallback`.
   - On exception → deterministic synthesis with `status: fallback_after_error`.
3. Render markdown via `render_market_report_markdown`.
4. `validate_report(markdown, packet, model_usage=...)`:
   - Reject raw triple-backtick fences, mid-sentence truncation tied to `possible_truncation`, banned phrases (`guaranteed buy`, `must buy`, `price target` without explicit "no price target", …), invented all-caps symbols not in allowed set, uncited event claims, and missing degraded/market-intel warnings.
5. If validation fails → rebuild with deterministic synthesis and re-validate. The deterministic path is kept regardless of its validation outcome.
6. Build Telegram summary (first 18 markdown lines + event/rank summary).
7. Persist seven artifacts.

## DQ / trust gates

- `validate_report` blocks markdown that fails guardrails; failure triggers deterministic fallback rather than raising.
- Synthesis-level validation lives in `validate_synthesis_json` inside [`event_llm_router.py`](../../src/ai_trading_system/domains/events/event_llm_router.py).
- Degraded `data_trust.status` or `market_intel_status` must appear in the markdown body or validation fails.

## Failure modes

- **Missing insight artifacts:** `context.require_artifact("insight", ...)` raises.
- **OpenRouter HTTP error or rate limit:** caught inside `build_market_synthesis`; deterministic fallback with `status: fallback_after_error`.
- **Truncated LLM output:** flagged via `possible_truncation`; validation rejects mid-sentence endings.
- **Malformed `llm_brain.yaml`:** route load falls back to `DEFAULT_ROUTES`.

## Retry behavior

No internal retry of the LLM call (single attempt per stage attempt). Orchestrator-level retry creates a new `attempt_<n>` directory. Deterministic fallback ensures the stage almost always produces a usable artifact.

## Downstream consumers

- [`publish` stage](publish.md): reads `telegram_summary` for the Telegram channel and `daily_insight_json` / `weekly_insight_json` for dashboard overlay.

## Commands

```bash
# Daily run with OpenRouter LLM
OPENROUTER_KEY=sk-... ai-trading-pipeline

# Run without an LLM key (deterministic synthesis path)
ai-trading-pipeline

# Override LLM route config
LLM_BRAIN_CONFIG=/path/to/custom_llm_brain.yaml ai-trading-pipeline

# Weekly variant
ai-trading-pipeline --insight-report-type weekly

# Inspect last validation result
jq . data/pipeline_runs/<run_id>/narrative/attempt_1/validation_report.json
```
