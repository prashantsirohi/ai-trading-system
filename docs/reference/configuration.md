# Configuration

- **Purpose:** Configuration sources, CLI flags, and mode selectors. For env vars see [`environment_variables.md`](environment_variables.md). For commands see [`commands.md`](commands.md).
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `argparse` parsers in `pipeline/orchestrator.py` and `pipeline/daily_pipeline.py`; env loading in `platform/`; config files under `config/`.

---

## Configuration sources

Runtime behavior is controlled by:

- **CLI flags** on `ai-trading-pipeline` (orchestrator) and `ai-trading-daily` (legacy wrapper)
- **Environment variables** loaded from `.env` when present — see [`environment_variables.md`](environment_variables.md)
- **Config files** under `config/` and `src/ai_trading_system/platform/config/`
- **Request payloads** sent to the FastAPI backend (`ai-trading-execution-api`)

`platform/config/settings.py` defines a Pydantic `AppConfig` but it is **not** the canonical source of runtime configuration today — flags and env vars dominate.

## Config files

| Path | Purpose |
|---|---|
| `config/llm_brain.yaml` | LLM prompts + model selection (override via `LLM_BRAIN_CONFIG`) |
| `config/strategies/` | Strategy rule packs |
| `config/risk_profiles/` | Risk guardrail profiles (selected by `RISK_PROFILE`) |
| `src/ai_trading_system/platform/config/rank_factor_weights.json` | Composite scoring factor weights |
| `src/ai_trading_system/platform/config/events_filters.json` | Event materiality filters |
| `src/ai_trading_system/platform/config/research_recipes.toml` | Research workflow recipes |

## Stage and mode selection

Primary selectors on `ai-trading-pipeline` (orchestrator):

| Flag | Default | Effect |
|---|---|---|
| `--stages` | `ingest,features,rank,investigator,fundamentals,candidates,candidate_tracker,events,execute,insight,publish,perf_tracker` | Comma-separated logical stage subset. `features` expands to seven internal substages; `narrative` is available but is not in the current CLI default list. |
| `--run-id <id>` | new UUID | Reuse an existing run (mainly for stage retries) |
| `--run-date YYYY-MM-DD` | today | Logical trading date |
| `--data-domain operational\|research` | `operational` | Selects DuckDB paths via `platform/db/paths.py` |
| `--canary` | off | Reduced symbol set + trimmed stage list |
| `--symbol-limit N` | unlimited (25 in canary) | Cap symbols processed |

Wrapper-specific on `ai-trading-daily`:
- Same stage string default
- Preflight runs unless `--skip-preflight`
- Injects `nse_primary=True`
- Applies holiday + weekend checks unless `--force`

## Preflight controls

Orchestrator:
- `--skip-preflight` — default on CLI
- `--run-preflight` — opt into readiness checks
- `--skip-publish-network-checks` — skip DNS checks for Telegram + Google endpoints

Daily wrapper:
- `--skip-preflight` — required to disable readiness checks there

**Caveat:** Preflight is stricter than the default ingest path. Preflight wants Dhan credentials, but the orchestrated ingest path is `NSE bhavcopy → yfinance fallback` and works without them. Run with `--skip-preflight` for local verification when Dhan is not configured.

## Ingest and trust controls

| Flag | Notes |
|---|---|
| `--symbol-limit` | Smoke runs |
| `--canary` | Reduced run |
| `--skip-delivery-collect` | Skip delivery sidecar |
| `--auto-repair-quarantine` / `--no-auto-repair-quarantine` | Auto-repair toggle |
| `--stale-missing-symbol-grace-days` | Quarantine grace |

Daily wrapper DQ + bhavcopy validation flags:

- `--disable-bhavcopy-validation`
- `--bhavcopy-validation-date`, `--bhavcopy-validation-csv`
- `--bhavcopy-validation-source auto|bhavcopy|yfinance`
- `--bhavcopy-min-coverage`, `--bhavcopy-max-mismatch-ratio`, `--bhavcopy-close-tolerance-pct`
- `--dq-max-unknown-provider-pct`, `--dq-max-unresolved-dates`, `--dq-max-unresolved-symbol-dates`, `--dq-max-unresolved-symbol-ratio-pct`
- `--dq-features-max-quarantined-symbols`, `--dq-features-max-quarantined-symbol-ratio-pct`

Stage-level trust overrides (passed as stage params, not top-level CLI):

- `allow_untrusted_rank`, `allow_untrusted_execution`, `block_degraded_execution`

## Feature controls

- `--full-rebuild` — full feature recompute
- `--feature-tail-bars N` — tail-bar window

Operational default: incremental tail recompute. Research default: full rebuild.

## Ranking and sidecar controls

- `--top-n`, `--min-score`
- `--pattern-scan-enabled` / `--no-pattern-scan-enabled`
- `--pattern-max-symbols`, `--pattern-workers`, `--pattern-lookback-days`
- `--pattern-smoothing-method`, `--pattern-timeout-seconds`
- `--breakout-engine`, `--disable-breakout-legacy-families`
- `--breakout-market-bias-allowlist`, `--breakout-min-breadth-score`
- `--breakout-sector-rs-min`, `--breakout-sector-rs-percentile-min`
- `--breakout-qualified-min-score`, `--breakout-symbol-near-high-max-pct`
- `--disable-breakout-symbol-trend-gate`
- `ml_mode` stage param — `shadow_ml` is the supported overlay mode today

See [`docs/reference/ranking_factors.md`](ranking_factors.md) and [`docs/reference/breakout_and_patterns.md`](breakout_and_patterns.md).

## Execution safety controls

- `--strategy-mode technical|ml|hybrid_confirm|hybrid_overlay`
- `--execution-top-n`, `--execution-ml-horizon`, `--execution-ml-confirm-threshold`
- `--execution-capital`, `--execution-fixed-quantity`
- `--execution-regime`, `--execution-regime-multiplier`
- `--paper-slippage-bps`
- `--execution-breakout-linkage off|soft_gate`

**Current behavior:** stage always uses paper execution. Live Dhan adapter is disabled at the source — see [`docs/reference/execution_policy.md`](execution_policy.md). UI-triggered default pipeline runs do not include `execute`.

## Publish configuration

- `--local-publish` — switches publish to local summary only (no external delivery)
- `--skip-quantstats` / `--publish-quantstats` (legacy alias)
- `--quantstats-top-n`, `--quantstats-min-overlap`, `--quantstats-max-runs`, `--quantstats-write-core-html`

**Dedupe key:** `run_id + channel + artifact_hash`. Networked publish requires Google Sheets + Telegram configuration when those channels are enabled — see [`publish_contracts.md`](publish_contracts.md).

## Canary and local modes

`--canary` plus untouched default stage string trims runs to `ingest,features,rank`. `symbol_limit` defaults to 25 in canary unless overridden.

```bash
# Canary plus publish
ai-trading-pipeline --canary --stages ingest,features,rank,publish --local-publish

# Local operator verification
ai-trading-pipeline --skip-preflight --stages ingest,features,rank,publish --local-publish
```

## See also

- [`commands.md`](commands.md) — full command list
- [`environment_variables.md`](environment_variables.md) — env vars actually read
- [`docs/runbooks/daily_operations.md`](../runbooks/daily_operations.md)
- [`docs/runbooks/troubleshooting.md`](../runbooks/troubleshooting.md)
