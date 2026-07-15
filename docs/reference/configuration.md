# Configuration

- **Purpose:** Configuration sources, CLI flags, and mode selectors. For env vars see [`environment_variables.md`](environment_variables.md). For commands see [`commands.md`](commands.md).
- **Audience:** Operator, developer.
- **Last verified:** 2026-07-15
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
| `--opportunity-registry-mode` | `off` | `shadow` inserts the optional canonical opportunity reconciliation stage after Investigator; it never feeds execution. |
| `--opportunity-registry-dry-run` | false | Runs adapters, admission, lifecycle, retention, and audit output without opportunity-registry writes. |
| `--opportunity-scan-routing-mode` | `off` | `compare` writes Phase 3B sidecars; `shadow` also supplies them to opportunity reconciliation. |
| `--rank-deep-scan-limit` | 250 | Rank-selected daily deep-scan allocation. |
| `--stage-promoted-scan-limit` | 75 | Additional stage-promoted allocation; position/follow-through overrides are uncapped. |
| `--recent-exit-cooling-sessions` | 15 | Trading-session cooling window; accepted range is 10–20. |
| `--position-recovery-mode` | `report_only` | `reviewed` requires review metadata; `automatic` still requires the explicit legacy recovery enable flag. |
| `--recover-position-only-episodes` | false | Backward-compatible explicit enable used only by automatic recovery; does not bypass compatibility. |
| `--active-position-market-data-max-staleness-sessions` | 0 | Maximum stored trading sessions by which active-position market data may lag. |
| `--active-position-alert-enabled` | true | Persist critical missing-data incidents while Phase 3B/3C routing runs. |
| `--minimum-sector-constituents` | 5 | Minimum constituents for a known sector stage. |
| `--minimum-sector-stage-coverage-ratio` | 0.70 | Minimum classified constituent coverage. |
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

## Phase 3C-4 performance policy

`PerformanceConfig` is the typed source for `phase3c4-performance-policy-v1`.
Stage params accept `performance_instrumentation_enabled`,
`performance_threshold_evaluation_enabled`, `performance_fail_pipeline`, and
`performance_policy_version`, plus the typed thresholds below:

- upper bounds: weekly stage, sector aggregation, scan router, Investigator,
  opportunities, total shadow pipeline, peak RSS, maximum artifact size, and
  baseline regression;
- lower bounds: minimum symbols per second.

For upper bounds, values below warn pass, warn through below fail warn, and fail
or above fails. For throughput, warn or above passes, above fail through below
warn warns, and fail or below fails. Negative or reversed thresholds are
rejected. Defaults are conservative and informational;
`performance_fail_pipeline=false`. Cache labels are `COLD`, `WARM`, or `UNKNOWN`;
replay labels are `FIRST_RUN`, `EXACT_REPLAY`, or `NON_IDENTICAL_REPLAY`.

## Phase 3C-5 calibration policy

`CalibrationConfig` is the typed source for
`phase3c5-calibration-policy-v1`. Its sample requirements distinguish a
development-ready minimum from a limitation threshold and require outcome-class,
market-regime, stage, scan-tier, and setup-family coverage. Critical failures
include look-ahead inputs, duplicate identities, unresolved governance,
survivorship-biased populations, missing required outcome classes, and manifest
integrity mismatch.

`--profile small_fixture` is deterministic test evidence only.
`--profile copied_realistic` requires `--copied-control-plane`; the source is
opened read-only and is never accepted when it resolves to the configured
operator store. `--fail-on-not-ready` changes only the CLI exit code. It does
not change the verdict or mutate any store. No environment variable enables
Phase 4 or production calibration in Phase 3C-5.

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

## Phase 3C-1 annotation safety

`ai-trading-annotate-phase3c1-governance` is not a pipeline mode. It requires an
explicit `--copied-control-plane` path, refuses the configured operational
control plane, defaults to a read-only preview, and requires both `--apply` and
`--confirm-copied-store` before appending annotations to the copy. It has no
operator-store override.

## Phase 4A API configuration

Phase 4A uses environment-backed `ApiSettings`; secrets are never stored in a
config file. Defaults bind to `127.0.0.1`, require authentication, disable
caching, use 50 rows per page (500 maximum), and allow 120 requests per minute
per credential.

| Variable | Default | Meaning |
|---|---|---|
| `PHASE4_API_SOURCE_PROFILE` | `operator_read_only` | `small_fixture`, `copied_store`, or `operator_read_only`. |
| `PHASE4_API_COPIED_CONTROL_PLANE` | unset | Explicit copy; operator paths and symlinks are rejected. |
| `PHASE4_API_ARTIFACT_ROOT` | unset | Fixed immutable evidence root; safe-root and symlink checks apply. |
| `PHASE4_API_AUTH_ENABLED` | `true` | Require bearer or `X-API-Key` authentication outside public health. |
| `PHASE4_API_LOCAL_DEV_MODE` | `false` | Explicit local-only authentication bypass. |
| `PHASE4_API_KEY` | unset | Runtime secret; never logged. |
| `PHASE4_API_HOST` / `PHASE4_API_PORT` | `127.0.0.1` / `8765` | Bind address and port. |
| `PHASE4_API_DEFAULT_PAGE_SIZE` / `PHASE4_API_MAX_PAGE_SIZE` | `50` / `500` | Cursor-page bounds. |
| `PHASE4_API_RATE_LIMIT_PER_MINUTE` | `120` | Local in-memory per-key limit. |
| `PHASE4_API_CACHE_ENABLED` | `false` | Reserved in-memory cache toggle; never a file cache. |
| `PHASE4_API_INCLUDE_OPENAPI` | `true` | Expose OpenAPI and interactive docs. |

Requests cannot change source profile, database path, or runtime root.
Artifact selection prefers completed promoted registry entries and otherwise
uses semantic run/as-of identity. It never uses file mtime as freshness.

## Phase 4B dashboard configuration

The React/Vite dashboard is a separate static build and uses only the Phase 4A
HTTP contract.

| Variable | Default | Meaning |
|---|---|---|
| `VITE_PHASE4_API_BASE_URL` | same origin | Phase 4A origin; only HTTP(S) URLs are accepted. |
| `VITE_PHASE4_API_AUTH_MODE` | `bearer` | `bearer` or `api-key`; credentials remain in headers. |
| `VITE_PHASE4_API_KEY` | unset | Optional build-time development credential. Prefer session entry or a secure reverse proxy. |
| `VITE_PHASE4_DEFAULT_POLL_SECONDS` | `60` | Conservative polling interval for live operator pages. |

`VITE_*` values are public bundle configuration. Never use
`VITE_PHASE4_API_KEY` for a production secret. Credentials entered in the UI
remain in page memory and are never stored in browser storage or placed in
URLs. Filters, tabs, cursors, and supported `as_of` values may be in URLs.

## See also

- [`commands.md`](commands.md) — full command list
- [`environment_variables.md`](environment_variables.md) — env vars actually read
- [`docs/runbooks/daily_operations.md`](../runbooks/daily_operations.md)
- [`docs/runbooks/troubleshooting.md`](../runbooks/troubleshooting.md)
