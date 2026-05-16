# Configuration

## Configuration sources

Current runtime behavior is controlled by:
- CLI flags on `run.orchestrator` and `run.daily_pipeline`
- environment variables loaded from the repo `.env` when present
- explicit request payloads sent to `src/ai_trading_system/ui/execution_api/app.py`

`config/settings.py` is not the canonical source of runtime configuration.

## Environment variables

### Core selectors

Used directly by current code:
- `DATA_DOMAIN`: defaults to `operational`
- `ENV`: defaults to `local`
- `AI_TRADING_PROJECT_ROOT`: overrides FastAPI project-root resolution

### Dhan integration

Used by collectors, runtime config, token management, or preflight:
- `DHAN_API_KEY`
- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`
- `DHAN_REFRESH_TOKEN`
- `DHAN_TOTP`
- `DHAN_PIN`
- `DHAN_TOKEN_EXPIRY`
- `DHAN_ENABLE_RENEW_TOKEN`

Important current caveat:
- current preflight requires Dhan credentials for `ingest` and `features`
- the default orchestrated ingest path is still `NSE bhavcopy -> yfinance fallback`
- preflight is stricter than the current default ingest path

### Google Sheets publish

Used by preflight and publisher code:
- `GOOGLE_SPREADSHEET_ID`
- `GOOGLE_SHEETS_CREDENTIALS`
- `GOOGLE_TOKEN_PATH`

### Telegram publish

Used by runtime config and reporter code:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_CONNECT_TIMEOUT_SECONDS`
- `TELEGRAM_READ_TIMEOUT_SECONDS`
- `TELEGRAM_WRITE_TIMEOUT_SECONDS`
- `TELEGRAM_POOL_TIMEOUT_SECONDS`
- `TELEGRAM_SEND_ATTEMPTS`
- `TELEGRAM_DNS_PRECHECK_ENABLED`

### Miscellaneous

Used by isolated current modules:
- `OPENROUTER_KEY`: only for `channel/ai_analyzer.py`
- `MPLCONFIGDIR`: set internally by report-generation code when needed

## Stage and mode selection

Primary selectors on `run.orchestrator`:
- `--stages`: comma-separated stage list, default `ingest,features,rank,events,execute,insight,publish`
- `--run-id`: reuse an existing run id, mainly for stage retries
- `--run-date`: logical trading date
- `--data-domain operational|research`

Wrapper-specific behavior on `run.daily_pipeline`:
- same stage string default
- preflight runs unless `--skip-preflight`
- wrapper injects `nse_primary=True`
- wrapper applies holiday and weekend checks unless `--force`

## Preflight controls

Orchestrator flags:
- `--skip-preflight`: default on the CLI
- `--run-preflight`: opt into readiness checks
- `--skip-publish-network-checks`: skip DNS checks for Telegram and Google endpoints

Daily wrapper flag:
- `--skip-preflight`: required to disable readiness checks there

## Ingest and trust controls

Current operational flags include:
- `--symbol-limit`
- `--canary`
- `--skip-delivery-collect`
- `--auto-repair-quarantine` or `--no-auto-repair-quarantine`
- `--stale-missing-symbol-grace-days`

Daily wrapper validation and DQ controls include:
- `--disable-bhavcopy-validation`
- `--bhavcopy-validation-date`
- `--bhavcopy-validation-csv`
- `--bhavcopy-validation-source auto|bhavcopy|yfinance`
- `--bhavcopy-min-coverage`
- `--bhavcopy-max-mismatch-ratio`
- `--bhavcopy-close-tolerance-pct`
- `--dq-max-unknown-provider-pct`
- `--dq-max-unresolved-dates`
- `--dq-max-unresolved-symbol-dates`
- `--dq-max-unresolved-symbol-ratio-pct`
- `--dq-features-max-quarantined-symbols`
- `--dq-features-max-quarantined-symbol-ratio-pct`

Trust and execution overrides used in stage code:
- `allow_untrusted_rank`
- `allow_untrusted_execution`
- `block_degraded_execution`

These are currently stage params, not top-level CLI flags on the main orchestrator parser.

## Feature controls

Current feature flags:
- `--full-rebuild`
- `--feature-tail-bars`

Operational default:
- incremental tail recompute

Research default:
- full rebuild behavior

## Ranking and sidecar controls

Current ranking and sidecar flags include:
- `--top-n`
- `--min-score`
- `--pattern-scan-enabled` or `--no-pattern-scan-enabled`
- `--pattern-max-symbols`
- `--pattern-workers`
- `--pattern-lookback-days`
- `--pattern-smoothing-method`
- `--pattern-timeout-seconds`
- `--breakout-engine`
- `--disable-breakout-legacy-families`
- `--breakout-market-bias-allowlist`
- `--breakout-min-breadth-score`
- `--breakout-sector-rs-min`
- `--breakout-sector-rs-percentile-min`
- `--breakout-qualified-min-score`
- `--breakout-symbol-near-high-max-pct`
- `--disable-breakout-symbol-trend-gate`
- `ml_mode` stage param, with `shadow_ml` as the only supported overlay mode today

## Execution safety controls

Current execution flags include:
- `--strategy-mode technical|ml|hybrid_confirm|hybrid_overlay`
- `--execution-top-n`
- `--execution-ml-horizon`
- `--execution-ml-confirm-threshold`
- `--execution-capital`
- `--execution-fixed-quantity`
- `--execution-regime`
- `--execution-regime-multiplier`
- `--paper-slippage-bps`
- `--execution-breakout-linkage off|soft_gate`

Current behavior:
- the stage always uses paper execution
- UI-triggered default pipeline runs do not include `execute`

## Publish configuration

Current publish flags include:
- `--local-publish`
- `--skip-quantstats`
- `--publish-quantstats` as a legacy alias
- `--quantstats-top-n`
- `--quantstats-min-overlap`
- `--quantstats-max-runs`
- `--quantstats-write-core-html`

Current semantics:
- `--local-publish` switches publish to the local summary only
- networked publish requires Google Sheets and Telegram configuration when those channels are enabled
- publish dedupe is based on `run_id + channel + artifact hash`

## Canary and local modes

Current canary behavior:
- `--canary` plus the untouched default stage string trims CLI and wrapper runs to `ingest,features,rank`
- `symbol_limit` defaults to `25` in canary mode unless explicitly set

If you want canary plus publish, pass the stage list explicitly:
```bash
python -m ai_trading_system.pipeline.orchestrator --canary --stages ingest,features,rank,publish --local-publish
```

Current local mode for operator verification:
```bash
python -m ai_trading_system.pipeline.orchestrator --skip-preflight --stages ingest,features,rank,publish --local-publish
```
