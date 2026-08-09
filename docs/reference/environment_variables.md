# Environment Variables

- **Purpose:** All env vars actually read by the code, with source module and default.
- **Audience:** Operator, developer.
- **Last verified:** 2026-08-08
- **Source of truth:** Grep of `os.environ`, `os.getenv`, `getenv(`, and pydantic `Settings` in `src/`. Cited modules per row.


---

## Method

Sourced by grepping `os.environ`, `os.getenv`, `getenv(`, and pydantic `Settings` in `src/`. See truth map §9.

## Path roots

Resolved in `platform/db/paths.py`. The overrides are honored only when the
resolved project root looks like the repo checkout; otherwise the repo-relative
defaults are used.

| Name | Required | Used by | Default | Notes |
|---|---|---|---|---|
| `DATA_ROOT` | optional | [`platform/db/paths.py:213`](../../src/ai_trading_system/platform/db/paths.py) | `<repo>/data` | Relocates the whole runtime data tree, including `masterdata.db`. If set to a missing path (e.g. unmounted SSD), `require_data_root_available` raises. |
| `REPORTS_ROOT` | optional | same, `:214` | `<repo>/reports` | |
| `LOGS_ROOT` | optional | same, `:215` | `<repo>/logs` | |
| `MODELS_ROOT` | optional | same, `:216` | `<repo>/models` | |
| `AI_TRADING_PROJECT_ROOT` | optional | `interfaces/cli/*.py`, `domains/ingest/*.py`, [`ui/execution_api/routes/_deps.py:25`](../../src/ai_trading_system/ui/execution_api/routes/_deps.py) | CWD / packaged default | Normalized by `canonicalize_project_root`. |

## Variables

| Name | Required | Used by | Default | Notes |
|---|---|---|---|---|
| `TRADE_JOURNAL_SAMPLE_TRADEBOOK` | no | opt-in journal characterization test | — | Local path only; never committed. |
| `TRADE_JOURNAL_SAMPLE_HOLDINGS` | no | opt-in journal characterization test | — | Local path only; never committed. |
| `DHAN_API_KEY` | live trading | `domains/ingest/providers/dhan.py` | — | secret |
| `DHAN_CLIENT_ID` | live trading | same | — | |
| `DHAN_ACCESS_TOKEN` | live trading | same | — | secret |
| `DHAN_REFRESH_TOKEN` | live trading | `token_manager.py` | — | secret |
| `DHAN_PIN` | live trading | `token_manager.py` | — | secret |
| `DHAN_TOTP` | live trading | `token_manager.py` | — | secret |
| `DHAN_TOKEN_EXPIRY` | optional | `token_manager.py` | — | cached |
| `TELEGRAM_BOT_TOKEN` | publish | `publish/channels/telegram.py` | — | secret |
| `TELEGRAM_CHAT_ID` | publish | same | — | |
| `TELEGRAM_CONNECT_TIMEOUT_SECONDS` | optional | same | 5.0 | |
| `TELEGRAM_READ_TIMEOUT_SECONDS` | optional | same | 10.0 | |
| `TELEGRAM_WRITE_TIMEOUT_SECONDS` | optional | same | 10.0 | |
| `TELEGRAM_SEND_ATTEMPTS` | optional | same | — | |
| `TELEGRAM_POOL_TIMEOUT_SECONDS` | optional | [`platform/utils/runtime_config.py:64`](../../src/ai_trading_system/platform/utils/runtime_config.py) | 2.0 | |
| `TELEGRAM_DNS_PRECHECK_ENABLED` | optional | same, `:66` | `true` | Boolean; anything but `0`/`false`/`no`/`off`/empty is true. |
| `GOOGLE_SPREADSHEET_ID` | publish | `publish/channels/google_sheets.py` | — | |
| `GOOGLE_SHEETS_CREDENTIALS` | deprecated | `google_sheets_manager.py` | — | replaced by OAuth flow |
| `GOOGLE_TOKEN_PATH` | publish | `google_sheets_manager.py` | — | path to cached OAuth token |
| `GOOGLE_SHEETS_MAX_RETRIES` | optional | [`google_sheets_manager.py:149`](../../src/ai_trading_system/domains/publish/channels/google_sheets_manager.py) | 5 | Clamped to ≥ 0. |
| `GOOGLE_SHEETS_MAX_BACKOFF_SECONDS` | optional | same, `:150` | 64.0 | Clamped to ≥ 1.0. |
| `GOOGLE_SHEETS_WRITE_INTERVAL_SECONDS` | optional | same, `:151` | 1.2 | Clamped to ≥ 0.0. Quota pacing between writes. |
| `ALERT_TELEGRAM_MIN_SEVERITY` | optional | `pipeline/alerts.py` | — | e.g. `warning` |
| `RISK_PROFILE` | optional | execute stage | — | profile name |
| `LLM_BRAIN_CONFIG` | optional | `events/event_llm_router.py` | `config/llm_brain.yaml` | override path |
| `OPENROUTER_KEY` / `OPENROUTER_API_KEY` | LLM features | `event_llm_router.py` | — | secret |
| `DATA_DOMAIN` | optional | `platform/db/paths.py` | `operational` | `operational` or `research` |
| `ENV` | optional | `pipeline/daily_pipeline.py` | — | label only |
| `MPLCONFIGDIR` | optional | `platform/logging/` | auto-set | matplotlib cache |
| `EXECUTION_API_KEY` | optional on loopback; required for non-loopback binds | [`ui/execution_api/routes/_deps.py:31`](../../src/ai_trading_system/ui/execution_api/routes/_deps.py) | internal local proxy handshake | **secret for deployments**. The CLI supplies an internal development handshake only when bound to loopback; non-loopback startup fails closed if blank. Client header: `x-api-key`. |
| `SCREENER_USERNAME` | fundamentals import | [`domains/fundamentals/screener_client.py:30`](../../src/ai_trading_system/domains/fundamentals/screener_client.py) | — | Overridable by constructor argument. |
| `SCREENER_PASSWORD` | fundamentals import | same, `:31` | — | secret |

## Phase 4A read-only API

All read by `ApiSettings.from_env` in
[`interfaces/api/config.py:53-75`](../../src/ai_trading_system/interfaces/api/config.py).
This is the separate Phase 4A read-only API (`ai-trading-phase4-api`), not the
`ui/execution_api` operator API above — the two do not share configuration.
See [phase4a read-only API](../runbooks/phase4a_read_only_api.md).

| Name | Required | Default | Notes |
|---|---|---|---|
| `PHASE4_API_KEY` | when auth enabled | — | **secret**. Bearer token; compared in constant time and never logged. |
| `PHASE4_API_AUTH_ENABLED` | optional | `true` | Boolean. |
| `PHASE4_API_LOCAL_DEV_MODE` | optional | `false` | Boolean. |
| `PHASE4_API_SOURCE_PROFILE` | optional | `operator_read_only` | Lowercased into `SourceProfile`; `small_fixture` resolves no control plane. |
| `PHASE4_API_COPIED_CONTROL_PLANE` | optional | — | Path to a copied `control_plane.duckdb`. |
| `PHASE4_API_ARTIFACT_ROOT` | optional | — | Path to the artifact tree served for downloads. |
| `PHASE4_API_HOST` | optional | `127.0.0.1` | |
| `PHASE4_API_PORT` | optional | `8765` | |
| `PHASE4_API_DEFAULT_PAGE_SIZE` | optional | `50` | |
| `PHASE4_API_MAX_PAGE_SIZE` | optional | `500` | |
| `PHASE4_API_MAX_RESPONSE_ROWS` | optional | `500` | |
| `PHASE4_API_RATE_LIMIT_PER_MINUTE` | optional | `120` | |
| `PHASE4_API_CACHE_ENABLED` | optional | `false` | Boolean. |
| `PHASE4_API_CACHE_TTL_SECONDS` | optional | `30` | |
| `PHASE4_API_INCLUDE_OPENAPI` | optional | `true` | Boolean. |
| `PHASE4_API_CORS_ALLOWED_ORIGINS` | optional | `http://127.0.0.1:5173,http://localhost:5173` | Comma-separated. |

## EXECUTION_MODE

Not an explicit env var. Inferred from Dhan credential presence. Verify in execute stage before stating otherwise.
