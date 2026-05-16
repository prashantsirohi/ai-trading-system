# Environment Variables

- **Purpose:** All env vars actually read by the code, with source module and default.
- **Audience:** Operator, developer.
- **Last verified:** 2026-05-16
- **Source of truth:** Grep of `os.environ`, `os.getenv`, `getenv(`, and pydantic `Settings` in `src/`. Cited modules per row.


---

## Method

Sourced by grepping `os.environ`, `os.getenv`, `getenv(`, and pydantic `Settings` in `src/`. See truth map §9.

## Variables

| Name | Required | Used by | Default | Notes |
|---|---|---|---|---|
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
| `GOOGLE_SPREADSHEET_ID` | publish | `publish/channels/google_sheets.py` | — | |
| `GOOGLE_SHEETS_CREDENTIALS` | deprecated | `google_sheets_manager.py` | — | replaced by OAuth flow |
| `GOOGLE_TOKEN_PATH` | publish | `google_sheets_manager.py` | — | path to cached OAuth token |
| `ALERT_TELEGRAM_MIN_SEVERITY` | optional | `pipeline/alerts.py` | — | e.g. `warning` |
| `RISK_PROFILE` | optional | execute stage | — | profile name |
| `LLM_BRAIN_CONFIG` | optional | `events/event_llm_router.py` | `config/llm_brain.yaml` | override path |
| `OPENROUTER_KEY` / `OPENROUTER_API_KEY` | LLM features | `event_llm_router.py` | — | secret |
| `DATA_DOMAIN` | optional | `platform/db/paths.py` | `operational` | `operational` or `research` |
| `ENV` | optional | `pipeline/daily_pipeline.py` | — | label only |
| `MPLCONFIGDIR` | optional | `platform/logging/` | auto-set | matplotlib cache |
| `EXECUTION_API_KEY` | required for `/api/*` | [`ui/execution_api/routes/_deps.py:31`](../../src/ai_trading_system/ui/execution_api/routes/_deps.py) | — | **secret**. Blank/unset → every `/api/*` returns HTTP 500. Client header: `x-api-key`. |

## EXECUTION_MODE

Not an explicit env var. Inferred from Dhan credential presence. Verify in execute stage before stating otherwise.

