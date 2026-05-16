# Platform Domain

- **Purpose:** Cross-cutting foundations: config, database paths, logging, utilities.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/platform/`](../../src/ai_trading_system/platform/)

---

## Responsibility

Be the **boring foundation**. Other domains depend on `platform/` but `platform/` depends on nothing project-specific. Changes here ripple — keep the surface area small.

## Package / module ownership

| Module | Role |
|---|---|
| `config/settings.py` | Pydantic `AppConfig` (data, Dhan, collector, features, signals, backtest, risk, AI, execution). |
| `config/rank_factor_weights.json` | Rank stage factor weights. |
| `config/events_filters.json` | Event materiality filters. |
| `config/research_recipes.toml` | Research recipe definitions. |
| `db/paths.py::get_domain_paths` / `resolve_data_domain` | Resolve operational vs research DuckDB paths from `DATA_DOMAIN` env var. |
| `logging/` | Centralized logger config; sets `MPLCONFIGDIR` if unset. |
| `utils/` | Bootstrap helpers, env loading, data config. |

## Public contracts

- `get_domain_paths(...)` returns a `DomainPaths` with `root_dir`, `ohlcv_db_path`, etc. **Every** module that touches DuckDB uses this — do not hardcode `data/*.duckdb` paths.
- `AppConfig` is constructed once at startup; mutate via env vars, not code.
- Env vars consumed: `DATA_DOMAIN`, `ENV`, `MPLCONFIGDIR`, plus all Dhan/Telegram/Google/LLM keys (see [`reference/environment_variables.md`](../reference/environment_variables.md)).

## Storage ownership

None directly. Owns the **paths** to DuckDBs but not their contents.

## Dependencies

- External: Pydantic, python-dotenv.
- Internal: none (this is the floor).

## Extension points

- New config field: add to `AppConfig` with a sensible default; document in [`reference/configuration.md`](../reference/configuration.md).
- New domain (operational/research/...): extend `db/paths.py::DATA_DOMAINS` if needed.

## Known gaps

- Some legacy modules still import config via `os.getenv(...)` directly rather than through `AppConfig` — flagged in stale-reference report.

## See also

- [`docs/reference/configuration.md`](../reference/configuration.md)
- [`docs/reference/environment_variables.md`](../reference/environment_variables.md)
