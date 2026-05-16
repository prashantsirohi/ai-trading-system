# Storage and Lineage

- **Purpose:** Catalog every persistent store the pipeline touches, the directory layout for artifacts, and how runs/stages/attempts/artifacts are tied together.
- **Audience:** Operators recovering from a failed run, engineers adding a new artifact, reviewers tracing data lineage.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/domains/execution/store.py:29`, `src/ai_trading_system/pipeline/registry.py:300`, `src/ai_trading_system/pipeline/migrations/001_pipeline_governance.sql` through `017_strategy_optimizer_rename.sql`, `grep -rn duckdb.connect src/`.

## DuckDB stores

| Store | Path | Owner / writer | Purpose |
|---|---|---|---|
| Operational OHLCV | `data/ohlcv.duckdb` | `domains/ingest/*`, `platform/db/paths.py:98` | Source-of-record price/volume data for the operational domain. |
| Control plane | `data/control_plane.duckdb` | `pipeline/registry.py:300`, orchestrator, pattern cache, fundamentals readmodel, UI services | Pipeline governance (`pipeline_run`, `pipeline_stage_run`, `pipeline_artifact`, `dq_rule`, `dq_result`), model registry, pattern cache, watchlist/event tables. |
| Execution ledger | `data/execution.duckdb` | `domains/execution/store.py:29` (default `project_root / "data" / "execution.duckdb"`) | `execution_order`, `execution_fill` tables written by the `execute` stage. |
| Research OHLCV | `data/research/research_ohlcv.duckdb` | `platform/db/paths.py:107-111` (research domain) | Isolated OHLCV when `DATA_DOMAIN=research`. |
| Master data | `data/masterdata.db` | `platform/db/paths.py:101` | Shared between operational and research domains (`paths.py:113`). |

### Notes on the execution store

The earlier audit truth map asserted that `execution_order` / `execution_fill` live inside `data/control_plane.duckdb`. The current code disagrees: `ExecutionStore` in `domains/execution/store.py:29` defaults to `data/execution.duckdb` and the `execute` stage instantiates it without overriding `db_path` (`pipeline/stages/execute.py:183`). A separate file is the today-truth. Verify on disk before relying on the older docs.

### Research-domain path resolution

`get_domain_paths` returns different layouts per domain (`platform/db/paths.py:94-118`):
- `operational` → flat `data/` layout (legacy-compatible).
- `research` → re-rooted under `data/research/` with its own `research_ohlcv.duckdb`, `feature_store/`, `pipeline_runs/`, `training_datasets/`; `models/` and `reports/` re-root under `models/research/` and `reports/research/`.

There is no `data/research.duckdb` referenced in `platform/db/paths.py`. If you see one on disk, it is legacy; the canonical research OHLCV path is `data/research/research_ohlcv.duckdb`.

## Feature store layout

```
data/feature_store/<symbol_id>/features_<start_date>_<end_date>.parquet
```

Columnar Parquet (RSI, MACD, Supertrend, ATR, EMA_20/50/200, VWAP, volume_ratio, swing_low_20, sector_rs, etc.). Written by the `features` stage; read by `rank`, `candidates`, and downstream readmodels.

## Pipeline run artifacts

Every stage attempt gets a deterministic directory:

```
data/pipeline_runs/<run_id>/<stage>/attempt_<n>/<artifact_file>
```

Examples:
- `data/pipeline_runs/<run_id>/ingest/attempt_1/ohlc.csv`
- `data/pipeline_runs/<run_id>/rank/attempt_1/ranked_signals.csv`
- `data/pipeline_runs/<run_id>/execute/attempt_2/executed_orders.csv`

The orchestrator creates the directory at the start of each attempt; stage wrappers write artifacts; the artifact registry records the URI and content hash.

## Run / stage / attempt semantics

Defined in `pipeline/migrations/001_pipeline_governance.sql` and refined by 002–005:

- **`pipeline_run`** — one row per orchestrator invocation. Holds run id, run date, data domain, started/finished timestamps, final status, and a JSON `metadata` blob (which carries the preflight result among other things).
- **`pipeline_stage_run`** — one row per *(run, stage, attempt)*. Records start/finish, status, retry reason, and links back to `pipeline_run.run_id`.
- **`pipeline_artifact`** — registry of every artifact: URI, content hash, producing stage attempt, optional schema/version. Downstream stages resolve inputs through this table so re-runs are idempotent.

Migration 002 hardens these tables post-refactor; 003 adds the preflight/alerts tables; 004 adds shadow-monitoring rows; 005 adds ML dataset registration.

## DQ persistence

- **`dq_rule`** — per-stage rule definitions (rule_id, severity, optional SQL).
- **`dq_result`** — one row per *(run, stage, rule, attempt)* outcome, with status, failed_count, message, band (`green | amber | red_repairable | red_block`), and `relaxed_from` when downgraded.

See [data_trust_and_dq.md](./data_trust_and_dq.md).

## Model registry & adjacent tables

Migrations 005–007 introduce `model_registry`, monitoring tables, and guardrails. Migration 015–017 add strategy-optimizer tables (then rename them). Migration 011 adds pattern cache (read/written via `domains/ranking/patterns/cache.py`). Migration 016 adds universe-index tables.

## Artifact registry contract

`pipeline_artifact` rows are the only authoritative way to discover what an attempt produced. Filesystem listing under `data/pipeline_runs/...` is convenient but not guaranteed to be complete (e.g., quarantined attempts may leave partial files). Always join through `pipeline_artifact` when reading lineage.

## Snapshot model

- **OHLCV snapshot** — the latest validated trade date per exchange is captured inside `_catalog` / `_catalog_quarantine` (`domains/ingest/trust.py`), exposed via `load_data_trust_summary`.
- **Feature snapshot** — `(symbol_id, start_date, end_date)` triple is encoded in the Parquet filename; older snapshots are not deleted automatically.
- **Rank snapshot** — captured via the rank stage's attempt directory; the UI's "latest operational snapshot" readmodel (`ui/execution_api/services/readmodels/latest_operational_snapshot.py:56`) resolves it from `control_plane.duckdb`.

## Backup

See [../runbooks/backup_and_restore.md](../runbooks/backup_and_restore.md) (when present) for backup cadence and restore procedure. At minimum, treat `data/ohlcv.duckdb`, `data/control_plane.duckdb`, `data/execution.duckdb`, and `data/feature_store/` as the must-back-up set; `data/pipeline_runs/` is reproducible from those plus the SQL migrations.
