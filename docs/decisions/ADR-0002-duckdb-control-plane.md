# ADR-0002: DuckDB as Control Plane

- **Purpose:** Record the decision to use embedded DuckDB files for operational, control-plane, and execution storage rather than a server-based RDBMS.
- **Audience:** Developer, future agents.
- **Last verified:** 2026-05-16
- **Source of truth:** Code paths cited inline (file references in the Decision section) + [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md).
- **Status:** Accepted (historical).

---

## Context

The system needs persistent storage for:

- OHLCV history (millions of rows)
- Pipeline run / stage / attempt governance
- Artifact registry
- DQ rule + result history
- Model registry (ML overlay)
- Execution orders + fills
- Forward-return tracking (perf tracker)

Constraints:

- Runs on a single Mac mini (16 GB RAM)
- Single-operator deployment
- No DevOps overhead budget for Postgres/MySQL hosting
- Workload is read-heavy, batch-write (once per stage), analytical queries from the UI

Alternatives:

1. **Postgres/MySQL** — server-based, robust, but requires hosting + backups + auth + connection pooling. Overkill.
2. **SQLite** — embedded but slow for analytical queries; weaker SQL.
3. **DuckDB** — embedded, columnar, fast for analytics, no daemon, single-file backup, supports complex SQL + Parquet/CSV.

## Decision

Use **DuckDB**. Multiple files split by ownership:

- `data/ohlcv.duckdb` — OHLCV (write owner: ingest)
- `data/control_plane.duckdb` — pipeline governance, DQ, model registry, pattern cache, watchlist tables
- `data/execution.duckdb` — `execution_order`, `execution_fill`, etc. ([`execution/store.py:29`](../../src/ai_trading_system/domains/execution/store.py))
- `data/research.duckdb` — perf tracker
- `data/research_ohlcv.duckdb` — research-domain OHLCV isolation (selected by `DATA_DOMAIN=research`)
- `data/market_intel.duckdb` — read-only consumer of always-on market_intel runner

Paths are resolved through [`platform/db/paths.py`](../../src/ai_trading_system/platform/db/paths.py) so consumers don't hardcode them.

## Consequences

**Positive:**
- Zero deployment overhead: no daemon, no auth, no port.
- Single-file backup per store.
- Columnar query performance is excellent for the analytical workload.
- Schema lives in SQL migrations (`pipeline/migrations/*.sql`) — simple, auditable.
- `DATA_DOMAIN=research` swap is a one-line config change.

**Negative:**
- Single-writer per file. Concurrent writes from multiple processes can fail. Mitigation: stages are single-process; readers use `read_only=True`.
- No row-level locking. Mitigation: use DELETE+INSERT-keyed-on-PK patterns for upserts (see `perf_tracker/backfill.py`).
- DuckDB version upgrades require careful testing — the on-disk format can change.
- No multi-region replication. Mitigation: rsync `data/*.duckdb` to backup target.

## See also

- [`docs/architecture/storage_and_lineage.md`](../architecture/storage_and_lineage.md)
- [`docs/reference/database_schema.md`](../reference/database_schema.md)
- [`docs/runbooks/backup_and_restore.md`](../runbooks/backup_and_restore.md)
