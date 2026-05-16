# Backup and Restore

- **Purpose:** Back up and restore the operational data stores. `data/pipeline_runs/` is reproducible — only the stateful stores need backup.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md) section 4 (Storage), [`docs/architecture/storage_and_lineage.md`](../architecture/storage_and_lineage.md).

---

## What to back up

| Path | Purpose | Back up? |
|---|---|---|
| `data/ohlcv.duckdb` | Operational OHLCV catalog | Yes |
| `data/control_plane.duckdb` | Pipeline governance (runs, stages, artifacts, DQ, model registry, watchlist/event tables, delivery log) | Yes |
| `data/execution.duckdb` | `execution_order`, `execution_fill`, drawdown snapshots | Yes |
| `data/research.duckdb` | `rank_cohort_performance` (perf_tracker) | Yes |
| `data/research_ohlcv.duckdb` | Research-domain OHLCV isolation | Yes if research is in use |
| `data/feature_store/` | Per-symbol parquet feature files | Yes |
| `data/masterdata.db` | Symbol master | Yes |
| `data/market_intel.duckdb` | Populated by external runner; read-only consumer | Skip — rebuilt by the runner |
| `data/pipeline_runs/` | Per-run artifacts | Reproducible; back up only for audit |

## Backup procedure

Stop the pipeline before backing up DuckDB files to avoid copying mid-write state. The simplest safe path is to back up between runs.

```bash
DEST=backups/$(date +%Y-%m-%d_%H%M)
mkdir -p "$DEST"
cp data/ohlcv.duckdb         "$DEST/"
cp data/control_plane.duckdb "$DEST/"
cp data/execution.duckdb     "$DEST/"
cp data/research.duckdb      "$DEST/"
cp data/research_ohlcv.duckdb "$DEST/" 2>/dev/null || true
cp data/masterdata.db        "$DEST/"
cp -R data/feature_store     "$DEST/feature_store"
```

### Verify backup integrity

```bash
for f in "$DEST"/*.duckdb; do
  echo "--- $f"
  duckdb "$f" "SELECT 1;"
done
du -sh "$DEST"
```

### Offsite / rotation

Compress and move offsite per your retention policy:

```bash
tar -czf "$DEST.tgz" -C backups "$(basename "$DEST")"
```

> Current code status: no in-repo backup CLI is documented. Use the shell commands above as a starting point and adapt to your environment.

## Restore procedure

1. Stop any pipeline, API, or React console processes touching the data directory.
2. Move the current `data/` aside (do not delete until restore is verified):
   ```bash
   mv data data.broken.$(date +%s)
   mkdir data
   ```
3. Copy backed-up files into place:
   ```bash
   SRC=backups/<chosen-backup>
   cp "$SRC"/ohlcv.duckdb         data/
   cp "$SRC"/control_plane.duckdb data/
   cp "$SRC"/execution.duckdb     data/
   cp "$SRC"/research.duckdb      data/
   cp "$SRC"/research_ohlcv.duckdb data/ 2>/dev/null || true
   cp "$SRC"/masterdata.db        data/
   cp -R "$SRC"/feature_store     data/feature_store
   ```
4. Re-create runtime directories:
   ```bash
   python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
   ```
5. Spot-check:
   ```bash
   duckdb data/ohlcv.duckdb "SELECT MAX(timestamp), COUNT(*) FROM _catalog;"
   duckdb data/control_plane.duckdb "SELECT COUNT(*) FROM pipeline_run;"
   duckdb data/execution.duckdb "SELECT COUNT(*) FROM execution_order;"
   ```
6. Run a safe canary:
   ```bash
   python -m ai_trading_system.pipeline.orchestrator --canary --skip-preflight --stages ingest,features,rank,publish --local-publish
   ```
7. After verification, delete `data.broken.*`.

## What `data/pipeline_runs/` contains

It is the historical log of per-run, per-stage artifacts. The pipeline can produce future runs without it. Treat it as audit history, not state. If you need to rerun a specific historical `run_id` (e.g., publish retry), `pipeline_runs/<run_id>/` must still exist — back it up only if you anticipate that need.

## Notes

- `data/market_intel.duckdb` is owned by an external always-on runner; restoring stale data here will be overwritten.
- `pipeline_artifact` rows in `control_plane.duckdb` reference paths under `data/pipeline_runs/`. If you restore one but not the other, artifact lookups may return paths that no longer exist on disk.
