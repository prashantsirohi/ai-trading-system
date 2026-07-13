# Backup and Restore

- **Purpose:** Back up and restore stateful runtime stores resolved through `DATA_ROOT`.
- **Audience:** Operators preparing a repair, migration, or recovery.
- **Last verified:** 2026-07-13
- **Source of truth:** [storage and lineage](../architecture/storage_and_lineage.md) and `src/ai_trading_system/platform/db/paths.py`.

---

## Safety contract

Stop every pipeline, API, collector, and research process using the affected stores before copying DuckDB files. A filesystem copy of an actively written DuckDB file is not a valid backup. Do not terminate an unknown lock owner; coordinate a maintenance window.

Load the operator environment and require the configured root:

```bash
set -a
source .env
set +a
: "${DATA_ROOT:?DATA_ROOT must be configured}"
```

Never substitute a repo-local `data/` tree for an unavailable configured root.

## Stateful content

| Resolved path | Owner/content | Backup policy |
|---|---|---|
| `$DATA_ROOT/ohlcv.duckdb` | operational market, trust, provenance, and feature metadata | required |
| `$DATA_ROOT/control_plane.duckdb` | runs, attempts, artifacts, DQ, alerts, models, and operator state | required |
| `$DATA_ROOT/execution.duckdb` | orders, fills, stops, positions, and drawdown ledger | required when execution is used |
| `$DATA_ROOT/candidate_tracker.duckdb` | candidate lifecycle state | required when tracker is used |
| `$DATA_ROOT/masterdata.db` | shared instrument identity | required |
| `$DATA_ROOT/fundamentals/` | imported fundamental snapshots/read models | required when used |
| `$DATA_ROOT/feature_store/` and `$DATA_ROOT/stage_store/` | durable feature/stage materializations | required for fast exact recovery |
| `$DATA_ROOT/research/` and research stores | isolated research state | required when research is used |
| `$DATA_ROOT/pipeline_runs/` | attempt artifacts referenced by registry rows | retain for lineage/audit and retry recovery |
| `$DATA_ROOT/cache/`, `$DATA_ROOT/exports/` | disposable cache and explicit exports | optional |

Back up control-plane registry rows and referenced `pipeline_runs` artifacts together. Restoring only one side leaves broken lineage.

## Backup

Choose a destination outside `DATA_ROOT`, verify free space, then copy the stopped runtime tree:

```bash
BACKUP_PARENT="${BACKUP_ROOT:-$PWD/backups}"
DEST="$BACKUP_PARENT/$(date +%Y-%m-%d_%H%M%S)"
mkdir -p "$DEST"
du -sh "$DATA_ROOT"
df -h "$BACKUP_PARENT"
rsync -a "$DATA_ROOT/" "$DEST/data/"
```

Record hashes for database files and probe each DuckDB copy read-only:

```bash
: > "$DEST/database.sha256"
find "$DEST/data" -type f \( -name '*.duckdb' -o -name '*.db' \) -print \
  | while IFS= read -r path; do shasum -a 256 "$path"; done \
  >> "$DEST/database.sha256"

BACKUP_DATA_ROOT="$DEST/data" ./.venv/bin/python - <<'PY'
import os
from pathlib import Path
import duckdb

root = Path(os.environ["BACKUP_DATA_ROOT"])
for path in sorted(root.rglob("*.duckdb")):
    connection = duckdb.connect(str(path), read_only=True)
    try:
        connection.execute("SELECT 1").fetchone()
    finally:
        connection.close()
    print(path)
PY
```

Preserve the manifest with the backup and apply the operator retention/offsite policy.

## Restore rehearsal or recovery

Restore into a new directory first; never overwrite the active root in place:

```bash
SRC='<chosen-backup>/data'
RESTORE_ROOT="${TMPDIR:-/tmp}/ai-trading-restore-$(date +%s)"
mkdir -p "$RESTORE_ROOT"
rsync -a "$SRC/" "$RESTORE_ROOT/"
```

Verify the stored hashes from the backup directory and repeat the read-only DuckDB probes against `RESTORE_ROOT`. Then run the [copied-data canary](copied_data_canary.md) with the restored directory as its source or temporary runtime root.

For an actual replacement:

1. keep all live writers stopped;
2. take one final backup of the current root;
3. rename the current root to a timestamped quarantine path on the same filesystem;
4. place the verified restored tree at the configured `DATA_ROOT`;
5. run bootstrap only to create missing directories, never to synthesize market data;
6. run read-only health probes, then the operator-approved canary;
7. resume services one at a time and monitor lock, DQ, and path diagnostics;
8. retain the quarantined prior root until recovery acceptance is complete.

If any store fails to open, schema/migration compatibility fails, artifact paths resolve outside the restored root, or DQ blocks, stop and restore the quarantined prior root. Database repair or migration requires its own explicit authorization and backup.
