# Copied-Data Canary

- **Purpose:** Run a production-shaped canary against disposable copies of runtime stores without mutating the operator's live `DATA_ROOT`.
- **Audience:** Operators validating pipeline changes in a maintenance window.
- **Last verified:** 2026-07-13
- **Source of truth:** `src/ai_trading_system/platform/db/paths.py`, `src/ai_trading_system/pipeline/orchestrator.py`, and `docs/architecture/storage_and_lineage.md`.

---

## Safety contract

This procedure deliberately mutates the copied stores. It must never target the live runtime root. It does not enable live execution: the canary stage list excludes `execute`, `--local-publish` disables network publishers, and current pipeline execution is paper-only.

Use a maintenance window because copying an active DuckDB file is not a consistent backup. Coordinate with the owner of every pipeline, API, collector, and research process before stopping it. Do not terminate an unknown process merely to clear a lock.

Abort when:

- any writer still owns a source store;
- `DATA_ROOT` is unset, unavailable, or resolves inside the repository;
- the disposable root resolves to the source root;
- free space is less than the selected source content plus working headroom;
- a source database cannot be opened read-only after writers stop;
- a copied database checksum or read probe differs/fails;
- preflight, trust, or DQ blocks the canary.

## 1. Establish the maintenance window

From the repository root, load the operator environment without printing it:

```bash
set -a
source .env
set +a
: "${DATA_ROOT:?DATA_ROOT must be configured}"
PROJECT_ROOT="$PWD"
```

Stop the scheduled pipeline, execution API, React development server, collectors, and any research process that uses the same stores through their normal service controls. Inspect remaining candidates:

```bash
ps -Ao pid,command | rg 'ai_trading_system|daily_update_runner|openclaw|duckdb'
```

Every matching process must be understood. If a source DuckDB still reports a conflicting lock, abort and reschedule.

## 2. Allocate an isolated root and record source hashes

```bash
CANARY_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/ai-trading-canary.XXXXXX")"
CANARY_DATA_ROOT="$CANARY_ROOT/data"
mkdir -p "$CANARY_DATA_ROOT" "$CANARY_ROOT/reports" "$CANARY_ROOT/logs" "$CANARY_ROOT/models"
test "$(cd "$DATA_ROOT" && pwd -P)" != "$(cd "$CANARY_DATA_ROOT" && pwd -P)"
du -sh "$DATA_ROOT"
df -h "$CANARY_ROOT"
```

Record hashes for stateful database files that exist:

```bash
: > "$CANARY_ROOT/source-before.sha256"
for name in ohlcv.duckdb control_plane.duckdb execution.duckdb candidate_tracker.duckdb masterdata.db research.duckdb; do
  if test -f "$DATA_ROOT/$name"; then
    shasum -a 256 "$DATA_ROOT/$name" >> "$CANARY_ROOT/source-before.sha256"
  fi
done
```

## 3. Copy runtime inputs

Copy the configured runtime tree while excluding reproducible attempts and disposable caches:

```bash
rsync -a \
  --exclude 'pipeline_runs/' \
  --exclude 'cache/' \
  --exclude 'exports/' \
  "$DATA_ROOT/" "$CANARY_DATA_ROOT/"

MODEL_SOURCE="${MODELS_ROOT:-$PROJECT_ROOT/models}"
if test -d "$MODEL_SOURCE"; then
  rsync -a "$MODEL_SOURCE/" "$CANARY_ROOT/models/"
fi
```

Verify each copied database matches its source and can be opened read-only:

```bash
for name in ohlcv.duckdb control_plane.duckdb execution.duckdb candidate_tracker.duckdb masterdata.db research.duckdb; do
  if test -f "$DATA_ROOT/$name"; then
    test "$(shasum -a 256 "$DATA_ROOT/$name" | cut -d' ' -f1)" = \
         "$(shasum -a 256 "$CANARY_DATA_ROOT/$name" | cut -d' ' -f1)"
  fi
done

CANARY_DATA_ROOT="$CANARY_DATA_ROOT" ./.venv/bin/python - <<'PY'
import os
from pathlib import Path
import duckdb

root = Path(os.environ["CANARY_DATA_ROOT"])
for name in ("ohlcv.duckdb", "control_plane.duckdb", "execution.duckdb", "candidate_tracker.duckdb", "research.duckdb"):
    path = root / name
    if path.exists():
        connection = duckdb.connect(str(path), read_only=True)
        try:
            connection.execute("SELECT 1").fetchone()
        finally:
            connection.close()
PY
```

## 4. Run only against the copy

Keep every writable root below `CANARY_ROOT` and run the explicit non-execution stage list:

```bash
env \
  DATA_ROOT="$CANARY_DATA_ROOT" \
  REPORTS_ROOT="$CANARY_ROOT/reports" \
  LOGS_ROOT="$CANARY_ROOT/logs" \
  MODELS_ROOT="$CANARY_ROOT/models" \
  PYTHONPATH=src \
  ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
    --new-run \
    --stages ingest,features,rank,publish \
    --data-domain operational \
    --canary \
    --symbol-limit 25 \
    --local-publish \
    --run-preflight
```

Do not add `execute`, remove `--local-publish`, enable synthetic smoke data, or point any root back to the live environment.

## 5. Validate results

Record the emitted `run_id`, then inspect only the copied control plane and artifacts:

```bash
CANARY_RUN_ID='<run_id-from-command>'
CANARY_RUN_ID="$CANARY_RUN_ID" CANARY_DATA_ROOT="$CANARY_DATA_ROOT" ./.venv/bin/python - <<'PY'
import os
from pathlib import Path
import duckdb

run_id = os.environ["CANARY_RUN_ID"]
db_path = Path(os.environ["CANARY_DATA_ROOT"]) / "control_plane.duckdb"
connection = duckdb.connect(str(db_path), read_only=True)
try:
    queries = (
        "SELECT run_id, status, current_stage, error_class, error_message FROM pipeline_run WHERE run_id = ?",
        "SELECT stage_name, attempt_number, status FROM pipeline_stage_run WHERE run_id = ? ORDER BY started_at",
        "SELECT stage_name, artifact_type, row_count FROM pipeline_artifact WHERE run_id = ? ORDER BY created_at",
    )
    for query in queries:
        for row in connection.execute(query, [run_id]).fetchall():
            print(row)
finally:
    connection.close()
PY
find "$CANARY_DATA_ROOT/pipeline_runs/$CANARY_RUN_ID" -type f -print
```

Acceptance requires completed expected stages, no critical DQ failure, ranked output for the bounded live universe, local publish artifacts, and no broker/network publisher activity. Preserve the terminal output and copied run summary when investigating a failure.

Finally prove the source database files were unchanged during the canary:

```bash
shasum -a 256 -c "$CANARY_ROOT/source-before.sha256"
```

Any mismatch invalidates the isolation claim; stop and investigate before resuming writers.

## 6. Resume or clean up

Restart normal services only after the source hashes pass and no canary process remains. Keep the disposable root while diagnosing failures. When it is no longer needed, use this guarded cleanup:

```bash
case "$CANARY_ROOT" in
  "${TMPDIR:-/tmp}"/ai-trading-canary.*) rm -rf -- "$CANARY_ROOT" ;;
  *) echo "Refusing to remove unexpected path: $CANARY_ROOT"; exit 1 ;;
esac
```

This runbook does not replace [backup and restore](backup_and_restore.md). Take the required backup before any live-store repair or migration.
