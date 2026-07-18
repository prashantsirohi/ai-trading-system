# Commands

- **Purpose:** Authoritative runnable command and console-entrypoint reference.
- **Audience:** Operators and developers.
- **Last verified:** 2026-07-17
- **Source of truth:** `pyproject.toml [project.scripts]` and the referenced CLI parsers.

---

Start with the common workflows in the [System Guide](../SYSTEM_GUIDE.md). Commands below are run from the repository root unless they explicitly change directories.

## Environment

```bash
set -a
source .env
set +a
```

Use the virtual-environment interpreter. `PYTHONPATH=src` permits module execution without relying on an editable installation:

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
./.venv/bin/python -m pip install -e .
```

## Bootstrap and health

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data --refresh-masterdata
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.healthcheck
```

## Operational pipeline

Default operational run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --data-domain operational
```

Run readiness checks before stages:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --run-preflight
```

Reduced real-data canary with local publishing:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --canary --symbol-limit 25 --local-publish
```

Daily wrapper:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.daily_pipeline
```

## Stage selection and retry

The `features` alias expands to all feature substages. Explicit stage lists do not automatically add omitted upstream dependencies.

```bash
# One new ingest attempt.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --stages ingest

# Full feature expansion.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --stages features

# Retry publish against registered artifacts for an existing run.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages publish

# Force a new attempt for an already completed requested stage.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages rank --force-rerun

# Bypass same-date auto-resume and create a fresh run.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator --new-run
```

The default CLI stage string includes `fundamentals` and `candidate_tracker` but omits `weekly_stage`, `scan_router`, `opportunities`, and `narrative`. `--opportunity-scan-routing-mode compare|shadow` inserts Phase 3B after rank. `--opportunity-registry-mode shadow` inserts Phase 3A after Investigator. Existing execution and publish consumers are unchanged.

Opportunity shadow run and isolated retry:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-registry-mode shadow

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages opportunities \
  --opportunity-registry-mode shadow --opportunity-registry-dry-run
```

Phase 3B comparison and full shadow:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-scan-routing-mode compare --local-publish

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-registry-mode shadow \
  --opportunity-scan-routing-mode shadow --local-publish
```

Phase 3C-1 legacy annotation is restricted to a copied control plane. Preview is
read-only; apply initializes additive migrations on the copy and appends only
governance overlays:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.annotate_phase3c1_governance \
  --copied-control-plane /path/to/copied-control_plane.duckdb

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.annotate_phase3c1_governance \
  --copied-control-plane /path/to/copied-control_plane.duckdb \
  --run-id phase3c1-copied-validation --apply --confirm-copied-store
```

Phase 3C-4 deterministic performance benchmarks write only to the explicit
temporary output root. Cold means fresh application objects/connections, not OS
cache deletion. Warm reuses immutable fixture inputs in one process:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.benchmark_phase3c4 \
  --profile small_fixture --cache-mode cold --repetitions 2 \
  --as-of YYYY-MM-DD --output-root /tmp/phase3c4-small-cold

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.benchmark_phase3c4 \
  --profile small_fixture --cache-mode warm --repetitions 3 \
  --as-of YYYY-MM-DD --output-root /tmp/phase3c4-small-warm
```

`copied_realistic` additionally requires `--copied-control-plane` and opens it
read-only. Threshold failures remain advisory unless `--fail-on-threshold` is
explicitly supplied. See the [runbook](../runbooks/phase3c4_performance_benchmark.md).

Phase 3C-5 builds immutable calibration and readiness evidence beneath an
explicit temporary output root:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.build_phase3c5_calibration \
  --profile small_fixture --as-of YYYY-MM-DD \
  --output-root /tmp/phase3c5-small

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.check_phase4_readiness \
  --calibration-manifest /tmp/phase3c5-small/phase3c5_calibration_manifest.json \
  --output-root /tmp/phase3c5-readiness
```

For copied-realistic evidence, add `--profile copied_realistic
--copied-control-plane /path/to/temporary/control_plane.duckdb`. Never supply
the configured operator store. These commands do not apply migrations,
calibrate thresholds, or implement Phase 4. See the
[runbook](../runbooks/phase3c5_calibration_and_readiness.md).

## Publish and recovery

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.publish_test

# Dry-run ingest repair.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD

# Apply only after backup and explicit approval.
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.ingest.reset_reingest_validate \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD --apply
```

See [data repair](../runbooks/data_repair.md), [publish retry](../runbooks/publish_retry.md), and [backup and restore](../runbooks/backup_and_restore.md).

## API and operator console

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.ui.execution_api.app --port 8090
```

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
VITE_PHASE4_API_BASE_URL=http://127.0.0.1:8765 npm run dev -- --host 127.0.0.1
```

```bash
curl http://localhost:8090/api/execution/health
```

The Phase 4A API is a separate read-only process and defaults to loopback:

```bash
export PHASE4_API_KEY='<runtime-secret>'
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api \
  --fixture-profile operator_read_only --host 127.0.0.1 --port 8765
```

Use `small_fixture` for deterministic smoke tests. A copied store uses
`--fixture-profile copied_store --copied-control-plane /path/to/copy`; the CLI
rejects symlinks and the operator store. `--reload` is fixture-only. No option
applies migrations.

Build and verify the Phase 4B dashboard:

```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm run check:api
npm run gen:api
npm run typecheck
npm run lint
npm test
npm run build
npm run test:e2e
```

Regenerate the checked-in Phase 4A OpenAPI snapshot after an intentional API
contract change:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/export_phase4_openapi.py
```

The exporter constructs only the deterministic fixture-mode app and accesses no
operator store. The dashboard E2E flow asserts that all observed `/api/v1`
business requests are GET.

## Research and optimization

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.run_recipe --recipe <recipe_name>
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.run_recipe --bundle <bundle_name>
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.shadow_monitor
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.shadow_monitor --backfill-days 30
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.research.optimization.cli --help
```

Research commands must preserve `DATA_DOMAIN=research` isolation where required by their contracts.

### ADR-0007 R0 pattern calibration

Run the four-lane classifier and exact history-band detector policies against
read-only operational history, writing a new immutable research bundle:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.research.pattern_lane_calibration.cli \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD --cadence weekly \
  --winner-windows reports/winner_analysis/funnel_autopsy/winner_funnel_autopsy.csv \
  --output-dir /path/to/new/pattern-r0-bundle
```

Use repeated `--as-of-date YYYY-MM-DD` arguments for a pre-registered date set,
`--symbols-file` for a bounded real-data canary, and `--exclusions-csv` for
dated DQ or corporate-action exclusions. The exclusion CSV requires
`symbol_id,effective_from`; optional `effective_to` bounds the exclusion.
Undated exclusion lists are rejected because they are not point-in-time safe.

Verify an exact replay without writing another retained bundle:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.research.pattern_lane_calibration.cli \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD --cadence weekly \
  --winner-windows reports/winner_analysis/funnel_autopsy/winner_funnel_autopsy.csv \
  --verify-against /path/to/pattern-r0-bundle/r0_pattern_manifest.json
```

The command opens DuckDB read-only and never writes a pipeline attempt,
operator database, pattern cache, rank artifact, or consumer state. The known
winner file feeds only `r0_pattern_winner_recall.csv`; it is not included in
precision metrics.

Progress is written to stderr with date position, symbol position, processing
rate, per-date ETA, overall ETA, signal counts, and checkpoint commits. The
default is up to four parallel symbol workers; override with `--workers N` and
set reporting frequency with `--progress-every N`. Completed dates are written
atomically to `<output-dir>.checkpoints` and automatically resumed on an exact
policy/source signature match. Use `--checkpoint-dir` to relocate them or
`--no-resume` to recompute all dates. `Ctrl-C` preserves completed-date
checkpoints and exits with status 130.

## Installed console scripts

After `pip install -e .`, these aliases are defined by `pyproject.toml`:

| Alias | Entrypoint |
|---|---|
| `ai-trading-pipeline` | Canonical pipeline orchestrator |
| `ai-trading-daily` | Daily pipeline wrapper |
| `ai-trading-healthcheck` | Operator health probe |
| `ai-trading-publish-test` | Publish-channel health check |
| `ai-trading-execution-api` | FastAPI backend |
| `ai-trading-bootstrap-data` | Runtime-data bootstrap |
| `ai-trading-repair-ingest-schema` | Ingest schema repair |
| `ai-trading-repair-control-plane-timestamps` | Control-plane timestamp repair |
| `ai-trading-benchmark-phase3c4` | Isolated Phase 3C-4 performance/replay benchmark |
| `ai-trading-build-phase3c5-calibration` | Immutable calibration/readiness evidence builder |
| `ai-trading-check-phase4-readiness` | Re-evaluate Phase 4 readiness from a calibration manifest |
| `ai-trading-pattern-r0-calibrate` | Read-only four-lane pattern R0 calibration and replay verifier |
| `ai-trading-phase4-api` | Strictly read-only Phase 4A API |
| `ai-trading-annotate-phase3c1-governance` | Copied-store Phase 3B governance annotation |
| `ai-trading-research-recipe` | Research recipe runner |
| `ai-trading-optimize` | Optimization runner |
| `ai-trading-optimize-promote` | Optimization promotion workflow |
| `ai-trading-fundamentals-sync` | Screener fundamentals sync |
| `ai-trading-fundamentals-refresh-readmodels` | Fundamentals read-model refresh |
| `ai-trading-fundamentals-validate-exports` | Fundamentals export validation |
| `ai-trading-valuation-refresh` | Valuation feature refresh |
| `ai-trading-sector-earnings-refresh` | Sector earnings refresh |
| `ai-trading-backfill-operational-valuation` | Operational valuation backfill |
| `ai-trading-daily-gainers-report` | Daily gainers report |
| `ai-trading-fundamental-opportunity-report` | Fundamental opportunities report |
| `ai-trading-winner-validation-report` | Winner validation report |
| `ai-trading-early-accumulation-validate` | Early accumulation validation |
| `ai-trading-symbol-report` | Symbol research report |

For any mutating repair, migration, backfill, promotion, or live execution command, inspect `--help`, confirm the target data domain, and take the required backup first.

For a read-only Phase 4A copied-store smoke with immutable evidence:

```bash
PHASE4_API_SOURCE_PROFILE=copied_store \
PHASE4_API_COPIED_CONTROL_PLANE=/path/to/control_plane.copy.duckdb \
PHASE4_API_ARTIFACT_ROOT=/path/to/immutable/evidence \
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api
```

The command opens DuckDB read-only and invokes no migration, pipeline, or
broker operation.
