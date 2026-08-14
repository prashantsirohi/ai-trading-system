# Commands

- **Purpose:** Authoritative runnable command and console-entrypoint reference.
- **Audience:** Operators and developers.
- **Last verified:** 2026-08-11
- **Source of truth:** `pyproject.toml [project.scripts]` and the referenced CLI parsers.

---

Start with the common workflows in the [System Guide](../SYSTEM_GUIDE.md). Commands below are run from the repository root unless they explicitly change directories.

## Actual Trading Journal

`ai-trading-journal` (or `python -m ai_trading_system.domains.trade_journal`) provides `migrate --apply`, tradebook and holdings preview/commit imports, `reconstruct`, `reconcile`, and `analyze`. Reviewed governance commands are `propose-opening-lot`, `approve-opening-lot`, `propose-adjustment`, `approve-adjustment`, `propose-corporate-action`, and `approve-corporate-action`. Imports and governance mutations require explicit `--commit`; otherwise they preview. Holdings accept `--mode reconciliation_only` or the explicit `opening_anchor` bootstrap mode. See the [operator runbook](../runbooks/trade_journal.md) for safe examples.

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

Unified BSE-only new-symbol onboarding (preview, then apply):

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.ingest.new_symbol_onboarding \
  --symbol SYMBOL1 --symbol SYMBOL2 \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD

PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.ingest.new_symbol_onboarding \
  --symbol SYMBOL1 --symbol SYMBOL2 \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD --apply
```

Read-only discovery before master insertion:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.ingest.new_symbol_onboarding \
  --discover-missing --symbols-file proposed_bse_symbols.txt \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD
```

Discovery verifies active BSE identity, company ISIN, local collisions, market
capitalization/group metadata, and official classification. Discovery by itself
is always read-only. After approving a clean preview, promote the exact scope
and run all onboarding stages with:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.ingest.new_symbol_onboarding \
  --discover-missing --promote-discovered --apply \
  --symbols-file proposed_bse_symbols.txt \
  --from-date YYYY-MM-DD --to-date YYYY-MM-DD
```

This mode fails closed on any discovery or classification gap, checkpoints the
master before a transactional no-replacement insert, and then performs the
standard classification, official BSE history, technical, Phase 1,
fundamentals, and verification workflow.

Use `--symbols-file FILE` for a larger explicit scope,
`--allow-fundamentals-download` only when authenticated Screener acquisition is
intended, or `--skip-fundamentals` to omit that non-critical stage. The command
never expands its scope to every recently updated master row. Apply mode exits
non-zero when official history or its dependent feature stages fail and stores
the verification report under `$REPORTS_ROOT/symbol_onboarding/`.

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

The default CLI stage string includes `fundamentals` and `candidate_tracker` but omits `weekly_stage`, `pattern_lane_scan`, `scan_router`, `opportunities`, and `narrative`. `--opportunity-scan-routing-mode compare|shadow` inserts Phase 3B after rank. `--pattern-lane-scan-mode shadow` inserts the ADR-0007 R1a lane scan after `weekly_stage`, adding `weekly_stage` first if it is not already scheduled; `--pattern-lane-scan-workers` (default 1) sets its process-pool size. `--opportunity-registry-mode shadow` inserts Phase 3A after Investigator. These insertions apply only while `--stages` is left at its default. Existing execution and publish consumers are unchanged.

Opportunity shadow run and isolated retry:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --opportunity-registry-mode shadow

PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages opportunities \
  --opportunity-registry-mode shadow --opportunity-registry-dry-run
```

Reconstruct same-run Investigator attribution and performance on a copied
control plane only:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.interfaces.cli.reconstruct_investigator_performance \
  --copied-control-plane /path/to/copied/control_plane.duckdb \
  --ohlcv-db /path/to/ohlcv.duckdb \
  --from-date 2026-07-20 --to-date 2026-07-24 --apply
```

The command rejects the configured operator store and symlinks, applies
additive migrations only to the copy, rejects later-than-decision artifacts,
and labels accepted historical context `RECONSTRUCTED_SAME_RUN`.

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

For local journal use, no API-key setup or browser login is required. When the
repository-root `.env` has no `EXECUTION_API_KEY`, the loopback-bound API and
Vite use an internal development handshake. Vite injects it only in the
server-side proxy. A non-loopback API bind still requires an explicit key, and
production builds do not enable this local shortcut.

```bash
curl http://localhost:8090/api/execution/health
```

The Phase 4A API is a separate read-only process and defaults to loopback:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api \
  --fixture-profile operator_read_only --host 127.0.0.1 --port 8765
```

When no key is configured, loopback CLI startup automatically enables local
development access. Binding to a non-loopback address requires an explicit
`PHASE4_API_KEY`.

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

## Read-only MCP server

`ai-trading-mcp` (or `python -m ai_trading_system.interfaces.mcp.server`) serves
the read surfaces over stdio for an AI agent. It is strictly read-only: every
DuckDB handle opens with `read_only=True`, every SQLite handle through a
`mode=ro` URI, and it never imports execution, trade-journal, broker, or
pipeline-orchestration code.

The default `operator` profile requires an explicit external `DATA_ROOT`; it
refuses to fall back to the repo-local `data/` tree. Use `--profile fixture`
only for temporary or repo-local roots.

```bash
set -a; source .env; set +a
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.mcp.server --self-test
```

`--self-test` calls every tool once at latest and once at `--self-test-as-of`
(default `2026-01-02`), prints each response's metadata, and exits non-zero on
any failure — including a point-in-time leak, which raises rather than
returning. `--list-tools` prints the tool catalog as JSON without opening a
store. Claude Code picks the server up from the repo-root `.mcp.json`.

See [MCP tools](mcp_tools.md) for the tool catalog and
[ADR-0008](../decisions/ADR-0008-read-only-mcp-interface.md) for the invariants.

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
| `ai-trading-migrate-control-plane` | Control-plane schema migration runner |
| `ai-trading-shadow-session-gate` | Score one shadow session (`--fail-on-not-counted` to hard-fail) |
| `ai-trading-shadow-ab-proof` | Build the shadow-stage A/B/C safety-proof bundle |
| `ai-trading-cross-shadow` | Read-only cross-shadow reconciliation report |
| `ai-trading-benchmark-phase3c4` | Isolated Phase 3C-4 performance/replay benchmark |
| `ai-trading-build-phase3c5-calibration` | Immutable calibration/readiness evidence builder |
| `ai-trading-check-phase4-readiness` | Re-evaluate Phase 4 readiness from a calibration manifest |
| `ai-trading-pattern-r0-calibrate` | Read-only four-lane pattern R0 calibration and replay verifier |
| `ai-trading-phase4-api` | Strictly read-only Phase 4A API |
| `ai-trading-mcp` | Strictly read-only MCP (stdio) server for AI agents |
| `ai-trading-annotate-phase3c1-governance` | Copied-store Phase 3B governance annotation |
| `ai-trading-research-recipe` | Research recipe runner |
| `ai-trading-optimize` | Optimization runner |
| `ai-trading-optimize-promote` | Optimization promotion workflow |
| `ai-trading-fundamentals-sync` | Basis-explicit Screener fundamentals sync (`--statement-basis` required) |
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

Screener sync accepts `--statement-basis standalone` or `consolidated`. When a
legacy `screener_market_valuation` key is detected, also supply
`--statement-basis-migration-backup-dir <directory>`; the deprecated
`--valuation-migration-backup-dir` spelling remains an alias. The command refuses to migrate
without creating and checksumming that backup.

For a read-only Phase 4A copied-store smoke with immutable evidence:

```bash
PHASE4_API_SOURCE_PROFILE=copied_store \
PHASE4_API_COPIED_CONTROL_PLANE=/path/to/control_plane.copy.duckdb \
PHASE4_API_ARTIFACT_ROOT=/path/to/immutable/evidence \
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.serve_phase4_api
```

The command opens DuckDB read-only and invokes no migration, pipeline, or
broker operation.
