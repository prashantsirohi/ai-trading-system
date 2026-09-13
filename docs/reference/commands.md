# Commands

- **Purpose:** Authoritative runnable command and console-entrypoint reference.
- **Audience:** Operators and developers.
- **Last verified:** 2026-09-13
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

`./scripts/run_daily_shadow.sh` runs the normal pipeline with the fundamental,
opportunity, routing, and pattern shadow lanes, then scores the advisory shadow
session gate. Extra pipeline arguments are forwarded, including `--local-publish`.
It also creates a blank M1 operator-review form at
`reports/research/shadow_sessions/<RUN_DATE>/operator_review_<run-id-hash>.md`
and prints its absolute path. The filename uses the first 16 SHA-256 characters
of the run ID; the form includes the full ID. Retrying the same run preserves
existing notes; another run gets its own form. This follows the wrapper's
existing repository-relative session-report directory.

After reviewing the results, fill the form and mark it completed. Five distinct
market-session reviews make the baseline; repeated runs do not create extra
review sessions. Review time and observations are manual, never inferred from
pipeline timing or the separate `COUNTED` verdict. Forms are also offered after
failed runs so gaps can be recorded. Form creation is non-interactive and
best-effort; the script still returns the pipeline's exit code. See the
[baseline protocol](../development/m1_decision_ownership_audit.md#five-session-baseline-protocol).

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

The default CLI stage string includes `fundamentals` and `candidate_tracker` but omits `weekly_stage`, `pattern_lane_scan`, `scan_router`, `fundamental_discovery`, `opportunities`, and `narrative`. `--fundamental-discovery-mode compare|shadow` inserts the discovery stage after `fundamentals`; `compare` cannot write the registry and `shadow` only supplies an isolated input to opportunity registry shadow. `--opportunity-scan-routing-mode compare|shadow` and pattern lane modes retain their documented insertions. `--opportunity-registry-mode shadow` inserts opportunities after fundamental discovery when present. These insertions apply only while `--stages` is left at its default. Existing ranking, execution, candidate, and publish consumers are unchanged.

Fundamental discovery shadow example:

```bash
ai-trading-pipeline --run-date 2026-08-15 \
  --fundamental-discovery-mode shadow \
  --opportunity-registry-mode shadow \
  --local-publish
```

`ai-trading-fundamentals-sync` is a separate ingestion command and is never invoked by this pipeline mode. Its `--fundamentals-duckdb-path` selects the append-only receipt store.
Before first registry-shadow use, apply migration 044 with the backup-gated `ai-trading-migrate-control-plane --backup-dir <verified-dir> --from-migration 044 --to-migration 044 --apply`. Compare mode does not require the control-plane observation table.

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

## Read-only MCP servers

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
store. Claude Code picks the server up from the repo-root `.mcp.json`; OpenCode
picks it up from the repo-root `opencode.json`. Both registrations launch the
same local stdio server, so no HTTP/SSE endpoint is required.

See [MCP tools](mcp_tools.md) for the tool catalog and
[ADR-0008](../decisions/ADR-0008-read-only-mcp-interface.md) for the invariants.

`ai-trading-journal-mcp` serves private journal evidence in a separate,
account-scoped stdio process. It requires journal schema `002` and an explicit
external `DATA_ROOT`. A single journal account is selected automatically;
multi-account stores require `AI_TRADING_JOURNAL_MCP_ACCOUNT_REF`.

```bash
ai-trading-journal-mcp --list-tools
```

See [Journal MCP tools](journal_mcp_tools.md) and
[ADR-0009](../decisions/ADR-0009-private-trade-journal-mcp.md).

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
| `ai-trading-journal-mcp` | Private account-scoped read-only journal MCP (stdio) server |
| `ai-trading-annotate-phase3c1-governance` | Copied-store Phase 3B governance annotation |
| `ai-trading-research-recipe` | Research recipe runner |
| `ai-trading-optimize` | Optimization runner |
| `ai-trading-optimize-promote` | Optimization promotion workflow |
| `ai-trading-fundamentals-sync` | Unified resumable Screener sync (defaults to standalone + consolidated, then one resolved readmodel refresh) |
| `ai-trading-fundamentals-refresh-readmodels` | Fundamentals read-model refresh |
| `ai-trading-fundamentals-validate-exports` | Fundamentals export validation |
| `ai-trading-valuation-refresh` | Valuation feature refresh |
| `ai-trading-sector-earnings-refresh` | Sector earnings refresh |
| `ai-trading-backfill-operational-valuation` | Operational valuation backfill |
| `ai-trading-onboard-symbols` | Preview/apply unified BSE-only new-symbol onboarding |
| `ai-trading-repair-demerger` | Preview/apply one evidence-bound demerger adjustment after backing up OHLCV |
| `ai-trading-daily-gainers-report` | Daily gainers report |
| `ai-trading-fundamental-opportunity-report` | Fundamental opportunities report |
| `ai-trading-winner-validation-report` | Winner validation report |
| `ai-trading-early-accumulation-validate` | Early accumulation validation |
| `ai-trading-symbol-report` | Symbol research report |
| `ai-trading-research-screener` | Isolated persistent screener (`regression_replay`, `live_canary`, `full_universe`, or `filing_discovery`) |
| `ai-trading-annual-report-discovery` | Immutable official annual-report evidence discovery for a completed filing-grade cohort |
| `ai-trading-jcurve` | Isolated market-intel import, bounded OpenRouter evaluation, and immutable J-curve reporting |

For any mutating repair, migration, backfill, promotion, or live execution command, inspect `--help`, confirm the target data domain, and take the required backup first.

The research screener requires `--as-of-date YYYY-MM-DD` and `--run-mode`
(`regression_replay`, `live_canary`, `full_universe`, or `filing_discovery`). It writes only beneath
`$DATA_ROOT/research_screener/` unless explicit test paths are supplied. Live
runs stop if the combined official security master is unavailable and preserve
typed unknown/repair outcomes for other fixed-source failures.
Regression replay automatically selects checksum-locked canary fixture v1.0.0;
live canary selects the registered current fixture. `--canary-file` is an
audited override and cannot bypass the registered version/mode contract.
The current default screen version is `0.2.5`; pass `--screen-version` only when
intentionally replaying or introducing different rule/parser semantics.
`full_universe` rejects `--canary-file`, defaults to the separate
`persistent_screener_phase1` definition at version `1.0.0`, and produces
`universe_company_status.csv`, `universe_decision_explanations.md`, and
`universe_summary.md` beside the common P0 Parquet/manifest files. It is a
fail-closed discovery pass; in-band membership is not a qualified investment
decision.
`filing_discovery` also rejects `--canary-file`, defaults to definition
`persistent_screener_filing_discovery` version `1.2.0`, and should name a
completed same-date `full_universe` run with `--parent-run-id`. If omitted, the
latest completed same-date parent is selected. `--batch-size` controls durable
progress/checkpoint cadence, not cohort membership. Its output uses
`filing_company_status.csv`, `filing_decision_explanations.md`, and
`filing_summary.md`; it never ranks or writes to execution. Version 1.2.0
freezes read-only exact-ISIN sector/industry evidence from the current master,
routes bank, financial-institution, market-infrastructure, and industrial
contracts explicitly, and blocks unclassified issuers instead of using company
name similarity or symbol-only fallback.
The CLI default `--workers 4` divides the single-session request cadence across
four sessions, preserving the aggregate rate while overlapping network latency.
Explicit values from five through sixty-four remain per-session rate-limited and cap
the aggregate request cadence at twice the default.

Run annual-report discovery only after naming a completed filing-discovery
parent. The command preserves the exact parent `BOUNDARY_REVIEW` cohort, uses
NSE then official BSE fallback, checkpoints each ISIN, and writes only beneath
the isolated research-screener root:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.research_screener.annual_report_service \
  --as-of-date YYYY-MM-DD --parent-run-id <completed-filing-run-id> \
  --workers 4 --batch-size 25
```

Its text matches are LOW-confidence page anchors requiring human review.
Missing topics remain `NOT_DISCLOSED`; the command does not score, rank,
recommend, schedule, publish, or execute.

The J-curve command is separate from the pipeline. `seed-screener` freezes the
authenticated screen 317873 export and classifies a bounded union of screen
members, the versioned baseline, and policy supplemental symbols. It reads the
Screener fundamentals database without mutation. Missing local history is
reported as non-accepted `HISTORY_UNAVAILABLE` coverage rather than failing the
whole seed. `discover-v2` freezes the four governed lifecycle screens, applies
the official-universe and consensus gates, classifies the focused set plus the
100-case baseline, and writes a maximum 250-name ramp/commissioning primary
queue while retaining every exclusion. `bootstrap` imports up to ten
years of official `market_intel` announcements for the versioned 25-company
capex baseline by default; `ingest` derives its start from
the latest completed import and applies a configurable 1–90 day overlap.
`evaluate` requires `OPENROUTER_API_KEY` or `OPENROUTER_KEY`, a completed import
run, and optionally point-in-time materiality inputs and audited human
verifications. It defaults to 25 resolved companies; `--company-limit` accepts
1 through 250. `calibrate` requires exactly 25 labeled companies unless
`--allow-nonstandard-cohort` is explicitly supplied. `report` reads an existing
immutable result and performs no model call:

```bash
ai-trading-jcurve seed-screener --as-of-date YYYY-MM-DD --screen-id 317873
ai-trading-jcurve seed-screener --as-of-date YYYY-MM-DD --screen-id 317873 \
  --screen-export /path/to/screener-screen.csv
ai-trading-jcurve profile-baseline --as-of-date YYYY-MM-DD \
  --cohort-config configs/research_screener/jcurve/capex_baseline_v2.json
ai-trading-jcurve discover-v2 --as-of-date YYYY-MM-DD
ai-trading-jcurve discover-v2 --as-of-date YYYY-MM-DD \
  --screen-export 3901581=/path/to/screen1.xlsx \
  --screen-export 3901588=/path/to/screen2.xlsx \
  --screen-export 3901589=/path/to/screen3.xlsx \
  --screen-export 3901592=/path/to/screen4.xlsx
ai-trading-jcurve bootstrap --as-of-date YYYY-MM-DD --lookback-years 5
ai-trading-jcurve bootstrap --as-of-date YYYY-MM-DD --lookback-years 5 \
  --seed-run-id <completed-seed-run-id>
ai-trading-jcurve bootstrap --as-of-date YYYY-MM-DD --lookback-years 5 \
  --discovery-run-id <completed-discovery-run-id>
ai-trading-jcurve bootstrap --as-of-date YYYY-MM-DD --lookback-years 5 \
  --upstream-filter-policy market-intel-high-value-filter-v1
ai-trading-jcurve bootstrap --as-of-date YYYY-MM-DD --lookback-years 5 \
  --cohort-config /path/to/versioned-cohort.json
ai-trading-jcurve bootstrap --as-of-date YYYY-MM-DD --lookback-years 5 \
  --all-companies
ai-trading-jcurve ingest --as-of-date YYYY-MM-DD --overlap-days 7
ai-trading-jcurve ingest --as-of-date YYYY-MM-DD --overlap-days 7 \
  --upstream-filter-policy market-intel-high-value-filter-v1
ai-trading-jcurve evaluate --parent-run-id <run-id> --as-of-date YYYY-MM-DD \
  --materiality-inputs /path/to/materiality.json \
  --human-verifications /path/to/reviews.json
ai-trading-jcurve report --run-id <evaluation-run-id>
ai-trading-jcurve calibrate --evaluation-run-id <evaluation-run-id> \
  --labels /path/to/reviewed-labels.json
```

Global test/diagnostic overrides are `--store-path` and `--output-root`.
Import accepts `--market-intel-db`; all market-intel access remains read-only.
Bootstrap defaults to
`configs/research_screener/jcurve/capex_baseline_v1.json`; `--cohort-config`
supplies another contract-compatible cohort, while `--all-companies` disables
cohort filtering. `--seed-run-id` instead uses accepted, resolved candidates
from a completed seed run. Those three modes are mutually exclusive.
Seed acquisition uses cached Screener authentication and requires
`SCREENER_USERNAME`/`SCREENER_PASSWORD` unless `--screen-export` is supplied.
The provided export must include an NSE symbol column or one Screener company
hyperlink per row. `--supplemental-cohort` defaults to the 25-company baseline.
Bootstrap freezes only upstream rows already present and reports historical
coverage as unproven unless contiguous completed NSE and BSE receipts cover the
requested interval.
The optional `--upstream-filter-policy` is shadow-only and currently accepts
only `market-intel-high-value-filter-v1`. It requires the corresponding
upstream receipt and decision tables, imports only `KEEP` and
`FETCH_ATTACHMENT` rows, and records incomplete NSE/BSE coverage as degraded.

Preview the evidence-bound STLTECH demerger repair before applying it:

```bash
PYTHONPATH=src ./.venv/bin/python -m \
  ai_trading_system.domains.ingest.demerger_repair \
  --evidence-file configs/corporate_actions/stltech_2025_demerger.json
```

Add `--apply` only after reviewing the preview. Apply mode creates and checksums
a full `ohlcv.duckdb` backup, reconciles the single named action, and recomputes
adjusted prices only for the evidence-bound symbol.

Screener sync defaults to `--statement-basis both`, which resumes standalone
and consolidated independently and refreshes resolved readmodels once. Use
`--statement-basis standalone` or `consolidated` only for diagnostics, targeted
replay, or canaries. Add `--missing-current-results` for a quarterly update; it
selects only symbols missing the inferred expected quarter and forces a fresh
download when `--allow-download` is present. A fresh export that still lacks
the quarter is not retried for 72 hours by default; change this with
`--missing-results-retry-cooldown-hours`, or set it to `0` to disable the
cooldown. Persisted terminal standalone-only classifications are excluded from
the consolidated missing-results pass. When a
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


## Monthly universe refresh

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.ingest.universe_refresh
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.domains.ingest.universe_refresh --apply
```

Preview can use `--screen-export /path/to/export.xlsx` to avoid browser login.
`--cadence monthly|28-days` defaults to monthly; `--lookback-years 5` accepts
1–20. `--force` bypasses cadence; `--as-of YYYY-MM-DD` defaults to today and
apply rejects a historical date. Apply permits authenticated company-fundamentals
downloads and backs up affected stores. Exit 1 reports pending supported-data
backfill failures. Discovery-only quarantine returns success with gaps. Current-date operational ingest runs this
check first, including direct orchestrator and daily shadow runs; see [ingest](../stages/ingest.md#monthly-universe-onboarding).

## Local final-review evidence

`./scripts/run_daily_shadow.sh` now enables the shadow review sidecar. Append `--final-review-mode off` to disable it. Use `--local-publish` for local publisher outputs; that switch alone does not isolate database writes.

Replay from a separately captured runtime copy, keeping `.env` pointed at the real operational root so the isolation guard can reject it:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.replay_final_review \
  --copied-data-root /path/to/isolated-copy/data \
  --run-id <completed-source-run> --session YYYY-MM-DD \
  --decision-at <capture-time-with-UTC-offset> \
  --output-dir /path/to/new/local-review-output
```

This command opens copied stores read-only, verifies copied registered artifacts and only creates the new output directory. It never publishes externally. A current captured store is retrospective evidence, not reconstruction of an overwritten historical vintage. See [validation and remaining gates](../development/u2_u3_final_review_validation.md).
