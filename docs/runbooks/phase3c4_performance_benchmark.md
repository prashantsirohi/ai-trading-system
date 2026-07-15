# Phase 3C-4 Performance Benchmark

- **Purpose:** Reproduce advisory shadow-path performance measurements and exact replay checks safely.
- **Audience:** Operators and developers.
- **Last verified:** 2026-07-15
- **Source of truth:** `platform/telemetry/performance.py` and `interfaces/cli/benchmark_phase3c4.py`.
- **Policy:** `phase3c4-performance-policy-v1`

## Safety contract

The benchmark is non-mutating. Fixture profiles do not open runtime stores.
`copied_realistic` opens an explicitly supplied copied control plane read-only.
The command resolves paths and refuses the configured operator control plane,
output beneath operator `DATA_ROOT`, and symlinked copied/output targets. It does
not delete or flush OS caches and does not apply migrations 034–036 or any new
migration.

## Fixture smoke runs

```bash
PYTHONPATH=src ./.venv/bin/python \
  -m ai_trading_system.interfaces.cli.benchmark_phase3c4 \
  --profile small_fixture --cache-mode cold --repetitions 1 \
  --as-of 2026-07-15 --output-root /tmp/phase3c4-small-cold

PYTHONPATH=src ./.venv/bin/python \
  -m ai_trading_system.interfaces.cli.benchmark_phase3c4 \
  --profile small_fixture --cache-mode warm --repetitions 3 \
  --as-of 2026-07-15 --output-root /tmp/phase3c4-small-warm
```

Cold mode creates fresh collector and fixture state per repetition. Warm mode
reuses the same immutable fixture inputs and process. Runtime variation is
expected; routing hashes, decision IDs, row counts, and artifact hashes must be
identical.

## Copied-store and baseline runs

```bash
PYTHONPATH=src ./.venv/bin/python \
  -m ai_trading_system.interfaces.cli.benchmark_phase3c4 \
  --profile copied_realistic --cache-mode cold --repetitions 3 \
  --copied-control-plane /tmp/phase3c4-control-plane.duckdb \
  --as-of 2026-07-15 --output-root /tmp/phase3c4-copied

PYTHONPATH=src ./.venv/bin/python \
  -m ai_trading_system.interfaces.cli.benchmark_phase3c4 \
  --profile medium_fixture --cache-mode warm --repetitions 10 \
  --baseline-summary /tmp/baseline/phase3c4_performance_summary.json \
  --as-of 2026-07-15 --output-root /tmp/phase3c4-current
```

The summary reports minimum, maximum, median, p50, and coefficient of variation;
p90 appears with three or more samples but is explicitly descriptive until ten
samples exist. Baseline comparisons use advisory warn/fail percentage bands.

## Outputs and interpretation

Review the five `phase3c4_*` files documented in the
[artifact reference](../reference/artifacts.md). Exact replay ignores only run
IDs, timestamps, durations, memory observations, and temporary paths. A semantic
replay mismatch exits nonzero. Performance `WARN` or `FAIL` remains nonblocking
unless `--fail-on-threshold` is supplied. Functional status and performance
status must be reviewed separately.

Phase 3C-4 does not change ranking, routing, Investigator logic, candidate
lifecycle, recovery, execution, publishing, or broker state. Phase 3C-5 is not
implemented by this runbook.
