# Pattern Lane Scan

- **Purpose:** Define the ADR-0007 R1a shadow-only, non-actionable lane-aware pattern scan stage.
- **Audience:** Operators and engineers running or reviewing the R1a shadow period.
- **Last verified:** 2026-08-08
- **Source of truth:** `pipeline/stages/pattern_lane_scan.py` and `research/pattern_lane_calibration/shadow.py`.

---

Start with the [System Guide](../SYSTEM_GUIDE.md).

## Purpose

Run the ADR-0007 lane-aware scanner inside the production pipeline on a strictly
observational basis. The stage writes only new evidence artifacts that no
decision consumer reads: ranking, candidates, opportunities, execution, and
lifecycle remain authoritative and untouched. It mirrors the shadow trio
(`weekly_stage` / `scan_router` / `opportunities`): self-gate on mode,
register or verify the policy snapshot before any stage-owned write, time every
phase, and register downloadable artifacts.

## Entrypoints

`PatternLaneScanStage.run` is registered as logical stage `pattern_lane_scan`.
It is in `OPTIONAL_STAGES` and returns `{"status": "skipped", "mode": "off"}`
unless `pattern_lane_scan_mode` is `shadow`.

## Input data

The daily universe for the run date (`domains/opportunities/coverage.load_daily_universe`,
read from the operational OHLCV database) and governed weekly-stage observations
loaded in `governed_current` mode, which requires classifier version
`weekly-stage-v2`. Legacy `pattern_scan` output from the `rank` stage is read as
the parity baseline. If the current run skipped rank because its inputs were
unchanged, the orchestrator supplies the latest promoted rank artifacts from a
completed run.

## Output artifacts

Seven artifacts per attempt:

| Artifact | Content |
|---|---|
| `pattern_lane_scan.csv` | Lane-classified signal rows with attached evidence. |
| `pattern_lane_summary.json` | Shadow summary: symbols scanned, diagnostics, parity, status. |
| `pattern_lane_runtime.json` | Per-phase timings, invocation counts, lane distribution. |
| `pattern_lane_source_diagnostics.csv` | Weekly-stage source freshness and admission diagnostics. |
| `pattern_lane_parity_report.json` | Comparison against the legacy `rank`/`pattern_scan` artifact. |
| `pattern_lane_manifest.json` | `pattern-r1a-manifest-v1`: policy versions/hashes, source hashes and row counts, dataset hashes, `code_commit`, `operational_side_effects: false`. |
| `pattern_lane_shadow_report.html` | Rendered operator-readable shadow report. |

All artifacts are written under the attempt directory. The stage never calls
`write_calibration_result`, which forbids the `pipeline_runs` tree.

## Main modules

`pipeline/stages/pattern_lane_scan.py`,
`research/pattern_lane_calibration/shadow.py`,
`research/pattern_lane_calibration/policy.py` (`default_r0_policy`,
`pattern-lane-r0-policy-v1`),
`research/pattern_lane_calibration/stage_source.py`, and
`domains/opportunities/policy_snapshot.py`.

## Process flow

Self-gate on mode; compute and register/verify the policy snapshot and append
the snapshot event; load the daily universe and governed weekly-stage frame;
run the lane shadow scan with `pattern_lane_scan_workers` process-pool workers;
build source diagnostics, parity against the legacy pattern artifact, shadow
summary, and runtime report; write and register the seven artifacts, including
the manifest built from the already-written artifact hashes.

## DQ

Policy content is verified against `policy_version_registry` before any
stage-owned write; a label reappearing with different content raises
`POLICY_VERSION_CONTENT_MISMATCH`. Source diagnostics must be constructible for
the required weekly-stage policy version, and any non-zero
`stale_admitted_as_fresh_count` fails the stage. The manifest records source and
dataset hashes so a run can be reproduced and independently checked.

## Failure modes

Every stage-level failure raises `PatternLaneScanStageError`, which the
orchestrator treats as **non-blocking**: it emits an
`opportunity_shadow_degraded` warning alert, finishes the stage run as `failed`
with `{"non_blocking": true, "mode": "shadow"}`, and continues the pipeline.
Triggering conditions are a policy-version content mismatch, a weekly-stage
source load failure, unbuildable source diagnostics, and stale weekly-stage
observations admitted as fresh.

## Retry behavior

Artifacts are attempt-scoped and the stage is pure with respect to operational
state, so a re-run recomputes cleanly. As with other stages, an unchanged input
hash lets the orchestrator skip the stage unless `--force-rerun` is passed.

## Downstream consumers

None in the operational path — that is the R1a contract. The registered
artifacts are read only by the shadow session gate
(`ai-trading-shadow-session-gate`) and the read-only cross-shadow
reconciliation (`ai-trading-cross-shadow`).

## Commands

```bash
ai-trading-pipeline --run-date <session> \
  --pattern-lane-scan-mode shadow \
  --pattern-lane-scan-workers 4 --local-publish
```

`--pattern-lane-scan-mode` accepts `off` (default) or `shadow`. With the default
`--stages` list, `shadow` also schedules `weekly_stage` immediately before this
stage. `--pattern-lane-scan-workers` defaults to `1`.

For the daily operator routine see
[shadow daily session](../runbooks/shadow_daily_session.md); for the A/B safety
proof see [shadow stage A/B parity](../runbooks/shadow_stage_ab_parity.md) and
[ADR-0007](../decisions/ADR-0007-two-lane-pattern-scan.md).

## Performance instrumentation

The stage records durations for `load_daily_universe`, `load_weekly_stage`,
`run_scan`, and `write_artifacts`, plus a combined `load_scan_inputs` database
metric (query counts, read milliseconds, rows read) and per-artifact write
timings. Timings are also summarized in `pattern_lane_runtime.json`.
