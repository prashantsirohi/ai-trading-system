# R1a Shadow — Daily Session (Day 2+)

- **Purpose:** Run and score one R1a shadow session without the full A/B/C parity test.
- **Audience:** Operator.
- **Last verified:** 2026-07-19.
- **Source of truth:** the run's registered `pattern_lane_*` artifacts + the pipeline registry.

---

The Day-1 safety proof (byte-identical legacy decisions) is frozen at
`docs/evidence/adr-0007/r1a-safety-proof/2026-07-17-7d5f03a/`. From Day 2 you do
**not** repeat it. Run your normal production pipeline with the three shadow
flags and score the session with the gate.

## One command (recommended)

`scripts/run_daily_shadow.sh` runs the full production pipeline (rank →
investigator → execute → publish) with the three shadow flags folded in, then
scores the session — **report-only**. It returns the *pipeline's* exit code; a
shadow day that does not count never fails the script.

```bash
scripts/run_daily_shadow.sh                 # today, real publish
RUN_DATE=2026-07-18 scripts/run_daily_shadow.sh --local-publish
```

- Env overrides: `RUN_DATE` (default today), `RUN_ID` (default
  `shadow-<date>-<HHMMSS>`, unique per run), `PATTERN_LANE_WORKERS` (default 4).
- Extra args pass straight through to the pipeline (e.g. `--local-publish`,
  `--data-domain`).
- Holds a single-run lock (`$TMPDIR/run_daily_shadow.lock`) so two daily runs
  cannot overlap.
- Writes the gate verdict to `reports/research/shadow_sessions/<date>/` and
  prints a summary: run id · pipeline SUCCESS/FAILED · shadow session
  COUNTED/NOT COUNTED · failed gate ids · report path.

The manual two-step form below is what the wrapper runs underneath.

## Run (manual)

```bash
ai-trading-pipeline --run-date <session> \
  --opportunity-registry-mode shadow \
  --opportunity-scan-routing-mode shadow \
  --pattern-lane-scan-mode shadow \
  --pattern-lane-scan-workers 4 --local-publish
```

`--pattern-lane-scan-mode shadow` also schedules `weekly_stage`. A lone
`opportunities.opportunity_shadow` *degraded* task is pre-existing and
non-blocking.

## Score the session (manual)

```bash
ai-trading-shadow-session-gate --run-id <run_id> \
  --output-root reports/research/shadow_sessions/<session>   # add --fail-on-not-counted to hard-fail
```

The session counts (`day_counts: true`) when all eight hold: lane stage
completed · seven lane artifacts registered · runtime passes (≤ 15 min; ≤ 10 min
p95 target) · policy diagnostics pass · source diagnostics present · no
stale-as-fresh · no malformed signal rows · registry + routing shadows complete
(degraded `opportunities` tolerated) · no operational consumer changed.

A `WARN` verdict still counts (e.g. runtime in the 10–15 min band, or the
tolerated `opportunities` degradation); a `FAIL` does not. Investigate a FAIL via
`session_gate_checks.csv` before counting the day.

## During the shadow period

Run the read-only cross-shadow reconciliation to see whether the lane scan adds
early discovery vs. confirms rank/Investigator:

```bash
ai-trading-cross-shadow --run-id <run_id> --through-date <session> \
  --output-dir reports/research/cross_shadow/<session>
```

## Review points

See ADR-0007 → D7 → **R1a review points** for the 5-session and 20-session
review criteria. Monitor the snapshot-fallback rate (~22.5% at Day 1) and keep
each fallback's reason visible.
