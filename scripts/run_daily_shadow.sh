#!/usr/bin/env bash
# Daily shadow routine: run the full production pipeline with the three R1a
# shadow flags, then verify the shadow session (report-only). One operator
# command for Day-2+ operation of the pattern_lane_scan shadow.
#
#   Gate is NON-BLOCKING: a shadow day that does not count never fails this
#   script. The PRODUCTION pipeline's exit code IS blocking — it is returned
#   verbatim, so your normal routine still surfaces real pipeline failures.
#
# Usage:
#   scripts/run_daily_shadow.sh [extra ai-trading-pipeline args...]
#   RUN_DATE=2026-07-18 scripts/run_daily_shadow.sh --local-publish
#
# Env overrides:
#   RUN_DATE               session date (default: today, YYYY-MM-DD)
#   RUN_ID                 pinned run id (default: shadow-<RUN_DATE>-<HHMMSS>)
#   PATTERN_LANE_WORKERS   process-pool workers for the lane scan (default: 4)

set -uo pipefail

# 1. Resolve repo + interpreter (do NOT depend on console-script install state).
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
PY="$REPO/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "[daily-shadow] error: python not found at $PY" >&2
  exit 2
fi

RUN_DATE="${RUN_DATE:-$(date +%F)}"
RUN_ID="${RUN_ID:-shadow-$RUN_DATE-$(date +%H%M%S)}"     # 2. unique per invocation
WORKERS="${PATTERN_LANE_WORKERS:-4}"
OUT="reports/research/shadow_sessions/$RUN_DATE"

# 3. Single-run lock (portable: mkdir is atomic; flock is absent on macOS).
LOCK="${TMPDIR:-/tmp}/run_daily_shadow.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "[daily-shadow] another run is in progress (lock: $LOCK) — aborting" >&2
  exit 1
fi
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT

echo "[daily-shadow] run_id=$RUN_ID  run_date=$RUN_DATE  workers=$WORKERS"

# 4. Full production pipeline + three shadow flags. Default --stages, so the
#    orchestrator auto-injects weekly_stage, pattern_lane_scan, scan_router and
#    opportunities around the normal rank/investigator/execute/publish routine.
"$PY" -m ai_trading_system.pipeline.orchestrator \
  --run-id "$RUN_ID" --run-date "$RUN_DATE" \
  --opportunity-registry-mode shadow \
  --opportunity-scan-routing-mode shadow \
  --pattern-lane-scan-mode shadow \
  --pattern-lane-scan-workers "$WORKERS" \
  "$@"
PIPELINE_RC=$?
echo "[daily-shadow] pipeline exit=$PIPELINE_RC"

# 5. Score the shadow session even if the pipeline failed (best-effort,
#    report-only: no --fail-on-not-counted). A gate error never aborts here.
echo "[daily-shadow] scoring session gate -> $OUT"
"$PY" -m ai_trading_system.interfaces.cli.check_shadow_session_gate \
  --run-id "$RUN_ID" --output-root "$OUT" >/dev/null 2>&1 || true

# 6. Structured final summary (parsed from the verdict, if the gate produced one).
VERDICT="$OUT/session_gate_verdict.json"
PIPE_STATUS=$([[ "$PIPELINE_RC" -eq 0 ]] && echo SUCCESS || echo FAILED)
echo "----------------------------------------------------------------"
echo "Run ID: $RUN_ID"
echo "Pipeline: $PIPE_STATUS"
if [[ -f "$VERDICT" ]]; then
  "$PY" - "$VERDICT" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))
counted = "COUNTED" if v.get("day_counts") else "NOT COUNTED"
print(f"Shadow session: {counted} (gate {v.get('status')})")
fails = [c["check_id"] for c in v.get("checks", []) if c.get("status") == "FAIL"]
if fails:
    print("Failed gates:")
    for cid in fails:
        print(f"  - {cid}")
PY
  echo "Report: $VERDICT"
else
  echo "Shadow session: NOT SCORED (no gate verdict — pattern_lane_scan likely did not complete)"
  echo "Report: (none)"
fi
echo "----------------------------------------------------------------"

# 7. Exit with the PRODUCTION pipeline's code. The shadow gate is advisory and
#    never turns a successful production run into a failure.
exit "$PIPELINE_RC"
