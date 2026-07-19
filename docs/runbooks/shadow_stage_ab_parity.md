# Shadow-Stage A/B Parity Proof

- **Purpose:** Prove that enabling an optional *shadow* pipeline stage leaves every legacy decision artifact byte-identical under identical inputs — the core safety gate before a shadow stage counts toward its observation clock.
- **Audience:** Operator / engineer.
- **Last verified:** 2026-07-19 (R1a `pattern_lane_scan`, run date 2026-07-17).
- **Source of truth:** side-by-side pipeline artifacts under `pipeline_runs/<run_id>/<stage>/attempt_*`.

---

## When to use

Run this before letting any new shadow stage count a "clean" session — R1a
`pattern_lane_scan`, or any future stage that claims to be *observational only*.
It empirically confirms the architectural claim ("this stage is a pure sink that
no decision consumer reads") rather than trusting it.

The method generalises: it is an **A/B/C controlled comparison on isolated
copy-on-write clones**. A = feature off, B = feature on, C = feature off again
(control). C is what makes the proof rigorous — it separates a genuine flag
effect from ordinary run-to-run nondeterminism.

## Why a control run (C) is mandatory

Two independent full pipeline runs are **never** byte-identical: many artifacts
embed generated UUIDs, wall-clock timestamps, content-hash-derived IDs, and
float aggregates whose summation order varies with worker scheduling. Without a
control you cannot tell "the flag changed this" from "this always jitters." The
rule is:

> A difference counts **against** the shadow stage **only if it appears in A~B
> but is *not* reproduced in the A~C control.**

## Prerequisites

- An operational `DATA_ROOT` on an APFS volume (macOS) so `cp -c` makes instant,
  near-zero-disk copy-on-write clones. On non-APFS storage substitute a real
  copy (slower, more disk) or a snapshot.
- The as-of date's OHLCV already ingested (pick a completed NSE session).
- Governed weekly-stage rows present in `control_plane.duckdb`
  (`weekly_stock_stage_history` / `weekly_stage_backfill_observation`) for stages
  that read them (e.g. `pattern_lane_scan`).

Set once:

```bash
OP=/Volumes/MacData/Trading/data          # operational DATA_ROOT (never written by this runbook)
AB=/Volumes/MacData/Trading/ab            # throwaway workspace
RUN_DATE=2026-07-17
RUN_ID=ABRUN                              # pinned across A/B/C so run-scoped fields align
FEATURE_FLAG=--pattern-lane-scan-mode     # the flag under test (off | shadow)
FEATURE_STAGE=pattern_lane_scan           # its stage dir name
```

## Step 1 — clone the operational data root (×3, isolated)

Operational data is **never** touched; every run writes only to its clone.

```bash
clone_root() {  # $1 = destination
  rm -rf "$1"; mkdir -p "$1/pipeline_runs"
  cd "$OP"
  for item in *; do
    case "$item" in
      pipeline_runs|backups|*.backup*|*.lock) ;;      # exclude bulky/stale/lock files
      *) cp -c -R "$item" "$1/$item" ;;               # APFS copy-on-write clone
    esac
  done
}
clone_root "$AB/root_A"
clone_root "$AB/root_B"
clone_root "$AB/root_C"
```

## Step 2 — force a full recompute on each clone

A clone inherits the operational run history, so `rank`/`features` would
cache-skip (ingest-fingerprint) and never produce fresh artifacts — and
downstream stages that don't rehydrate cached `rank` (e.g. `fundamentals`) would
hard-fail. Clear **only** the pipeline-run history tables; keep the governed
stage and opportunity/candidate lifecycle tables intact.

```bash
for R in root_A root_B root_C; do
  ./.venv/bin/python - "$AB/$R" <<'PY'
import sys, duckdb
c = duckdb.connect(f"{sys.argv[1]}/control_plane.duckdb")
for t in ("pipeline_artifact","pipeline_stage_run","pipeline_run",
          "pipeline_alert_incident","pipeline_alert"):
    c.execute(f"DELETE FROM {t}")
c.close()
PY
done
```

Preserved (verify non-zero): `weekly_stock_stage_history`,
`weekly_stage_backfill_observation`, `candidate_episode`, and the other
`candidate_*` / `opportunity_*` / `investigator_lifecycle` tables.

## Step 3 — run A (off), B (on), C (off)

Hold **everything** constant except the feature flag and its stage. Same commit,
run date, `--run-id`, routing/registry shadow modes, `--local-publish` (never
touch external channels). B additionally schedules the feature stage.

```bash
run() {  # $1=root  $2=off|shadow
  ( export DATA_ROOT="$AB/$1"
    ./.venv/bin/ai-trading-pipeline \
      --run-id "$RUN_ID" --run-date "$RUN_DATE" \
      --opportunity-registry-mode shadow \
      --opportunity-scan-routing-mode shadow \
      $FEATURE_FLAG "$2" \
      --local-publish --force-rerun \
      > "$AB/run_$1.log" 2>&1 & echo "$1 pid $!" )
}
run root_A off
run root_B shadow            # add: --pattern-lane-scan-workers 4   (parallelism, optional)
run root_C off               # control
```

**Isolation check (run immediately):** every pipeline PID's env must point at its
clone, and operational must never gain the run:

```bash
for p in $(pgrep -f "ai-trading-pipeline --run-id $RUN_ID"); do ps eww $p | tr ' ' '\n' | grep DATA_ROOT; done
ls "$OP/pipeline_runs/" | grep -i "$RUN_ID" && echo "FATAL: operational polluted" || echo "GOOD: operational untouched"
```

Wait for all three to finish; each should report `status=completed` with the
full stage count (a lone `opportunities.opportunity_shadow` degraded task is
pre-existing and unrelated).

## Step 4 — classify every difference against the control

Save as `$AB/classify.py` and run with `./.venv/bin/python $AB/classify.py`.
It walks every CSV artifact, and flags a difference as **flag-caused only if it
is *not* reproduced in the A~C control**.

```python
import hashlib
from pathlib import Path
import pandas as pd

R = {k: Path(f"/Volumes/MacData/Trading/ab/root_{k}/pipeline_runs/ABRUN") for k in "ABC"}

def sha(p):
    h = hashlib.sha256(); h.update(p.read_bytes()); return h.hexdigest()

def diff_cols(pa, pb):
    da = pd.read_csv(pa, dtype=str, keep_default_na=False)
    db = pd.read_csv(pb, dtype=str, keep_default_na=False)
    if list(da.columns) != list(db.columns): return {"__COLSET__"}
    if len(da) != len(db):                   return {f"__ROWCOUNT__({len(da)}v{len(db)})"}
    out = set()
    for c in da.columns:
        if not da[c].equals(db[c]) and sorted(da[c]) != sorted(db[c]):  # ignore pure row-order
            out.add(c)
    return out

flag_caused = []
for sd in sorted(R["A"].glob("*/attempt_1")):
    stage = sd.parent.name
    for pa in sorted(sd.glob("*.csv")):
        pb, pc = R["B"]/stage/"attempt_1"/pa.name, R["C"]/stage/"attempt_1"/pa.name
        if not pb.exists() or sha(pa) == sha(pb):        # missing in B, or identical -> skip
            continue
        ab = diff_cols(pa, pb)
        ac = set() if (pc.exists() and sha(pa)==sha(pc)) else (diff_cols(pa, pc) if pc.exists() else {"__C_MISSING__"})
        flag_only = ab - ac                              # in A~B but NOT reproduced in control
        tag = "nondeterministic" if not flag_only else f"FLAG-CAUSED extra={sorted(flag_only)}"
        if flag_only: flag_caused.append((stage, pa.name, sorted(flag_only)))
        print(f"{stage+'/'+pa.name:52} A~B={sorted(ab)}  {tag}")

print("\nFLAG-CAUSED legacy differences (exclude performance/* telemetry):")
legacy = [x for x in flag_caused if not x[0].startswith("performance")]
print("  NONE — PASS" if not legacy else "\n".join(f"  {s}/{f}: {c}" for s,f,c in legacy))
```

### Strict gate on the headline artifact

For the artifact the shadow stage claims not to affect (here
`rank/pattern_scan.csv`), require exact equivalence A vs B:

```python
import pandas as pd, hashlib
A = Path("/Volumes/MacData/Trading/ab/root_A/pipeline_runs/ABRUN/rank/attempt_1/pattern_scan.csv")
B = Path("/Volumes/MacData/Trading/ab/root_B/pipeline_runs/ABRUN/rank/attempt_1/pattern_scan.csv")
da, db = pd.read_csv(A), pd.read_csv(B)
assert A.read_bytes() == B.read_bytes()          # SHA-256 + byte size
assert len(da) == len(db)                        # row count
assert list(da.columns) == list(db.columns)      # column order
```

### Existence checks

- Run B has the feature stage's artifact dir with the expected files; Run A does
  **not** have a `$FEATURE_STAGE` directory at all.

## Decision rule

- **All legacy decision datasets byte-identical → PASS (session counts).**
- **Only timestamps / generated IDs / run-scoped metadata / float-ordering differ,
  AND each is reproduced in the A~C control → PASS.** Document the metadata
  difference; compare the decision columns (scores, selected symbols, positions,
  admissions) separately and require those identical.
- **Any legacy *data* difference that is present in A~B but *absent* in A~C → FAIL.**
  Do not count the session until the cause is found and resolved.

`performance/phase3c4_*` gaining rows in B is **expected** — it is the shadow
stage's own duration/artifact/db-metric telemetry, additive observability, not a
decision artifact.

## When a diff looks flag-caused (drill-down)

Nondeterminism can land on different columns per run, so the classifier may
mislabel it. Before concluding FAIL:

1. **Structural check:** grep the suspect stage's domain for the feature stage
   name — if it never reads the shadow artifacts, the flag cannot reach it.
   ```bash
   grep -rn "pattern_lane" src/ai_trading_system/domains/investigator/   # expect: 0 hits
   ```
2. **Content check:** confirm the *decision* columns are identical and only an
   ordering/ID/timestamp reshuffled. Example that resolved R1a — investigator
   scores byte-identical across A/B/C, only `rank` jittered a few cells, and the
   A~C control jittered too:
   ```python
   d = {k: pd.read_csv(R[k]/"investigator/attempt_1/investigator_scores.csv",
                       dtype=str, keep_default_na=False).set_index("symbol_id") for k in "ABC"}
   for x,y in [("A","B"),("A","C"),("B","C")]:
       common = d[x].index.intersection(d[y].index)
       scores_same = all((d[x].loc[common,c].sort_index()==d[y].loc[common,c].sort_index()).all()
                         for c in d["A"].columns if "score" in c)
       rank_diff = int((d[x].loc[common,"rank"].sort_index()!=d[y].loc[common,"rank"].sort_index()).sum())
       print(x,y,"scores_identical",scores_same,"rank_cells_differ",rank_diff)
   ```
   If the control pair (`A,C`) also differs, it is nondeterminism → PASS.

> **Field comparison policy.** The manual classification above is codified in
> `platform/parity/comparison_policy.py` (`shadow-parity-policy-v1`): the
> STRICT / CONTENT / RUN_SCOPED / TELEMETRY field classes and the accepted
> run-scoped/float-jitter column catalogs. `compare_runs(A, B, control_c=C)`
> reproduces the control-subtraction rule programmatically. Prefer it over
> ad-hoc scripts, and extend the catalog there (not inline) when a new
> run-scoped field appears.

## Step 5 — freeze the proof bundle, then clean up

**Before deleting the clones**, build and verify the permanent, signed proof
bundle. This is the durable, independently-reviewable evidence — do not rely on
the clones or a hand-written verdict.

```bash
ai-trading-shadow-ab-proof \
  --run-a "$AB/root_A/pipeline_runs/$RUN_ID" \
  --run-b "$AB/root_B/pipeline_runs/$RUN_ID" \
  --run-c "$AB/root_C/pipeline_runs/$RUN_ID" \
  --staging-root "$DATA_ROOT/audit/pattern_lane_r1a/<date>-<commit7>" \
  --git-record-root docs/evidence/adr-0007/r1a-safety-proof/<date>-<commit7> \
  --as-of-date "$RUN_DATE" --run-id "$RUN_ID" --code-commit "$(git rev-parse HEAD)"

shasum -a 256 -c "$DATA_ROOT/audit/pattern_lane_r1a/<date>-<commit7>/bundle.sha256"
```

Then commit the git audit record, attach the compressed staging bundle as a
GitHub Release asset (tag from `bundle_reference.json`), verify, and only then
remove the clones and re-confirm isolation:

```bash
rm -rf "$AB"
ls "$OP/pipeline_runs/" | grep -i "$RUN_ID" || echo "GOOD: operational has no $RUN_ID"
```

The 2026-07-17 baseline bundle lives at
`docs/evidence/adr-0007/r1a-safety-proof/2026-07-17-7d5f03a/`; the original prose
verdict is at `reports/research/r1a_shadow_ab_2026-07-17/AB_VERDICT.md`.

## Notes and gotchas

- **Pin `--run-id` across A/B/C** so run-id-derived fields (`scan_run_id`,
  `routing_decision_id`) align; residual differences then come only from genuine
  nondeterminism, shrinking the noise you must explain.
- **`--force-rerun` does not bypass the ingest-fingerprint downstream-skip** — that
  is why Step 2 clears the run-history tables. Skip Step 2 and `rank` will not
  recompute and `fundamentals` will fail with `Missing required artifact
  'ranked_signals'`.
- **Never omit `--local-publish`** — shadow parity runs must not hit Google
  Sheets / Telegram.
- `execute` runs the paper adapter only; on an isolated clone it is safe. Its
  `executed_orders`/`executed_fills` differ every run by design (fresh UUIDs +
  timestamps) while `positions.csv` / `trade_actions.csv` — the actual decisions —
  stay identical.
