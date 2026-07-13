# Stage: candidate_tracker

- **Purpose:** Maintain durable lifecycle state for selected candidates independently of research performance tracking.
- **Audience:** Operator, developer, debugging.
- **Last verified:** 2026-07-13
- **Source of truth:** `src/ai_trading_system/pipeline/stages/candidate_tracker.py`, `src/ai_trading_system/domains/candidate_tracker/service.py`, and the candidate-tracker CLI flags in `src/ai_trading_system/pipeline/orchestrator.py`.

---

## Purpose

`candidate_tracker` runs after `candidates`. It converts each selected candidate into durable episode, review, snapshot, alert, and current-state records. It does not place orders and is separate from `perf_tracker`, which matures research return cohorts.

## Entrypoints

- Stage wrapper: `ai_trading_system.pipeline.stages.candidate_tracker.CandidateTrackerStage`.
- Domain service: `ai_trading_system.domains.candidate_tracker.run_candidate_tracker`.
- Orchestrator stage name: `candidate_tracker`; enabled by default and controlled by `--enable-candidate-tracker` / `--no-enable-candidate-tracker`.

## Input data

- Required: `candidates.final_candidates`.
- Optional: fundamentals watchlist, quarterly results, valuation bands, bucket shortlist, ranked signals, and rank/fundamentals sector dashboards.
- Operational OHLCV is read from the configured domain path.

Missing optional frames are supplied as empty data frames; the required candidate artifact is a hard gate.

## Output artifacts

Artifacts are written beneath `$DATA_ROOT/pipeline_runs/<run_id>/candidate_tracker/attempt_<n>/`:

| Artifact type | File |
|---|---|
| `candidate_tracker_current` | `candidate_tracker_current.csv` |
| `candidate_tracker_alerts` | `candidate_tracker_alerts.csv` |
| `candidate_tracker_summary` | `candidate_tracker_summary.json` |
| `candidate_fundamental_reviews` | `candidate_fundamental_reviews.csv` |
| `candidate_fundamental_bucket_reviews` | `candidate_fundamental_bucket_reviews.csv` |
| `candidate_tracking_snapshots` | `candidate_tracking_snapshots.csv` |

## Main modules

- `pipeline/stages/candidate_tracker.py` resolves artifacts, configuration, and output registration.
- `domains/candidate_tracker/service.py` owns episode transitions, snapshots, reviews, alerts, and DuckDB persistence.
- Publish payloads and weekly/fundamental-opportunity channels consume current tracker outputs.

## Process flow

1. Require `final_candidates` and load all available enrichment artifacts.
2. Resolve `$DATA_ROOT/candidate_tracker.duckdb` unless an explicit stage parameter overrides it.
3. Reconcile selected candidates with active and historical episodes.
4. Refresh lifecycle state from OHLCV and fundamental evidence.
5. Persist current state, snapshots, reviews, transitions, and alerts.
6. Materialize the six attempt-scoped artifacts.

## DQ / trust gates

The stage inherits rank/candidate trust and requires the registered final-candidate artifact. It reads operational OHLCV through the configured path contract. It does not independently authorize execution.

## Failure modes

- Missing `final_candidates` fails before domain processing.
- An unavailable operational data root or unreadable tracker/OHLCV database fails the attempt.
- Invalid or incompatible tracker schema fails persistence; do not repair a live database without a backup.

## Retry behavior

Retries receive a new stage-attempt directory. Domain writes use stable episode/snapshot identities so replaying the same run date updates or deduplicates current tracker state rather than creating parallel active episodes.

## Downstream consumers

- `publish` resolves current-state and alert artifacts.
- Dashboard, weekly PDF, fundamental-opportunity, and decision-bundle builders use tracker state.
- Operators use lifecycle records for review; `execute` continues to use its own policies and gates.

## Commands

Run only the tracker for a run whose candidates artifact is registered:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --run-id <run_id> --stages candidate_tracker
```

Disable it in a broader run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.pipeline.orchestrator \
  --no-enable-candidate-tracker
```
