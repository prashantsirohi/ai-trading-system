# Stage: candidates

- **Purpose:** Deterministic post-rank selection — combines composite rank, breakout/pattern evidence, sector state, and (optional) fundamentals into a single `final_candidates.csv` with explicit groups and per-row reasons.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:**
  - `src/ai_trading_system/pipeline/stages/candidates.py` (`CandidatesStage`)
  - `src/ai_trading_system/domains/candidates/builder.py` (`build_final_candidates`, `ExecutionCandidateBuilder` re-exports)
  - `src/ai_trading_system/domains/candidates/contracts.py` (`FINAL_CANDIDATE_COLUMNS`, `CANDIDATE_GROUP_PRIORITY`, defaults)

---

## Purpose

`candidates` runs after `rank` (and after `fundamentals` when enabled). It is a **pure, deterministic** stage — no LLM, no external calls, no DB writes — that filters and re-ranks the top technical pool into the operator-facing shortlist used by `execute`, `insight`, `narrative`, and `publish`.

## Entrypoints

- Stage wrapper: `src/ai_trading_system/pipeline/stages/candidates.py::CandidatesStage.run`.
- Builder (also CLI-callable): `python -m ai_trading_system.domains.candidates.builder` (`builder.py:397`).
- Invoked by the orchestrator as part of `PIPELINE_ORDER` (`pipeline/orchestrator.py:41`).

## Input data

- Required (hard fail if missing): `rank.ranked_signals` artifact → `ranked_signals.csv` (`candidates.py:26`).
- Optional rank artifacts (read via `context.artifact_for("rank", …)`):
  - `breakout_scan`
  - `pattern_scan`
  - `sector_dashboard`
- Optional fundamentals artifact: `fundamentals.watchlist_candidates` → `watchlist_candidates.csv`.

If any optional input is missing, the stage records a warning in the summary and continues with the corresponding bonus disabled (`builder.py:51`–`54`).

## Output artifacts

Under `data/pipeline_runs/<run_id>/candidates/attempt_<n>/`:

| Artifact type | File | Notes |
|---|---|---|
| `final_candidates` | `final_candidates.csv` | Columns from `FINAL_CANDIDATE_COLUMNS` (`contracts.py:23`): `symbol, name, industry_group, composite_score, breakout_score, pattern_score, fundamental_score, fundamental_tier, fundamental_trend_label, final_candidate_score, candidate_group, candidate_reason, next_action`. |
| `candidate_summary` | `candidate_summary.json` | Status, generated_at, `rows_ranked`, `rows_selected`, `candidate_group_counts`, `warnings`. |

## Main modules

- `pipeline/stages/candidates.py::CandidatesStage` — thin wrapper: reads CSVs, calls builder, registers artifacts.
- `domains/candidates/builder.py::build_final_candidates` — the only business-logic function; everything below is implementation detail in the same file:
  - `_best_by_symbol` (per-symbol top row by score)
  - `_sector_lookup` (LEADING / IMPROVING tagging from sector dashboard)
  - `_final_score` (composite weighted score)
  - `_candidate_group` (group assignment)
  - `_candidate_reason` and `_next_action` (operator-facing strings)
- `domains/candidates/contracts.py` — constants:
  - `DEFAULT_MIN_CANDIDATES = 10`
  - `DEFAULT_MAX_CANDIDATES = 25`
  - `DEFAULT_TECHNICAL_POOL_SIZE = 100`
  - `CANDIDATE_GROUPS`, `CANDIDATE_GROUP_PRIORITY`, `FINAL_CANDIDATE_COLUMNS`.

## Process flow

1. Load `ranked_signals.csv`; coerce `composite_score` to numeric; keep top `technical_pool_size` (default 100) sorted by composite score (`builder.py:43`).
2. Best-row-per-symbol on optional inputs:
   - breakout: best by `breakout_score`
   - pattern: best by `pattern_score` then `setup_quality`
   - watchlist (fundamentals): best by `final_watchlist_score` then `fundamental_score`.
3. Build sector lookup from `sector_dashboard` (`_sector_lookup`, `builder.py:199`): a sector is tagged `LEADING` when quadrant is `LEADING` or RS rank ≤ 5; `IMPROVING` when quadrant is `IMPROVING` or RS momentum > 0; first five rows fall back to `LEADING` if no explicit signal.
4. Left-merge breakout/pattern/watchlist into the top pool, then compute predicates per row (`builder.py:80`–`90`):
   - `_hard_red_flag` — truthy `hard_red_flag` column or `fundamental_tier == REJECT`.
   - `_qualified_breakout` — truthy `qualified` or `breakout_score ≥ 75`.
   - `_strong_pattern` — `pattern_score ≥ 75` or `setup_quality ≥ 70`.
   - `_near_high` — composite of `near_52w_high_pct ≤ 10`, `proximity_to_highs ≥ 70` (or `≤ 10`), `prox_high ≤ 10`, `prox_high_score ≥ 70`.
   - `_has_setup` — `_qualified_breakout ∨ _strong_pattern ∨ _near_high`.
   - `_leading_sector`, `_improving_sector`, `_stage2`, `_catalyst_present`.
5. Compute `final_candidate_score` (`builder.py:278`, clipped to `[0, 100]`):
   ```
   0.60 * composite_score
   + 0.15 * max(breakout_score, pattern_score, _near_high*75)
   + 0.10 * fundamental_score
   + 6.0  if fundamental_tier ∈ {A, B}
   + 5.0  if fundamental_trend_label == IMPROVING
   + 4.0  if _leading_sector
   + 2.0  if (_improving_sector ∧ ¬_leading_sector)
   + 3.0  if _catalyst_present
   − 30.0 if _hard_red_flag
   ```
6. Assign `candidate_group` (`builder.py:303`) in this priority order:
   1. `AVOID_RED_FLAG`
   2. `RESULTS_OR_CATALYST_PENDING`
   3. `FUNDAMENTAL_IMPROVER`
   4. `LEADING_SECTOR_BREAKOUT` (requires `_leading_sector ∧ _qualified_breakout`)
   5. `IMPROVING_SECTOR_STAGE2` (requires `_improving_sector ∧ _stage2`)
   6. `HIGH_RS_PULLBACK` (default)
7. Build the candidate set: keep rows with `_has_setup ∧ ¬_hard_red_flag` (normal) plus all `_hard_red_flag` rows (so AVOID rows are still surfaced). Sort by `(_group_priority asc, final_candidate_score desc, composite_score desc)` and `head(max(min_candidates, max_candidates))` (`builder.py:97`–`109`).
8. Reproject onto `FINAL_CANDIDATE_COLUMNS`; round `final_candidate_score` to 2 dp. Write CSV + summary JSON.

## DQ / trust gates

- **Hard input gate.** `ranked_signals` artifact must exist (`context.require_artifact("rank", "ranked_signals")`); otherwise the stage raises before doing any work.
- **Empty-input branch.** When the loaded ranked frame is empty, the builder returns `status="completed_empty"` with `rows_ranked=0` and a warning (`builder.py:39`–`41`).
- **Soft missing-input warnings** for `sector_dashboard` and `watchlist_candidates` are emitted in `candidate_summary.json.warnings`.
- The stage does **not** consult `dq_rule` / `dq_result` directly; it inherits DQ trust from the rank stage. Truth-map item "Candidate count bounds, duplicate-symbol checks" is achieved structurally by the deterministic head-N selection and the per-symbol best-row pre-merge.

## Failure modes

- Missing `ranked_signals` artifact → `KeyError`/`StageContextError` from `require_artifact`.
- Malformed CSV → `pd.read_csv` raises; not caught.
- Empty ranked frame → empty `final_candidates.csv` with `status="completed_empty"`. Downstream stages must tolerate.

## Retry behavior

- Stateless and idempotent: rerunning with the same inputs produces a byte-identical artifact (sort is stable, deterministic predicates). A retry simply re-writes the attempt directory.
- No registry side effects beyond standard `StageArtifact` registration.

## Downstream consumers

- `execute` — reads `final_candidates.csv` to materialize order intents.
- `insight` / `narrative` — read both `final_candidates.csv` and `candidate_summary.json` for the LLM brief.
- `publish` — uses the same artifacts for Telegram / Google Sheets / PDF deliveries.

## Commands

```bash
# Run candidates as part of the full pipeline.
ai-trading-pipeline --run-date <yyyy-mm-dd>

# Re-run only the candidates stage (requires an existing rank attempt).
ai-trading-pipeline --run-date <yyyy-mm-dd> --stages candidates

# Build candidates ad-hoc from files (no orchestrator).
python -m ai_trading_system.domains.candidates.builder \
  --ranked-signals data/pipeline_runs/<run_id>/rank/attempt_1/ranked_signals.csv \
  --breakout-scan data/pipeline_runs/<run_id>/rank/attempt_1/breakout_scan.csv \
  --pattern-scan data/pipeline_runs/<run_id>/rank/attempt_1/pattern_scan.csv \
  --sector-dashboard data/pipeline_runs/<run_id>/rank/attempt_1/sector_dashboard.csv \
  --watchlist-candidates data/pipeline_runs/<run_id>/fundamentals/attempt_1/watchlist_candidates.csv \
  --output-dir /tmp/candidates_check
```

Tunable params (CLI flag → pipeline param):

| Param | Default | Source |
|---|---|---|
| `candidates_min` | `10` (`DEFAULT_MIN_CANDIDATES`) | `contracts.py:39` |
| `candidates_max` | `25` (`DEFAULT_MAX_CANDIDATES`) | `contracts.py:40` |
| `candidates_technical_pool` | `100` (`DEFAULT_TECHNICAL_POOL_SIZE`) | `contracts.py:41` |
