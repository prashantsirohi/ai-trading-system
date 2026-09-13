# U2 integration and U3 copied-real validation

- **Purpose:** Record the final-review adapter contract, reproducible validation and remaining acceptance evidence.
- **Audience:** Opportunity and publishing maintainers; operator.
- **Last verified:** 2026-09-13
- **Source of truth:** `domains/opportunities/review_sources.py`, `review_projection.py`, `pipeline/stages/final_review.py`, `interfaces/cli/replay_final_review.py` under `src/ai_trading_system/`, their targeted tests, and the captured validation bundle described below.
- **Status:** U2 implemented; U3 engineering validation passed, real-case acceptance OPEN. U4/U5 not started. M1/G1 and M2/G2 remain open.

## Implemented boundary

The [U1 policy](u1_review_policy_contract.md) now has a read-only source adapter and an optional artifact materializer inside `opportunities`, after both lanes. The orchestrator's `--final-review-mode shadow` requires registry shadow; its default is `off`. The daily shadow wrapper enables it. Existing ranking, candidate lifecycle, position management and publisher ownership are unchanged. There is no Sheets or Telegram consumer yet.

The source adapter requires completed, promoted producer attempts, verifies SHA-256 and registered row counts, preserves producer run/attempt identity and rejects availability after the explicit cutoff. It uses full `ranked_universe`, governed weekly stages, pattern assessments plus exact signal links, fundamental thesis projections and registered router position coverage. Stage reconciliation uses correction-aware history. Missing or invalid evidence remains visible; a later run's fundamental repair is not silently joined to an earlier run's pattern output.

The union of market, master and every source listing is assessed before ordering. Liquidity percentiles use the broad current-session market denominator. The reader opens configured market/control-plane stores read-only and master SQLite with `mode=ro`; it checks DQ, quarantine, provider/trust state, price basis and availability. Late-base calculations require complete 220-bar windows. Recent calendar validation compares NIFTY_50 with local holidays and broad NSE coverage; the full metric window also requires holiday-year coverage. BSE calendar support is absent and remains an explicit exception.

`final-review-adapter-v1` separately fingerprints setup choice, metric window and `review-setup-extension-v1`. The selected bullish allowed setup is ordered by confirmed status, descending priority, descending pattern score, then signal ID. Trigger, invalidation and extension refer to that same setup. Extension is max(0, close/trigger − 1) × 100: LOW ≤5%, MEDIUM ≤10%, HIGH >10%. This review-specific rule does not rewrite operational extension. Raw pattern levels affected by a nonunit adjustment inside the signal window are rejected. Technical S2 contradiction uses the adapter's 220-bar high, not the scanner's longer high window.

`final_review_universe.csv` retains exclusions, lane outcomes and exceptions. `final_review_list.csv` contains only selected listings with resolved readiness, sorted by readiness, upstream rank and identity, without truncation or backfill. `final_review_summary.json` carries denominators, source metadata, snapshot hashes, policy snapshot and deterministic decision-content hash. Empty CSVs retain the complete schema. Generation failure emits fresh empty files and a failed summary rather than retaining a previous list. Existing opportunity failure/status semantics are otherwise unchanged.

Position flags preserve the registered scan-router pre-execution cycle scope even for excluded listings. They do not reconstruct or certify the full live ledger. F-only technical readiness currently relies on independent evidence from the pattern scan, including explicit successful NONE; an absent scan cannot become NONE.

## Captured-real evidence

The local bundle is `/private/tmp/review-u3-1q4zzeg7`. Its `copy_manifest.json` records capture time, four store hashes and copied artifact hashes. `validation.json` records replay counts/hashes, byte comparisons, semantic parity exceptions and implementation file hashes. These temporary files are local evidence and are not committed runtime data.

Capture time/cutoff: **2026-09-13T14:20:33.971546+00:00**. Source session: **2026-09-11**. DuckDB copies were taken while read-only handles held the sources, with before/after/copied hashes verified. Master SQLite was copied using its read-only backup source. Replays are explicitly retrospective against the captured current store; they do not claim September 11 recording-time vintage reconstruction.

| Check | Result |
|---|---|
| Original run `shadow-2026-09-11-112408`, two identical replays | 1,918 assessed each; zero ordered; degraded |
| Exact decision-content retry hash | Both `3cbb4086abb6c83272d900b5a8c03e7d4cc8cca8101eaaf726aa71f88619de9e` |
| Original source row counts | Rank 503; stage 1,308; pattern assessments 1,808; signals 436; fundamentals 503; position cycles 5 |
| Original lane results | P qualified 231; all 503 present F projections unsupported admission v1; remaining F missing |
| Repaired-F run `pipeline-2026-09-11-6246553d` | Rank/F 546; stage 1,308; P/signals/position artifacts absent; 241 F qualified; 1,918 assessed; zero ordered |
| Universe accounting | Both runs retain all 1,918 listings as evidence exceptions; no top-N fill or silently dropped exception rows |
| DQ source acceptance | True in both replays; stricter adapter calendar failure remains separately visible |
| Original position coverage | All 5 declared cycles represented despite opportunity exclusion; repaired run's position coverage unknown |
| Read-only replay writes | All four copied database hashes and registered source artifact bytes unchanged |

The actual source rank artifacts were loaded and hash/row-count checked, and recorded DQ inputs were examined. Ranking itself was not rerun, and no new rank-quality or profitability claim is made.

A copied-store `OpportunityStage` canary ran with identical inputs in off/shadow mode, `local_publish=True`, `dry_run=True`, and recovery `report_only`. All existing artifact bytes matched except the summary's `adapter_seconds`, `persistence_seconds`, `total_seconds`, and convergence horizons' `updated_at`. Parsed comparisons confirmed those were the only differing fields. Exactly three final-review artifacts were added. Candidate/Investigator/opportunity business-table row counts were unchanged, and the copied execution database hash was unchanged. This verifies semantic output parity; it is not a full-pipeline canary or proof that every business-table byte was unchanged. Policy registration may write the copied control plane during the stage canary. No live repair, publisher or broker operation ran.

## Acceptance blockers and next work

1. The captured NIFTY_50 series lacks **2026-08-21** and **2026-09-08**, both scheduled sessions according to the local holiday calendar. Recent-session verification fails. Investigate the source/calendar discrepancy before any evidence-backed repair; no dates were fabricated or gates relaxed.
2. The local holiday table covers 2026 only. The full 220-bar late-base window crosses an uncovered year, so long-window continuity cannot be certified. BSE calendar validation also remains unsupported.
3. The original run has old F admission v1; the repaired v1.1 run lacks pattern and router artifacts. A complete same-run source set is needed for sourced P-only/F-only/P+F and readiness walkthroughs.
4. After those source issues are resolved, rerun copied validation and inspect real late/early S1, S2, setup-ready, developing, extended and evidence-exception cases. Unit fixtures cover policy branches but cannot replace these real acceptance examples.

These are U3 acceptance prerequisites, not approval for live database repairs. U4's Sheets pilot remains pending the accepted queue meanings and usable M1 comparison baseline. No persisted feature rebuild or database migration is required by U2; source/calendar corrections may require regenerating affected producer artifacts. Existing primary rank and publishers remain in place.

## Verification commands

```bash
PYTHONPATH=src ./.venv/bin/python -m pytest \
  tests/domains/opportunities/test_review_policy.py \
  tests/domains/opportunities/test_review_projection.py \
  tests/domains/opportunities/test_review_sources.py \
  tests/pipeline/stages/test_final_review_stage.py \
  tests/pipeline/stages/test_opportunities_stage.py \
  tests/scripts/test_run_daily_shadow.py -q
PYTHONPATH=src ./.venv/bin/python scripts/check_docs.py
bash -n scripts/run_daily_shadow.sh
```

The targeted suite passed 104 tests. Replay usage is in the [command reference](../reference/commands.md#local-final-review-evidence). The validation helper retained at `/private/tmp/validate_review_u3.py` captures the copied-stage parity procedure; it is not an operator repair command.
