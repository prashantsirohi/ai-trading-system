# U2 integration and U3 copied-real validation

- **Purpose:** Record the final-review adapter contract, reproducible validation and remaining acceptance evidence.
- **Audience:** Opportunity and publishing maintainers; operator.
- **Last verified:** 2026-09-19
- **Source of truth:** `domains/opportunities/review_sources.py`, `review_projection.py`, `pipeline/stages/final_review.py`, `interfaces/cli/replay_final_review.py` under `src/ai_trading_system/`, their targeted tests, and the captured validation bundle described below.
- **Status:** U2 implemented; U3 accepted for the NSE shadow pilot. The U4 external Sheets pilot is published and awaiting five completed operator reviews. U5 not started. M1/G1 and M2/G2 remain open.

## Implemented boundary

The [U1 policy](u1_review_policy_contract.md) now has a read-only source adapter and an optional artifact materializer inside `opportunities`, after both lanes. The orchestrator's `--final-review-mode shadow` requires registry shadow; its default is `off`. The daily shadow wrapper enables it. Existing ranking, candidate lifecycle, position management and publisher ownership are unchanged. The first pilot was copied manually into the operator workbook; no runtime Sheets or Telegram consumer has been added.

The source adapter requires completed, promoted producer attempts, verifies SHA-256 and registered row counts, preserves producer run/attempt identity and rejects availability after the explicit cutoff. It uses full `ranked_universe`, governed weekly stages, pattern assessments plus exact signal links, fundamental thesis projections and registered router position coverage. Stage reconciliation uses correction-aware history. Missing or invalid evidence remains visible; a later run's fundamental repair is not silently joined to an earlier run's pattern output.

The union of market, master and every source listing is assessed before ordering. Liquidity percentiles use the broad current-session market denominator. The reader opens configured market/control-plane stores read-only and master SQLite with `mode=ro`; it checks DQ, quarantine, provider/trust state, price basis and availability. Late-base calculations require complete 220-bar windows. Trusted broad NSE bhavcopy population is the calendar authority; local holidays define expected weekdays, and NIFTY_50 is diagnostic corroboration. A broad-market hole fails the review, while a missing NIFTY diagnostic session degrades it. The full metric window requires holiday-year coverage. BSE calendar support is absent and remains an explicit exception.

`final-review-adapter-v2` separately fingerprints setup choice, metric window and `review-setup-extension-v1`. It also makes the correction-aware governed terminal payload the owner of review-stage semantics; a hash difference from the pre-correction promoted row is expected after a governed correction and is not itself an exception. The selected bullish allowed setup is ordered by confirmed status, descending priority, descending pattern score, then signal ID. Trigger, invalidation and extension refer to that same setup. Extension is max(0, close/trigger − 1) × 100: LOW ≤5%, MEDIUM ≤10%, HIGH >10%. This review-specific rule does not rewrite operational extension. Raw pattern levels affected by a nonunit adjustment inside the signal window are rejected. Technical S2 contradiction uses the adapter's 220-bar high, not the scanner's longer high window.

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

## Source repair and accepted U3 replay — 2026-09-14

The two disputed sessions each had a complete trusted NSE equity population: 1,781 listings on 2026-08-21 and 1,797 on 2026-09-08. Official NSE all-index close reports supplied exact NIFTY_50 rows for both dates. The master calendar lacked 2025, so all 14 equity-market holidays from the official 2025 NSE circular were added. Before either live-store repair, `masterdata.db` and `ohlcv.duckdb` were backed up under `/Volumes/MacData/Trading/data/backups/final-review-calendar-repair-20260914/`; SQLite integrity was `ok`, and the OHLCV source/copy SHA-256 was `6c4db0c6eed713dc5b12441f36a1175fdd119fb1e0de43dfe5c52be8ecedb977`. The repair added only the 14 holiday rows and two `_index_catalog` rows. No candidate, execution, broker or publisher state changed.

Run `u3-rerun-2026-09-11-1554` rebuilt rank, weekly stage, pattern lane, scan router, fundamental discovery and opportunities on `/private/tmp/review-u3-rerun-KPBjF6`. All six stages completed; opportunity shadow remained degraded because BSE calendar support is intentionally absent. Sources were same-run: rank/fundamental 508 rows each, stage 1,308, pattern assessments 1,808, pattern signals 436 and five position cycles. The review assessed 1,918 listings and ordered 52: 3 `SETUP_REVIEW`, 34 `DEVELOPING_WATCH`, and 15 `DEFER`. All selected rows were `P_ONLY`; the fundamental lane supplied context but no qualifying v1.1 row in this session. This absence is a measured outcome rather than missing same-run evidence.

After inserting the two official index rows into the isolated copy, the calendar result was `PASS`: 220 trusted sessions, holiday coverage for 2025 and 2026, no missing broad or NIFTY diagnostic session, and no future diagnostic version. Two explicit read-only replays produced byte-identical list, universe and summary files with decision-content hash `ae30d985ccd1512e5d10a8723a5024db61beedce4d9d1dcb19ca831aea0bbbaf`. The three copied databases retained their pre-replay hashes. U3 is therefore accepted for an NSE shadow pilot. This is workflow evidence, not a profitability or primary-ranking claim.

## Remaining limits and next work

1. BSE calendar validation remains unsupported, so the combined-exchange summary remains degraded and BSE exceptions must stay visible.
2. The real selected cohort contains no F-only or P+F rows. Those policy paths remain covered by contract tests but need future real cohorts before adoption claims.
3. The U4 local workbook `Final_Review_Pilot_2026-09-11.xlsx` contains the 52 verified rows and operator-status fields. Its 33-column `09_Final_Review_Pilot` tab was added to the external `Stock_Analysis` workbook on 2026-09-19 with frozen identity columns, filters, research links, operator dropdowns and shadow warnings. This was a one-time authorized write, not a recurring publisher.
4. U4 still requires five completed operator reviews against the M1 baseline. The trial is 0/5 at publication. U5 adoption remains gated by M2/G2 measurement.

No persisted feature rebuild or database migration is required. The full copied run regenerated affected producer artifacts; future daily runs use adapter v2 and exact-date index archive ingest. Existing primary rank and publishers remain in place.

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

The final-review targeted suite and index-ingest source tests are recorded with the implementation run. Replay usage is in the [command reference](../reference/commands.md#local-final-review-evidence). Temporary validation helpers under `/private/tmp` are evidence tools, not operator repair commands.
