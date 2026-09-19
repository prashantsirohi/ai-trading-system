# U1 review policy contract and source mapping

- **Purpose:** Define the implemented pure review policy, input obligations, source mapping and remaining integration evidence for U1.
- **Audience:** Opportunity, ranking and publishing maintainers; operator reviewing the shadow policy.
- **Last verified:** 2026-09-13
- **Source of truth:** `src/ai_trading_system/domains/opportunities/review_policy.py` and `tests/domains/opportunities/test_review_policy.py`. Source producers are identified below.
- **Status:** Pure policy and U2 shadow pipeline adapters implemented; no publisher or execution consumer. U3 is accepted for an NSE shadow pilot, with BSE calendar support and future real F-qualified cohorts still open. See [integration and validation](u2_u3_final_review_validation.md).

## Boundary and version

This implements the first code slice of [U1](stage_universe_final_review_plan.md). `evaluate_review` evaluates one normalized listing; `ordered_review_list` orders one session's selected listings. Neither function opens a database, reads a file, registers a policy, emits an artifact, changes rank, transitions a candidate, publishes a message or grants execution eligibility.

`stage-universe-review-v1` is the initial review-only specification. The frozen policy's canonical JSON produces a SHA-256 content hash included in every result. Future semantic changes require a successor version; U2 registers this and the independent adapter policy in the existing policy registry. This is an implementation choice for shadow evaluation, not an accepted economic rule or primary-review cutover.

## Input obligations

All input dataclasses are immutable. The boundary expects canonical explicit exchange (`NSE` or `BSE`), uppercase symbol, native finite numbers, explicit booleans and timezone-aware availability/decision timestamps. Adapters must normalize raw CSV encodings; strings such as `"True"` cannot pass trust or fundamental admission checks. No implicit NSE fallback is provided.

Every evidence stamp binds exchange, symbol, evidence session, availability timestamp, source policy and hash. Cross-listing evidence, missing lineage, future session/availability and naive availability fail that evidence source. Market, pattern, fundamental daily projection, setup and rank must match the exact decision market session. A reused prior-session rank becomes missing rank context, not a current score. Stage has a separate maximum age of ten exchange-market sessions; its source date and availability must still precede the decision.

U2 must validate completed/promoted attempts, verify source hashes, retain each run/attempt identity, resolve corrections as known at the cutoff, and compute stage age using an independently checked exchange calendar. A nonnegative age supplied by a caller is not a calendar audit. Composite market/setup evidence must bind every contributing source and its latest availability; a single source's timestamp cannot conceal a later input. Raw source records remain available with the result.

`has_open_position` is an explicit caller-owned boolean from a separately reconciled ledger scope. The result retains it regardless of opportunity exclusion; the policy does not discover or certify all positions.

## Universe rules

Required: verified listing identity, trusted market data, resolved stage governance, supported `weekly-stage-v2` evidence and known locked/provisional status. Preserve stage status and transition in output. Both locked and provisional classifications may support **review** membership; this does not satisfy normal-entry locking rules.

Use at least 180 observed bars, close at least INR 20 and a broad-universe liquidity percentile at least 0.20. Liquidity is the existing pattern context's latest close-times-volume percentile (average rank); it is not a 20-day median turnover percentile. U2 must preserve the broad cross-section before the stage filter. These reuse initial scanner thresholds; they do not redefine liquidity or broad rank inputs.

- Governed S2 passes the structural universe check.
- Governed S1 must pass all late-base checks below.
- S3/S4, failed liquidity and fewer than 180 bars are excluded with reasons.
- Unknown/stale stage, unresolved governance, malformed market inputs or missing required late-base metrics produce evidence exceptions.
- Normalize transitions separately. An S1→S2 transition must have current S2; it cannot override S4. The v1 normalized transition vocabulary is NONE, S1_TO_S2, S4_TO_S1, S2_TO_S3 and S3_TO_S4. Other values require explicit adapter resolution and otherwise fail closed.
- A technical Stage-2 label cannot replace governed stage. S2 with explicit failed technical Stage-2 evidence is deferred for setup review; other disagreements remain visible as reasons.

### Late Stage 1 formulas and bounds

Reuse the underlying definitions of `research/pattern_lane_calibration/harness.py:_stage1_metrics` and its frozen `Stage1StructurePolicy`, through a governed adapter. The normalized contract uses the following fields; no pattern detection success is needed to calculate them.

| Field | Formula/window | Inclusive passing bound |
|---|---|---|
| `close_to_sma150_ratio` | Latest adjusted close / same-basis SMA150 | 0.85 to 1.15 |
| `sma150_slope_pct` | Existing producer's 20-session SMA150 percentage slope | −2 to +2 percentage points |
| `base_depth_pct` | 100 × (max high − min low) / max high over last 65 bars | 0 to 35 |
| `range_contraction_ratio` | Median high-low of latest 20 bars / preceding 20 bars | 0 to 0.90 |
| `pivot_distance_pct` | 100 × max(0, 65-bar max high − latest close) / max high | 0 to 10 |
| `return_delta_pct` | 100 × [(close / close 20 sessions ago − 1) − (close / close 60 sessions ago − 1)] | At least 0 |
| `volume_dry_up_ratio` | Median volume of latest 20 bars / preceding 20 bars | 0 to 0.90 |
| `close_to_sma200_ratio` | Latest adjusted close / same-basis SMA200 | At least 0.85 |
| `sma200_slope_pct` | Existing producer's 20-session SMA200 percentage slope | At least −1 |

The existing producer calls the return-delta metric `rs_trend_delta_pct`; it compares the stock's own returns and is **not benchmark-relative RS**. It is named accurately at this boundary. Missing or nonfinite measurements cannot pass. Required averages/slopes must exist even after the 180-bar floor; the floor does not authorize shorter substitute averages. U2 must verify complete windows, valid positive denominators, price basis and quarantine propagation before supplying these metrics. Weekly base metrics are different windows/formulas and cannot substitute for daily metrics by matching similar column names.

## Independent lane qualification

Pattern requires current-session `KNOWN` evidence, explicit `FRESH` status, supported `pattern-lane-r0-policy-v1`, unique nonempty signal IDs, and at least one bullish `watchlist` or `confirmed` signal from the frozen family allowlist. Inspect all linked signals rather than just the normalized primary signal. `NONE`, `NOT_ELIGIBLE`, scanner `ERROR`, missing, stale and unsupported policy remain separate non-qualifying states. Contradictory known/empty or none/nonempty receipts fail.

The allowlist is cup/handle, round bottom, double bottom, flag, high tight flag, ascending/symmetrical triangle, ascending base, VCP, flat base, Stage-2 reclaim, Darvas box, pocket pivot, inside-week breakout, three-weeks-tight and inside-day. These use their exact existing snake-case family IDs. Head-and-shoulders suppression and early IPO families cannot supply P membership in v1.

Evidence classes `evidence_supported`, `evidence_supported_smaller_sample`, `evidence_supported_low_volume`, `observational`, `negative_evidence` and `insufficient_evidence` are retained as explicit annotations. They describe research evidence, not the bullish/bearish direction. This review-only policy admits technical hypotheses without asserting they are effective strategies; negative/insufficient labels remain visible and must not be portrayed as validated setups. Unknown or suppression-only classes cannot qualify. No score threshold or P+F bonus is invented.

Fundamental requires current-session projection under **`fundamental-thesis-admission-v1.1`**, usable standalone/consolidated basis, accounting hash and availability date no later than the session and no more than 550 calendar days old. It additionally requires classification `QUALIFIED`, a known primary thesis from the existing seven-family taxonomy, explicit true daily admission and no blockers. Accounting success cannot override daily context exclusions. Existing v1 artifacts remain unsupported by this new policy even when their stored admission flag is true; do not relabel them v1.1.

Selection is eligible universe AND (P OR F). Retain P+F/P_ONLY/F_ONLY and each independent lane result. P-success/F-missing differs from P-success/F-evaluated-none. Investigator is not a membership input. Source-wide outages and population denominators belong to U2's summary, not inference from the number of qualifying rows.

## Readiness and ordering

Readiness consumes independent, current technical evidence; it does not read canonical candidate state. F-only may reach setup review when valid independent setup evidence exists.

1. Missing/stale/future setup source produces an evidence exception. An explicit current `NONE` scan produces developing watch; absence cannot be turned into `NONE`.
2. Recorded setup/sector/risk blockers defer the listing.
3. A watchlist/confirmed setup needs finite positive invalidation strictly below its trigger. Close at or below invalidation defers it.
4. Governed extension risk must be LOW. MEDIUM/HIGH defer; UNKNOWN is an evidence exception. U2 supplies a separately frozen `review-setup-extension-v1`: max(0, close / selected trigger − 1) × 100, LOW through 5%, MEDIUM through 10%, HIGH above 10%. It is review-only and does not change operational extension policy.
5. Governed S2 with explicitly contradictory technical S2 evidence defers.
6. A watchlist state or close below trigger remains developing watch. Otherwise a confirmed setup with low extension reaches setup review.

U2 must bind trigger, invalidation, extension and blockers to the same chosen setup and as-of context; it must not mix the best price from unrelated signals. A completed no-setup assessment is distinct from a missing signal file. The U2 adapter chooses confirmed before watchlist, then descending priority and pattern score, then signal ID; all setup prices and extension bind to that signal. It currently uses linked pattern signals as the technical setup source, including for F-only listings; other independent setup producers are not integrated.

Order setup review, developing watch, then defer; within each group sort by descending current upstream rank, missing rank last, then exchange/symbol. Do not average lane scores. Duplicate listings, mixed sessions or mixed decision cutoffs raise errors; different exchanges remain separate identities. Qualified evidence exceptions retain `selected=True` in the full per-listing assessment, but have no ordinal priority and are omitted from `ordered_review_list`. U2 must expose them in the exception view and summary. No top-N backfill exists.

## Producer mapping and U2 prerequisites

| Normalized input | Current source | Integration requirement |
|---|---|---|
| Governed stage/status/transition | `weekly_stage/weekly_stock_stage_universe`: effective_stage, stage_status, stage_transition, classifier_version, source_week_end | Exchange-aware identity join; correction/availability reconciliation; normalize labels without conflating transitions |
| Market trust, identity and history | Existing trust/DQ, master and point-in-time OHLCV coverage | Do not infer trust from presence of a pattern or a rank score; attach all source lineage |
| Broad liquidity, late-base metrics | Pattern calibration context builder / adjusted OHLCV | Normalized assessments do not export these measurements. Reuse/recompute on trusted data; no weekly-field substitution or filtered-universe percentile |
| Pattern state and signal IDs | `pattern_lane_assessments` | Preserve raw receipt, date, source version and evidence hash |
| Pattern direction/family/state | `pattern_lane_scan` rows linked through exact signal IDs | Join all rows by exchange/symbol/session; validate receipt counts and reject missing/duplicate signal links |
| Fundamental classification and daily state | `fundamental_thesis_universe` | Map classification_status, primary_thesis, admission_eligible, admission_blockers_json, statement_basis, source_available_at, source_data_hash and admission_version |
| Trigger/invalidation | Full pattern signal breakout_level or watchlist_trigger_level and invalidation_price; independently governed technical setup when available | Deterministic setup selection with source binding; normalized assessment alone is insufficient |
| Extension | Existing source-owned extension_risk / extension_risk_level (Investigator adapter currently consumes these) | Do not require Investigator membership; unavailable extension is unknown. Freeze an additional source contract before deriving a replacement |
| Rank | Promoted full `ranked_universe` rank context | Exact session, explicit score, full-universe context; no shortlist-only coverage or substitution of Investigator final_score |
| Position flag | Existing cycle/account/mode-scoped position reconciliation | Preserve the separate position queue and complete ledger denominator |

The orchestrator currently places `opportunities` after `fundamental_discovery`; that remains the proposed U2 artifact owner. The current opportunity stage receives normalized pattern assessments and fundamental projections, but this evaluator additionally requires full signal links, broad rank and market context. Do not wire it to an incomplete convergence row and default the missing fields to pass.

## Real source walkthrough — read-only 2026-09-13

Inspected completed/promoted artifacts for run `shadow-2026-09-11-112408` through a read-only control-plane handle; all file bytes matched registered SHA-256 hashes:

| Artifact | Rows | SHA-256 |
|---|---|---|
| weekly_stock_stage_universe | 1,308 | f49af956b6682780312be12a314e8849eadaec75f366a090df1f14338653d373 |
| pattern_lane_assessments | 1,808 | de713dcf87d3c3fd84812e4d533cb19b7a8b8e809bbac3da63ae40a22f88a343 |
| fundamental_thesis_universe | 503 | 2dbfef0510f1c0ad24d0970c0650c78d478957d6e86c64478310d1f23a729ddc |

The linked `pattern_lane_scan` was also hash-verified and contained 436 signal rows, including bullish/bearish directions and watchlist/confirmed states. These counts are producer populations, not final-review counts.

| Real listing | Recorded evidence | Contract implication, not a completed new-policy result |
|---|---|---|
| NSE:20MICRONS | Locked governed S2; P NOT_ELIGIBLE; absent F projection | Stage and lane membership are separate; no assumed F failure or qualification |
| NSE:360ONE | Locked S1; P NOT_ELIGIBLE | S1 label alone cannot establish a late base; required daily metrics must be obtained |
| NSE:AARTIIND | Locked S2; bullish flag watchlist with negative_evidence; producer F QUALIFIED/HIGH_GROWTH_EMERGING | Technical direction and research support differ; retain the negative-evidence annotation |
| NSE:ABSLAMC | Locked S2; known Darvas pattern; F UNSUPPORTED_FINANCIAL_MODEL | Unsupported F is distinct from an evaluated non-qualifying accounting thesis |
| NSE:ADANIENSOL | Locked S2; P NONE; producer F QUALIFIED/EARNINGS_ACCELERATION | F-only hypothesis must remain possible without fabricating P membership |
| NSE:EQUITASBNK | Locked S2; primary head_shoulders suppression; F unsupported financial model | KNOWN pattern membership alone cannot establish positive P qualification |
| NSE:AAVAS | Locked S4 | Outside new-opportunity universe even if another lane or held-position queue references it |

This run's F projections use admission v1, so they cannot qualify under the new v1.1 requirement. Later repaired September 11 F attempts exist in another run; they were not spliced into this historical source set. These are source-contract walkthroughs, not a new-policy market replay, forward evidence or a canary. No live store was changed.

## Verification and open acceptance evidence

The isolated contract tests cover stage/transition precedence, every late-base bound, failed trust/history/liquidity, stale/future and cross-listing sources, suppression vs positive secondary signals, F basis/version/freshness/admission, F-only readiness, invalidation, extension, deterministic ordering, duplicate sessions/listings, missing rank and preserved position attention. Adapter v2 also covers correction-aware governed terminal-stage ownership and separates trusted broad NSE session authority from NIFTY_50 diagnostic continuity. Fixtures are synthetic unit inputs only; they are never written to operational stores.

U1 acceptance still needs fully sourced late-versus-early Stage 1 and extended/setup-ready walkthroughs after the normalized adapters exist. U2 must freeze deterministic setup selection, confirm extension-source coverage, certify daily metric/calendar inputs and produce the full population/exception summary. Unit tests cannot supply those missing market facts. M1/G1 and M2/G2 remain open.

No persisted feature rebuild or database migration is required. U2 computes review metrics from existing trusted OHLCV. The [U3 evidence record](u2_u3_final_review_validation.md) supersedes the initial no-canary status: copied-source replays and a local-only opportunity-stage canary have now run; the full pipeline was not rerun.

## U2 adapter resolution — 2026-09-13

The prerequisite language above records the original source obligations. U2 implements completed/promoted/hash-checked reads, correction-aware stage reconciliation, explicit missingness, a broad liquidity denominator and full exception artifacts. Its daily metrics require 220 complete bars for SMA200 and its 20-session slope. The technical S2 contradiction check uses close > SMA150 > SMA200, positive SMA200 slope and close within 25% of the 220-bar high; it is an adapter-specific review check, not a claim of exact parity with the operational scanner's longer high window. BSE calendar validation remains unsupported and fails closed. Position coverage is explicitly scoped to registered pre-execution router cycles; it does not certify the complete live ledger. See the [U2/U3 contract](u2_u3_final_review_validation.md) for remaining evidence gates.
