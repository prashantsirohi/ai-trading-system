# Stage: fundamental_discovery

- **Purpose:** Classify cached accounting evidence into versioned fundamental thesis families and project it against current market context.
- **Audience:** Operators, developers, reviewers.
- **Last verified:** 2026-08-15
- **Source of truth:** `src/ai_trading_system/pipeline/stages/fundamental_discovery.py` and `src/ai_trading_system/domains/fundamentals/discovery.py`.

---

## Purpose

Provide a shadow-only fundamental discovery lane with quarterly accounting classification and daily market-context projection.

## Entrypoints

`FundamentalDiscoveryStage.run` is scheduled by `ai-trading-pipeline --fundamental-discovery-mode compare|shadow`. The stage has no standalone network entrypoint.

## Input data

The stage reads promoted `ranked_universe`, optional weekly-stage, pattern and Investigator artifacts, local `screener_financials.db`, and local fundamentals readmodels. It reads no provider endpoint.

## Runtime contract

`--fundamental-discovery-mode off|compare|shadow` controls this optional stage; `off` is the default. `compare` writes cache/projection artifacts but never writes the opportunity registry. `shadow` also makes eligible fundamental projections available to the optional `opportunities` shadow stage. Neither mode changes rank, `final_candidates.csv`, publishing, execution, or broker state.

The stage reads only promoted local artifacts and the local Screener/readmodel stores. It never invokes `ai-trading-fundamentals-sync` or any network provider. Missing, stale, unresolved, future-dated, or unsupported financial-company evidence produces exclusions and cannot open an episode.

The seven families under `fundamental-discovery-taxonomy-v1` are `QUALITY_COMPOUNDER`, `HIGH_GROWTH_EMERGING`, `EARNINGS_ACCELERATION`, `UNDERVALUED_QUALITY`, `CASHFLOW_BALANCE_SHEET_INFLECTION`, `TURNAROUND_CYCLICAL_RECOVERY`, and `CAPITAL_RETURN_INCOME`. Every family is evaluated independently with explicit gates. The existing aggregate `hard_red_flag` is not an admission veto; each family evaluates its own cash-flow, leverage, growth, and quality requirements.

Rule policy `fundamental-thesis-rules-v1.1` normalizes persisted boolean forms
such as `1`, `1.0`, and `true` consistently. Only an explicit false
`is_not_sme` value produces `SME_INELIGIBLE`; missing or unrecognized legacy
values are not reinterpreted as proof that a security is SME.

## Cadence and caching

The fundamental source hash contains accounting facts, statement basis, source dates, and identity fields. It excludes rank, structural stage, valuation, patterns, and Investigator evidence. An existing `(symbol, exchange, source_data_hash, taxonomy_version, rule_version)` classification is reused; only the daily projection is recalculated.

Daily admission is allowed only for `transition_4_to_1`, `stage_1_basing`, `transition_1_to_2`, and `stage_2_advancing`. Other stages remain visible with `STAGE_BLOCKED`. Missing stage or valuation context produces `DAILY_CONTEXT_INCOMPLETE`.

## Output artifacts

The attempt directory contains:

- `fundamental_thesis_universe.csv`: one projected row per security.
- `fundamental_thesis_evaluations.csv`: seven evaluations per security.
- `fundamental_thesis_exclusions.csv`: blocked, unsupported, stale, missing, and unclassified rows.
- `fundamental_thesis_changes.csv`: source-version or primary-thesis changes.
- `fundamental_thesis_summary.json`: reuse/recompute, coverage, exclusion, DQ, and policy lineage.

All rows retain statement basis, source report/availability dates, source-data hash, and policy versions.

## Main modules

- `pipeline/stages/fundamental_discovery.py`: artifact orchestration, cache lookup, projection, and summaries.
- `domains/fundamentals/discovery.py`: taxonomy gates, hashes, immutable persistence, and sync receipts.
- `domains/opportunities/orchestration/`: typed bundle admission and parallel registry episode handling.

## Process flow

Resolve local inputs, compute each accounting source hash, reuse an identical classification or evaluate all seven families, project current daily context, persist immutable classifications/projections, emit artifacts, and—only in dual shadow mode—hand eligible typed snapshots to opportunities.

## DQ

The stage fails closed for unresolved identity or statement basis, future-dated facts, missing required evidence/freshness, stale sources, unsupported financial models, and incomplete stage/valuation context. One family diagnostic never overrides another family's explicit gates.

## Failure modes

A policy-label/content mismatch fails before stage artifacts or registry writes. Missing stores and per-symbol incomplete evidence produce exclusions. A persistence error rolls back the fundamentals transaction and fails only this optional stage.

## Retry behavior

An exact source hash and policy reuses the immutable classification. A same-day projection insert is idempotent. Operators fix or refresh the source and rerun the optional stage; it never performs a fallback download.

## Downstream consumers

`compare` has no downstream consumer. In `shadow`, the `opportunities` shadow stage may read `fundamental_thesis_universe`; rank, candidates, publisher, execution, and broker integrations do not.

## Persistence and isolation

`$DATA_ROOT/fundamentals.duckdb` owns append-oriented sync receipts, immutable thesis classifications, and daily projections. In registry shadow mode, `$DATA_ROOT/control_plane.duckdb` stores `fundamental_thesis` episodes and append-only `candidate_fundamental_observation` rows. Fundamental bundles are evaluated separately from technical and Investigator-primary bundles, so their episodes do not replace or attach to those families. Existing technical/setup evidence remains responsible for lifecycle promotion beyond investigation.

## Sync schedule

Run `ai-trading-fundamentals-sync` before the evening pipeline: daily during 10 Jan–20 Feb, 10 Apr–15 Jun, 10 Jul–20 Aug, and 10 Oct–20 Nov; weekly outside those windows. The sync command only ingests and refreshes readmodels, then appends a receipt. A failed symbol is recorded as degraded and the last trusted local snapshot remains available.

## Commands

```bash
ai-trading-fundamentals-sync --missing-current-results --allow-download
ai-trading-migrate-control-plane --backup-dir /path/to/verified-backup --from-migration 044 --to-migration 044 --apply
ai-trading-pipeline --run-date 2026-08-15 --fundamental-discovery-mode compare
ai-trading-pipeline --run-date 2026-08-15 --fundamental-discovery-mode shadow --opportunity-registry-mode shadow --local-publish
```
