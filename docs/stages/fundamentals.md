# Stage: fundamentals

- **Purpose:** Optional enrichment of rank artifacts from the canonical Screener.in SQLite fundamentals store — adds tiering, watchlist bucketing, hard red-flag flags, and industry-level scores.
- **Audience:** Operator, developer, debugging
- **Last verified:** 2026-05-16
- **Source of truth:**
  - `src/ai_trading_system/pipeline/stages/fundamentals.py` (`FundamentalsStage`)
  - `src/ai_trading_system/domains/fundamentals/enrich_rank.py`
  - `src/ai_trading_system/domains/fundamentals/enrich_sector_dashboard.py`
  - `src/ai_trading_system/domains/fundamentals/{scoring,import_screener,trends,industry_scoring,industry_trends,industry_schema,schema,contracts}.py`
  - `src/ai_trading_system/pipeline/orchestrator.py` (gating, lines 271 / 578 / 775)

---

## Purpose

`fundamentals` is an **optional** stage that runs between `rank` and `candidates`. It enriches the rank attempt directory with a Screener.in scoring snapshot derived from `screener_financials.db`, builds a fundamentals-aware watchlist, and (when industry inputs are present) scores industry groups and enriches the sector dashboard.

It does **not**:

- require live scraping during pipeline runs,
- modify `ranked_signals.csv` in place,
- block the pipeline if its snapshot is missing or stale — it short-circuits to a `skipped_missing_snapshot` summary.

## Entrypoints

- Stage wrapper: `src/ai_trading_system/pipeline/stages/fundamentals.py::FundamentalsStage.run` (no separate `service.py` exists for this stage; logic lives in the wrapper and the enrichment modules).
- CLI: `ai-trading-pipeline --enable-fundamentals` or auto-enabled when `data/fundamentals/fundamental_scores_latest.csv` or `data/fundamentals/screener_financials.db` is present.
- Screener SQLite sync / readmodel scripts: `ai-trading-fundamentals-sync` and `ai-trading-fundamentals-refresh-readmodels`.
- Legacy manual CSV import remains available via `python -m ai_trading_system.domains.fundamentals.import_screener …`.

## Input data

- `data/fundamentals/screener_financials.db` (canonical SQLite source; overridable via `--screener-financials-db-path` / param `screener_financials_db_path`).
- `data/fundamentals/exports/*_screener.xlsx` (local Screener Excel exports used by sync).
- `data/fundamentals/fundamental_scores_latest.csv` (generated compatibility readmodel; overridable via `--fundamental-scores-path` / param `fundamental_scores_path`).
- `data/fundamentals/fundamental_trends_latest.csv` (optional; default from `DEFAULT_TRENDS_PATH`).
- `data/fundamentals/industry_fundamental_scores_latest.csv` (optional).
- `data/fundamentals/industry_fundamental_trends_latest.csv` (optional).
- `data/fundamentals/catalyst_scores_latest.csv` (optional).
- Rank attempt directory: `data/pipeline_runs/<run_id>/rank/attempt_<n>/` — must contain `ranked_signals.csv` (`fundamentals.py:31`).
- `data/fundamentals.duckdb` is the canonical analytical fundamentals store for mirrored Screener facts, company growth features, insight tags, sector earnings leadership, and universe valuation-cycle readmodels.

Operator workflow for syncing Screener Excel exports into the canonical DB:

```bash
ai-trading-fundamentals-sync \
  --db-path /Volumes/MacData/Trading/data/fundamentals/screener_financials.db \
  --exports-dir /Volumes/MacData/Trading/data/fundamentals/exports

ai-trading-fundamentals-refresh-readmodels \
  --db-path /Volumes/MacData/Trading/data/fundamentals/screener_financials.db
```

## Output artifacts

Under `data/pipeline_runs/<run_id>/fundamentals/attempt_<n>/`:

| Artifact type | File | Notes |
|---|---|---|
| `fundamental_scores` | `fundamental_scores.csv` | Copy of the source snapshot (`fundamentals.py:81`). |
| `watchlist_candidates` | `watchlist_candidates.csv` | Output of `enrich_rank_artifacts`. Consumed by `candidates` stage. |
| `fundamental_summary` | `fundamental_summary.json` | Status, snapshot date, staleness, tier counts, bucket counts, hard red-flag count, warnings. |
| `industry_fundamental_scores` (optional) | `industry_fundamental_scores.csv` | Present only when industry input file exists. |
| `industry_fundamental_trends` (optional) | `industry_fundamental_trends.csv` | Present only when industry trends input file exists. |
| `sector_dashboard_enriched` (optional) | Written into the **rank** attempt dir as `sector_dashboard_enriched.csv` | `fundamentals.py:147`, registered as a `fundamentals` artifact. |
| `company_insight_tags` (optional) | `company_insight_tags.csv` | Derived turnaround / great-result / compounder tags from `fundamentals.duckdb`. |
| `sector_earnings_leadership` (optional) | `sector_earnings_leadership.csv` | Sector growth, breadth, margin, and insight-count leadership metrics. |
| `universe_valuation_daily` (optional) | `universe_valuation_daily.csv` | Universe PE, PE moving average/median bands, percentiles, and loss-market-cap context. |
| `valuation_cycle_features` (optional) | `valuation_cycle_features.csv` | Market-cycle signals derived from universe valuation history. |

Skip path: when the scores CSV is missing, the stage writes only `fundamental_summary.json` with `status="skipped_missing_snapshot"` (`fundamentals.py:44`–`68`).

## Main modules

- `pipeline/stages/fundamentals.py` — wrapper, resolves input paths, copies snapshots, calls enrichers, builds summary.
- `domains/fundamentals/enrich_rank.py::enrich_rank_artifacts` — joins scores + trends + (optional) industry + catalysts onto ranked symbols and produces watchlist buckets.
- `domains/fundamentals/enrich_sector_dashboard.py::enrich_sector_dashboard` — joins industry scores onto `sector_dashboard.csv`.
- `domains/fundamentals/scoring.py` — composite score, tier assignment, hard red-flag detection.
- `domains/fundamentals/screener_store.py` / `screener_sync.py` / `screener_readmodels.py` — SQLite storage, Excel sync, and generated score/trend readmodels.
- `domains/fundamentals/trends.py` — period-over-period deltas.
- `domains/fundamentals/import_screener.py` / `import_screener_industries.py` — manual CSV importers (normalize symbols, persist snapshots).
- `domains/fundamentals/{industry_scoring,industry_trends,industry_schema}.py` — industry-group scoring + labelling.

## Process flow

1. Resolve all input paths (`_resolve_scores_path` / `_resolve_trends_path` / `_resolve_catalysts_path` / `_resolve_industry_*`, `fundamentals.py:234`–`266`).
2. If `fundamental_scores` snapshot is missing, attempt to regenerate it from `screener_financials.db`; if still missing, write `fundamental_summary.json` with `status="skipped_missing_snapshot"` and return.
3. Read scores; compute `snapshot_date` (first non-null `screener_snapshot_date` / `snapshot_date`) and `stale_days = run_date − snapshot_date` (`fundamentals.py:276`–`293`).
4. Append warning if snapshot date is missing or `stale_days > fundamental_max_stale_days` (default **135**).
5. Copy `fundamental_scores.csv` into the attempt dir; call `enrich_rank_artifacts(...)` to produce `watchlist_candidates.csv` and `EnrichmentMetrics`.
6. Tier counts and hard-red-flag counts computed from the scores frame (`fundamentals.py:99`–`112`).
7. If industry scores file exists: copy it, register the artifact, then call `enrich_sector_dashboard` and write `sector_dashboard_enriched.csv` into the **rank** attempt dir.
8. If industry trends file exists: copy it and register the artifact.
9. Build `fundamental_summary.json` (status `completed`) with all counts, warnings, and statuses. Return `StageResult(artifacts=[watchlist, scores, *industry_artifacts, summary])`.

## DQ / trust gates

- **Snapshot freshness** — soft gate. Warns when `stale_days > fundamental_max_stale_days` (default 135 in `fundamentals.py:41`). Never fails the stage.
- **Snapshot presence** — soft gate. Missing snapshot results in `skipped_missing_snapshot` status; downstream stages must tolerate missing `watchlist_candidates`.
- **Hard red flags** — counted (not rejected) at this stage; downstream `candidates` stage routes flagged symbols into the `AVOID_RED_FLAG` group.
- Tier values produced by `scoring.py` (per legacy doc): `A`, `B`, `C`, `Reject`. Confirm in `domains/fundamentals/scoring.py` before quoting numerical thresholds.

## Failure modes

- Missing scores snapshot → soft skip with warning.
- Industry scores read error → recorded as `industry_status="error"`, warning appended, continues with non-industry path (`fundamentals.py:122`).
- `enrich_sector_dashboard` exception → warning appended, continues (`fundamentals.py:151`).
- `enrich_rank_artifacts` exception (e.g., malformed snapshot) → propagates and fails the stage.

## Retry behavior

- The stage has no internal task-level resumability; a retry re-runs the full enrichment.
- Because `enable_fundamentals` may be auto-derived from snapshot presence (`orchestrator.py:578`), a missing-snapshot retry will continue to emit a `skipped_missing_snapshot` summary until the operator imports a new CSV.

## Downstream consumers

- [`candidates`](./candidates.md) — reads `watchlist_candidates` to drive the fundamental bonus / `AVOID_RED_FLAG` / `FUNDAMENTAL_IMPROVER` grouping.
- `sector_dashboard_enriched.csv` is read by downstream sector views in the FastAPI UI and publish channels.
- `fundamental_summary.json` is surfaced by the FastAPI `fundamentals` router and reflected in `dashboard_payload.json` augmentation.

## Commands

```bash
# Sync downloaded Screener Excel files and refresh derived scoring CSVs.
ai-trading-fundamentals-sync

# Refresh only derived scoring/trend CSVs from SQLite.
ai-trading-fundamentals-refresh-readmodels

# Run the pipeline with fundamentals explicitly enabled.
ai-trading-pipeline --run-date <yyyy-mm-dd> --enable-fundamentals

# Manually enrich a specific rank attempt without running the stage wrapper.
python -m ai_trading_system.domains.fundamentals.enrich_rank \
  --rank-dir data/pipeline_runs/<run_id>/rank/attempt_1 \
  --fundamental-scores data/fundamentals/fundamental_scores_latest.csv \
  --run-id <run_id>
```

> **Status:** This stage is part of the canonical 11-stage `ai-trading-pipeline` (`PIPELINE_ORDER`, `orchestrator.py:41`) but is registered as **optional** (`OPTIONAL_STAGES`, `orchestrator.py:44`). The legacy `ai-trading-daily` 5-stage wrapper does not invoke it.
