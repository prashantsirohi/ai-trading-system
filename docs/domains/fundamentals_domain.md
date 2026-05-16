# Fundamentals Domain

- **Purpose:** Import Screener.in fundamentals, score them, and enrich rank outputs. **Optional** stage — skipped if Screener credentials are missing.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/domains/fundamentals/`](../../src/ai_trading_system/domains/fundamentals/), [`src/ai_trading_system/pipeline/stages/fundamentals.py`](../../src/ai_trading_system/pipeline/stages/fundamentals.py)

---

## Responsibility

Bring fundamental data (PE, PB, ROE, debt/equity, profit/revenue growth, dividend yield) into the pipeline as an **enrichment** layer. No blocking dependency on technical ranking — when fundamentals are unavailable, ranking proceeds without them.

## Package / module ownership

| Module | Role |
|---|---|
| `service.py::FundamentalsOrchestrationService` | Stage orchestration. |
| `import_screener.py` | Screener.in API client (or CSV importer — verify in code). |
| `scoring.py` | Valuation score computation. |
| `enrich_rank.py` | Merge fundamentals into rank artifacts. |

## Public contracts

Stage artifacts under `data/pipeline_runs/<run_id>/fundamentals/attempt_<n>/`:

- `fundamental_scores.csv` — computed scores
- `fundamental_summary.csv` — enriched rank summary

When enabled, downstream consumers (publish, UI) see fundamentals columns appended to rank artifacts via `enrich_rank.py`.

## Storage ownership

- Stage artifacts only.
- May write a fundamentals snapshot to control_plane DuckDB (**verify table when writing `reference/database_schema.md`**).

## Dependencies

- External: Screener.in (CSV import or API — verify).
- Reads rank artifacts.

## Extension points

- Add a new fundamental factor: extend `scoring.py` and `enrich_rank.py`.
- Add a new fundamental source (e.g., Tijori, Tickertape): subclass the importer in `import_screener.py` or add a sibling.

## Known gaps

- The legacy doc `docs/fundamental_layer.md` (slated for migration) describes the scoring model in more detail. Migrate into this file then archive.
- Stage is currently optional — promotion to mandatory needs a fallback policy when Screener is down.

## See also

- [`docs/stages/fundamentals.md`](../stages/fundamentals.md)
- [`docs/fundamental_layer.md`](../_legacy/archived_2026-05-16/fundamental_layer.md) (to be migrated then archived)
