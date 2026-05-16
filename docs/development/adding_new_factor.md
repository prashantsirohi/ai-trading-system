# Adding a New Ranking Factor

- **Purpose:** Add a new factor to the composite ranking score.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/ranking/factors.py`, `platform/config/rank_factor_weights.json`.

---

## Checklist

### Feature source

- [ ] If the factor needs a new feature column, add an indicator computation to `src/ai_trading_system/domains/features/indicators.py` and wire into `compute_features_batch.py`.
- [ ] If using an existing feature, confirm it's persisted in the feature store.

### Factor implementation

- [ ] Add the factor function to `src/ai_trading_system/domains/ranking/factors.py`. Keep it pure: takes a row / DataFrame, returns a score column.
- [ ] Normalize to a comparable scale (typically 0–100 or z-score). See existing factors for the convention.
- [ ] Add the factor weight to `src/ai_trading_system/platform/config/rank_factor_weights.json`. Weight 0.0 means defined-but-disabled.
- [ ] Wire the factor into `ranker.py::compute_composite_score` so it's included in the weighted sum.

### Output columns

- [ ] Confirm the factor's raw + adjusted columns appear in `ranked_signals.csv` (via `payloads.py` if explicit, or by the ranker's default column emission).
- [ ] If the factor should also feed `perf_tracker`, add a column to `research/perf_tracker/schema.py::RANK_COHORT_DDL` and map it in `backfill.py::RANKED_TO_TRACKER`.

### DQ

- [ ] Add a DQ rule to `pipeline/dq/` if the factor can produce NaN/garbage when an upstream signal is degraded.

### Validation

- [ ] Compute the factor on a backtest window and confirm direction (positive IC for momentum-style; negative for mean-revert).
- [ ] Compare composite score distribution before/after — should not be wildly skewed.

### Tests

- [ ] Unit test the factor function in `tests/rank/`.
- [ ] Integration test that confirms the factor column appears in a ranked artifact.

### Docs

- [ ] Add a row to `docs/reference/ranking_factors.md` (factor name, raw column, transformation, weight, interpretation, caveats).
- [ ] Update `docs/stages/rank.md` if the new factor changes stage behavior.

## See also

- [`docs/reference/ranking_factors.md`](../reference/ranking_factors.md)
- [`docs/stages/rank.md`](../stages/rank.md)
