# Features Domain

- **Purpose:** Compute and store technical indicators + sector relative strength for every (symbol, date), keyed for fast rank-stage lookup.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** [`src/ai_trading_system/domains/features/`](../../src/ai_trading_system/domains/features/), [`src/ai_trading_system/pipeline/stages/features.py`](../../src/ai_trading_system/pipeline/stages/features.py)

---

## Responsibility

Transform OHLCV into a columnar feature store. Maintain incremental updates so a daily run only recomputes what changed.

## Package / module ownership

| Module | Role |
|---|---|
| `service.py::FeaturesOrchestrationService` | Stage orchestration + trust envelope assembly. |
| `feature_store.py::FeatureStore` | Parquet store, indexing, `compute_and_store_features`. |
| `indicators.py` | RSI, MACD, Supertrend, ATR, EMA, VWAP, volume ratio, swing lows. |
| `compute_features_batch.py` | Vectorized batch computation. |
| `sector_rs.py::compute_all_symbols_rs` | Sector relative strength. |
| `universe_index.py` | Symbol universe filtering. |
| `pattern_features.py` | Pattern-derived features (consumed by ranking). |

## Public contracts

- **Feature store layout:** `data/feature_store/<symbol_id>/features_<start>_<end>.parquet` (columnar).
- **Snapshot metadata** persisted via service — describes rows computed, indicators present, computation time.
- **Sector RS artifacts** consumed by ranking/sector_dashboard.

## Storage ownership

- `data/feature_store/` — sole writer.
- Snapshot rows in control_plane DuckDB (verify table name when writing `reference/database_schema.md`).

## Dependencies

- Reads `data/ohlcv.duckdb`.
- Reads trust envelope from ingest stage context (degraded data may flow through with flags).

## Extension points

- New indicator: add to `indicators.py`, wire into `compute_features_batch.py`, update DQ in `pipeline/dq/`.
- New feature type: extend `FeatureStore` schema and bump snapshot metadata.

## Known gaps

- No central feature registry yet; new indicators must be threaded through `compute_features_batch.py` and the rank stage. See [`docs/development/adding_new_factor.md`](../development/adding_new_factor.md).

## See also

- [`docs/stages/features.md`](../stages/features.md)
- [`docs/reference/ranking_factors.md`](../reference/ranking_factors.md)
