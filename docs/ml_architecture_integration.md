# ML Architecture Integration Proposal

## Intent

This document upgrades the proposed `ml_module/` design into an architecture that fits the current trading system instead of creating a parallel stack.

The existing repo already has the right macro-shape:

- `collectors/` for ingestion
- `features/` for deterministic feature computation and storage
- `analytics/` for ranking, ML, backtesting, and monitoring
- `research/` for offline experimentation and training entrypoints
- `run/` for production orchestration
- `analytics/registry/` plus `sql/migrations/` for governance and lineage

The recommendation is therefore:

- do **not** add a new top-level `ml_module/`
- evolve the current `analytics/`, `research/`, `run/`, and governance layers
- keep live trading on the current technical ranker until ML clears shadow and approval gates

## Why The Original Proposal Should Not Be Added As-Is

The original proposal is directionally good, but it duplicates responsibilities the repo already owns:

- `config.py` overlaps with `core/`, `config/`, and domain-layout helpers
- `data_loader.py` overlaps with `collectors/`, DuckDB storage, and the prepared dataset builder
- `feature_engineering.py` overlaps with `features/` and `analytics/training_dataset.py`
- `training_pipeline.py` overlaps with `research/train_pipeline.py`
- `backtest_analyzer.py` overlaps with `analytics/backtester.py`, `analytics/rank_backtester.py`, and shadow monitoring
- `inference_engine.py` belongs in `analytics/` and `run/stages/`, not as a separate standalone package

Adding the module unchanged would increase coupling, create feature drift risk, and split governance across two implementations.

## Design Principles

The upgraded architecture should follow these industry practices:

1. One feature definition for both research and production.
2. Strict time-aware validation only.
3. Prepared, versioned datasets for every training run.
4. Model registration, approval, deployment, and rollback as first-class workflows.
5. Shadow deployment before any production influence.
6. Artifact-driven pipelines, not ad hoc notebook state.
7. Deterministic ranking fallback when ML is unavailable.
8. Separation of concerns between:
   - data ingestion
   - deterministic feature computation
   - labeling and training dataset assembly
   - model training and calibration
   - inference and ranking overlay
   - backtesting and post-trade evaluation
   - governance and monitoring

## Proposed Target Architecture

```text
ai-trading-system/
  analytics/
    alpha/
      __init__.py
      feature_schema.py        # canonical ML feature contract
      labeling.py              # target definitions, horizons, meta-labels
      dataset_builder.py       # joins OHLCV + stored features + research enrichments
      training.py              # train/validate/calibrate models
      scoring.py               # batch scoring for research and operational use
      blending.py              # technical + ML blending policies
      drift.py                 # feature drift / prediction drift checks
      monitoring.py            # realized outcome tracking, hit rate, decile stats
      policy.py                # promotion guardrails and deployment rules
    registry/
      __init__.py
      store.py                 # existing governance store, extended
    ml_engine.py               # compatibility wrapper during migration
    lightgbm_engine.py         # compatibility wrapper during migration
    shadow_monitor.py          # keep, but migrate internals to alpha/*
    ranker.py                  # remains the primary production baseline
  research/
    prepare_training_dataset.py
    train_pipeline.py
    eval_pipeline.py
    backtest_pipeline.py
    shadow_monitor.py
  run/
    stages/
      ingest.py
      features.py
      rank.py                  # baseline rank + optional ML overlay
      publish.py
      monitor.py               # optional post-close monitoring stage later
  features/
    feature_store.py           # deterministic feature persistence
    indicators.py
    compute_all_features.py
  sql/
    migrations/
      005_ml_datasets.sql
      006_prediction_monitoring.sql
      007_model_guardrails.sql
```

## Recommended Component Boundaries

### 1. Deterministic Feature Layer

Owner:

- `features/`
- parts of `analytics/training_dataset.py`

Responsibilities:

- compute reusable technical and market-structure features
- store them in DuckDB / parquet with snapshotability
- guarantee offline/online parity

Rules:

- no model-specific transforms here
- no labels here
- no target leakage from future bars

Examples:

- RSI, ADX, ATR, Bollinger, ROC, delivery, sector-relative measures
- rolling liquidity, volatility, breakout context

### 2. Dataset Assembly Layer

Owner:

- new `analytics/alpha/dataset_builder.py`

Responsibilities:

- join `_catalog` with feature tables and research enrichments
- create point-in-time correct training frames
- enforce a canonical feature schema
- produce versioned dataset artifacts and metadata

Inputs:

- OHLCV snapshot ref
- feature snapshot ref
- label config
- universe definition

Outputs:

- prepared dataset parquet
- metadata JSON
- registry row for reproducibility

Best practices:

- every dataset gets a stable `dataset_ref`
- every dataset stores `feature_schema_hash`
- every row is traceable to prediction timestamp and source snapshot

### 3. Labeling Layer

Owner:

- new `analytics/alpha/labeling.py`

Responsibilities:

- define future-return targets by horizon
- define binary, ranking, and meta-label targets
- centralize label thresholds and holding-period logic

Recommended label types:

- `forward_return_{horizon}`
- `target_top_quantile_{horizon}`
- `target_positive_return_{horizon}`
- `meta_label_trade_quality`

Best practices:

- label definitions are versioned
- backtests and training consume the same label code
- cross-sectional targets and absolute targets are kept distinct

### 4. Training And Validation Layer

Owner:

- new `analytics/alpha/training.py`
- driven by `research/train_pipeline.py`

Responsibilities:

- model fitting
- walk-forward validation
- calibration
- hyperparameter search
- feature importance and diagnostics

Recommended model order:

- baseline: LightGBM / XGBoost
- optional second stage: logistic calibration or meta-label model
- avoid LSTM/transformer complexity until the tabular baseline saturates

Validation best practices:

- rolling or expanding walk-forward splits
- validation windows based on date, never random shuffles
- purge / embargo around target horizon if needed
- compare against technical baseline, not only against previous ML versions

Required evaluation outputs:

- AUC / PR-AUC for classification
- precision@top-decile
- average forward return of top bucket
- hit rate
- drawdown and turnover in backtest
- regime-sliced metrics
- calibration plots and Brier score when probabilities are used

### 5. Model Registry And Promotion Layer

Owner:

- `analytics/registry/store.py`
- `sql/migrations/`

Existing tables already provide a base:

- `model_registry`
- `model_eval`
- `model_deployment`

Recommended additions:

- `dataset_registry`
- `prediction_log`
- `shadow_eval`
- `drift_metric`
- `promotion_gate_result`

Promotion policy:

- `research` training only writes candidate models
- candidate must pass offline metric thresholds
- candidate must run in shadow mode for a minimum observation window
- only approved models may influence operational ranking

### 6. Inference And Ranking Overlay

Owner:

- new `analytics/alpha/scoring.py`
- new `analytics/alpha/blending.py`
- integrated into `run/stages/rank.py`

Responsibilities:

- load the approved operational model
- score the current universe using the same canonical feature schema
- optionally blend ML probabilities with the technical composite score

Recommended deployment modes:

1. `baseline_only`
2. `shadow_ml`
3. `blend_ml`
4. `ml_primary`

Recommended initial production mode:

- keep `baseline_only` or `shadow_ml` as default
- use `blend_ml` only after governance approval

Blend policy example:

- technical score remains the primary signal
- ML acts as a ranking tilt, conviction score, or veto layer
- position sizing can use model confidence only after calibration quality is proven

### 7. Monitoring Layer

Owner:

- `analytics/shadow_monitor.py`
- new `analytics/alpha/monitoring.py`
- optional later `run/stages/monitor.py`

Responsibilities:

- log daily predictions
- attach realized returns after horizon maturity
- compare baseline vs ML vs blended outcomes
- monitor drift and degradation

Core monitoring metrics:

- prediction coverage
- feature null rate
- feature drift PSI / KS
- score distribution drift
- top-decile realized return
- hit rate by horizon
- decile spread
- live-vs-research schema mismatch

## Control Plane Extensions

Extend the existing DuckDB control plane instead of creating a separate ML metadata store.

### Recommended Tables

#### `dataset_registry`

Tracks:

- dataset ref
- dataset URI
- source domain
- label version
- feature schema hash
- snapshot refs
- date range
- row and symbol counts

#### `prediction_log`

Tracks one row per symbol per scoring event:

- prediction date
- model id
- symbol id
- horizon
- score / probability
- rank
- deployment mode
- feature schema hash

#### `shadow_eval`

Tracks matured live results:

- prediction date
- horizon
- model id
- baseline rank
- ml rank
- blended rank
- realized return
- hit flag

#### `drift_metric`

Tracks:

- run date
- model id
- feature name
- drift statistic
- threshold
- pass/fail

#### `promotion_gate_result`

Tracks:

- candidate model id
- gate name
- result
- measured value
- threshold
- evaluated at

## Integration With Existing Stages

### `ingest`

No ML-specific changes needed beyond maintaining data quality and timestamps.

### `features`

Additions:

- compute any missing deterministic features required by the canonical ML schema
- publish a feature snapshot reference consumable by dataset assembly

Do not:

- compute labels
- compute model-specific transforms that depend on future information

### `rank`

This is the key integration point.

Recommended flow:

1. Produce the existing technical ranking exactly as today.
2. If an approved operational model exists, build the current-universe ML frame.
3. Score symbols with the approved model.
4. Depending on deployment mode:
   - store shadow scores only
   - or create a blended score for downstream artifacts
5. Record prediction rows for later outcome attribution.

Important:

- ranking must succeed even if ML scoring fails
- ML must be a non-blocking overlay at first
- publish artifacts should include baseline and ML/blended columns when available

### `publish`

Possible additions:

- publish shadow summary to dashboard
- publish drift or degradation warnings to operators
- keep external delivery independent from training and monitoring failures

## Proposed Package Mapping From The Original Module

Instead of adding the original files directly:

- `config.py` -> `config/` plus typed config objects in `analytics/alpha/`
- `data_loader.py` -> reuse `collectors/`, DuckDB storage, and dataset builder
- `feature_engineering.py` -> split between `features/` and `analytics/alpha/dataset_builder.py`
- `model_factory.py` -> `analytics/alpha/training.py`
- `training_pipeline.py` -> keep `research/train_pipeline.py`
- `inference_engine.py` -> `analytics/alpha/scoring.py`
- `backtest_analyzer.py` -> keep in `analytics/` and `research/`

## Recommended Runtime Modes

### Research

Purpose:

- training
- offline evaluation
- feature experiments
- backtesting

Data plane:

- `data/research/`
- `models/research/`
- `reports/research/`

### Operational

Purpose:

- daily ranking
- optional ML shadow scoring
- approved model overlay
- outcome monitoring

Data plane:

- `data/operational/`
- `models/operational/`
- `reports/operational/`

Rule:

- research models do not directly score live signals until promoted through registry + deployment approval

## Best-Practice Improvements To Add Immediately

These are the highest-value upgrades for this repo.

### A. Canonical Feature Schema Contract

Create one schema object that defines:

- feature names
- dtypes
- null handling policy
- required lookback window
- supported horizons

This prevents research/operational drift.

### B. Prepared Dataset Registry

Every training run should depend on a registered dataset artifact, not on a fresh ad hoc query.

### C. Walk-Forward Evaluation Standard

Make all official model evaluations use:

- expanding or rolling date windows
- top-decile return analysis
- baseline-vs-ML comparison
- regime breakdown

### D. Calibration Before Live Use

If probabilities are used for ranking or sizing:

- calibrate them
- store calibration metadata
- reject uncalibrated confidence for live sizing

### E. Shadow-First Deployment

Before blending into live ranks:

- run shadow for a fixed window such as 4-8 weeks
- compare against the technical baseline
- require no major drift or degradation

### F. Non-Blocking ML In Production

The live pipeline should still complete when:

- model file is missing
- scoring errors occur
- drift checks fail

In these cases it should:

- fall back to the technical ranker
- emit alerts
- preserve the run as operationally usable

## Phased Adoption Plan

### Phase 1: Formalize The Current ML Path

- create `analytics/alpha/` package
- move schema, dataset assembly, labeling, and scoring logic under it
- keep compatibility wrappers around current `ml_engine.py` and `lightgbm_engine.py`
- register prepared datasets in the control plane

### Phase 2: Tighten Governance

- add dataset, prediction, shadow, and drift tables
- record prediction logs from rank-stage shadow scoring
- add promotion gates

### Phase 3: Blend Into Live Ranking

- extend `run/stages/rank.py` to optionally produce ML overlay columns
- keep technical ranking as fallback
- expose baseline vs shadow vs blended results in the UI

### Phase 4: Add Monitoring And Guardrails

- nightly matured-outcome evaluation
- drift alerts
- automatic model-disable switch if critical checks fail

## Architectural Opinion

For this repository, the best industry-practice design is not a separate ML subsystem.

It is:

- one packaged Python application
- shared deterministic features
- separate research and operational data planes
- governed model lifecycle
- shadow-first ML deployment
- technical ranker retained as baseline and fallback

That gives strong reproducibility and operational safety without the complexity of microservices or a second pipeline stack.
