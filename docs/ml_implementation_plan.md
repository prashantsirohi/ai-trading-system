# ML Implementation Plan

## Goal

Integrate a production-safe ML layer into the existing trading system without disrupting the current operational pipeline.

The plan assumes:

- the current technical ranker remains the live baseline
- ML is introduced first in `research`, then `shadow`, then `blended production`
- all work stays inside the existing repo boundaries

## Target Outcomes

By the end of the implementation, the system should achieve these realistic targets.

### Operational Targets

- keep `ingest -> features -> rank -> publish` stable throughout rollout
- ensure rank stage still succeeds when ML is unavailable
- add approved-model loading and shadow scoring without breaking current outputs
- log predictions and realized outcomes for governance

### Research Targets

- one canonical ML feature schema shared by research and operational scoring
- reproducible prepared datasets with dataset metadata and schema hash
- walk-forward evaluation as the standard training method
- baseline LightGBM/XGBoost model trained on point-in-time correct data

### Governance Targets

- every trained model registered with artifact URI, dataset ref, and schema hash
- every operational prediction traceable to a model id and deployment mode
- shadow evaluation available before any production influence
- promotion gates defined before ML can alter live ranking

### Business-Achievable Targets

These are realistic first-release targets, not stretch goals:

- improve ranking discrimination vs technical baseline in offline top-decile studies
- produce a stable shadow-monitor dashboard for ML vs technical comparison
- enable `blend_ml` mode for a small controlled rollout only after shadow validation

These are not first-phase targets:

- full auto-trading based purely on ML
- deep learning sequence models in production
- intraday live inference or streaming model serving
- automatic retraining without review

## Delivery Plan

## Phase 1: Foundation And Contracts

### Scope

- create `analytics/alpha/`
- define canonical feature schema
- define label schema and horizon config
- centralize dataset assembly logic
- preserve compatibility with current `ml_engine.py` and `lightgbm_engine.py`

### Deliverables

- `analytics/alpha/feature_schema.py`
- `analytics/alpha/labeling.py`
- `analytics/alpha/dataset_builder.py`
- wrapper updates in existing ML modules
- documentation of supported features and targets

### Achievable Target

- one training dataset path used consistently by research workflows
- no duplicate feature definitions between old and new paths for core ML features

### Acceptance Criteria

- prepared datasets can be generated from research data domain
- dataset metadata includes:
  - dataset ref
  - date range
  - feature schema hash
  - target version
  - row count
- existing research flow still runs with compatibility wrappers

### Expected Effort

- 4 to 6 working days

## Phase 2: Governance And Dataset Registry

### Scope

- extend control-plane schema for ML datasets and predictions
- register prepared datasets
- store prediction logs and shadow evaluation rows

### Deliverables

- migration for `dataset_registry`
- migration for `prediction_log`
- migration for `shadow_eval`
- registry-store methods for dataset registration and prediction logging

### Achievable Target

- every training artifact and every scored operational snapshot becomes auditable

### Acceptance Criteria

- training run writes model metadata plus dataset metadata
- prediction rows can be written for one operational scoring date
- realized returns can later be joined back to those prediction rows

### Expected Effort

- 3 to 4 working days

## Phase 3: Walk-Forward Training Standard

### Scope

- replace ad hoc split logic with formal rolling or expanding walk-forward validation
- standardize metrics and evaluation artifacts
- keep LightGBM as the default model

### Deliverables

- `analytics/alpha/training.py`
- walk-forward split utilities
- training summary artifact
- feature importance and validation report output

### Achievable Target

- a repeatable training pipeline that produces consistent offline metrics across horizons

### Acceptance Criteria

- no random train/test split for official evaluations
- official metrics include:
  - validation AUC or PR-AUC
  - precision at top decile
  - average forward return of top bucket
  - hit rate
  - basic backtest summary
- model and metrics are written to registry

### Expected Effort

- 5 to 7 working days

## Phase 4: Shadow Scoring In Operational Rank Stage

### Scope

- integrate ML scoring into `run/stages/rank.py`
- keep technical ranker as the production baseline
- add deployment modes:
  - `baseline_only`
  - `shadow_ml`
  - `blend_ml`

### Deliverables

- `analytics/alpha/scoring.py`
- `analytics/alpha/blending.py`
- rank-stage integration
- prediction-log writes during operational runs

### Achievable Target

- live pipeline generates ML scores for the current universe without affecting trade decisions

### Acceptance Criteria

- rank stage completes successfully if ML model is missing or scoring fails
- shadow predictions are saved for later evaluation
- published artifacts can optionally include ML columns
- operational mode defaults to `shadow_ml` or `baseline_only`

### Expected Effort

- 4 to 6 working days

## Phase 5: Shadow Monitoring And Promotion Gates

### Scope

- measure realized outcomes of shadow predictions
- add drift checks
- define promotion thresholds for moving from shadow to blend

### Deliverables

- `analytics/alpha/monitoring.py`
- `analytics/alpha/drift.py`
- `analytics/alpha/policy.py`
- migration for `drift_metric`
- migration for `promotion_gate_result`

### Achievable Target

- operators can tell whether ML is genuinely better than the baseline before enabling it in production

### Acceptance Criteria

- matured predictions produce:
  - realized return
  - hit flag
  - decile performance summary
- feature drift and score drift can be recorded
- promotion policy checks can be run against a candidate model

### Expected Effort

- 4 to 5 working days

## Phase 6: Controlled Blend Rollout

### Scope

- enable approved model influence in ranking through blending only
- keep rollback path simple

### Deliverables

- config toggle for deployment mode
- approved-model lookup from registry
- blended score columns in rank artifacts
- rollback workflow using `model_deployment`

### Achievable Target

- ML contributes to live ranking in a limited, reversible way

### Acceptance Criteria

- only approved deployed models can affect rank output
- fallback to technical-only mode works immediately
- UI can show baseline vs blend deltas

### Expected Effort

- 3 to 4 working days

## Success Metrics By Stage

### Technical Success

- no regression in live pipeline stability
- no schema drift between research and operational scoring
- prediction logging and shadow evaluation are reliable

### Modeling Success

- offline top-decile precision and average forward returns beat technical-only benchmark in repeated walk-forward windows
- shadow performance is stable across at least one observation window
- no unacceptable drift or calibration failure

### Operational Success

- operators can identify:
  - which model scored a run
  - whether it was shadow or blended
  - what happened after horizon maturity

## Recommended Timeline

A realistic first implementation is:

- Week 1: Phase 1
- Week 2: Phase 2 and Phase 3 start
- Week 3: Phase 3 finish and Phase 4
- Week 4: Phase 5
- Week 5: Phase 6 if shadow results and gates are acceptable

That means:

- 3 to 4 weeks to reach stable shadow deployment
- 5 weeks or more to reach controlled blended production rollout

## MVP Cutline

If you want the fastest safe version, stop after Phase 4.

That MVP gives you:

- canonical feature schema
- prepared dataset path
- walk-forward training
- registered models
- operational shadow scoring
- prediction logging

This is already high value because it creates a governed ML workflow without risking live ranking quality.

## Stretch Goals After MVP

- probability calibration for position sizing
- sector- or regime-specific model families
- meta-labeling for trade filtering
- automated retraining candidate generation
- champion/challenger evaluation dashboards

## Recommended Order Of Actual Code Work

1. Add `analytics/alpha/feature_schema.py`, `labeling.py`, and `dataset_builder.py`
2. Add control-plane migrations and registry methods
3. Refactor `research/train_pipeline.py` to use the new dataset and training path
4. Add `scoring.py` and integrate shadow scoring into rank stage
5. Add monitoring, drift, and promotion policies
6. Enable controlled blending only after shadow evidence is strong

## Final Recommendation

The most achievable target for this repo in the near term is:

- **research-grade reproducible training**
- **production-grade shadow scoring**
- **governed blended rollout with technical fallback**

That is the highest-confidence path to introducing ML without weakening the current trading system.
