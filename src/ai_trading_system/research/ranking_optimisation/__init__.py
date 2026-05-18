"""Walk-forward ranking-factor weight optimiser.

This module searches for ranking-factor weights that maximise the rank
correlation between produced composite scores and realised forward returns,
walked forward year-by-year so the chosen weights are validated out-of-sample.

It is intentionally separate from ``research.optimization`` (the strategy
optimiser): that module tunes trade-execution rules around a fixed ranking;
this module tunes the ranking itself against ground-truth winners. The two
compose — find ranking weights here, then plug them into the strategy
optimiser as a fixed input.

Public API:
    - ``FactorPanel``                : per-year cross-section + forward labels
    - ``load_factor_panel``          : build a panel from research_ohlcv.duckdb
    - ``FoldScore``                  : single-fold fitness report
    - ``score_weights``              : compute IC / hit / lift for one weight vector
    - ``WalkForwardResult``          : result of the full walk-forward search
    - ``run_walkforward``            : expanding-window Optuna driver
"""

from __future__ import annotations

from ai_trading_system.research.ranking_optimisation.data import (
    FACTOR_NAMES,
    FactorPanel,
    load_factor_panel,
)
from ai_trading_system.research.ranking_optimisation.fitness import (
    FoldScore,
    score_weights,
)
from ai_trading_system.research.ranking_optimisation.runner import (
    FoldOutcome,
    WalkForwardResult,
    run_walkforward,
)

# v2 — production-factor based
from ai_trading_system.research.ranking_optimisation.data_v2 import (
    PRODUCTION_FACTOR_COLUMNS,
    SCORE_TO_WEIGHT_KEY,
    WEIGHT_KEYS,
    LiveFactorPanel,
    load_live_factor_panel,
    quarterly_anchors,
)
from ai_trading_system.research.ranking_optimisation.fitness_v2 import (
    V2FoldScore,
    combined_objective,
    normalise_weights_v2,
    score_weights_v2,
    single_metric_objective,
)
from ai_trading_system.research.ranking_optimisation.runner_v2 import (
    V2FoldOutcome,
    WalkForwardResultV2,
    run_walkforward_v2,
)
from ai_trading_system.research.ranking_optimisation.promote import (
    DEFAULT_CANDIDATE_CONFIG_PATH,
    DEFAULT_COMPARISON_REPORT_PATH,
    DEFAULT_WALKFORWARD_JSON_PATH,
    build_comparison_report,
    load_production_weights,
    write_candidate_config,
    write_walkforward_json,
)

__all__ = [
    # v1
    "FACTOR_NAMES",
    "FactorPanel",
    "FoldOutcome",
    "FoldScore",
    "WalkForwardResult",
    "load_factor_panel",
    "run_walkforward",
    "score_weights",
    # v2
    "PRODUCTION_FACTOR_COLUMNS",
    "SCORE_TO_WEIGHT_KEY",
    "WEIGHT_KEYS",
    "LiveFactorPanel",
    "V2FoldOutcome",
    "V2FoldScore",
    "WalkForwardResultV2",
    "combined_objective",
    "load_live_factor_panel",
    "normalise_weights_v2",
    "quarterly_anchors",
    "run_walkforward_v2",
    "score_weights_v2",
    "single_metric_objective",
    # promote
    "DEFAULT_CANDIDATE_CONFIG_PATH",
    "DEFAULT_COMPARISON_REPORT_PATH",
    "DEFAULT_WALKFORWARD_JSON_PATH",
    "build_comparison_report",
    "load_production_weights",
    "write_candidate_config",
    "write_walkforward_json",
]
