"""Rank-stage orchestration services."""

from .composite import (
    RANK_FACTOR_WEIGHTS_PATH,
    apply_rank_stability,
    compute_factor_scores,
    compute_rank_confidence,
    filter_ranked_scores,
    load_factor_weights,
    select_rank_output_columns,
)
from .contracts import (
    DEFAULT_FACTOR_WEIGHTS,
    PRIMARY_FACTORS,
    RANKED_SIGNAL_COLUMNS,
    RANK_MODES,
    RankFactorDefinition,
)
from .dashboard_payload import (
    augment_dashboard_payload_with_ml,
    build_rejection_reasons,
    build_dashboard_payload,
    build_score_breakdown,
    build_top_factors,
    summarize_task_statuses,
)
from .eligibility import apply_rank_eligibility
from .factors import (
    add_signal_freshness,
    apply_delivery,
    apply_proximity_highs,
    apply_relative_strength,
    apply_sector_strength,
    apply_trend_persistence,
    apply_volume_intensity,
    compute_penalty_score,
)
from .input_loader import RankerInputLoader
from .orchestration import RankOrchestrationService

__all__ = [
    "DEFAULT_FACTOR_WEIGHTS",
    "PRIMARY_FACTORS",
    "RANKED_SIGNAL_COLUMNS",
    "RANK_MODES",
    "RANK_FACTOR_WEIGHTS_PATH",
    "RankFactorDefinition",
    "RankerInputLoader",
    "augment_dashboard_payload_with_ml",
    "apply_rank_eligibility",
    "apply_rank_stability",
    "add_signal_freshness",
    "apply_delivery",
    "apply_proximity_highs",
    "apply_relative_strength",
    "apply_sector_strength",
    "apply_trend_persistence",
    "apply_volume_intensity",
    "build_dashboard_payload",
    "build_rejection_reasons",
    "build_score_breakdown",
    "build_top_factors",
    "compute_factor_scores",
    "compute_penalty_score",
    "compute_rank_confidence",
    "filter_ranked_scores",
    "load_factor_weights",
    "summarize_task_statuses",
    "select_rank_output_columns",
    "RankOrchestrationService",
]
