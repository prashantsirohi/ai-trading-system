"""Rank-stage orchestration services."""

from .composite import (
    RANK_FACTOR_WEIGHTS_PATH,
    compute_factor_scores,
    filter_ranked_scores,
    load_factor_weights,
    select_rank_output_columns,
)
from .contracts import DEFAULT_FACTOR_WEIGHTS, PRIMARY_FACTORS, RANKED_SIGNAL_COLUMNS, RankFactorDefinition
from .dashboard_payload import (
    augment_dashboard_payload_with_ml,
    build_dashboard_payload,
    summarize_task_statuses,
)
from .factors import (
    apply_delivery,
    apply_proximity_highs,
    apply_relative_strength,
    apply_sector_strength,
    apply_trend_persistence,
    apply_volume_intensity,
)
from .input_loader import RankerInputLoader
from .orchestration import RankOrchestrationService

__all__ = [
    "DEFAULT_FACTOR_WEIGHTS",
    "PRIMARY_FACTORS",
    "RANKED_SIGNAL_COLUMNS",
    "RANK_FACTOR_WEIGHTS_PATH",
    "RankFactorDefinition",
    "RankerInputLoader",
    "augment_dashboard_payload_with_ml",
    "apply_delivery",
    "apply_proximity_highs",
    "apply_relative_strength",
    "apply_sector_strength",
    "apply_trend_persistence",
    "apply_volume_intensity",
    "build_dashboard_payload",
    "compute_factor_scores",
    "filter_ranked_scores",
    "load_factor_weights",
    "summarize_task_statuses",
    "select_rank_output_columns",
    "RankOrchestrationService",
]
