"""Canonical ML alpha building blocks.

This package holds the repo-native contracts for feature schemas, labeling,
dataset assembly, training, and scoring. During the migration period, older
modules in `analytics/` can wrap these implementations for compatibility.
"""

from analytics.alpha.dataset_builder import AlphaDatasetBuilder, PreparedDataset
from analytics.alpha.feature_schema import DEFAULT_FEATURE_SCHEMA, FeatureSchema
from analytics.alpha.labeling import TargetSpec
from analytics.alpha.monitoring import summarize_model_shadow_performance
from analytics.alpha.policy import PromotionThresholds, evaluate_promotion_candidate

__all__ = [
    "AlphaDatasetBuilder",
    "PreparedDataset",
    "FeatureSchema",
    "DEFAULT_FEATURE_SCHEMA",
    "TargetSpec",
    "PromotionThresholds",
    "evaluate_promotion_candidate",
    "summarize_model_shadow_performance",
]
