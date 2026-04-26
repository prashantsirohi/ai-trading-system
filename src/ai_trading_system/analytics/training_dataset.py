"""Compatibility wrapper for the canonical alpha dataset builder."""

from ai_trading_system.analytics.alpha.dataset_builder import AlphaDatasetBuilder, PreparedDataset

TrainingDatasetBuilder = AlphaDatasetBuilder

__all__ = ["PreparedDataset", "TrainingDatasetBuilder"]
