"""Lazy exports for pipeline stage implementations."""

from importlib import import_module

from .base import (
    DataQualityCriticalError,
    PipelineStageError,
    PublishStageError,
    StageArtifact,
    StageContext,
    StageResult,
    compute_file_hash,
)

__all__ = [
    "StageArtifact",
    "StageResult",
    "StageContext",
    "PipelineStageError",
    "DataQualityCriticalError",
    "PublishStageError",
    "compute_file_hash",
    "FeaturesStage",
    "IngestStage",
    "PublishStage",
    "RankStage",
    "ExecuteStage",
    "classify_freshness_status",
]

_MODULE_MAP = {
    "ExecuteStage": (".execute", "ExecuteStage"),
    "FeaturesStage": (".features", "FeaturesStage"),
    "IngestStage": (".ingest", "IngestStage"),
    "PublishStage": (".publish", "PublishStage"),
    "RankStage": (".rank", "RankStage"),
    "classify_freshness_status": (".ingest", "classify_freshness_status"),
}


def __getattr__(name):
    if name not in _MODULE_MAP:
        raise AttributeError(name)
    module_name, attr_name = _MODULE_MAP[name]
    module = import_module(module_name, __name__)
    return getattr(module, attr_name)
