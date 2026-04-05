"""Lazy exports for pipeline stage implementations.

Keeping these imports lazy avoids circular dependencies between the publish
stage graph and helper modules such as ``run.publisher``.
"""

from importlib import import_module

from core.contracts import (
    DataQualityCriticalError,
    PublishStageError,
    StageArtifact,
    StageContext,
    StageResult,
)

__all__ = [
    "DataQualityCriticalError",
    "PublishStageError",
    "StageArtifact",
    "StageContext",
    "StageResult",
    "FeaturesStage",
    "IngestStage",
    "PublishStage",
    "RankStage",
    "ExecuteStage",
]

_MODULE_MAP = {
    "ExecuteStage": (".execute", "ExecuteStage"),
    "FeaturesStage": (".features", "FeaturesStage"),
    "IngestStage": (".ingest", "IngestStage"),
    "PublishStage": (".publish", "PublishStage"),
    "RankStage": (".rank", "RankStage"),
}


def __getattr__(name):
    if name not in _MODULE_MAP:
        raise AttributeError(name)
    module_name, attr_name = _MODULE_MAP[name]
    module = import_module(module_name, __name__)
    return getattr(module, attr_name)
