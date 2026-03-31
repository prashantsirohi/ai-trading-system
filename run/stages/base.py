"""Compatibility wrapper for shared stage contracts.

New code should import these types from ``core.contracts`` instead of
``run.stages.base`` so non-runtime layers do not depend on the run package.
"""

from core.contracts import (
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
]
