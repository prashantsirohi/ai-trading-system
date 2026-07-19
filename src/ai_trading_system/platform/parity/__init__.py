"""Reusable field-parity comparison policy for pipeline-artifact diffs."""

from ai_trading_system.platform.parity.comparison_policy import (
    FLOAT_TOLERANCE_COLUMNS,
    PARITY_POLICY_VERSION,
    RUN_SCOPED_COLUMNS,
    STRICT_ARTIFACTS,
    ArtifactComparison,
    ArtifactPolicy,
    FieldClass,
    RunComparison,
    classify_artifact,
    compare_artifact,
    compare_runs,
    normalized_sha256,
    raw_sha256,
)

__all__ = [
    "FieldClass",
    "PARITY_POLICY_VERSION",
    "RUN_SCOPED_COLUMNS",
    "FLOAT_TOLERANCE_COLUMNS",
    "STRICT_ARTIFACTS",
    "ArtifactPolicy",
    "ArtifactComparison",
    "RunComparison",
    "classify_artifact",
    "compare_artifact",
    "compare_runs",
    "raw_sha256",
    "normalized_sha256",
]
