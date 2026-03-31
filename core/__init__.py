"""Shared runtime primitives for the AI trading system."""

from core.contracts import (
    DataQualityCriticalError,
    PipelineStageError,
    PublishStageError,
    StageArtifact,
    StageContext,
    StageResult,
    compute_file_hash,
)
from core.bootstrap import ensure_project_root_on_path
from core.env import find_project_env, load_project_env
from core.logging import (
    clear_log_context,
    get_log_context,
    get_logger,
    log_context,
    logger,
    set_log_context,
)
from core.paths import (
    DataDomain,
    DataDomainPaths,
    ensure_domain_layout,
    get_domain_paths,
    research_static_end_date,
    resolve_data_domain,
)
from core.runtime_config import (
    DhanRuntimeConfig,
    GoogleSheetsRuntimeConfig,
    TelegramRuntimeConfig,
)

__all__ = [
    "DataDomain",
    "DataDomainPaths",
    "StageArtifact",
    "StageContext",
    "StageResult",
    "PipelineStageError",
    "DataQualityCriticalError",
    "PublishStageError",
    "compute_file_hash",
    "ensure_project_root_on_path",
    "find_project_env",
    "load_project_env",
    "logger",
    "get_logger",
    "get_log_context",
    "set_log_context",
    "clear_log_context",
    "log_context",
    "resolve_data_domain",
    "get_domain_paths",
    "ensure_domain_layout",
    "research_static_end_date",
    "DhanRuntimeConfig",
    "TelegramRuntimeConfig",
    "GoogleSheetsRuntimeConfig",
]
