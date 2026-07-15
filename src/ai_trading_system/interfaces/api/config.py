"""Typed, environment-backed configuration for the Phase 4A API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from ai_trading_system.platform.db.paths import get_domain_paths


class SourceProfile(str, Enum):
    SMALL_FIXTURE = "small_fixture"
    COPIED_STORE = "copied_store"
    OPERATOR_READ_ONLY = "operator_read_only"


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class ApiSettings:
    source_profile: SourceProfile = SourceProfile.OPERATOR_READ_ONLY
    copied_control_plane: Path | None = None
    artifact_root: Path | None = None
    auth_enabled: bool = True
    local_dev_mode: bool = False
    api_key: str | None = None
    host: str = "127.0.0.1"
    port: int = 8765
    default_page_size: int = 50
    max_page_size: int = 500
    rate_limit_per_minute: int = 120
    cache_enabled: bool = False
    cache_ttl_seconds: int = 30
    include_openapi: bool = True
    max_response_rows: int = 500

    @classmethod
    def from_env(cls) -> "ApiSettings":
        return cls(
            source_profile=SourceProfile(os.getenv("PHASE4_API_SOURCE_PROFILE", "operator_read_only").lower()),
            copied_control_plane=(Path(os.environ["PHASE4_API_COPIED_CONTROL_PLANE"]) if os.getenv("PHASE4_API_COPIED_CONTROL_PLANE") else None),
            artifact_root=(Path(os.environ["PHASE4_API_ARTIFACT_ROOT"]) if os.getenv("PHASE4_API_ARTIFACT_ROOT") else None),
            auth_enabled=_bool("PHASE4_API_AUTH_ENABLED", True),
            local_dev_mode=_bool("PHASE4_API_LOCAL_DEV_MODE", False),
            api_key=os.getenv("PHASE4_API_KEY"),
            host=os.getenv("PHASE4_API_HOST", "127.0.0.1"),
            port=int(os.getenv("PHASE4_API_PORT", "8765")),
            default_page_size=int(os.getenv("PHASE4_API_DEFAULT_PAGE_SIZE", "50")),
            max_page_size=int(os.getenv("PHASE4_API_MAX_PAGE_SIZE", "500")),
            rate_limit_per_minute=int(os.getenv("PHASE4_API_RATE_LIMIT_PER_MINUTE", "120")),
            cache_enabled=_bool("PHASE4_API_CACHE_ENABLED", False),
            cache_ttl_seconds=int(os.getenv("PHASE4_API_CACHE_TTL_SECONDS", "30")),
            include_openapi=_bool("PHASE4_API_INCLUDE_OPENAPI", True),
            max_response_rows=int(os.getenv("PHASE4_API_MAX_RESPONSE_ROWS", "500")),
        )

    def control_plane_path(self) -> Path | None:
        if self.source_profile is SourceProfile.SMALL_FIXTURE:
            return None
        if self.source_profile is SourceProfile.COPIED_STORE:
            return self._validated_copy()
        return get_domain_paths(data_domain="operational").root_dir / "control_plane.duckdb"

    def _validated_copy(self) -> Path:
        if self.copied_control_plane is None:
            raise ValueError("copied_store requires an explicit copied control-plane path")
        raw = self.copied_control_plane.expanduser()
        if raw.is_symlink():
            raise ValueError("copied control-plane path must not be a symlink")
        resolved = raw.resolve(strict=True)
        operator = (get_domain_paths(data_domain="operational").root_dir / "control_plane.duckdb").resolve()
        if resolved == operator:
            raise ValueError("copied_store refuses the configured operator control plane")
        if not resolved.is_file():
            raise ValueError("copied control-plane path must be a regular file")
        return resolved

    def auth_configured(self) -> bool:
        return (not self.auth_enabled) or self.local_dev_mode or bool(self.api_key)

    def artifact_roots(self) -> tuple[Path, ...]:
        """Configured immutable evidence roots; never accepts request input."""
        roots: list[Path] = []
        if self.artifact_root is not None:
            roots.append(self.artifact_root.expanduser())
        if self.source_profile is SourceProfile.COPIED_STORE and self.copied_control_plane:
            roots.append(self.copied_control_plane.expanduser().parent)
        if self.source_profile is SourceProfile.OPERATOR_READ_ONLY:
            operational = get_domain_paths(data_domain="operational")
            roots.extend((operational.root_dir, operational.root_dir / "pipeline_runs"))
        return tuple(dict.fromkeys(roots))
