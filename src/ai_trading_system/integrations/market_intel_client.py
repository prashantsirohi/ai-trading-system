"""Read-only client wrapper around the vendored ``market_intel`` package.

The trading pipeline never writes to ``market_intel``'s DuckDB store — that is
owned by the separately-running ``market_intel`` collector process. We always
open the connection in read-only mode to make the boundary explicit and to
enable concurrent readers while the collector writes.

Usage:

    from ai_trading_system.integrations.market_intel_client import (
        get_event_query_service,
    )

    svc = get_event_query_service()
    events = svc.get_events_for_symbol("RELIANCE", since=..., min_trust=80.0)
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Optional

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    from market_intel.services.event_query_service import EventQueryService


_DEFAULT_DB_PATH = "data/market_intel.duckdb"
_ENV_DB_PATH = "AI_TRADING_MARKET_INTEL_DB"

_lock = threading.Lock()
_cached_service: Optional["EventQueryService"] = None
_cached_db_path: Optional[str] = None


def resolve_db_path(explicit: str | None = None) -> str:
    """Resolve the market_intel DuckDB path.

    Order of precedence:
      1. ``explicit`` argument
      2. ``AI_TRADING_MARKET_INTEL_DB`` env var
      3. Default ``data/market_intel.duckdb`` relative to cwd
    """
    return explicit or os.environ.get(_ENV_DB_PATH) or _DEFAULT_DB_PATH


def get_event_query_service(
    db_path: str | None = None,
    *,
    refresh: bool = False,
) -> "EventQueryService":
    """Return a process-wide ``EventQueryService`` bound to the market_intel DB.

    Lazy-imports market_intel so this module can load even if the dependency is
    not installed (useful for partial installs / CI matrix shards). Raises
    ``ImportError`` only when the service is actually requested.
    """
    global _cached_service, _cached_db_path

    resolved = resolve_db_path(db_path)
    with _lock:
        if (
            not refresh
            and _cached_service is not None
            and _cached_db_path == resolved
        ):
            return _cached_service

        try:
            from market_intel.services.event_query_service import EventQueryService
            from market_intel.storage.db import Database
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "market_intel is not installed. Add it to your environment via "
                "`pip install -e ../market_intel` or via the git dependency in "
                "pyproject.toml."
            ) from exc

        if not Path(resolved).exists():
            raise FileNotFoundError(
                f"market_intel DuckDB not found at {resolved}; start the collector "
                "before expecting event snapshots."
            )

        if hasattr(EventQueryService, "from_readonly_path"):
            service = EventQueryService.from_readonly_path(resolved)
        else:  # pragma: no cover - compatibility with older market_intel
            db = Database.open_readonly(resolved)
            service = EventQueryService(db=db)
        _cached_service = service
        _cached_db_path = resolved
        return service


def reset_cache() -> None:
    """Drop the cached service. Used by tests."""
    global _cached_service, _cached_db_path
    with _lock:
        _cached_service = None
        _cached_db_path = None
