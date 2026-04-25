"""Shared dependencies for execution API routers.

Routers should pull project-root resolution and auth-header constants from
here rather than reaching into ``app.py`` (which would create an import cycle
between the bootstrap and the route modules it includes).
"""

from __future__ import annotations

import os
from pathlib import Path


# Same depth as the legacy ``interfaces/api/app.py`` — file lives at
# src/ai_trading_system/ui/execution_api/routes/_deps.py, so parents[5] is
# the repo root.
DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[5]

API_KEY_HEADER = "x-api-key"


def project_root() -> Path:
    """Resolve the active project root, honoring ``AI_TRADING_PROJECT_ROOT``."""

    return Path(os.getenv("AI_TRADING_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)).resolve()


def configured_api_key() -> str | None:
    """Return the configured API key (stripped) or ``None`` if unset/blank."""

    value = os.getenv("EXECUTION_API_KEY")
    if value is None:
        return None
    value = value.strip()
    return value or None


__all__ = [
    "API_KEY_HEADER",
    "DEFAULT_PROJECT_ROOT",
    "configured_api_key",
    "project_root",
]
