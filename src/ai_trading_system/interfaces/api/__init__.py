"""Deprecated shim — import from ``ai_trading_system.ui.execution_api`` instead.

This path will be removed one release after 2026-04-25.
"""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api is deprecated; "
    "import from ai_trading_system.ui.execution_api instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui import execution_api as _execution_api  # noqa: E402
from ai_trading_system.ui.execution_api import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _execution_api
