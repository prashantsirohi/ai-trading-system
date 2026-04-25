"""Deprecated shim — import from ``ai_trading_system.ui.execution_api.app`` instead."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.app is deprecated; "
    "import from ai_trading_system.ui.execution_api.app instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api import app as _app  # noqa: E402
from ai_trading_system.ui.execution_api.app import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _app
