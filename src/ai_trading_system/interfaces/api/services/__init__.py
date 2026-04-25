"""Deprecated shim — import from ``ai_trading_system.ui.execution_api.services`` instead."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.services is deprecated; "
    "import from ai_trading_system.ui.execution_api.services instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api import services as _services  # noqa: E402
from ai_trading_system.ui.execution_api.services import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _services
