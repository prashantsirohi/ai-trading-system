"""Deprecated shim — use ``ai_trading_system.ui.execution_api.services.readmodels``."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.services.readmodels is deprecated; "
    "import from ai_trading_system.ui.execution_api.services.readmodels instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api.services import readmodels as _readmodels  # noqa: E402
from ai_trading_system.ui.execution_api.services.readmodels import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _readmodels
