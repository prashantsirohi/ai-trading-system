"""Deprecated shim — use ``ai_trading_system.ui.execution_api.services.ml_workbench``."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.services.ml_workbench is deprecated; "
    "import from ai_trading_system.ui.execution_api.services.ml_workbench instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api.services import ml_workbench as _ml_workbench  # noqa: E402
from ai_trading_system.ui.execution_api.services.ml_workbench import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _ml_workbench
