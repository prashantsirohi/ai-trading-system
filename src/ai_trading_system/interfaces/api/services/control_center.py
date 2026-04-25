"""Deprecated shim — use ``ai_trading_system.ui.execution_api.services.control_center``."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.services.control_center is deprecated; "
    "import from ai_trading_system.ui.execution_api.services.control_center instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api.services import control_center as _control_center  # noqa: E402
from ai_trading_system.ui.execution_api.services.control_center import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _control_center
