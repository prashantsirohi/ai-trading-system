"""Deprecated shim — use ``ai_trading_system.ui.execution_api.services.execution_operator``."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.services.execution_operator is deprecated; "
    "import from ai_trading_system.ui.execution_api.services.execution_operator instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api.services import execution_operator as _execution_operator  # noqa: E402
from ai_trading_system.ui.execution_api.services.execution_operator import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _execution_operator
