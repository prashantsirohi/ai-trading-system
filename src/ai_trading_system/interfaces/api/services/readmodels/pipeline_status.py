"""Deprecated shim — use ``ai_trading_system.ui.execution_api.services.readmodels.pipeline_status``."""

from __future__ import annotations

import sys as _sys
import warnings as _warnings

_warnings.warn(
    "ai_trading_system.interfaces.api.services.readmodels.pipeline_status is deprecated; "
    "import from ai_trading_system.ui.execution_api.services.readmodels.pipeline_status instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ai_trading_system.ui.execution_api.services.readmodels import (  # noqa: E402
    pipeline_status as _mod,
)
from ai_trading_system.ui.execution_api.services.readmodels.pipeline_status import *  # noqa: E402,F401,F403

_sys.modules[__name__] = _mod
