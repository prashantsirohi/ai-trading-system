"""Compatibility shim for legacy config.settings imports."""

from __future__ import annotations

import sys as _sys

from ai_trading_system.platform.config.settings import *  # noqa: F401,F403
from ai_trading_system.platform.config import settings as _settings

_sys.modules[__name__] = _settings
