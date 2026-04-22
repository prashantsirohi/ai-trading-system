"""Compatibility shim for the canonical daily ingest runner.

Canonical module:
- ai_trading_system.domains.ingest.daily_update_runner
"""

from ai_trading_system.domains.ingest.daily_update_runner import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import daily_update_runner as _daily_update_runner
import sys as _sys

_sys.modules[__name__] = _daily_update_runner
