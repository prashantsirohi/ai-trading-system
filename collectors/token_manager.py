"""Compatibility shim for canonical Dhan token manager.

Canonical module:
- ai_trading_system.domains.ingest.token_manager
"""

from ai_trading_system.domains.ingest.token_manager import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import token_manager as _token_manager
import sys as _sys

_sys.modules[__name__] = _token_manager
