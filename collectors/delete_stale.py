"""Compatibility shim for canonical stale-symbol cleanup utility.

Canonical module:
- ai_trading_system.domains.ingest.delete_stale
"""

from ai_trading_system.domains.ingest.delete_stale import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import delete_stale as _delete_stale
import sys as _sys

_sys.modules[__name__] = _delete_stale

if __name__ == "__main__":
    _delete_stale.main()
