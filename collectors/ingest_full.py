"""Compatibility shim for canonical full-ingest module.

Canonical module:
- ai_trading_system.domains.ingest.ingest_full
"""

from ai_trading_system.domains.ingest.ingest_full import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import ingest_full as _ingest_full
import sys as _sys

_sys.modules[__name__] = _ingest_full

if __name__ == "__main__":
    _ingest_full.main()
