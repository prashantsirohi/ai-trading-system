"""Compatibility shim for canonical index backfill module.

Canonical module:
- ai_trading_system.domains.ingest.index_backfill
"""

from ai_trading_system.domains.ingest.index_backfill import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import index_backfill as _index_backfill
import sys as _sys

_sys.modules[__name__] = _index_backfill

if __name__ == "__main__":
    _index_backfill.main()
