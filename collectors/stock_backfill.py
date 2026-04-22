"""Compatibility shim for canonical stock backfill module.

Canonical module:
- ai_trading_system.domains.ingest.stock_backfill
"""

from ai_trading_system.domains.ingest.stock_backfill import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import stock_backfill as _stock_backfill
import sys as _sys

_sys.modules[__name__] = _stock_backfill

if __name__ == "__main__":
    _stock_backfill.main()
