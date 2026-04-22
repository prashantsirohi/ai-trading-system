"""Compatibility shim for canonical Zerodha sector collector.

Canonical module:
- ai_trading_system.domains.ingest.zerodha_sector_collector
"""

from ai_trading_system.domains.ingest.zerodha_sector_collector import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import zerodha_sector_collector as _zerodha_sector_collector
import sys as _sys

_sys.modules[__name__] = _zerodha_sector_collector

if __name__ == "__main__":
    _zerodha_sector_collector.main()
