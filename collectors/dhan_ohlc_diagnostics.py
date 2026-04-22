"""Compatibility shim for canonical Dhan OHLC diagnostics utility.

Canonical module:
- ai_trading_system.domains.ingest.dhan_ohlc_diagnostics
"""

from ai_trading_system.domains.ingest.dhan_ohlc_diagnostics import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import dhan_ohlc_diagnostics as _dhan_ohlc_diagnostics
import sys as _sys

_sys.modules[__name__] = _dhan_ohlc_diagnostics

if __name__ == "__main__":
    _dhan_ohlc_diagnostics.main()
