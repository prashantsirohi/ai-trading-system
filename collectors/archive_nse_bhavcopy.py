"""Compatibility shim for canonical NSE bhavcopy archiver.

Canonical module:
- ai_trading_system.domains.ingest.archive_nse_bhavcopy
"""

from ai_trading_system.domains.ingest.archive_nse_bhavcopy import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import archive_nse_bhavcopy as _archive_nse_bhavcopy
import sys as _sys

_sys.modules[__name__] = _archive_nse_bhavcopy

if __name__ == "__main__":
    _archive_nse_bhavcopy.main()
