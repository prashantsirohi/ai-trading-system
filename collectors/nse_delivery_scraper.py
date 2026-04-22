"""Compatibility shim for canonical NSE delivery scraper.

Canonical module:
- ai_trading_system.domains.ingest.nse_delivery_scraper
"""

from ai_trading_system.domains.ingest.nse_delivery_scraper import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import nse_delivery_scraper as _nse_delivery_scraper
import sys as _sys

_sys.modules[__name__] = _nse_delivery_scraper

if __name__ == "__main__":
    import runpy

    runpy.run_module("ai_trading_system.domains.ingest.nse_delivery_scraper", run_name="__main__")
