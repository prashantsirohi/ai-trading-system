from ai_trading_system.domains.ingest.providers.yfinance import *  # noqa
from ai_trading_system.domains.ingest.providers import yfinance as _yfinance
import sys as _sys

_sys.modules[__name__] = _yfinance

if __name__ == "__main__":
    import runpy

    runpy.run_module("ai_trading_system.domains.ingest.providers.yfinance", run_name="__main__")
