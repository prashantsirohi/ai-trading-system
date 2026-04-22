"""Compatibility shim for canonical marketfeed OHLC test utility.

Canonical module:
- ai_trading_system.domains.ingest.test_marketfeed_ohlc
"""

from ai_trading_system.domains.ingest.test_marketfeed_ohlc import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import test_marketfeed_ohlc as _test_marketfeed_ohlc
import sys as _sys

_sys.modules[__name__] = _test_marketfeed_ohlc

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--large", action="store_true")
    args = parser.parse_args()
    if args.large:
        _test_marketfeed_ohlc.test_large_batch()
    else:
        _test_marketfeed_ohlc.test_api()
