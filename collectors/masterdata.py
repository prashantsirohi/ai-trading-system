from ai_trading_system.domains.ingest.masterdata import *  # noqa
from ai_trading_system.domains.ingest import masterdata as _masterdata
import sys as _sys

_sys.modules[__name__] = _masterdata

if __name__ == "__main__":
    import runpy

    runpy.run_module("ai_trading_system.domains.ingest.masterdata", run_name="__main__")
