from ai_trading_system.interfaces.api.app import *  # noqa: F401,F403
from ai_trading_system.interfaces.api import app as _app
import runpy as _runpy
import sys as _sys

_sys.modules[__name__] = _app

if __name__ == "__main__":
    _runpy.run_module("ai_trading_system.interfaces.api.app", run_name="__main__")

