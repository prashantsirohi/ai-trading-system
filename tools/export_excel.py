"""Compatibility shim for legacy tools.export_excel entrypoint."""

from ai_trading_system.interfaces.cli.export_excel import *  # noqa
from ai_trading_system.interfaces.cli import export_excel as _export_excel
import sys as _sys

_sys.modules[__name__] = _export_excel

if __name__ == "__main__":
    import runpy

    runpy.run_module("ai_trading_system.interfaces.cli.export_excel", run_name="__main__")
