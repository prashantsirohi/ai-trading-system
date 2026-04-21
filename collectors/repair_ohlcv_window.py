from ai_trading_system.domains.ingest.repair import *  # noqa
from ai_trading_system.domains.ingest import repair as _repair
import sys as _sys

_sys.modules[__name__] = _repair

_backup_current_rows = _repair._backup_current_rows
_build_comparison_results = _repair._build_comparison_results
_compare_trade_frames = _repair._compare_trade_frames
_delete_window_rows = _repair._delete_window_rows

if __name__ == "__main__":
    import runpy

    runpy.run_module("ai_trading_system.domains.ingest.repair", run_name="__main__")
