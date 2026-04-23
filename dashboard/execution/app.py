"""Compatibility wrapper for the execution NiceGUI UI."""

from __future__ import annotations

import sys as _sys

from ui.execution.app import *  # noqa: F401,F403
from ui.execution import app as _execution_app

_sys.modules[__name__] = _execution_app

if __name__ == "__main__":
    _execution_app.main()
