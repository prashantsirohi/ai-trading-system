"""Compatibility wrapper for the research Streamlit UI."""

from __future__ import annotations

import sys as _sys

from ui.research.app import *  # noqa: F401,F403
from ui.research import app as _research_app

_sys.modules[__name__] = _research_app

if __name__ == "__main__":
    _research_app.main()
