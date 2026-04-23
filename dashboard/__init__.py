"""Compatibility shim for legacy ``dashboard`` package imports."""

from __future__ import annotations

import sys as _sys

from ui.research.app import *  # noqa: F401,F403
from ui.research import app as _research_app

_sys.modules[__name__] = _research_app

