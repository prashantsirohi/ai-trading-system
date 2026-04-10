"""Compatibility shim for Streamlit path shadowing.

When Streamlit launches a UI module directly, it can place ``ui/`` ahead of the
project root on ``sys.path``. In that case, ``import execution`` may resolve to
this UI package instead of the repo-level execution package. Replace this
module with the canonical root package so downstream imports keep working.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


_ROOT_INIT = Path(__file__).resolve().parents[2] / "execution" / "__init__.py"
_SPEC = importlib.util.spec_from_file_location(
    __name__,
    _ROOT_INIT,
    submodule_search_locations=[str(_ROOT_INIT.parent)],
)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load execution module from {_ROOT_INIT}")

_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[__name__] = _MODULE
_SPEC.loader.exec_module(_MODULE)
