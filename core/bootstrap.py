"""Centralized script bootstrap helpers."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_project_root_on_path(anchor: str | Path) -> Path:
    """Ensure the repo root for an anchor file is available on sys.path."""
    path = Path(anchor).resolve()
    root = path.parent if path.is_dir() else path.parent
    if root.name in {"run", "dashboard", "collectors", "channel", "features", "analytics", "research"}:
        root = root.parent
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root
