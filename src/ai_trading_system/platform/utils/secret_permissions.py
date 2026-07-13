"""Permission checks and private writes for local credential files."""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path

logger = logging.getLogger(__name__)

_WARNED_SECRET_PATHS: set[str] = set()


def warn_if_insecure_secret_file(path: str | Path) -> bool:
    """Warn once when an existing POSIX secret file is accessible by others."""
    secret_path = Path(path)
    if os.name != "posix" or not secret_path.is_file():
        return False

    resolved = str(secret_path.resolve())
    mode = stat.S_IMODE(secret_path.stat().st_mode)
    insecure = bool(mode & 0o077)
    if insecure and resolved not in _WARNED_SECRET_PATHS:
        logger.warning(
            "Secret file %s has permissions %04o; use owner-only permissions 0600",
            secret_path,
            mode,
        )
        _WARNED_SECRET_PATHS.add(resolved)
    return insecure


def write_private_text(path: str | Path, value: str) -> Path:
    """Write a local secret and enforce owner read/write permissions on POSIX."""
    secret_path = Path(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(secret_path, flags, 0o600)
    try:
        if os.name == "posix":
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(value)
    finally:
        if fd >= 0:
            os.close(fd)
    return secret_path
