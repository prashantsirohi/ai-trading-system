"""Shared environment loading helpers for repo-local .env files."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv

_LOADED_ENV_PATHS: set[str] = set()


def _candidate_roots(start: Path) -> Iterable[Path]:
    start = start.resolve()
    if start.is_file():
        start = start.parent
    yield start
    yield from start.parents


def find_project_env(anchor: str | Path | None = None) -> Path | None:
    """Return the nearest repo-local .env path for a file or directory anchor."""
    start = Path(anchor).resolve() if anchor is not None else Path(__file__).resolve()
    for root in _candidate_roots(start):
        env_path = root / ".env"
        if env_path.exists():
            return env_path
    return None


def load_project_env(anchor: str | Path | None = None, override: bool = False) -> Path | None:
    """Load the nearest repo-local `.env` into os.environ once per process."""
    env_path = find_project_env(anchor)
    if env_path is None:
        return None

    cache_key = str(env_path.resolve())
    if not override and cache_key in _LOADED_ENV_PATHS:
        return env_path

    load_dotenv(env_path, override=override)
    _LOADED_ENV_PATHS.add(cache_key)
    return env_path
