"""Shared bootstrap setup for ranking script-like output modules."""

from __future__ import annotations

from pathlib import Path

from ai_trading_system.platform.utils.bootstrap import ensure_project_root_on_path
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.platform.utils.env import load_project_env


def bootstrap_script_environment(anchor: str | Path) -> str:
    project_root = str(ensure_project_root_on_path(Path(anchor).resolve().parents[4]))
    load_project_env(project_root)
    logger.disable("googleapiclient")
    logger.disable("google.auth")
    return project_root
