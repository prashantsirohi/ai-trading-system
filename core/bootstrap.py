from pathlib import Path
import sys


def _ensure_src_on_path() -> None:
    """Allow wrappers to resolve src-layout modules without package install."""
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[1]
    src_path = project_root / "src"

    project_root_str = str(project_root)
    src_path_str = str(src_path)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)


_ensure_src_on_path()

from ai_trading_system.platform.utils.bootstrap import *  # noqa: F401,F403,E402
