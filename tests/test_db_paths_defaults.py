from __future__ import annotations

from pathlib import Path

from ai_trading_system.platform.db.paths import get_domain_paths


def test_get_domain_paths_default_project_root_resolves_repo_root() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    paths = get_domain_paths()
    assert paths.root_dir == repo_root / "data"

