from __future__ import annotations

from pathlib import Path

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths


def test_get_domain_paths_default_project_root_resolves_repo_root() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    paths = get_domain_paths()
    assert paths.root_dir == repo_root / "data"


def test_canonicalize_project_root_prefers_single_repo_child(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    repo_root = workspace_root / "ai-trading-system"
    (repo_root / "src" / "ai_trading_system").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='ai-trading-system'\n", encoding="utf-8")

    assert canonicalize_project_root(workspace_root) == repo_root.resolve()

    paths = get_domain_paths(project_root=workspace_root)
    assert paths.root_dir == repo_root / "data"
    assert paths.model_dir == repo_root / "models"
    assert paths.reports_dir == repo_root / "reports"


def test_registry_store_uses_canonical_repo_child(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    repo_root = workspace_root / "ai-trading-system"
    (repo_root / "src" / "ai_trading_system").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='ai-trading-system'\n", encoding="utf-8")

    registry = RegistryStore(workspace_root)

    assert registry.project_root == repo_root.resolve()
    assert registry.db_path == repo_root / "data" / "control_plane.duckdb"
    assert registry.db_path.exists()
    assert not (workspace_root / "data" / "control_plane.duckdb").exists()
