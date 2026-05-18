from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.platform.db.paths import (
    canonicalize_project_root,
    get_domain_paths,
    require_data_root_available,
)


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


def test_canonicalize_project_root_climbs_from_package_dir(tmp_path: Path) -> None:
    repo_root = tmp_path / "ai-trading-system"
    package_dir = repo_root / "src" / "ai_trading_system" / "analytics"
    package_dir.mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='ai-trading-system'\n", encoding="utf-8")

    assert canonicalize_project_root(package_dir) == repo_root.resolve()

    paths = get_domain_paths(project_root=package_dir)
    assert paths.root_dir == repo_root / "data"
    assert not str(paths.root_dir).endswith("src/ai_trading_system/data")


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


def test_env_var_overrides_relocate_all_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "ssd" / "data"
    reports_root = tmp_path / "ssd" / "reports"
    logs_root = tmp_path / "ssd" / "logs"
    models_root = tmp_path / "ssd" / "models"
    for d in (data_root, reports_root, logs_root, models_root):
        d.mkdir(parents=True)

    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("REPORTS_ROOT", str(reports_root))
    monkeypatch.setenv("LOGS_ROOT", str(logs_root))
    monkeypatch.setenv("MODELS_ROOT", str(models_root))

    paths = get_domain_paths()

    assert paths.root_dir == data_root.resolve()
    assert paths.ohlcv_db_path == data_root.resolve() / "ohlcv.duckdb"
    assert paths.feature_store_dir == data_root.resolve() / "feature_store"
    assert paths.pipeline_runs_dir == data_root.resolve() / "pipeline_runs"
    assert paths.optuna_dir == data_root.resolve() / "optuna"
    assert paths.fundamentals_dir == data_root.resolve() / "fundamentals"
    assert paths.reports_dir == reports_root.resolve()
    assert paths.logs_dir == logs_root.resolve()
    assert paths.model_dir == models_root.resolve()

    # master_db_path is special-cased: always in-repo, never under DATA_ROOT
    repo_root = Path(__file__).resolve().parents[1]
    assert paths.master_db_path == repo_root / "data" / "masterdata.db"


def test_research_domain_nests_under_relocated_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "ssd" / "data"
    reports_root = tmp_path / "ssd" / "reports"
    models_root = tmp_path / "ssd" / "models"
    for d in (data_root, reports_root, models_root):
        d.mkdir(parents=True)

    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("REPORTS_ROOT", str(reports_root))
    monkeypatch.setenv("MODELS_ROOT", str(models_root))

    paths = get_domain_paths(data_domain="research")

    assert paths.root_dir == data_root.resolve() / "research"
    assert paths.ohlcv_db_path == data_root.resolve() / "research" / "research_ohlcv.duckdb"
    assert paths.reports_dir == reports_root.resolve() / "research"
    assert paths.model_dir == models_root.resolve() / "research"


def test_require_data_root_available_raises_when_path_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing = tmp_path / "not-mounted"
    monkeypatch.setenv("DATA_ROOT", str(missing))
    with pytest.raises(RuntimeError, match="DATA_ROOT"):
        require_data_root_available()


def test_require_data_root_available_noop_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATA_ROOT", raising=False)
    # Must not raise — in-repo fallback is always valid.
    require_data_root_available()
