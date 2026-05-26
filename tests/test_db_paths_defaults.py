from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.platform.db.paths import (
    canonicalize_project_root,
    get_domain_paths,
    find_latest_pipeline_artifact,
    require_data_root_available,
    resolve_artifact_path,
)


def _disable_root_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("DATA_ROOT", "REPORTS_ROOT", "LOGS_ROOT", "MODELS_ROOT", "DATA_DOMAIN"):
        monkeypatch.setenv(key, "")


def test_get_domain_paths_default_project_root_resolves_repo_root(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_root_env(monkeypatch)
    repo_root = Path(__file__).resolve().parents[1]
    paths = get_domain_paths()
    assert paths.root_dir == repo_root / "data"


def test_canonicalize_project_root_prefers_single_repo_child(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_root_env(monkeypatch)
    workspace_root = tmp_path / "workspace"
    repo_root = workspace_root / "ai-trading-system"
    (repo_root / "src" / "ai_trading_system").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='ai-trading-system'\n", encoding="utf-8")

    assert canonicalize_project_root(workspace_root) == repo_root.resolve()

    paths = get_domain_paths(project_root=workspace_root)
    assert paths.root_dir == repo_root / "data"
    assert paths.model_dir == repo_root / "models"
    assert paths.reports_dir == repo_root / "reports"


def test_canonicalize_project_root_climbs_from_package_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_root_env(monkeypatch)
    repo_root = tmp_path / "ai-trading-system"
    package_dir = repo_root / "src" / "ai_trading_system" / "analytics"
    package_dir.mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='ai-trading-system'\n", encoding="utf-8")

    assert canonicalize_project_root(package_dir) == repo_root.resolve()

    paths = get_domain_paths(project_root=package_dir)
    assert paths.root_dir == repo_root / "data"
    assert not str(paths.root_dir).endswith("src/ai_trading_system/data")


def test_registry_store_uses_canonical_repo_child(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_root_env(monkeypatch)
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

    assert paths.master_db_path == data_root.resolve() / "masterdata.db"


def test_resolve_artifact_path_remaps_legacy_pipeline_run_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "external" / "data"
    run_id = "pipeline-2026-05-26-demo"
    migrated = data_root / "pipeline_runs" / run_id / "rank" / "attempt_1" / "ranked_signals.csv"
    migrated.parent.mkdir(parents=True)
    migrated.write_text("symbol_id\nAAA\n", encoding="utf-8")
    monkeypatch.setenv("DATA_ROOT", str(data_root))

    legacy_uri = tmp_path / "repo" / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1" / "ranked_signals.csv"

    assert resolve_artifact_path(legacy_uri, project_root=Path(__file__).resolve().parents[1]) == migrated.resolve()


def test_find_latest_pipeline_artifact_scans_migrated_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "external" / "data"
    older = data_root / "pipeline_runs" / "pipeline-2026-05-25-old" / "rank" / "attempt_1" / "ranked_signals.csv"
    newer = data_root / "pipeline_runs" / "pipeline-2026-05-26-new" / "rank" / "attempt_1" / "ranked_signals.csv"
    older.parent.mkdir(parents=True)
    newer.parent.mkdir(parents=True)
    older.write_text("symbol_id\nOLD\n", encoding="utf-8")
    newer.write_text("symbol_id\nNEW\n", encoding="utf-8")
    monkeypatch.setenv("DATA_ROOT", str(data_root))

    result = find_latest_pipeline_artifact(project_root=Path(__file__).resolve().parents[1])

    assert result == ("pipeline-2026-05-26-new", newer.resolve())


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
