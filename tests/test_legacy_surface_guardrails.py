from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_tools_export_excel_is_compatibility_shim() -> None:
    source = (_repo_root() / "tools" / "export_excel.py").read_text(encoding="utf-8")
    assert "ai_trading_system.interfaces.cli.export_excel" in source
    assert "sqlite3.connect" not in source


def test_dashboard_wrappers_delegate_to_ui_modules() -> None:
    dashboard_files = [
        _repo_root() / "dashboard" / "app.py",
        _repo_root() / "dashboard" / "execution" / "app.py",
        _repo_root() / "dashboard" / "research" / "app.py",
    ]
    for path in dashboard_files:
        source = path.read_text(encoding="utf-8")
        assert "from ui." in source
        assert "duckdb.connect" not in source
        assert "sqlite3.connect" not in source


def test_main_entrypoint_message_points_to_canonical_runner() -> None:
    source = (_repo_root() / "main.py").read_text(encoding="utf-8")
    assert "deprecated" in source
    assert "python -m run.orchestrator" in source
