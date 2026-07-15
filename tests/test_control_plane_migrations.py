from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

from ai_trading_system.interfaces.cli.migrate_control_plane import run as run_migration
from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator
from ai_trading_system.pipeline.registry import (
    ControlPlaneMigrationRequiredError,
    RegistryStore,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_orchestrator_requires_current_schema_or_explicit_migration_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "runtime"
    monkeypatch.setenv("DATA_ROOT", str(data_root))

    with pytest.raises(ControlPlaneMigrationRequiredError):
        PipelineOrchestrator(tmp_path)
    assert not (data_root / "control_plane.duckdb").exists()

    PipelineOrchestrator(tmp_path, allow_control_plane_migrations=True)
    RegistryStore(tmp_path, allow_migrations=False).verify_schema_current()


def test_explicit_range_migration_is_backup_gated_and_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "runtime"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    db_path = data_root / "control_plane.duckdb"
    store = RegistryStore(tmp_path, db_path=db_path, initialize=False)
    store.apply_migration_range(first="001", last="032")

    with pytest.raises(ControlPlaneMigrationRequiredError):
        RegistryStore(tmp_path, db_path=db_path, allow_migrations=False)

    backup_dir = data_root / "backups" / "pre_migration"
    backup_dir.mkdir(parents=True)
    copied = backup_dir / db_path.name
    shutil.copy2(db_path, copied)
    digest = _sha256(copied)
    (backup_dir / "SHA256SUMS.txt").write_text(f"{digest}  {db_path.name}\n", encoding="utf-8")

    result = run_migration(
        project_root=tmp_path,
        db_path=db_path,
        backup_dir=backup_dir,
        first="033",
        last="036",
        apply=True,
    )

    assert result["status"] == "completed"
    assert result["applied_migrations"] == [
        "033_opportunity_phase3b.sql",
        "034_opportunity_phase3c1_governance.sql",
        "035_opportunity_phase3c1a_governance_hardening.sql",
        "036_opportunity_phase3c3_position_monitoring.sql",
    ]
    RegistryStore(tmp_path, db_path=db_path, allow_migrations=False).verify_schema_current()


def test_explicit_range_migration_rejects_stale_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "runtime"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    db_path = data_root / "control_plane.duckdb"
    RegistryStore(tmp_path, db_path=db_path)
    backup_dir = data_root / "backups" / "stale"
    backup_dir.mkdir(parents=True)
    copied = backup_dir / db_path.name
    shutil.copy2(db_path, copied)
    digest = _sha256(copied)
    (backup_dir / "SHA256SUMS.txt").write_text(f"{digest}  {db_path.name}\n", encoding="utf-8")
    with db_path.open("ab") as handle:
        handle.write(b"changed")

    with pytest.raises(RuntimeError, match="no longer matches"):
        run_migration(
            project_root=tmp_path,
            db_path=db_path,
            backup_dir=backup_dir,
            first="033",
            last="036",
            apply=True,
        )


def test_explicit_range_migration_rejects_backup_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "runtime"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    db_path = data_root / "control_plane.duckdb"
    RegistryStore(tmp_path, db_path=db_path)
    backup_dir = data_root / "backups" / "linked"
    backup_dir.mkdir(parents=True)
    (backup_dir / db_path.name).symlink_to(db_path)
    digest = _sha256(db_path)
    (backup_dir / "SHA256SUMS.txt").write_text(f"{digest}  {db_path.name}\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="distinct regular-file copy"):
        run_migration(
            project_root=tmp_path,
            db_path=db_path,
            backup_dir=backup_dir,
            first="033",
            last="036",
            apply=True,
        )
