"""Backup-gated, explicitly invoked control-plane migration command."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

import duckdb

from ai_trading_system.pipeline.registry import CONTROL_PLANE_CURRENT_SCHEMA, RegistryStore
from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths
from ai_trading_system.platform.utils.env import load_project_env


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_hash(manifest_path: Path, filename: str) -> str:
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        parts = raw_line.split()
        if len(parts) >= 2 and parts[-1].lstrip("*") == filename:
            return parts[0]
    raise RuntimeError(f"{filename} is not recorded in {manifest_path}")


def _required_row_counts(db_path: Path) -> dict[str, int]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        counts = {}
        for table_name in sorted(CONTROL_PLANE_CURRENT_SCHEMA):
            identifier = '"' + table_name.replace('"', '""') + '"'
            counts[table_name] = int(conn.execute(f"SELECT COUNT(*) FROM {identifier}").fetchone()[0])
        return counts
    finally:
        conn.close()


def run(
    *,
    project_root: Path,
    db_path: Path,
    backup_dir: Path,
    first: str,
    last: str,
    apply: bool,
) -> dict[str, object]:
    if not apply:
        raise RuntimeError("Refusing to migrate without --apply")
    target = db_path.expanduser().resolve()
    backup = backup_dir.expanduser().resolve()
    copied_db = backup / target.name
    manifest = backup / "SHA256SUMS.txt"
    if not target.is_file():
        raise RuntimeError(f"Control-plane database does not exist: {target}")
    if not copied_db.is_file() or not manifest.is_file():
        raise RuntimeError("Backup must contain the control-plane copy and SHA256SUMS.txt")
    if copied_db.is_symlink() or copied_db.resolve() == target:
        raise RuntimeError("Backup control-plane path must be a distinct regular-file copy")

    manifest_digest = _manifest_hash(manifest, target.name)
    copied_digest = _sha256(copied_db)
    target_digest = _sha256(target)
    if copied_digest != manifest_digest:
        raise RuntimeError("Backup control-plane hash does not match SHA256SUMS.txt")
    if target_digest != copied_digest:
        raise RuntimeError(
            "Live control plane no longer matches the verified backup; take a fresh backup before migrating"
        )

    store = RegistryStore(
        project_root=project_root,
        db_path=target,
        initialize=False,
        allow_migrations=True,
    )
    applied = store.apply_migration_range(first=first, last=last)
    schema = RegistryStore(
        project_root=project_root,
        db_path=target,
        allow_migrations=False,
    ).verify_schema_current()
    return {
        "status": "completed",
        "db_path": str(target),
        "backup_dir": str(backup),
        "backup_sha256": copied_digest,
        "post_migration_sha256": _sha256(target),
        "applied_migrations": applied,
        "verified_schema": schema,
        "row_counts": _required_row_counts(target),
    }


def main() -> int:
    project_root = canonicalize_project_root(os.getenv("AI_TRADING_PROJECT_ROOT") or Path.cwd())
    load_project_env(project_root / ".env")
    default_db = get_domain_paths(project_root).root_dir / "control_plane.duckdb"
    parser = argparse.ArgumentParser(
        description="Apply an explicit, backup-gated control-plane migration range outside pipeline execution."
    )
    parser.add_argument("--db-path", default=str(default_db))
    parser.add_argument("--backup-dir", required=True)
    parser.add_argument("--from-migration", default="033")
    parser.add_argument("--to-migration", default="036")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = run(
        project_root=project_root,
        db_path=Path(args.db_path),
        backup_dir=Path(args.backup_dir),
        first=str(args.from_migration).zfill(3),
        last=str(args.to_migration).zfill(3),
        apply=bool(args.apply),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
