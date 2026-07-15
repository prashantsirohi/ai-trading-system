"""Safely annotate Phase 3B stage history in an explicitly confirmed copied store."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ai_trading_system.domains.opportunities.stage_governance import (
    annotate_legacy_stage_history,
    preview_legacy_stage_history,
)
from ai_trading_system.platform.db.paths import canonicalize_project_root, get_domain_paths
from ai_trading_system.pipeline.registry import RegistryStore


PROJECT_ROOT = canonicalize_project_root(os.getenv("AI_TRADING_PROJECT_ROOT") or Path.cwd())


def annotate_copied_store(
    copied_control_plane: Path,
    *,
    apply: bool,
    confirmed_copied_store: bool,
    run_id: str | None = None,
) -> dict[str, object]:
    path = copied_control_plane.expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    live_path = (get_domain_paths(PROJECT_ROOT).root_dir / "control_plane.duckdb").resolve()
    if path == live_path:
        raise ValueError("refusing to annotate the configured operator control_plane.duckdb")
    preview = preview_legacy_stage_history(path)
    if not apply:
        return {"status": "dry_run", "path": str(path), "annotations": preview}
    if not confirmed_copied_store:
        raise ValueError("--confirm-copied-store is required with --apply")
    annotation_run_id = run_id or f"phase3c1-legacy-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    registry = RegistryStore(PROJECT_ROOT, db_path=path)
    applied = annotate_legacy_stage_history(
        registry, run_id=annotation_run_id, recorded_at=datetime.now(timezone.utc), apply=True
    )
    return {
        "status": "completed", "path": str(path), "run_id": annotation_run_id,
        "preview": preview, "applied": applied,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preview or append Phase 3C-1 governance overlays in a copied control-plane store.",
    )
    parser.add_argument("--copied-control-plane", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-copied-store", action="store_true")
    parser.add_argument("--run-id")
    args = parser.parse_args()
    result = annotate_copied_store(
        args.copied_control_plane, apply=bool(args.apply),
        confirmed_copied_store=bool(args.confirm_copied_store), run_id=args.run_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
