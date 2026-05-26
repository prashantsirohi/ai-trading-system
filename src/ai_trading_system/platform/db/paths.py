"""Core path-resolution API."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

DataDomain = Literal["operational", "research"]


@dataclass(frozen=True)
class DataDomainPaths:
    """Resolved storage layout for one data domain."""

    domain: DataDomain
    root_dir: Path
    ohlcv_db_path: Path
    feature_store_dir: Path
    master_db_path: Path
    pipeline_runs_dir: Path
    dataset_dir: Path
    model_dir: Path
    reports_dir: Path
    logs_dir: Path
    optuna_dir: Path
    fundamentals_dir: Path
    stage_store_dir: Path
    raw_dir: Path
    cache_dir: Path
    exports_dir: Path


def _looks_like_repo_root(path: Path) -> bool:
    return (
        (path / "src" / "ai_trading_system").exists()
        and (path / "pyproject.toml").exists()
    )


def canonicalize_project_root(project_root: Path | str | None = None) -> Path:
    """Normalize a project root to the actual repo when given a workspace parent.

    This guards against launch contexts that pass a parent workspace directory
    containing exactly one repo checkout, which would otherwise create sibling
    `data/`, `models/`, and `reports/` folders beside the repo. It also
    normalizes paths inside the repo, such as package directories under `src/`,
    back to the checkout root.
    """

    root = Path(project_root).resolve() if project_root else _default_project_root()
    if _looks_like_repo_root(root) or not root.exists():
        return root

    for parent in root.parents:
        if _looks_like_repo_root(parent):
            return parent

    candidates = [
        child.resolve()
        for child in root.iterdir()
        if child.is_dir() and _looks_like_repo_root(child)
    ]
    if len(candidates) == 1:
        return candidates[0]
    return root


def resolve_data_domain(data_domain: str | None = None) -> DataDomain:
    """Normalize the configured data domain."""
    domain = (data_domain or os.getenv("DATA_DOMAIN") or "operational").lower()
    if domain not in {"operational", "research"}:
        raise ValueError(f"Unsupported data domain: {domain}")
    return domain  # type: ignore[return-value]


def _default_project_root() -> Path:
    """Infer repository root from this module location."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "src" / "ai_trading_system").exists():
            return parent
    return here.parents[4]


def _resolve_root(env_var: str, default: Path, *, honor_env: bool = True) -> Path:
    """Return the env-var override (expanded/resolved) or the default."""
    raw = os.getenv(env_var) if honor_env else None
    if raw:
        return Path(raw).expanduser().resolve()
    return default


def require_data_root_available(paths: DataDomainPaths | None = None) -> None:
    """Raise if DATA_ROOT is set to a missing path (e.g. SSD unmounted).

    When DATA_ROOT is unset the in-repo fallback is always valid, so this is a
    no-op. When set, the directory must exist — otherwise pipelines would
    silently recreate the layout in the wrong place.
    """
    if not os.getenv("DATA_ROOT"):
        return
    target = paths.root_dir if paths is not None else Path(os.environ["DATA_ROOT"]).expanduser().resolve()
    if not target.exists():
        raise RuntimeError(
            f"DATA_ROOT is set to {target} but the directory does not exist. "
            "Is the external storage mounted?"
        )


def resolve_artifact_path(
    uri: str | Path,
    *,
    project_root: Path | str | None = None,
    data_domain: str | None = "operational",
) -> Path:
    """Resolve migrated pipeline artifact URIs against the configured data root."""
    path = Path(uri)
    if path.exists():
        return path

    parts = path.parts
    try:
        pipeline_idx = parts.index("pipeline_runs")
    except ValueError:
        return path

    relative_parts = parts[pipeline_idx + 1 :]
    if not relative_parts:
        return path

    candidate = get_domain_paths(project_root=project_root, data_domain=data_domain).pipeline_runs_dir.joinpath(*relative_parts)
    if candidate.exists():
        return candidate
    return path


def find_latest_pipeline_artifact(
    *,
    project_root: Path | str | None = None,
    data_domain: str | None = "operational",
    stage_name: str = "rank",
    filename: str = "ranked_signals.csv",
    limit: int = 200,
) -> tuple[str, Path] | None:
    """Return the latest run directory containing a stage artifact file."""
    pipeline_runs_dir = get_domain_paths(project_root=project_root, data_domain=data_domain).pipeline_runs_dir
    if not pipeline_runs_dir.is_dir():
        return None

    def _run_key(path: Path) -> tuple[date, str, float]:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0.0
        match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", path.name)
        run_date = date.min
        if match:
            try:
                run_date = date.fromisoformat(match.group(1))
            except ValueError:
                run_date = date.min
        return run_date, path.name, mtime

    def _attempt_key(path: Path) -> int:
        try:
            return int(path.name.rsplit("_", 1)[-1])
        except (IndexError, ValueError):
            return -1

    checked = 0
    for run_dir in sorted((p for p in pipeline_runs_dir.iterdir() if p.is_dir()), key=_run_key, reverse=True):
        checked += 1
        if checked > int(limit):
            break
        stage_dir = run_dir / stage_name
        if not stage_dir.is_dir():
            continue
        for attempt_dir in sorted((p for p in stage_dir.glob("attempt_*") if p.is_dir()), key=_attempt_key, reverse=True):
            artifact_path = attempt_dir / filename
            if artifact_path.exists():
                return run_dir.name, artifact_path
    return None


def get_domain_paths(
    project_root: Path | str | None = None,
    data_domain: str | None = None,
) -> DataDomainPaths:
    """Resolve filesystem paths for the requested data domain.

    Honors `DATA_ROOT`, `REPORTS_ROOT`, `LOGS_ROOT`, and `MODELS_ROOT` env vars
    to relocate large trees outside the repo. Falls back to repo-relative paths
    when the env vars are unset, preserving the legacy in-repo layout.

    `master_db_path` follows `DATA_ROOT` when configured. This keeps the
    operational master database colocated with migrated runtime data.
    """
    root = canonicalize_project_root(project_root)
    if _looks_like_repo_root(root):
        try:
            from ai_trading_system.platform.utils.env import load_project_env

            load_project_env(root)
        except Exception:
            pass
    domain = resolve_data_domain(data_domain)

    honor_env_roots = _looks_like_repo_root(root)
    data_root = _resolve_root("DATA_ROOT", root / "data", honor_env=honor_env_roots)
    reports_root = _resolve_root("REPORTS_ROOT", root / "reports", honor_env=honor_env_roots)
    logs_root = _resolve_root("LOGS_ROOT", root / "logs", honor_env=honor_env_roots)
    models_root = _resolve_root("MODELS_ROOT", root / "models", honor_env=honor_env_roots)
    master_db_path = data_root / "masterdata.db"

    if domain == "operational":
        return DataDomainPaths(
            domain=domain,
            root_dir=data_root,
            ohlcv_db_path=data_root / "ohlcv.duckdb",
            feature_store_dir=data_root / "feature_store",
            master_db_path=master_db_path,
            pipeline_runs_dir=data_root / "pipeline_runs",
            dataset_dir=data_root / "training_datasets",
            model_dir=models_root,
            reports_dir=reports_root,
            logs_dir=logs_root,
            optuna_dir=data_root / "optuna",
            fundamentals_dir=data_root / "fundamentals",
            stage_store_dir=data_root / "stage_store",
            raw_dir=data_root / "raw",
            cache_dir=data_root / "cache",
            exports_dir=data_root / "exports",
        )

    domain_root = data_root / domain
    return DataDomainPaths(
        domain=domain,
        root_dir=domain_root,
        ohlcv_db_path=domain_root / "research_ohlcv.duckdb",
        feature_store_dir=domain_root / "feature_store",
        master_db_path=master_db_path,
        pipeline_runs_dir=domain_root / "pipeline_runs",
        dataset_dir=domain_root / "training_datasets",
        model_dir=models_root / domain,
        reports_dir=reports_root / domain,
        logs_dir=logs_root / domain,
        optuna_dir=domain_root / "optuna",
        fundamentals_dir=domain_root / "fundamentals",
        stage_store_dir=domain_root / "stage_store",
        raw_dir=domain_root / "raw",
        cache_dir=domain_root / "cache",
        exports_dir=domain_root / "exports",
    )


def ensure_domain_layout(
    project_root: Path | str | None = None,
    data_domain: str | None = None,
) -> DataDomainPaths:
    """Create the directory layout for a data domain and return the resolved paths."""
    paths = get_domain_paths(project_root=project_root, data_domain=data_domain)
    paths.root_dir.mkdir(parents=True, exist_ok=True)
    paths.feature_store_dir.mkdir(parents=True, exist_ok=True)
    paths.pipeline_runs_dir.mkdir(parents=True, exist_ok=True)
    paths.dataset_dir.mkdir(parents=True, exist_ok=True)
    paths.model_dir.mkdir(parents=True, exist_ok=True)
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    return paths


def research_static_end_date(today: date | None = None) -> str:
    """Return the default research snapshot ceiling: Dec 31 of the prior year."""
    current = today or date.today()
    return date(current.year - 1, 12, 31).isoformat()


__all__ = [
    "DataDomain",
    "DataDomainPaths",
    "canonicalize_project_root",
    "resolve_data_domain",
    "get_domain_paths",
    "ensure_domain_layout",
    "resolve_artifact_path",
    "find_latest_pipeline_artifact",
    "require_data_root_available",
    "research_static_end_date",
]
