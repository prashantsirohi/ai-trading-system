"""Core path-resolution API."""

from __future__ import annotations

import os
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


def resolve_data_domain(data_domain: str | None = None) -> DataDomain:
    """Normalize the configured data domain."""
    domain = (data_domain or os.getenv("DATA_DOMAIN") or "operational").lower()
    if domain not in {"operational", "research"}:
        raise ValueError(f"Unsupported data domain: {domain}")
    return domain  # type: ignore[return-value]


def get_domain_paths(
    project_root: Path | str | None = None,
    data_domain: str | None = None,
) -> DataDomainPaths:
    """Resolve filesystem paths for the requested data domain.

    Operational paths fall back to the legacy flat `data/` layout when it already
    exists, which keeps this refactor incremental and low risk.
    """
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    domain = resolve_data_domain(data_domain)
    data_root = root / "data"

    if domain == "operational":
        return DataDomainPaths(
            domain=domain,
            root_dir=data_root,
            ohlcv_db_path=data_root / "ohlcv.duckdb",
            feature_store_dir=data_root / "feature_store",
            master_db_path=data_root / "masterdata.db",
            pipeline_runs_dir=data_root / "pipeline_runs",
            dataset_dir=data_root / "training_datasets",
            model_dir=root / "models",
            reports_dir=root / "reports",
        )

    domain_root = data_root / domain
    return DataDomainPaths(
        domain=domain,
        root_dir=domain_root,
        ohlcv_db_path=domain_root / ("ohlcv.duckdb" if domain == "operational" else "research_ohlcv.duckdb"),
        feature_store_dir=domain_root / "feature_store",
        master_db_path=data_root / "masterdata.db",
        pipeline_runs_dir=domain_root / "pipeline_runs",
        dataset_dir=domain_root / "training_datasets",
        model_dir=root / "models" / domain,
        reports_dir=root / "reports" / domain,
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
    return paths


def research_static_end_date(today: date | None = None) -> str:
    """Return the default research snapshot ceiling: Dec 31 of the prior year."""
    current = today or date.today()
    return date(current.year - 1, 12, 31).isoformat()


__all__ = [
    "DataDomain",
    "DataDomainPaths",
    "resolve_data_domain",
    "get_domain_paths",
    "ensure_domain_layout",
    "research_static_end_date",
]
