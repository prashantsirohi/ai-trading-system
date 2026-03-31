"""Core path-resolution API."""

from utils.data_domains import (
    DataDomain,
    DataDomainPaths,
    ensure_domain_layout,
    get_domain_paths,
    research_static_end_date,
    resolve_data_domain,
)

__all__ = [
    "DataDomain",
    "DataDomainPaths",
    "resolve_data_domain",
    "get_domain_paths",
    "ensure_domain_layout",
    "research_static_end_date",
]
