"""MCP-owned readers for stores whose existing helpers open writable handles.

Every SQLite reader in the wider codebase opens a read-write connection, and
``analytics/feature_reader.py`` opens DuckDB read-write. Rather than loosening
those call sites, the MCP owns its own read-only readers for SQLite and
Parquet. DuckDB stores that already open ``read_only=True`` elsewhere are
reused directly by the tool modules.
"""

from __future__ import annotations

__all__: list[str] = []
