"""Resolved paths and read-only connections for the MCP interface.

Invariant I1: the MCP owns every SQLite and Parquet handle it uses, and every
handle it opens is read-only. DuckDB stores open with ``read_only=True``;
SQLite stores open through a ``file:...?mode=ro`` URI; Parquet is read through
an in-memory DuckDB connection that never touches a live store file.

Invariant I3: the ``operator`` profile requires an explicit ``DATA_ROOT``.
``require_data_root_available()`` alone is not enough — it is a no-op when the
variable is unset, which is exactly the case where the resolver would silently
fall back to the repo-local ``data/`` tree.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterator

import duckdb

from ai_trading_system.platform.db.paths import (
    DataDomainPaths,
    canonicalize_project_root,
    control_plane_db_path,
    get_domain_paths,
    require_data_root_available,
)


class McpProfile(str, Enum):
    """Where the server is allowed to read from."""

    OPERATOR = "operator"
    FIXTURE = "fixture"


class McpConfigurationError(RuntimeError):
    """The server cannot start safely with the current configuration."""


class StoreUnavailableError(RuntimeError):
    """A requested store file does not exist."""


class StoreBusyError(RuntimeError):
    """Another process holds a conflicting lock on the store.

    DuckDB refuses a read-only attach while a writer holds the file, so a
    running pipeline makes ``ohlcv.duckdb`` and ``control_plane.duckdb``
    temporarily unreadable. This is raised rather than returning an empty
    result, because "no rows" would read as "no such data".
    """


@dataclass(frozen=True, slots=True)
class McpContext:
    """Read-only access to the operational stores."""

    profile: McpProfile
    paths: DataDomainPaths
    project_root: Path

    # -- construction ----------------------------------------------------

    @classmethod
    def from_env(
        cls,
        profile: McpProfile | str = McpProfile.OPERATOR,
        *,
        project_root: Path | str | None = None,
        data_domain: str | None = "operational",
    ) -> "McpContext":
        resolved_profile = (
            profile
            if isinstance(profile, McpProfile)
            else McpProfile(str(profile).strip().lower())
        )
        root = canonicalize_project_root(project_root)

        # get_domain_paths loads .env when the root looks like the repo, so
        # DATA_ROOT is visible after this call even if the caller did not
        # source the environment first.
        paths = get_domain_paths(project_root=root, data_domain=data_domain)

        if resolved_profile is McpProfile.OPERATOR:
            cls._require_external_data_root(paths, root)
        require_data_root_available(paths)

        return cls(profile=resolved_profile, paths=paths, project_root=root)

    @staticmethod
    def _require_external_data_root(paths: DataDomainPaths, project_root: Path) -> None:
        """Fail closed when the operator profile would read a repo-local tree."""

        import os

        configured = os.getenv("DATA_ROOT")
        if not configured:
            raise McpConfigurationError(
                "DATA_ROOT is not set. The 'operator' MCP profile refuses to fall "
                "back to the repo-local data/ tree. Load the operator environment "
                "(set -a; source .env; set +a) or start with --profile fixture."
            )

        root_dir = paths.root_dir
        if not root_dir.exists():
            raise McpConfigurationError(
                f"DATA_ROOT resolves to {root_dir}, which does not exist. "
                "Is the external storage mounted?"
            )

        repo = project_root.resolve()
        try:
            root_dir.resolve().relative_to(repo)
        except ValueError:
            return
        raise McpConfigurationError(
            f"DATA_ROOT resolves to {root_dir}, which is inside the repository at "
            f"{repo}. The 'operator' profile requires an external data root; use "
            "--profile fixture for repo-local or temporary trees."
        )

    # -- store paths -----------------------------------------------------

    @property
    def ohlcv_db(self) -> Path:
        return self.paths.ohlcv_db_path

    @property
    def control_plane_db(self) -> Path:
        return control_plane_db_path(
            project_root=self.project_root, data_domain=self.paths.domain
        )

    @property
    def master_db(self) -> Path:
        return self.paths.master_db_path

    @property
    def fundamentals_db(self) -> Path:
        return self.paths.root_dir / "fundamentals.duckdb"

    @property
    def screener_db(self) -> Path:
        return self.paths.fundamentals_dir / "screener_financials.db"

    @property
    def feature_store_dir(self) -> Path:
        return self.paths.feature_store_dir

    # -- read-only connections -------------------------------------------

    @contextmanager
    def duckdb(self, db_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
        """Open a DuckDB store read-only."""

        if not db_path.exists():
            raise StoreUnavailableError(f"DuckDB store is unavailable: {db_path}")
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower():
                raise
            raise StoreBusyError(
                f"{db_path.name} is locked by another process, most likely a "
                "running pipeline. DuckDB refuses a read-only attach while a "
                "writer holds the file. Retry once the run finishes; no data "
                "was read."
            ) from exc
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def ohlcv(
        self, *, adjusted_view: bool = False
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """Open ``ohlcv.duckdb`` read-only.

        With ``adjusted_view`` the ``_catalog_feature_source`` TEMP VIEW is
        created, exposing ``COALESCE(adjusted_*, raw)`` OHLC under the plain
        column names — the same basis every technical feature is computed on.
        Temp objects live in DuckDB's temp catalog, so this works on a
        read-only connection.
        """

        with self.duckdb(self.ohlcv_db) as conn:
            if adjusted_view:
                from ai_trading_system.domains.features.repository import (
                    ensure_feature_catalog_source,
                )

                ensure_feature_catalog_source(conn)
            yield conn

    @contextmanager
    def control_plane(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self.duckdb(self.control_plane_db) as conn:
            yield conn

    @contextmanager
    def fundamentals(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self.duckdb(self.fundamentals_db) as conn:
            yield conn

    @contextmanager
    def sqlite(self, db_path: Path) -> Iterator[sqlite3.Connection]:
        """Open a SQLite store read-only via a ``mode=ro`` URI."""

        if not db_path.exists():
            raise StoreUnavailableError(f"SQLite store is unavailable: {db_path}")
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def parquet(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """An in-memory DuckDB connection for ``read_parquet`` scans.

        Deliberately not backed by a store file, so a Parquet read cannot hold
        a handle on a live database.
        """

        conn = duckdb.connect(":memory:")
        try:
            yield conn
        finally:
            conn.close()

    # -- normalization ---------------------------------------------------

    @staticmethod
    def normalize_symbol(symbol: str | None) -> str:
        """Canonical uppercase symbol, matching the rest of the repository."""

        return str(symbol or "").strip().upper()

    @staticmethod
    def resolve_exchange(exchange: str | None) -> str:
        """Normalize an exchange code, defaulting to NSE."""

        value = str(exchange or "NSE").strip().upper()
        if value not in {"NSE", "BSE"}:
            raise ValueError(f"Unsupported exchange: {exchange!r} (expected NSE or BSE)")
        return value

    def store_label(self, db_path: Path, table: str) -> str:
        """``file:table`` label used in every response's ``meta.source``.

        Two different tables share the name ``sector_earnings_leadership`` (and
        ``valuation_cycle_features``) across stores, so the file name is part
        of the identifier.
        """

        return f"{db_path.name}:{table}"


__all__ = [
    "McpConfigurationError",
    "McpContext",
    "McpProfile",
    "StoreBusyError",
    "StoreUnavailableError",
]
