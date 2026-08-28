"""Read-only, account-scoped context for the private journal MCP server."""

from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import duckdb

from ai_trading_system.interfaces.mcp.context import (
    McpConfigurationError,
    McpContext,
    McpProfile,
    StoreBusyError,
    StoreUnavailableError,
)
from ai_trading_system.platform.db.paths import trade_journal_db_path

JOURNAL_SCHEMA_VERSION = "002"


class JournalSchemaError(RuntimeError):
    """The journal store is absent or has not been migrated to this contract."""


@dataclass(frozen=True, slots=True)
class JournalMcpContext:
    """One private journal database pinned to exactly one account."""

    profile: McpProfile
    project_root: Path
    db_path: Path
    account_ref: str

    @classmethod
    def from_env(
        cls,
        profile: McpProfile | str = McpProfile.OPERATOR,
        *,
        project_root: Path | str | None = None,
    ) -> "JournalMcpContext":
        base = McpContext.from_env(
            profile,
            project_root=project_root,
            data_domain="operational",
        )
        configured_account = os.getenv(
            "AI_TRADING_JOURNAL_MCP_ACCOUNT_REF", ""
        ).strip()
        probe = cls(
            profile=base.profile,
            project_root=base.project_root,
            db_path=trade_journal_db_path(base.project_root),
            account_ref=configured_account,
        )
        probe.verify_schema()
        if configured_account:
            return probe
        with probe.reader() as conn:
            accounts = [
                str(row[0])
                for row in conn.execute(
                    """SELECT account_ref FROM (
                         SELECT account_ref FROM journal_import_file
                         UNION SELECT account_ref FROM journal_analysis_run
                       ) GROUP BY account_ref ORDER BY account_ref"""
                ).fetchall()
            ]
        if len(accounts) != 1:
            raise McpConfigurationError(
                "AI_TRADING_JOURNAL_MCP_ACCOUNT_REF is required when the journal "
                f"contains {len(accounts)} accounts; the server will not guess."
            )
        return cls(
            profile=probe.profile,
            project_root=probe.project_root,
            db_path=probe.db_path,
            account_ref=accounts[0],
        )

    @property
    def account_scope(self) -> str:
        digest = hashlib.sha256(self.account_ref.encode("utf-8")).hexdigest()[:12]
        return f"account_{digest}"

    @contextmanager
    def reader(self) -> Iterator[duckdb.DuckDBPyConnection]:
        if not self.db_path.is_file():
            raise StoreUnavailableError(
                f"Trade journal store is unavailable: {self.db_path}"
            )
        try:
            conn = duckdb.connect(str(self.db_path), read_only=True)
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower():
                raise
            raise StoreBusyError(
                "trade_journal.duckdb is locked by a journal writer; retry after "
                "the import, reconstruction, reconciliation, or analysis finishes."
            ) from exc
        try:
            yield conn
        finally:
            conn.close()

    def verify_schema(self) -> None:
        try:
            with self.reader() as conn:
                row = conn.execute(
                    "SELECT schema_version FROM journal_schema WHERE schema_name = ?",
                    ["trade_journal"],
                ).fetchone()
        except duckdb.Error as exc:
            raise JournalSchemaError("trade journal schema is incomplete") from exc
        if row is None or str(row[0]) != JOURNAL_SCHEMA_VERSION:
            observed = None if row is None else str(row[0])
            raise JournalSchemaError(
                f"trade journal schema must be {JOURNAL_SCHEMA_VERSION}; found {observed}"
            )


__all__ = ["JOURNAL_SCHEMA_VERSION", "JournalMcpContext", "JournalSchemaError"]
