"""Transactional DuckDB persistence for the Actual Portfolio journal."""

from __future__ import annotations

import shutil
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any, Iterator

import duckdb

from ai_trading_system.platform.db.paths import trade_journal_db_path

SCHEMA_VERSION = "001"
_FALLBACK_LOCK = threading.RLock()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def rows_as_dicts(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    names = [item[0] for item in (cursor.description or [])]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


class JournalMigrationRequiredError(RuntimeError):
    pass


class TradeJournalStore:
    def __init__(self, project_root: Path | str, db_path: Path | str | None = None):
        self.project_root = Path(project_root)
        self.db_path = Path(db_path) if db_path else trade_journal_db_path(project_root)

    def _connect(self, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=read_only)

    @contextmanager
    def writer_lock(self) -> Iterator[None]:
        lock_path = self.db_path.with_suffix(f"{self.db_path.suffix}.writer.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+")
        try:
            try:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            except ImportError:  # pragma: no cover
                _FALLBACK_LOCK.acquire()
            yield
        finally:
            try:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except ImportError:  # pragma: no cover
                _FALLBACK_LOCK.release()
            handle.close()

    @contextmanager
    def writer(self) -> Iterator[duckdb.DuckDBPyConnection]:
        self.verify_schema()
        with self.writer_lock():
            conn = self._connect()
            try:
                conn.execute("BEGIN TRANSACTION")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    @contextmanager
    def reader(self) -> Iterator[duckdb.DuckDBPyConnection]:
        self.verify_schema()
        conn = self._connect(read_only=True)
        try:
            yield conn
        finally:
            conn.close()

    def migrate(self, *, apply: bool) -> dict[str, Any]:
        if not apply:
            return {"status": "preview", "db_path": str(self.db_path), "schema_version": SCHEMA_VERSION}
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        backup = None
        if self.db_path.exists():
            stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
            backup = self.db_path.with_name(f"{self.db_path.name}.backup-{stamp}")
            shutil.copy2(self.db_path, backup)
        migration = resources.files("ai_trading_system.domains.trade_journal.migrations").joinpath("001_initial.sql")
        with self.writer_lock():
            conn = self._connect()
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute(migration.read_text(encoding="utf-8"))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        self.verify_schema()
        return {"status": "applied", "db_path": str(self.db_path), "schema_version": SCHEMA_VERSION, "backup": str(backup) if backup else None}

    def verify_schema(self) -> None:
        if not self.db_path.is_file():
            raise JournalMigrationRequiredError(
                "trade journal schema is not initialized; run ai-trading-journal migrate --apply"
            )
        conn = self._connect(read_only=True)
        try:
            row = conn.execute(
                "SELECT schema_version FROM journal_schema WHERE schema_name = ?",
                ["trade_journal"],
            ).fetchone()
        except duckdb.Error as exc:
            raise JournalMigrationRequiredError("trade journal schema is incomplete") from exc
        finally:
            conn.close()
        if row is None or str(row[0]) != SCHEMA_VERSION:
            raise JournalMigrationRequiredError(
                f"trade journal schema version must be {SCHEMA_VERSION}"
            )

    def list_imports(self, *, account_ref: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        limit = min(250, max(1, int(limit)))
        with self.reader() as conn:
            if account_ref:
                cursor = conn.execute(
                    "SELECT * FROM journal_import_file WHERE account_ref = ? ORDER BY created_at DESC, import_id DESC LIMIT ?",
                    [account_ref, limit],
                )
            else:
                cursor = conn.execute(
                    "SELECT * FROM journal_import_file ORDER BY created_at DESC, import_id DESC LIMIT ?",
                    [limit],
                )
            return rows_as_dicts(cursor)

    def list_dq(self, *, account_ref: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        limit = min(250, max(1, int(limit)))
        with self.reader() as conn:
            if account_ref:
                cursor = conn.execute(
                    "SELECT * FROM journal_dq_issue WHERE account_ref = ? ORDER BY created_at DESC, issue_id DESC LIMIT ?",
                    [account_ref, limit],
                )
            else:
                cursor = conn.execute(
                    "SELECT * FROM journal_dq_issue ORDER BY created_at DESC, issue_id DESC LIMIT ?",
                    [limit],
                )
            return rows_as_dicts(cursor)

    def accounts(self) -> list[str]:
        with self.reader() as conn:
            return [row[0] for row in conn.execute(
                "SELECT DISTINCT account_ref FROM journal_import_file ORDER BY account_ref"
            ).fetchall()]

    def positions(self, account_ref: str) -> list[dict[str, Any]]:
        with self.reader() as conn:
            return rows_as_dicts(conn.execute(
                "SELECT * FROM journal_current_positions WHERE account_ref = ? ORDER BY instrument_id",
                [account_ref],
            ))

    def episodes(self, account_ref: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.reader() as conn:
            return rows_as_dicts(conn.execute(
                """SELECT e.* FROM trade_episode e JOIN journal_latest_analysis a USING(analysis_run_id)
                   WHERE e.account_ref = ? AND a.analysis_type='reconstruction' AND a.status='COMPLETED'
                   ORDER BY e.opened_at DESC,e.episode_id DESC LIMIT ?""",
                [account_ref, min(250, max(1, int(limit)))],
            ))
