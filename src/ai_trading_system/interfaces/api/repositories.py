"""Read-only canonical-source access for Phase 4A.

Only fixed internal identifiers are interpolated.  All values are bound.  The
class has no write method and never initializes a schema.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import duckdb

from ai_trading_system.domains.opportunities.coverage import (
    read_sector_stage_as_of,
    read_stock_stage_as_of,
)

from .config import ApiSettings, SourceProfile


class _CanonicalReaderAdapter:
    def __init__(self, access: "ReadOnlyDataAccess") -> None:
        self.access = access

    @contextmanager
    def _reader(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self.access.connection() as conn:
            yield conn


class ReadOnlyDataAccess:
    """The sole Phase 4A boundary to DuckDB and immutable evidence."""

    def __init__(self, settings: ApiSettings):
        self.settings = settings
        self.path = settings.control_plane_path()

    @contextmanager
    def connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        if self.path is None:
            raise FileNotFoundError("fixture profile has no database")
        conn = duckdb.connect(str(self.path), read_only=True)
        try:
            yield conn
        finally:
            conn.close()

    def source_readable(self) -> bool:
        if self.settings.source_profile is SourceProfile.SMALL_FIXTURE:
            return True
        try:
            with self.connection() as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except (OSError, duckdb.Error, ValueError):
            return False

    def tables(self) -> set[str]:
        if self.settings.source_profile is SourceProfile.SMALL_FIXTURE or not self.source_readable():
            return set()
        with self.connection() as conn:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                    ["main"],
                ).fetchall()
            }

    def rows(self, table: str) -> list[dict[str, Any]]:
        allowed = {
            "candidate_episode", "candidate_snapshot", "candidate_decision_context",
            "candidate_outcome_attribution", "opportunity_scan_routing_history",
            "pipeline_alert", "pipeline_alert_incident", "position_recovery_proposal",
            "stage_observation_governance", "stage_correction_impact",
            "sector_membership_history",
        }
        if table not in allowed or table not in self.tables():
            return []
        with self.connection() as conn:
            cursor = conn.execute(f"SELECT * FROM {table}")  # noqa: S608 -- allowlisted table
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def stages(self, *, scope: str, as_of: datetime) -> list[dict[str, Any]]:
        required = {
            "stage_observation_governance",
            "weekly_stock_stage_history" if scope == "stock" else "weekly_sector_stage_history",
        }
        if not required.issubset(self.tables()):
            return []
        adapter = _CanonicalReaderAdapter(self)
        if scope == "stock":
            frame = read_stock_stage_as_of(
                adapter, as_of=as_of.isoformat(), available_at=as_of, exchange="NSE"
            )
        else:
            frame = read_sector_stage_as_of(
                adapter, as_of=as_of.isoformat(), available_at=as_of
            )
        return frame.to_dict(orient="records")

    def artifact_json(self, artifact_type: str) -> dict[str, Any] | None:
        """Read the newest promoted JSON artifact registered by a completed attempt."""
        if not {"pipeline_artifact", "pipeline_stage_run"}.issubset(self.tables()):
            return None
        with self.connection() as conn:
            row = conn.execute(
                """SELECT artifact.uri FROM pipeline_artifact artifact
                   JOIN pipeline_stage_run stage
                     ON stage.run_id = artifact.run_id
                    AND stage.stage_name = artifact.stage_name
                    AND stage.attempt_number = artifact.attempt_number
                   WHERE artifact.artifact_type = ?
                     AND artifact.lifecycle_status = 'promoted'
                     AND stage.status = 'completed'
                   ORDER BY artifact.promoted_at DESC NULLS LAST, artifact.created_at DESC
                   LIMIT 1""",
                [artifact_type],
            ).fetchone()
        if row is None:
            return None
        path = Path(str(row[0]))
        if not path.is_file() or path.is_symlink():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None


def parse_json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)

