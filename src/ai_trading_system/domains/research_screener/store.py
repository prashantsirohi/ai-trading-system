from __future__ import annotations

import fcntl
import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Iterator

import duckdb

from ai_trading_system.platform.db.paths import get_domain_paths, require_data_root_available


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(value: object) -> str:
    payload = value if isinstance(value, bytes) else canonical_json(value).encode("utf-8")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def default_store_path(project_root: str | Path | None = None) -> Path:
    paths = get_domain_paths(project_root=project_root, data_domain="operational")
    return paths.root_dir / "research_screener" / "control_plane.duckdb"


class ResearchScreenerStore:
    """Single-writer, append-oriented store isolated from the pipeline control plane."""

    def __init__(self, db_path: str | Path | None = None, *, initialize: bool = True):
        self.db_path = Path(db_path) if db_path else default_store_path()
        if initialize:
            if db_path is None:
                require_data_root_available()
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.apply_migrations()

    @contextmanager
    def writer(self) -> Iterator[duckdb.DuckDBPyConnection]:
        lock_path = self.db_path.with_suffix(self.db_path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            conn = duckdb.connect(str(self.db_path))
            try:
                yield conn
            finally:
                conn.close()
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def apply_migrations(self) -> None:
        migration_root = files("ai_trading_system.domains.research_screener.migrations")
        migrations = sorted((entry for entry in migration_root.iterdir() if entry.name.endswith(".sql")), key=lambda entry: entry.name)
        with self.writer() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS schema_migration (version VARCHAR PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")
            applied = {row[0] for row in conn.execute("SELECT version FROM schema_migration").fetchall()}
            for migration in migrations:
                version = migration.name.removesuffix(".sql")
                if version in applied:
                    continue
                conn.execute("BEGIN TRANSACTION")
                try:
                    conn.execute(migration.read_text(encoding="utf-8"))
                    conn.execute("INSERT INTO schema_migration(version) VALUES (?)", [version])
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

    def completed_run(self, run_id: str) -> bool:
        if not self.db_path.exists():
            return False
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                "SELECT status FROM screening_run WHERE run_id = ?", [run_id]
            ).fetchone()
            return bool(row and row[0] == "COMPLETED")
        finally:
            conn.close()

    def completed_research_run(self, run_id: str) -> bool:
        if not self.db_path.exists():
            return False
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                "SELECT status FROM research_discovery_run WHERE run_id = ?", [run_id]
            ).fetchone()
            return bool(row and row[0] == "COMPLETED")
        finally:
            conn.close()

    def persist_research_success(self, payload: dict) -> None:
        """Persist an immutable qualitative-research result without changing screening data."""
        now = datetime.now(UTC)
        with self.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    "INSERT INTO research_discovery_run VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    [payload["run_id"], payload["parent_run_id"], "ANNUAL_REPORT_DISCOVERY",
                     payload["version"], payload["as_of_date"], payload["snapshot_hash"], "COMPLETED",
                     len(payload["documents"]), len(payload["evidence"]), payload["started_at"], now],
                )
                for artifact in payload["artifacts"]:
                    ingestion_id = f"ingest:{payload['run_id']}:{artifact['artifact_id']}"
                    conn.execute(
                        "INSERT INTO ingestion_run VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [ingestion_id, None, artifact["source_key"], payload["as_of_date"],
                         artifact["retrieved_at"], now, artifact["validation_status"],
                         None if artifact["validation_status"] == "VALID" else "ACQUISITION_FAILED",
                         artifact.get("metadata", {}).get("error")],
                    )
                    conn.execute(
                        """INSERT INTO source_artifact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT DO NOTHING""",
                        [artifact["artifact_id"], ingestion_id, artifact["source_key"], artifact["provider"],
                         artifact.get("source_url"), artifact.get("local_dataset_id"), artifact.get("effective_date"),
                         artifact.get("published_at"), artifact["retrieved_at"], artifact["content_hash"],
                         artifact["byte_count"], artifact.get("row_count"), artifact["parser_version"],
                         artifact["schema_version"], artifact["validation_status"],
                         artifact.get("parent_artifact_id"), canonical_json(artifact.get("metadata", {}))],
                    )
                    conn.execute(
                        "INSERT INTO ingestion_artifact VALUES (?,?,?) ON CONFLICT DO NOTHING",
                        [ingestion_id, artifact["artifact_id"], now],
                    )
                for row in payload["documents"]:
                    body = {k: v for k, v in row.items() if k not in {"evidence", "listings"}}
                    conn.execute(
                        "INSERT INTO research_document VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [row["document_id"], row["company_id"], row.get("published_at") or now,
                         canonical_json(body | {"research_run_id": payload["run_id"]}),
                         row.get("source_artifact_id")],
                    )
                for row in payload["evidence"]:
                    body = {k: v for k, v in row.items() if k != "listings"}
                    conn.execute(
                        "INSERT INTO research_evidence VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [row["evidence_id"], row["company_id"], payload["as_of_date"],
                         canonical_json(body | {"research_run_id": payload["run_id"]}),
                         row.get("source_artifact_id")],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def latest_completed_run(self, run_mode: str, as_of_date) -> str | None:
        if not self.db_path.exists():
            return None
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                """SELECT run_id FROM screening_run
                   WHERE run_mode = ? AND as_of_date = ? AND status = 'COMPLETED'
                   ORDER BY ended_at DESC, run_id DESC LIMIT 1""",
                [run_mode, as_of_date],
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def allocate_run_id(self, base_run_id: str) -> tuple[str, str | None]:
        """Return a new retry ID when the content-addressed base already failed."""
        if not self.db_path.exists():
            return base_run_id, None
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                "SELECT run_id, status FROM screening_run WHERE run_id = ? OR run_id LIKE ? ORDER BY run_id",
                [base_run_id, f"{base_run_id}-retry%"],
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            return base_run_id, None
        completed = [run_id for run_id, status in rows if status == "COMPLETED"]
        if completed:
            return sorted(completed)[0], None
        used = {run_id for run_id, _ in rows}
        attempt = 1
        while f"{base_run_id}-retry{attempt}" in used:
            attempt += 1
        return f"{base_run_id}-retry{attempt}", base_run_id

    def persist_success(self, payload: dict) -> None:
        """Atomically persist a complete run; no successful partial run can commit."""
        now = datetime.now(UTC)
        with self.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                registry = payload["source_registry"]
                for key, policy in registry["sources"].items():
                    policy_json = canonical_json(policy)
                    conn.execute(
                        "INSERT INTO source_registry VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [key, registry["registry_version"], policy_json, content_hash(policy), now],
                    )

                definition_id = f"screen:{payload['definition']}"
                definition_version_id = f"{definition_id}:{payload['screen_version']}"
                rules = payload["rules"]
                conn.execute(
                    "INSERT INTO screen_definition VALUES (?,?,?) ON CONFLICT DO NOTHING",
                    [definition_id, payload["definition"], now],
                )
                conn.execute(
                    "INSERT INTO screen_definition_version VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [definition_version_id, definition_id, payload["screen_version"], canonical_json(rules), content_hash(rules), now],
                )
                conn.execute(
                    """
                    INSERT INTO screening_run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        payload["run_id"], definition_version_id, payload["run_mode"], payload["as_of_date"],
                        payload["as_of_date"], payload["price_cutoff"], payload["min_market_cap_cr"],
                        payload["max_market_cap_cr"], payload["input_snapshot_hash"], payload.get("code_version"),
                        "COMPLETED", payload["eligible_count"], len(payload["members"]), payload["started_at"], now,
                        None, None, payload.get("supersedes_failed_run_id"),
                    ],
                )
                for ingest in payload.get("ingestions", []):
                    conn.execute(
                        "INSERT INTO ingestion_run VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [ingest["ingestion_run_id"], payload["run_id"], ingest["source_key"], ingest["effective_date"],
                         ingest["started_at"], ingest.get("ended_at", now), ingest["status"],
                         ingest.get("error_code"), ingest.get("error_message")],
                    )
                for artifact in payload["artifacts"]:
                    conn.execute(
                        """INSERT INTO source_artifact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT (artifact_id) DO UPDATE SET
                           ingestion_run_id = coalesce(source_artifact.ingestion_run_id, excluded.ingestion_run_id)""",
                        [
                            artifact["artifact_id"], artifact.get("ingestion_run_id"), artifact["source_key"],
                            artifact["provider"], artifact.get("source_url"), artifact.get("local_dataset_id"),
                            artifact.get("effective_date"), artifact.get("published_at"), artifact["retrieved_at"],
                            artifact["content_hash"], artifact["byte_count"], artifact.get("row_count"),
                            artifact["parser_version"], artifact["schema_version"], artifact["validation_status"],
                            artifact.get("parent_artifact_id"), canonical_json(artifact.get("metadata", {})),
                        ],
                    )
                    conn.execute(
                        "INSERT INTO ingestion_artifact VALUES (?,?,?) ON CONFLICT DO NOTHING",
                        [artifact["ingestion_run_id"], artifact["artifact_id"], now],
                    )
                conn.execute(
                    "INSERT INTO dataset_snapshot VALUES (?,?,?,?,?,?,?,?)",
                    [f"snapshot:{payload['run_id']}:inputs", payload["run_id"], "screening_inputs",
                     payload["as_of_date"], payload["input_snapshot_hash"], len(payload["members"]),
                     canonical_json(sorted(a["artifact_id"] for a in payload["artifacts"])), now],
                )
                universe_snapshot_id = f"universe:{payload['run_id']}"
                conn.execute(
                    "INSERT INTO universe_snapshot VALUES (?,?,?,?,?)",
                    [universe_snapshot_id, payload["run_id"], payload["universe_hash"], len(payload["members"]), now],
                )
                for member in payload["members"]:
                    self._persist_member(conn, universe_snapshot_id, member, payload["run_id"], now)
                for issue in payload.get("dq_issues", []):
                    issue_id = f"dq:{payload['run_id']}:{issue['issue_id']}"
                    conn.execute(
                        "INSERT INTO data_quality_issue VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        [issue_id, payload["run_id"], issue.get("company_id"), issue.get("security_id"),
                         issue["domain"], issue["code"], issue["severity"], issue["state"], issue["message"],
                         issue.get("source_artifact_id"), now],
                    )
                for repair in payload.get("repairs", []):
                    repair_id = f"repair:{payload['run_id']}:{repair['repair_id']}"
                    conn.execute(
                        "INSERT INTO data_repair_queue VALUES (?,?,?,?,?,?,?,?)",
                        [repair_id, payload["run_id"], repair.get("company_id"), repair["domain"],
                         repair["reason_code"], "OPEN", repair["required_action"], now],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _persist_member(self, conn, universe_snapshot_id: str, member: dict, run_id: str, now: datetime) -> None:
        identity = member["identity"]
        source_artifact_id = identity["source_artifact_id"]
        conn.execute(
            "INSERT INTO company_master VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
            [member["company_id"], member["company"], member["company_type"], member["as_of_date"], None, source_artifact_id],
        )
        if member["identity_status"] == "RESOLVED":
            security_valid_from = identity.get("security_valid_from", member["as_of_date"])
            conn.execute(
                "INSERT INTO security_master VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [member["security_id"], member["company_id"], member["isin"], member["company"], "EQUITY",
                 identity.get("face_value"), "INR", security_valid_from, None, source_artifact_id],
            )
            identifier_rows = list(identity.get("identifier_history", []))
            if not any(row.get("identifier_type") == "ISIN" and row.get("identifier_value") == member["isin"] for row in identifier_rows):
                identifier_rows.append({
                    "identifier_type": "ISIN", "identifier_value": member["isin"], "exchange": None,
                    "valid_from": member["as_of_date"], "valid_to": None, "source_artifact_id": source_artifact_id,
                })
            for listing in identity.get("listings", []):
                listing_id = listing["listing_id"]
                conn.execute(
                    "INSERT INTO listing_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [listing_id, member["security_id"], listing["exchange"], listing.get("exchange_security_id"),
                     listing.get("symbol"), listing.get("bse_code"), listing.get("series"), listing.get("board"),
                     listing.get("active_flag", True), listing.get("listing_date"), None, member["as_of_date"], None,
                     source_artifact_id],
                )
                if listing.get("exchange") == "NSE" and listing.get("symbol"):
                    identifier_rows.append({"identifier_type": "NSE_SYMBOL", "identifier_value": listing["symbol"], "exchange": "NSE", "valid_from": member["as_of_date"], "valid_to": None, "source_artifact_id": source_artifact_id})
                if listing.get("exchange") == "BSE" and listing.get("bse_code"):
                    identifier_rows.append({"identifier_type": "BSE_CODE", "identifier_value": listing["bse_code"], "exchange": "BSE", "valid_from": member["as_of_date"], "valid_to": None, "source_artifact_id": source_artifact_id})
                if listing.get("exchange_security_id"):
                    identifier_rows.append({"identifier_type": "EXCHANGE_SECURITY_ID", "identifier_value": listing["exchange_security_id"], "exchange": listing.get("exchange"), "valid_from": member["as_of_date"], "valid_to": None, "source_artifact_id": source_artifact_id})
            for identifier in identifier_rows:
                identifier_type = identifier["identifier_type"]
                identifier_value = identifier["identifier_value"]
                exchange = identifier.get("exchange")
                valid_from = identifier["valid_from"]
                valid_to = identifier.get("valid_to")
                identifier_source_artifact_id = identifier["source_artifact_id"]
                identifier_id = content_hash([member["security_id"], identifier_type, identifier_value, exchange, valid_from, valid_to, identifier_source_artifact_id])[:24]
                exists = conn.execute(
                    """SELECT 1 FROM security_identifier_history
                       WHERE security_id = ? AND identifier_type = ? AND identifier_value = ?
                         AND coalesce(exchange, '') = coalesce(?, '') AND valid_from = ?
                         AND valid_to IS NOT DISTINCT FROM ? LIMIT 1""",
                    [member["security_id"], identifier_type, identifier_value, exchange, valid_from, valid_to],
                ).fetchone()
                if exists:
                    continue
                conn.execute(
                    "INSERT INTO security_identifier_history VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [f"identifier:{identifier_id}", member["security_id"], identifier_type, identifier_value,
                     exchange, valid_from, valid_to, identifier_source_artifact_id],
                )
        member_key = member.get("member_key", member["symbol"])
        member_id = f"member:{run_id}:{member_key}"
        conn.execute(
            """INSERT INTO universe_member VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [member_id, universe_snapshot_id, member["company_id"], member["security_id"], member["symbol"],
             member["identity_status"], member.get("market_cap_cr"), member.get("market_cap_as_of"),
             member["market_cap_status"], member["statement_scope"], member.get("annual_completeness"),
             member.get("quarterly_completeness"), member["corporate_action_status"], member["data_confidence"],
             member["technical_status"], member["disposition"], canonical_json(member["inputs"])],
        )
        fundamentals = member.get("inputs", {}).get("fundamentals", {})
        for period_type, rows in (
            ("annual", fundamentals.get("annual_statements", [])),
            ("quarterly", fundamentals.get("quarterly_statements", [])),
        ):
            aggregate_completeness = member.get(f"{period_type}_completeness")
            for statement in rows:
                version_material = [
                    member["company_id"], member["security_id"], period_type,
                    statement["period_end"], statement["scope"], statement.get("source_row_hash"),
                ]
                version_id = f"statement:{content_hash(version_material)[:28]}"
                published_at = statement.get("published_at")
                available_from = published_at.date() if hasattr(published_at, "date") else published_at
                conn.execute(
                    "INSERT INTO financial_statement_version VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [version_id, member["company_id"], member["security_id"], period_type,
                     statement["period_end"], statement["scope"], available_from,
                     statement.get("source_artifact_id"), statement.get("source_row_hash"),
                     aggregate_completeness, statement.get(
                         "normalization_status",
                         "NORMALIZED_FROM_ISSUER_PDF"
                         if statement.get("formula_version") == "issuer-pdf-curated-v1"
                         else "NORMALIZED_FROM_EXCHANGE_XBRL",
                     )],
                )

        action_validation = member.get("inputs", {}).get("corporate_action_validation")
        if action_validation is not None:
            action_payload = {
                "official_actions": member["inputs"].get("official_corporate_actions", []),
                "stored_actions": member["inputs"].get("corporate_actions", []),
                "validation": action_validation,
            }
            conn.execute(
                "INSERT INTO corporate_action_version VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [f"actions:{run_id}:{member['symbol']}", member["security_id"], member["as_of_date"],
                 "official-taxonomy-v1", member["corporate_action_status"], content_hash(action_payload)],
            )

        kpi_contract = member.get("inputs", {}).get("kpi_contract")
        if kpi_contract:
            conn.execute(
                "INSERT INTO quarterly_kpi_definition VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
                [f"kpi:{run_id}:{member['symbol']}", member["company_id"], now,
                 canonical_json(kpi_contract), kpi_contract.get("source_artifact_id")],
            )
        decision_id = f"decision:{run_id}:{member_key}"
        decision_body = {
            "symbol": member["symbol"], "fundamental_score": member.get("fundamental_score"),
            "data_confidence": member["data_confidence"], "technical_status": member["technical_status"],
            "disposition": member["disposition"], "reasons": member["reasons"],
        }
        conn.execute(
            "INSERT INTO candidate_decision VALUES (?,?,?,?,?,?,?,?,?)",
            [decision_id, run_id, member_id, member.get("fundamental_score"), member["data_confidence"],
             member["technical_status"], member["disposition"], content_hash(decision_body), now],
        )
        for ordinal, reason in enumerate(member["reasons"], start=1):
            conn.execute(
                "INSERT INTO decision_reason VALUES (?,?,?,?,?,?)",
                [f"reason:{run_id}:{member_key}:{ordinal}", decision_id, ordinal, reason["code"],
                 reason["message"], canonical_json(reason.get("source_artifact_ids", []))],
            )
        if member["disposition"] == "BOUNDARY_REVIEW":
            conn.execute(
                "INSERT INTO boundary_review VALUES (?,?,?,?,?)",
                [f"review:{run_id}:{member_key}", decision_id, "OPEN",
                 canonical_json([r["code"] for r in member["reasons"]]), now],
            )

    def persist_failure(self, payload: dict, *, error_code: str, error_message: str) -> None:
        """Persist only terminal run metadata after rolling back all material results."""
        now = datetime.now(UTC)
        definition_id = f"screen:{payload['definition']}"
        definition_version_id = f"{definition_id}:{payload['screen_version']}"
        with self.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("INSERT INTO screen_definition VALUES (?,?,?) ON CONFLICT DO NOTHING", [definition_id, payload["definition"], now])
                conn.execute(
                    "INSERT INTO screen_definition_version VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [definition_version_id, definition_id, payload["screen_version"], canonical_json(payload["rules"]), content_hash(payload["rules"]), now],
                )
                conn.execute(
                    """INSERT INTO screening_run VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [payload["run_id"], definition_version_id, payload["run_mode"], payload["as_of_date"],
                     payload["as_of_date"], payload["as_of_date"], payload["min_market_cap_cr"],
                     payload["max_market_cap_cr"], payload.get("input_snapshot_hash"), payload.get("code_version"),
                     "FAILED", 0, 0, payload["started_at"], now, error_code, error_message, None],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def compare_latest_regression(self, live_run_id: str) -> str | None:
        """Persist a deterministic comparison without blending member input datasets."""
        with self.writer() as conn:
            regression = conn.execute(
                """SELECT run_id FROM screening_run
                   WHERE run_mode = 'regression_replay' AND status = 'COMPLETED'
                   ORDER BY ended_at DESC, run_id DESC LIMIT 1"""
            ).fetchone()
            if not regression:
                return None
            left_run_id = regression[0]
            rows = conn.execute(
                """SELECT r.run_id, r.run_mode, r.eligible_count, r.evaluated_count,
                          u.fixture_symbol, u.identity_status, u.market_cap_status, u.disposition
                   FROM screening_run r
                   JOIN universe_snapshot s ON s.run_id = r.run_id
                   JOIN universe_member u ON u.universe_snapshot_id = s.universe_snapshot_id
                   WHERE r.run_id IN (?, ?) ORDER BY r.run_id, u.fixture_symbol""",
                [left_run_id, live_run_id],
            ).fetchall()
            summary = {
                "left_run_id": left_run_id, "right_run_id": live_run_id,
                "datasets_separate": True,
                "rows": [list(row) for row in rows],
            }
            digest = content_hash(summary)
            comparison_id = f"comparison:{left_run_id}:{live_run_id}:{digest[:16]}"
            conn.execute(
                "INSERT INTO screening_run_comparison VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [comparison_id, left_run_id, live_run_id, digest, canonical_json(summary), datetime.now(UTC)],
            )
            return comparison_id
