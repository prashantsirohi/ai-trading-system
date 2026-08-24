from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from ai_trading_system.domains.research_screener.store import (
    ResearchScreenerStore,
    canonical_json,
)


class JCurveStore:
    def __init__(self, db_path: str | Path):
        self.base = ResearchScreenerStore(db_path)
        self.db_path = Path(db_path)

    def completed_run(self, run_id: str) -> bool:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                "SELECT status FROM jcurve_research_run WHERE run_id = ?", [run_id]
            ).fetchone()
            return bool(row and row[0] == "COMPLETED")
        finally:
            conn.close()

    def completed_seed_run(self, run_id: str) -> bool:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                "SELECT status FROM jcurve_seed_run WHERE run_id = ?", [run_id]
            ).fetchone()
            return bool(row and row[0] == "COMPLETED")
        finally:
            conn.close()

    def completed_discovery_run(self, run_id: str) -> bool:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                "SELECT status FROM jcurve_discovery_run WHERE run_id = ?", [run_id]
            ).fetchone()
            return bool(row and row[0] == "COMPLETED")
        finally:
            conn.close()

    def persist_discovery(self, payload: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        with self.base.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                for artifact in payload["artifacts"]:
                    conn.execute(
                        "INSERT INTO ingestion_run VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [artifact["ingestion_run_id"], None, artifact["source_key"],
                         payload["as_of_date"], payload["started_at"], now, "COMPLETED", None, None],
                    )
                    conn.execute(
                        """INSERT INTO source_artifact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT DO NOTHING""",
                        [artifact["artifact_id"], artifact["ingestion_run_id"], artifact["source_key"],
                         artifact["provider"], artifact.get("source_url"),
                         artifact.get("local_dataset_id"), artifact.get("effective_date"),
                         artifact.get("published_at"), artifact["retrieved_at"],
                         artifact["content_hash"], artifact["byte_count"], artifact.get("row_count"),
                         artifact["parser_version"], artifact["schema_version"],
                         artifact["validation_status"], artifact.get("parent_artifact_id"),
                         canonical_json(artifact.get("metadata", {}))],
                    )
                    conn.execute(
                        "INSERT INTO ingestion_artifact VALUES (?,?,?) ON CONFLICT DO NOTHING",
                        [artifact["ingestion_run_id"], artifact["artifact_id"], now],
                    )
                conn.execute(
                    """INSERT INTO jcurve_discovery_run VALUES (
                       ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [payload["run_id"], payload["as_of_date"], payload["universe_run_id"],
                     payload["policy_version"], payload["policy_hash"], payload["cohort_version"],
                     payload["cohort_hash"], payload["snapshot_hash"], "COMPLETED",
                     payload["union_count"], payload["universe_eligible_count"],
                     payload["focus_count"], payload["evaluated_count"],
                     payload["primary_queue_count"], payload["started_at"], now],
                )
                for screen in payload["screens"]:
                    conn.execute(
                        """INSERT INTO jcurve_discovery_screen VALUES (
                           ?,?,?,?,?,?,?,?,?)""",
                        [payload["run_id"], screen["screen_id"], screen["lane"],
                         screen["screen_url"], screen["query_text"], screen["query_hash"],
                         screen["source_artifact_id"], screen["source_row_count"],
                         screen["acquisition_mode"]],
                    )
                for row in payload["candidates"]:
                    conn.execute(
                        """INSERT INTO jcurve_discovery_candidate VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [payload["run_id"], row["candidate_key"], row.get("company_name"),
                         row.get("isin"), row.get("nse_symbol"), row.get("bse_code"),
                         row.get("company_id"), row.get("security_id"), row.get("listing_id"),
                         row["identity_status"], canonical_json(row["matched_screen_ids"]),
                         canonical_json(row["matched_lanes"]), row["screen_match_count"],
                         row["baseline_member"], row.get("baseline_case_role"),
                         row["universe_status"], row["focus_eligible"],
                         row.get("statement_basis"), row.get("basis_resolution_reason"),
                         row.get("accounting_disposition"), row["queue_disposition"],
                         row.get("queue_rank"), canonical_json(row["reason_codes"]),
                         canonical_json(row["metrics"]), row["decision_hash"], now],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def persist_seed(self, payload: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        artifact = payload["artifact"]
        with self.base.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    "INSERT INTO ingestion_run VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [artifact["ingestion_run_id"], None, artifact["source_key"], payload["as_of_date"],
                     payload["started_at"], now, "COMPLETED", None, None],
                )
                conn.execute(
                    """INSERT INTO source_artifact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT DO NOTHING""",
                    [artifact["artifact_id"], artifact["ingestion_run_id"], artifact["source_key"],
                     artifact["provider"], artifact.get("source_url"), artifact.get("local_dataset_id"),
                     artifact.get("effective_date"), artifact.get("published_at"), artifact["retrieved_at"],
                     artifact["content_hash"], artifact["byte_count"], artifact.get("row_count"),
                     artifact["parser_version"], artifact["schema_version"], artifact["validation_status"],
                     artifact.get("parent_artifact_id"), canonical_json(artifact.get("metadata", {}))],
                )
                conn.execute(
                    "INSERT INTO ingestion_artifact VALUES (?,?,?) ON CONFLICT DO NOTHING",
                    [artifact["ingestion_run_id"], artifact["artifact_id"], now],
                )
                conn.execute(
                    """INSERT INTO jcurve_seed_run VALUES (
                       ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [payload["run_id"], payload["screen_id"], payload["screen_url"],
                     payload["as_of_date"], payload["policy_version"], payload["policy_hash"],
                     payload["query_text"], payload["query_hash"], payload["source_artifact_id"],
                     payload["snapshot_hash"], "COMPLETED", payload["source_row_count"],
                     payload["evaluated_count"], payload["resolved_count"], payload["candidate_count"],
                     payload["started_at"], now],
                )
                for row in payload["candidates"]:
                    conn.execute(
                        """INSERT INTO jcurve_seed_candidate (
                           run_id, symbol, company_name, screen_member, supplemental_member,
                           screen_exchange, screen_listing_code, screen_isin,
                           screen_nse_symbol, screen_bse_code,
                           company_id, security_id,
                           listing_id, identity_status, statement_basis, basis_resolution_reason,
                           annual_period_count, aligned_quarter_count, accounting_visible,
                           build_signal, commissioning_signal, clean_transfer_signal, ramp_signal,
                           disposition, accepted, reason_codes_json, metrics_json,
                           source_artifact_id, decision_hash, created_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [payload["run_id"], row["symbol"], row.get("company_name"), row["screen_member"],
                         row["supplemental_member"],
                         row.get("screen_exchange"), row.get("screen_listing_code"),
                         row.get("screen_isin"), row.get("screen_nse_symbol"),
                         row.get("screen_bse_code"),
                         row.get("company_id"), row.get("security_id"), row.get("listing_id"),
                         row["identity_status"], row.get("statement_basis"),
                         row["basis_resolution_reason"], row["annual_period_count"],
                         row["aligned_quarter_count"], row["accounting_visible"], row["build_signal"],
                         row["commissioning_signal"], row["clean_transfer_signal"], row["ramp_signal"],
                         row["disposition"], row["accepted"], canonical_json(row["reason_codes"]),
                         canonical_json(row["metrics"]), row["source_artifact_id"], row["decision_hash"], now],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def load_seed_cohort(self, run_id: str) -> dict[str, Any]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            run = conn.execute(
                """SELECT policy_version, policy_hash, as_of_date, snapshot_hash
                   FROM jcurve_seed_run WHERE run_id = ? AND status = 'COMPLETED'""",
                [run_id],
            ).fetchone()
            if not run:
                raise ValueError(f"completed J-curve seed run not found: {run_id}")
            rows = conn.execute(
                """SELECT symbol, company_name, company_id, security_id, listing_id,
                          disposition, decision_hash
                   FROM jcurve_seed_candidate
                   WHERE run_id = ? AND accepted = TRUE AND identity_status = 'RESOLVED'
                   ORDER BY company_id, symbol""",
                [run_id],
            ).fetchall()
        finally:
            conn.close()
        members = [
            {
                "nse_symbol": row[0], "legal_name": row[1], "company_id": row[2],
                "security_id": row[3], "nse_listing_id": row[4],
                "disposition": row[5], "decision_hash": row[6],
            }
            for row in rows
        ]
        return {
            "version": f"{run[0]}:{run_id}",
            "policy_hash": run[1],
            "as_of_date": run[2],
            "snapshot_hash": run[3],
            "company_ids": tuple(sorted({str(row["company_id"]) for row in members})),
            "members": tuple(members),
        }

    def load_discovery_cohort(self, run_id: str) -> dict[str, Any]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            run = conn.execute(
                """SELECT policy_version, policy_hash, as_of_date, snapshot_hash
                   FROM jcurve_discovery_run WHERE run_id = ? AND status = 'COMPLETED'""",
                [run_id],
            ).fetchone()
            if not run:
                raise ValueError(f"completed J-curve discovery run not found: {run_id}")
            rows = conn.execute(
                """SELECT nse_symbol, company_name, company_id, security_id, listing_id,
                          accounting_disposition, decision_hash, queue_rank
                   FROM jcurve_discovery_candidate
                   WHERE run_id = ? AND queue_disposition = 'PRIMARY_RESEARCH'
                     AND identity_status = 'RESOLVED'
                   ORDER BY queue_rank, company_id""",
                [run_id],
            ).fetchall()
        finally:
            conn.close()
        members_list: list[dict[str, Any]] = []
        seen_company_ids: set[str] = set()
        for row in rows:
            company_id = str(row[2])
            if company_id in seen_company_ids:
                continue
            seen_company_ids.add(company_id)
            members_list.append({
                "nse_symbol": row[0], "legal_name": row[1], "company_id": company_id,
                "security_id": row[3], "listing_id": row[4], "disposition": row[5],
                "decision_hash": row[6], "queue_rank": row[7],
            })
        members = tuple(members_list)
        return {
            "version": f"{run[0]}:{run_id}", "policy_hash": run[1],
            "as_of_date": run[2], "snapshot_hash": run[3],
            "company_ids": tuple(str(row["company_id"]) for row in members),
            "members": members,
        }

    def persist_import(self, payload: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        with self.base.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._insert_run(conn, payload, now)
                for artifact in payload["artifacts"]:
                    ingestion_id = artifact["ingestion_run_id"]
                    conn.execute(
                        "INSERT INTO ingestion_run VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        [ingestion_id, None, artifact["source_key"], payload["as_of_date"],
                         payload["started_at"], now, "COMPLETED", None, None],
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
                for row in payload["announcements"]:
                    conn.execute(
                        """INSERT INTO jcurve_announcement VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        [row["announcement_id"], payload["run_id"], row["upstream_raw_event_id"],
                         row["upstream_event_hash"], row.get("company_id"), row.get("security_id"),
                         row.get("listing_id"), row["identity_status"], row["source"], row.get("external_id"),
                         row.get("category"), row["title"], row.get("description"), row["published_at"],
                         row.get("event_at"), row["retrieved_at"], row["source_artifact_id"],
                         row.get("attachment_artifact_id"), now],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def persist_evaluation(self, payload: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        with self.base.writer() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                self._insert_run(conn, payload, now)
                for call in payload["calls"]:
                    conn.execute(
                        """INSERT INTO jcurve_agent_request VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        [call["request_id"], payload["run_id"], call["announcement_id"], call["role"],
                         call["route"], call["model_id"], call.get("provider"), call["prompt_version"],
                         call["prompt_hash"], canonical_json(call["source_page_hashes"]), call["input_tokens"],
                         call["output_tokens"], call.get("cost_usd"), call["schema_valid"], call["attempt_count"],
                         canonical_json(call["retry_history"]), call.get("response_hash"), call["status"],
                         call.get("error_code"), now],
                    )
                for claim in payload["claims"]:
                    conn.execute(
                        """INSERT INTO jcurve_claim VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        [claim["claim_id"], payload["run_id"], claim["announcement_id"], claim["company_id"],
                         claim["security_id"], claim["schema_version"], claim["claim_type"],
                         claim["evidence_state"], claim.get("numeric_value"), claim.get("boolean_value"),
                         claim.get("text_value"), claim.get("unit"), claim.get("project_name"),
                         claim.get("project_location"), claim.get("exact_excerpt"), claim.get("page"),
                         claim["source_artifact_id"], claim["source_content_hash"], claim["published_at"],
                         claim.get("effective_at"), claim["confidence"], claim["status"],
                         canonical_json(claim["claim_json"]), now],
                    )
                for review in payload["reviews"]:
                    conn.execute(
                        """INSERT INTO jcurve_claim_review VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        [review["review_id"], review["claim_id"], review["request_id"], review["decision"],
                         review["reviewer_model"], review.get("provider"), review["prompt_version"],
                         review["independent_context"], review["normalized_claim_hash"],
                         canonical_json(review["issue_codes"]), canonical_json(review["review_json"]), now],
                    )
                for episode in payload["episodes"]:
                    conn.execute(
                        """INSERT INTO jcurve_episode VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        [episode["episode_id"], episode["company_id"], episode["archetype"],
                         episode["project_key"], episode.get("project_name"), episode.get("project_location"),
                         episode["opened_at"], episode.get("closed_at"), episode["status"],
                         episode["first_trigger_artifact_id"], episode["clustering_policy_version"], now],
                    )
                for link in payload["episode_evidence"]:
                    conn.execute(
                        "INSERT INTO jcurve_episode_evidence VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                        [link["episode_id"], link["claim_id"], link["relation"], now],
                    )
                for observation in payload["observations"]:
                    conn.execute(
                        """INSERT INTO jcurve_stage_observation VALUES (
                           ?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        [observation["observation_id"], observation["episode_id"], observation["as_of_date"],
                         observation["stage"], observation["score"], observation["confidence"],
                         observation["materiality_passed"], canonical_json(observation["materiality_metrics"]),
                         canonical_json(observation["reason_codes"]), canonical_json(observation["evidence_ids"]),
                         observation["policy_version"], observation["policy_hash"], now],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _insert_run(conn, payload: dict[str, Any], ended_at: datetime) -> None:
        conn.execute(
            """INSERT INTO jcurve_research_run VALUES (
               ?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
            [payload["run_id"], payload["run_type"], payload.get("parent_run_id"), payload["as_of_date"],
             payload["policy_version"], payload["policy_hash"], payload["snapshot_hash"], "COMPLETED",
             payload.get("announcement_count", 0), payload.get("claim_count", 0),
             payload.get("episode_count", 0), payload.get("degraded_reason"), payload["started_at"], ended_at],
        )

    def load_announcements(self, run_id: str) -> list[dict[str, Any]]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                """SELECT a.*, raw.content_hash AS source_content_hash,
                          attachment.content_hash AS attachment_content_hash,
                          s.isin
                   FROM jcurve_announcement a
                   JOIN source_artifact raw ON raw.artifact_id = a.source_artifact_id
                   LEFT JOIN source_artifact attachment ON attachment.artifact_id = a.attachment_artifact_id
                   LEFT JOIN security_master s ON s.security_id = a.security_id
                   WHERE a.run_id = ? ORDER BY a.published_at, a.announcement_id""",
                [run_id],
            ).fetchall()
            columns = [column[0] for column in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def latest_import_publication(self) -> datetime | None:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute(
                """SELECT max(a.published_at) FROM jcurve_announcement a
                   JOIN jcurve_research_run r ON r.run_id = a.run_id
                   WHERE r.run_type IN ('BOOTSTRAP', 'INCREMENTAL_IMPORT')
                     AND r.status = 'COMPLETED'"""
            ).fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()
