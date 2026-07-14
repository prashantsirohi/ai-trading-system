"""Schema metadata and verification for the opportunity registry."""

from __future__ import annotations

from typing import Any

from .models import REGISTRY_SCHEMA_VERSION


TABLES = (
    "opportunity_registry_schema",
    "candidate_episode",
    "candidate_snapshot",
    "candidate_stage_observation",
    "candidate_evidence_observation",
    "candidate_opportunity_observation",
    "candidate_transition",
    "candidate_progress_observation",
    "candidate_decision_context",
    "candidate_outcome_attribution",
)
VIEWS = ("candidate_current_state",)


def verify_schema(conn: Any) -> None:
    relations = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    missing = (set(TABLES) | set(VIEWS)) - relations
    if missing:
        raise RuntimeError(f"opportunity registry schema is incomplete: {sorted(missing)}")
    row = conn.execute(
        "SELECT schema_version FROM opportunity_registry_schema WHERE schema_name = ?",
        ["opportunity_registry"],
    ).fetchone()
    if row is None or row[0] != REGISTRY_SCHEMA_VERSION:
        raise RuntimeError("unexpected opportunity registry schema version")
