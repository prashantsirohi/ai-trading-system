from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.opportunities.performance_evaluation import (
    mature_performance_events,
)
from ai_trading_system.pipeline.registry import RegistryStore


def _seed_market(path: Path) -> list[date]:
    sessions = [date(2026, 1, 1) + timedelta(days=index) for index in range(30)]
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR, exchange VARCHAR, timestamp TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                is_benchmark BOOLEAN
            )
            """
        )
        conn.executemany(
            "INSERT INTO _catalog VALUES (?, 'NSE', ?, ?, ?, ?, ?, FALSE)",
            [
                (
                    "AAA",
                    session,
                    100.0 + index,
                    101.0 + index,
                    99.0 + index,
                    100.0 + index,
                )
                for index, session in enumerate(sessions)
            ],
        )
        conn.execute(
            """
            CREATE TABLE _index_catalog (
                index_code VARCHAR, date DATE, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE
            )
            """
        )
        conn.executemany(
            "INSERT INTO _index_catalog VALUES ('NIFTY_50', ?, ?, ?, ?, ?)",
            [
                (
                    session,
                    200.0 + index,
                    201.0 + index,
                    199.0 + index,
                    200.0 + index,
                )
                for index, session in enumerate(sessions)
            ],
        )
        conn.executemany(
            "INSERT INTO _index_catalog VALUES ('NIFTY_BANK', ?, ?, ?, ?, ?)",
            [
                (
                    session,
                    300.0 + index,
                    301.0 + index,
                    299.0 + index,
                    300.0 + index,
                )
                for index, session in enumerate(sessions)
            ],
        )
        conn.execute(
            """
            CREATE TABLE sector_to_index (
                system_sector VARCHAR, index_code VARCHAR, index_name VARCHAR,
                is_primary BOOLEAN, fallback_index VARCHAR, created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO sector_to_index
            VALUES ('Finance', 'NIFTY_BANK', 'NIFTY BANK', TRUE, NULL, CURRENT_TIMESTAMP)
            """
        )
    finally:
        conn.close()
    return sessions


def _insert_event(
    conn: duckdb.DuckDBPyConnection,
    *,
    event_id: str,
    event_type: str,
    event_date: date,
    anchor: float,
) -> None:
    conn.execute(
        """
        INSERT INTO investigator_performance_event (
            event_id, candidate_id, setup_id, symbol_id, exchange, sector_name,
            overlap_group_id, event_type, event_at, session_date, anchor_price,
            anchor_price_basis, source_snapshot_id, source_transition_id,
            attribution_mode, primary_eligible, context_as_of, context_json,
            source_run_id, source_artifact_hash, data_quality_status,
            semantic_payload_hash, idempotency_key
        ) VALUES (?, 'candidate-1', 'setup-1', 'AAA', 'NSE', 'Finance',
                  'overlap-1', ?, ?, ?, ?, 'DECISION_SESSION_CLOSE',
                  'snapshot-1', NULL, 'OBSERVED_AT_DECISION', TRUE, ?, '{}',
                  'run-1', 'hash-1', 'PENDING', ?, ?)
        """,
        [
            event_id,
            event_type,
            datetime.combine(event_date, datetime.min.time()),
            event_date,
            anchor,
            datetime.combine(event_date, datetime.min.time()),
            f"semantic-{event_id}",
            f"idempotency-{event_id}",
        ],
    )


def test_matures_discovery_and_confirmed_entry_metrics(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    sessions = _seed_market(ohlcv)
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    with registry._writer() as conn:  # noqa: SLF001
        _insert_event(
            conn,
            event_id="event-discovery",
            event_type="CANDIDATE_DISCOVERED",
            event_date=sessions[0],
            anchor=100.0,
        )
        _insert_event(
            conn,
            event_id="event-entry",
            event_type="ENTRY_CONFIRMED",
            event_date=sessions[2],
            anchor=102.0,
        )
        conn.execute(
            """
            INSERT INTO candidate_transition (
                transition_id, candidate_id, setup_id, from_state, to_state,
                transition_reason, transitioned_at, triggering_snapshot_id,
                rule_version, metadata_json, run_id, stage_name, stage_attempt,
                source_artifact_hash, semantic_payload_hash, idempotency_key
            ) VALUES (
                'transition-1', 'candidate-1', 'setup-1',
                'pending_followthrough', 'confirmed', 'followthrough_confirmed',
                ?, 'snapshot-1', 'rule-v1', '{}', 'run-1', 'opportunities', 1,
                'hash-1', 'semantic-transition', 'idempotency-transition'
            )
            """,
            [datetime.combine(sessions[2], datetime.min.time())],
        )
        conn.execute(
            """
            UPDATE investigator_performance_event
            SET invalidation_price = 103.0
            WHERE event_id = 'event-entry'
            """
        )

    outputs = mature_performance_events(registry, ohlcv_db_path=ohlcv)

    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        discovery_3d = conn.execute(
            """
            SELECT close_to_close_return_pct, maximum_favourable_excursion_pct,
                   days_to_2pct, benchmark_relative_return_pct,
                   sector_relative_return_pct, lifecycle_outcome,
                   data_quality_status
            FROM investigator_performance_horizon
            WHERE event_id = 'event-discovery' AND horizon_sessions = 3
            """
        ).fetchone()
        entry_10d = conn.execute(
            """
            SELECT close_to_close_return_pct, lifecycle_outcome
            FROM investigator_performance_horizon
            WHERE event_id = 'event-entry' AND horizon_sessions = 10
            """
        ).fetchone()
        executable = conn.execute(
            """
            SELECT session_date, next_session_open, simulated_fill_price,
                   fill_policy_version, policy_version
            FROM investigator_performance_event
            WHERE candidate_id = 'candidate-1'
              AND event_type = 'EXECUTABLE_AVAILABLE'
            """
        ).fetchone()
        stop_day = conn.execute(
            """
            SELECT days_to_stop
            FROM investigator_performance_horizon
            WHERE event_id = 'event-entry' AND horizon_sessions = 3
            """
        ).fetchone()[0]
        evaluation_states = {
            row[0]
            for row in conn.execute(
                """
                SELECT to_state
                FROM investigator_evaluation_transition
                WHERE candidate_id = 'candidate-1'
                """
            ).fetchall()
        }
        evaluation_policy_versions = {
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT policy_version
                FROM investigator_evaluation_transition
                WHERE candidate_id = 'candidate-1'
                """
            ).fetchall()
        }

    assert discovery_3d[0] == pytest.approx(3.0)
    assert discovery_3d[1] == pytest.approx(4.0)
    assert discovery_3d[2] == 1
    assert discovery_3d[3] == pytest.approx(1.5)
    assert discovery_3d[4] == pytest.approx(2.0)
    assert discovery_3d[5] == "CONFIRMED"
    assert discovery_3d[6] == "MATURED"
    assert entry_10d[0] == pytest.approx((112.0 / 102.0 - 1.0) * 100.0)
    assert entry_10d[1] == "SUSTAINED_10D"
    assert executable[0] == sessions[3]
    assert executable[1] == 103.0
    assert executable[2] == pytest.approx(103.0515)
    assert executable[3] == "investigator-shadow-fill-v1"
    assert executable[4] == "investigator-attribution-policy-v1"
    assert stop_day == 1
    assert {"PENDING_3D", "CONFIRMED", "EXECUTABLE", "SUSTAINED_10D"}.issubset(
        evaluation_states
    )
    assert evaluation_policy_versions == {"investigator-attribution-policy-v1"}
    assert outputs["investigator_discovery_scorecard"]
    assert outputs["investigator_entry_scorecard"]
    assert outputs["investigator_executable_scorecard"]


def test_daily_coverage_receipt_preserves_unknown_failure_modes(
    tmp_path: Path,
) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    _seed_market(ohlcv)
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    required = {
        "candidate_id": "candidate",
        "setup_id": "setup",
        "as_of": datetime(2026, 1, 2),
        "observed_at": datetime(2026, 1, 2),
        "run_id": "shadow-run",
        "stage_name": "opportunities",
        "stage_attempt": 1,
        "source_artifact_type": "investigator_scores",
        "source_artifact_path": "/tmp/scores.csv",
        "source_artifact_hash": "hash",
        "lifecycle_state": "pending_followthrough",
        "followthrough_status": "pending_3d",
        "days_in_state": 0,
        "days_without_progress": 0,
        "active_position": False,
        "latest_action": "watch",
        "eligibility": "unknown",
        "contract_version": "opportunity-contract-v1",
        "serialization_version": "opportunity-serialization-v1",
        "snapshot_json": "{}",
        "semantic_payload_hash": "semantic",
        "idempotency_key": "idempotency",
    }
    known_states = {
        "stage": "KNOWN",
        "pattern_attempted": "KNOWN",
        "pattern": "NONE",
        "setup_quality": "KNOWN",
        "breakout": "NONE",
        "regime": "KNOWN",
        "breadth": "KNOWN",
        "sector": "KNOWN",
        "lineage": "KNOWN",
    }
    unknown_states = {key: "UNKNOWN" for key in known_states}
    with registry._writer() as conn:  # noqa: SLF001
        for index, states in enumerate((known_states, unknown_states)):
            row = {
                **required,
                "snapshot_id": f"snapshot-{index}",
                "candidate_id": f"candidate-{index}",
                "setup_id": f"setup-{index}",
                "semantic_payload_hash": f"semantic-{index}",
                "idempotency_key": f"idempotency-{index}",
                "review_eligible": index == 0,
                "investigator_context_json": "{}",
                "investigator_evaluation_states_json": json.dumps(states),
                "investigator_missing_fields_json": "[]",
            }
            columns = ", ".join(row)
            placeholders = ", ".join("?" for _ in row)
            conn.execute(
                f"INSERT INTO candidate_snapshot ({columns}) VALUES ({placeholders})",
                list(row.values()),
            )

    outputs = mature_performance_events(registry, ohlcv_db_path=ohlcv)
    stage = next(
        row
        for row in outputs["investigator_coverage_receipt"]
        if row["metric_name"] == "stage_attribution"
    )
    setup = next(
        row
        for row in outputs["investigator_coverage_receipt"]
        if row["metric_name"] == "setup_quality"
    )
    assert stage["coverage_pct"] == 50
    assert stage["status"] == "FAIL"
    assert stage["unexplained_unknown_count"] == 1
    assert setup["coverage_pct"] == 100
    assert setup["status"] == "PASS"


def test_missing_sector_mapping_is_partial_not_fallback(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    sessions = _seed_market(ohlcv)
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    with registry._writer() as conn:  # noqa: SLF001
        _insert_event(
            conn,
            event_id="event-missing-sector",
            event_type="CANDIDATE_DISCOVERED",
            event_date=sessions[0],
            anchor=100.0,
        )
        conn.execute(
            "UPDATE investigator_performance_event SET sector_name = 'Unmapped' WHERE event_id = ?",
            ["event-missing-sector"],
        )

    outputs = mature_performance_events(registry, ohlcv_db_path=ohlcv)

    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        row = conn.execute(
            """
            SELECT sector_index_code, sector_return_pct, data_quality_status,
                   data_quality_reason
            FROM investigator_performance_horizon
            WHERE event_id = 'event-missing-sector' AND horizon_sessions = 3
            """
        ).fetchone()
    assert row[0] is None
    assert row[1] is None
    assert row[2] == "PARTIAL_MATURED"
    assert "sector_index_mapping_missing" in row[3]
    assert any(
        item["reason_scope"] == "HORIZON"
        and "sector_index_mapping_missing" in item["data_quality_reason"]
        for item in outputs["investigator_missing_data_reasons"]
    )


def test_governed_sector_alias_resolves_primary_index_mapping(tmp_path: Path) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    sessions = _seed_market(ohlcv)
    with duckdb.connect(str(ohlcv)) as conn:
        conn.execute(
            """
            INSERT INTO sector_to_index
            VALUES ('Pharma', 'NIFTY_PHARMA', 'NIFTY PHARMA', TRUE, NULL, CURRENT_TIMESTAMP)
            """
        )
        conn.executemany(
            "INSERT INTO _index_catalog VALUES ('NIFTY_PHARMA', ?, ?, ?, ?, ?)",
            [
                (session, 400 + index, 401 + index, 399 + index, 400 + index)
                for index, session in enumerate(sessions)
            ],
        )
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    with registry._writer() as conn:  # noqa: SLF001
        _insert_event(
            conn,
            event_id="event-pharma-alias",
            event_type="CANDIDATE_DISCOVERED",
            event_date=sessions[0],
            anchor=100.0,
        )
        conn.execute(
            "UPDATE investigator_performance_event SET sector_name = ? WHERE event_id = ?",
            ["Pharmaceuticals & Biotechnology", "event-pharma-alias"],
        )

    mature_performance_events(registry, ohlcv_db_path=ohlcv)

    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        row = conn.execute(
            """
            SELECT sector_index_code, sector_return_pct, data_quality_reason
            FROM investigator_performance_horizon
            WHERE event_id = 'event-pharma-alias' AND horizon_sessions = 3
            """
        ).fetchone()
    assert row[0] == "NIFTY_PHARMA"
    assert row[1] is not None
    assert not row[2] or "sector_index_mapping_missing" not in row[2]


def test_missing_pending_3d_sequence_does_not_force_transition_label(
    tmp_path: Path,
) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    sessions = _seed_market(ohlcv)
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    with registry._writer() as conn:  # noqa: SLF001
        _insert_event(
            conn,
            event_id="event-no-pending-sequence",
            event_type="CANDIDATE_DISCOVERED",
            event_date=sessions[0],
            anchor=100.0,
        )
        conn.execute(
            """
            UPDATE investigator_performance_event
            SET lifecycle_evaluable = FALSE,
                data_quality_reason = 'pending_3d_sequence_absent'
            WHERE event_id = 'event-no-pending-sequence'
            """
        )

    outputs = mature_performance_events(registry, ohlcv_db_path=ohlcv)

    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        outcome = conn.execute(
            """
            SELECT lifecycle_outcome
            FROM investigator_performance_horizon
            WHERE event_id = 'event-no-pending-sequence'
              AND horizon_sessions = 3
            """
        ).fetchone()[0]
    assert outcome is None
    assert outputs["investigator_transition_matrix"] == []


def test_projects_discovery_to_legacy_without_repainting_context(
    tmp_path: Path,
) -> None:
    ohlcv = tmp_path / "ohlcv.duckdb"
    sessions = _seed_market(ohlcv)
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    context = {
        "stage_label": "STAGE_2_EARLY",
        "stage_confidence": 82.0,
        "pattern_family": "ACCUMULATION",
        "pattern_state": "CONFIRMED",
        "setup_quality_bucket": "HIGH",
        "breakout_type": "WEEKLY",
        "candidate_tier": "A",
        "qualified_breakout": True,
        "confirmed_regime": "RISK_ON",
        "raw_regime": "RISK_ON",
        "regime_confidence": 0.9,
        "breadth_velocity_bucket": "ACCELERATING",
        "breadth_velocity_quantile": "Q4",
        "regime_score_chg_5d": 7.0,
        "sector_relative_strength_bucket": "HIGH",
        "missing_fields": [],
    }
    with registry._writer() as conn:  # noqa: SLF001
        _insert_event(
            conn,
            event_id="event-projection",
            event_type="CANDIDATE_DISCOVERED",
            event_date=sessions[0],
            anchor=100.0,
        )
        conn.execute(
            "UPDATE investigator_performance_event SET context_json = ? WHERE event_id = ?",
            [json.dumps(context), "event-projection"],
        )

    mature_performance_events(registry, ohlcv_db_path=ohlcv)
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE investigator_performance_event SET context_json = ? WHERE event_id = ?",
            [json.dumps({**context, "stage_label": "STAGE_4"}), "event-projection"],
        )
    mature_performance_events(registry, ohlcv_db_path=ohlcv)

    with registry._connect(read_only=True) as conn:  # noqa: SLF001
        projected = conn.execute(
            """
            SELECT stage_label, pattern_family, fwd_20d_return
            FROM investigator_cohort_performance
            WHERE trade_date = ? AND symbol_id = 'AAA' AND exchange = 'NSE'
            """,
            [sessions[0]],
        ).fetchone()
    assert projected[0] == "STAGE_2_EARLY"
    assert projected[1] == "ACCUMULATION"
    assert projected[2] == pytest.approx(20.0)
