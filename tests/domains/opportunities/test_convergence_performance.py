from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.opportunities.convergence_performance import (
    ConvergencePerformanceConflictError,
    _anchor_candidates,
    _confidence_label,
    _observation,
    evaluate_convergence_performance,
)
from ai_trading_system.pipeline.registry import RegistryStore


SESSION = date(2026, 1, 2)
OBSERVED_AT = datetime(2026, 1, 2, 12, tzinfo=timezone.utc)


def _convergence_row(**overrides):
    row = {
        "exchange": "NSE",
        "symbol_id": "ABC",
        "session_date": SESSION.isoformat(),
        "policy_snapshot_id": "policy-snapshot",
        "convergence_policy_version": "opportunity-convergence-v1.2",
        "convergence_cohort": "I_F_P",
        "investigator_member": True,
        "fundamental_member": True,
        "pattern_member": True,
        "sector_name": "Pharma",
        "sector_evaluation_state": "KNOWN",
        "investigator_invalidation_price": 96.0,
        "evidence_hash": "evidence-hash",
    }
    row.update(overrides)
    return row


def _ohlcv(path: Path, *, sessions: int) -> None:
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE _catalog (
                exchange VARCHAR, symbol_id VARCHAR, timestamp TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                is_benchmark BOOLEAN
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE _index_catalog (
                index_code VARCHAR, date DATE, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE sector_to_index (
                system_sector VARCHAR, index_code VARCHAR, is_primary BOOLEAN
            )
            """
        )
        conn.execute(
            "INSERT INTO sector_to_index VALUES ('Pharma', 'NIFTY_PHARMA', TRUE)"
        )
        _append_market_rows(conn, start=0, stop=sessions)


def _append_market_rows(
    conn: duckdb.DuckDBPyConnection, *, start: int, stop: int
) -> None:
    for offset in range(start, stop):
        session = SESSION + timedelta(days=offset)
        close = 100.0 + offset
        conn.execute(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)",
            ["NSE", "ABC", session, close - 0.25, close + 1, close - 1, close],
        )
        for index_code, base in (("NIFTY_50", 200.0), ("NIFTY_PHARMA", 300.0)):
            index_close = base + offset
            conn.execute(
                "INSERT INTO _index_catalog VALUES (?, ?, ?, ?, ?, ?)",
                [
                    index_code,
                    session,
                    index_close - 0.5,
                    index_close + 1,
                    index_close - 1,
                    index_close,
                ],
            )


def test_persists_and_matures_discovery_and_next_open_anchors(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    ohlcv = tmp_path / "ohlcv.duckdb"
    _ohlcv(ohlcv, sessions=25)

    outputs = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-1",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=ohlcv,
        persist=True,
    )

    assert len(outputs["opportunity_convergence_observations"]) == 1
    anchors = outputs["opportunity_convergence_anchors"]
    assert {row["anchor_type"] for row in anchors} == {
        "DISCOVERY_CLOSE",
        "DISCOVERY_NEXT_OPEN_FILL",
    }
    horizons = outputs["opportunity_convergence_horizons"]
    assert len(horizons) == 8
    twenty_day = [row for row in horizons if row["horizon_sessions"] == 20]
    assert all(row["data_quality_status"] == "MATURED" for row in twenty_day)
    assert all(row["return_pct"] is not None for row in twenty_day)
    assert all(row["benchmark_relative_return_pct"] is not None for row in twenty_day)
    assert all(row["sector_relative_return_pct"] is not None for row in twenty_day)
    discovery_20 = next(
        row for row in twenty_day if row["anchor_type"] == "DISCOVERY_CLOSE"
    )
    assert discovery_20["maximum_favourable_excursion_pct"] is not None
    assert discovery_20["maximum_adverse_excursion_pct"] is not None
    assert discovery_20["days_to_2pct"] == 1
    assert discovery_20["days_to_5pct"] == 4
    assert discovery_20["days_to_stop"] is None
    assert outputs["opportunity_convergence_primary_cohorts"]

    second = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-2",
        stage_attempt=2,
        observed_at=OBSERVED_AT + timedelta(hours=1),
        ohlcv_db_path=ohlcv,
        persist=True,
    )
    assert len(second["opportunity_convergence_observations"]) == 1
    assert len(second["opportunity_convergence_anchors"]) == 2
    assert len(second["opportunity_convergence_horizons"]) == 8


def test_partial_horizons_remain_pending_and_are_not_zero_returns(
    tmp_path: Path,
) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    ohlcv = tmp_path / "ohlcv.duckdb"
    _ohlcv(ohlcv, sessions=4)

    outputs = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-partial",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=ohlcv,
        persist=True,
    )

    discovery = [
        row
        for row in outputs["opportunity_convergence_horizons"]
        if row["anchor_type"] == "DISCOVERY_CLOSE"
    ]
    matured_3 = next(row for row in discovery if row["horizon_sessions"] == 3)
    pending_20 = next(row for row in discovery if row["horizon_sessions"] == 20)
    assert matured_3["data_quality_status"] == "MATURED"
    assert pending_20["data_quality_status"] == "PENDING"
    assert pending_20["observed_sessions"] == 3
    assert pending_20["partial_return_pct"] == 3.0
    assert pending_20["return_pct"] is None
    assert "awaiting_17_sessions" in pending_20["data_quality_reason"]


def test_pending_horizon_advances_but_terminal_result_is_not_repainted(
    tmp_path: Path,
) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    ohlcv = tmp_path / "ohlcv.duckdb"
    _ohlcv(ohlcv, sessions=4)
    first = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-partial",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=ohlcv,
        persist=True,
    )
    first_20 = next(
        row
        for row in first["opportunity_convergence_horizons"]
        if row["anchor_type"] == "DISCOVERY_CLOSE" and row["horizon_sessions"] == 20
    )
    assert first_20["data_quality_status"] == "PENDING"

    with duckdb.connect(str(ohlcv)) as conn:
        _append_market_rows(conn, start=4, stop=25)
    matured = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-matured",
        stage_attempt=2,
        observed_at=OBSERVED_AT + timedelta(days=25),
        ohlcv_db_path=ohlcv,
        persist=True,
    )
    matured_20 = next(
        row
        for row in matured["opportunity_convergence_horizons"]
        if row["anchor_type"] == "DISCOVERY_CLOSE" and row["horizon_sessions"] == 20
    )
    assert matured_20["data_quality_status"] == "MATURED"
    frozen_return = matured_20["return_pct"]

    with duckdb.connect(str(ohlcv)) as conn:
        conn.execute(
            """
            UPDATE _catalog SET close = 999
            WHERE symbol_id = ? AND CAST(timestamp AS DATE) = ?
            """,
            ["ABC", SESSION + timedelta(days=20)],
        )
    replayed = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-replayed",
        stage_attempt=3,
        observed_at=OBSERVED_AT + timedelta(days=26),
        ohlcv_db_path=ohlcv,
        persist=True,
    )
    replayed_20 = next(
        row
        for row in replayed["opportunity_convergence_horizons"]
        if row["anchor_type"] == "DISCOVERY_CLOSE" and row["horizon_sessions"] == 20
    )
    assert replayed_20["return_pct"] == frozen_return


def test_none_cohort_is_retained_without_generating_performance_rows(
    tmp_path: Path,
) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    outputs = evaluate_convergence_performance(
        registry,
        convergence_rows=[
            _convergence_row(
                convergence_cohort="NONE",
                investigator_member=False,
                fundamental_member=False,
                pattern_member=False,
            )
        ],
        run_id="run-none",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=None,
        persist=True,
    )

    assert len(outputs["opportunity_convergence_observations"]) == 1
    assert outputs["opportunity_convergence_anchors"] == []
    assert outputs["opportunity_convergence_horizons"] == []


def test_missing_benchmark_and_sector_are_explicit_partial_maturation(
    tmp_path: Path,
) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    ohlcv = tmp_path / "ohlcv.duckdb"
    _ohlcv(ohlcv, sessions=25)
    with duckdb.connect(str(ohlcv)) as conn:
        conn.execute("DROP TABLE _index_catalog")
        conn.execute("DROP TABLE sector_to_index")

    outputs = evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-gaps",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=ohlcv,
        persist=True,
    )
    discovery_20 = next(
        row
        for row in outputs["opportunity_convergence_horizons"]
        if row["anchor_type"] == "DISCOVERY_CLOSE" and row["horizon_sessions"] == 20
    )
    assert discovery_20["data_quality_status"] == "PARTIAL_MATURED"
    assert discovery_20["return_pct"] is not None
    assert discovery_20["benchmark_relative_return_pct"] is None
    assert discovery_20["sector_relative_return_pct"] is None
    assert set(discovery_20["data_quality_reason"].split(";")) == {
        "benchmark_history_missing",
        "sector_index_mapping_missing",
    }


def test_changed_same_policy_observation_fails_closed(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    ohlcv = tmp_path / "ohlcv.duckdb"
    _ohlcv(ohlcv, sessions=2)
    evaluate_convergence_performance(
        registry,
        convergence_rows=[_convergence_row()],
        run_id="run-1",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=ohlcv,
        persist=True,
    )

    with pytest.raises(ConvergencePerformanceConflictError):
        evaluate_convergence_performance(
            registry,
            convergence_rows=[
                _convergence_row(
                    convergence_cohort="I_ONLY",
                    fundamental_member=False,
                    pattern_member=False,
                )
            ],
            run_id="run-2",
            stage_attempt=2,
            observed_at=OBSERVED_AT + timedelta(hours=1),
            ohlcv_db_path=ohlcv,
            persist=True,
        )


def test_canonical_confirmation_and_executable_anchors_are_independent() -> None:
    observation = _observation(_convergence_row(), "run-1", 1, OBSERVED_AT)
    prices = {
        ("NSE", "ABC"): [
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": SESSION,
                "open": 99.0,
                "high": 101.0,
                "low": 98.0,
                "close": 100.0,
            },
            {
                "exchange": "NSE",
                "symbol_id": "ABC",
                "session_date": SESSION + timedelta(days=1),
                "open": 101.0,
                "high": 103.0,
                "low": 100.0,
                "close": 102.0,
            },
        ]
    }
    events = [
        {
            "event_id": "discovery-event",
            "candidate_id": "candidate-1",
            "exchange": "NSE",
            "symbol_id": "ABC",
            "session_date": SESSION,
            "event_type": "CANDIDATE_DISCOVERED",
        },
        {
            "event_id": "confirmation-event",
            "candidate_id": "candidate-1",
            "exchange": "NSE",
            "symbol_id": "ABC",
            "session_date": SESSION + timedelta(days=1),
            "event_type": "ENTRY_CONFIRMED",
            "anchor_price": 102.0,
            "anchor_price_basis": "DECISION_SESSION_CLOSE",
            "source_run_id": "run-2",
        },
        {
            "event_id": "executable-event",
            "candidate_id": "candidate-1",
            "exchange": "NSE",
            "symbol_id": "ABC",
            "session_date": SESSION + timedelta(days=2),
            "event_type": "EXECUTABLE_AVAILABLE",
            "anchor_price": 103.5,
            "anchor_price_basis": "DETERMINISTIC_SHADOW_FILL",
            "fill_policy_version": "investigator-shadow-fill-v1",
            "source_run_id": "run-3",
        },
    ]

    anchors = _anchor_candidates(
        [observation], prices=prices, performance_events=events, run_id="run-3"
    )

    assert {row["anchor_type"] for row in anchors} == {
        "DISCOVERY_CLOSE",
        "DISCOVERY_NEXT_OPEN_FILL",
        "CONFIRMATION_CLOSE",
        "EXECUTABLE_SHADOW_FILL",
    }
    assert (
        next(row for row in anchors if row["anchor_type"] == "CONFIRMATION_CLOSE")[
            "anchor_price"
        ]
        == 102.0
    )
    assert (
        next(row for row in anchors if row["anchor_type"] == "EXECUTABLE_SHADOW_FILL")[
            "anchor_price"
        ]
        == 103.5
    )


@pytest.mark.parametrize(
    ("sample_count", "expected"),
    [
        (29, "EXPLORATORY"),
        (30, "PROVISIONAL"),
        (60, "MODERATE"),
        (120, "POLICY_ELIGIBLE"),
    ],
)
def test_confidence_bands(sample_count: int, expected: str) -> None:
    assert _confidence_label(sample_count) == expected
