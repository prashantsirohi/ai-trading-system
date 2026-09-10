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
        "convergence_policy_version": "opportunity-convergence-v1.3",
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
        conn.execute("""
            CREATE TABLE _catalog (
                exchange VARCHAR, symbol_id VARCHAR, timestamp TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                is_benchmark BOOLEAN
            )
            """)
        conn.execute("""
            CREATE TABLE _index_catalog (
                index_code VARCHAR, date DATE, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE
            )
            """)
        conn.execute("""
            CREATE TABLE sector_to_index (
                system_sector VARCHAR, index_code VARCHAR, is_primary BOOLEAN
            )
            """)
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
    assert discovery_20["data_quality_status"] == "INSUFFICIENT_PRICE_DATA"
    assert discovery_20["return_pct"] is None
    assert discovery_20["benchmark_relative_return_pct"] is None
    assert discovery_20["sector_relative_return_pct"] is None
    assert set(discovery_20["data_quality_reason"].split(";")) == {
        "market_calendar_unavailable_or_anchor_session_missing",
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
        [observation],
        prices=prices,
        performance_events=events,
        run_id="run-3",
        indices={"NIFTY_50": prices[("NSE", "ABC")]},
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


def _evaluate(registry, ohlcv, rows=None):
    return evaluate_convergence_performance(
        registry,
        convergence_rows=rows if rows is not None else [_convergence_row()],
        run_id="m2-validation",
        stage_attempt=1,
        observed_at=OBSERVED_AT,
        ohlcv_db_path=ohlcv,
        persist=True,
    )


def _horizon(outputs, kind="DISCOVERY_CLOSE", horizon=3):
    return next(
        r
        for r in outputs["opportunity_convergence_horizons"]
        if r["anchor_type"] == kind and r["horizon_sessions"] == horizon
    )


def test_missing_stock_session_does_not_shift_target_or_fill(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "DELETE FROM _catalog WHERE CAST(timestamp AS DATE) = ?",
            [SESSION + timedelta(days=1)],
        )
    outputs = _evaluate(registry, path)
    row = _horizon(outputs)
    assert row["data_quality_status"] == "INSUFFICIENT_PRICE_DATA"
    assert row["target_session_date"] == SESSION + timedelta(days=3)
    assert row["return_pct"] is None
    assert "DISCOVERY_NEXT_OPEN_FILL" not in {
        r["anchor_type"] for r in outputs["opportunity_convergence_anchors"]
    }
    assert not outputs["opportunity_convergence_primary_cohorts"]
    with duckdb.connect(str(path)) as conn:
        # Restore only the missing stock bar; do not alter the reference calendar.
        conn.execute(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)",
            ["NSE", "ABC", SESSION + timedelta(days=1), 100.75, 102, 100, 101],
        )
    assert _horizon(_evaluate(registry, path))["return_pct"] == 3.0


def test_missing_discovery_price_can_arrive_without_rewriting_anchor(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    first = _evaluate(registry, path)
    assert first["opportunity_convergence_anchors"] == []
    _ohlcv(path, sessions=25)
    restored = _evaluate(registry, path)
    assert _horizon(restored)["return_pct"] == 3.0
    assert len(restored["opportunity_convergence_anchors"]) == 2
    with registry._reader() as conn:
        original = conn.execute(
            "SELECT * FROM opportunity_convergence_anchor ORDER BY anchor_id"
        ).fetchall()
    _evaluate(registry, path)
    with registry._reader() as conn:
        assert (
            conn.execute(
                "SELECT * FROM opportunity_convergence_anchor ORDER BY anchor_id"
            ).fetchall()
            == original
        )


def test_open_fill_includes_entry_day_risk_at_day_zero(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "UPDATE _catalog SET high = ?, low = ? WHERE CAST(timestamp AS DATE) = ?",
            [130, 80, SESSION + timedelta(days=1)],
        )
    result = _evaluate(registry, path)
    fill = _horizon(result, "DISCOVERY_NEXT_OPEN_FILL")
    assert fill["days_to_stop"] == fill["days_to_5pct"] == 0
    assert fill["maximum_adverse_excursion_pct"] < -20
    assert fill["maximum_favourable_excursion_pct"] > 28
    assert fill["target_session_date"] == SESSION + timedelta(days=4)
    # Discovery close was held through that same bar on its first forward day.
    assert _horizon(result)["days_to_stop"] == 1


@pytest.mark.parametrize(
    "defect", ["duplicate", "nan", "infinite", "negative", "missing_low"]
)
def test_invalid_bars_cannot_mature(tmp_path, defect):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    with duckdb.connect(str(path)) as conn:
        if defect == "duplicate":
            conn.execute(
                "INSERT INTO _catalog SELECT * FROM _catalog WHERE CAST(timestamp AS DATE) = ?",
                [SESSION + timedelta(days=2)],
            )
        else:
            value = {
                "nan": float("nan"),
                "infinite": float("inf"),
                "negative": -1,
                "missing_low": None,
            }[defect]
            conn.execute(
                "UPDATE _catalog SET low = ? WHERE CAST(timestamp AS DATE) = ?",
                [value, SESSION + timedelta(days=2)],
            )
    assert _horizon(_evaluate(registry, path))["return_pct"] is None


def test_partial_window_and_pending_rows_cannot_pass_stability():
    from ai_trading_system.domains.opportunities.convergence_performance import (
        _calendar_windows,
    )

    rows = [
        {
            "observation_id": f"o-{i}",
            "symbol_id": "ABC",
            "exchange": "NSE",
            "policy_snapshot_id": "p",
            "session_date": SESSION + timedelta(days=i),
            "anchor_type": "DISCOVERY_NEXT_OPEN_FILL",
            "horizon_sessions": 20,
            "convergence_cohort": "I_ONLY",
            "investigator_member": True,
            "return_pct": 2,
            "benchmark_relative_return_pct": 1,
            "data_quality_status": "MATURED",
        }
        for i in range(21)
    ]
    indices = {
        "NIFTY_50": [{"session_date": SESSION + timedelta(days=i)} for i in range(21)]
    }
    windows = [
        w
        for w in _calendar_windows(rows, indices=indices, observations=rows)
        if w["window_scope"] == "INVESTIGATOR_ANY"
    ]
    assert [w["observed_session_count"] for w in windows] == [10, 10, 1]
    assert [w["window_stable"] for w in windows] == [True, True, False]
    missing_first = _calendar_windows(rows[1:], indices=indices, observations=rows)
    first_window = next(w for w in missing_first if w["window_index"] == 0)
    assert first_window["window_start"] == SESSION
    assert first_window["missing_anchor_count"] == 1
    assert first_window["window_stable"] is False
    rows[0].update(return_pct=None, data_quality_status="PENDING")
    windows = [
        w
        for w in _calendar_windows(rows, indices=indices, observations=rows)
        if w["window_scope"] == "INVESTIGATOR_ANY"
    ]
    assert windows[0]["window_start"] == SESSION
    assert windows[0]["window_stable"] is False
    # Rows from a second snapshot cannot complete a window for the first.
    rows[1]["policy_snapshot_id"] = "other"
    assert not any(
        w["window_stable"]
        for w in _calendar_windows(rows[:10], indices=indices, observations=rows[:10])
    )


def test_policy_snapshots_are_not_pooled(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    outputs = _evaluate(
        registry, path, [_convergence_row(policy_snapshot_id=p) for p in ("p1", "p2")]
    )
    cohorts = outputs["opportunity_convergence_primary_cohorts"]
    assert {r["policy_snapshot_id"] for r in cohorts} == {"p1", "p2"}
    assert all(r["sample_count"] == 1 for r in cohorts)
    checks = outputs["opportunity_convergence_performance_readiness"]
    assert all(
        r["policy_snapshot_ids_json"] in ('["p1"]', '["p2"]')
        for r in checks
        if "policy_snapshot_ids_json" in r
    )


def test_legacy_observations_and_pending_outcomes_are_untouched(tmp_path):
    from ai_trading_system.domains.opportunities.convergence_performance import (
        _append_observations,
    )

    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    old = _observation(
        _convergence_row(
            convergence_policy_version="opportunity-convergence-v1.2",
            policy_snapshot_id="old-policy",
        ),
        "old-run",
        1,
        OBSERVED_AT,
    )
    with registry._writer() as conn:
        _append_observations(conn, [old])
        original = conn.execute(
            "SELECT * FROM opportunity_convergence_observation"
        ).fetchall()
    outputs = _evaluate(registry, None, [])
    assert not outputs["opportunity_convergence_observations"]
    with registry._reader() as conn:
        assert (
            conn.execute("SELECT * FROM opportunity_convergence_observation").fetchall()
            == original
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM opportunity_convergence_anchor"
            ).fetchone()[0]
            == 0
        )


def test_common_close_anchor_matches_legacy_calculator_on_complete_inputs(tmp_path):
    from ai_trading_system.domains.opportunities import performance_evaluation as legacy
    from ai_trading_system.domains.opportunities.convergence_performance import (
        _load_market_data,
    )

    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    observation = _observation(_convergence_row(), "compare", 1, OBSERVED_AT)
    prices, indices, sectors = _load_market_data(path, [observation])
    event = {
        "event_id": "comparison-only",
        "candidate_id": "comparison-only",
        "exchange": "NSE",
        "symbol_id": "ABC",
        "session_date": SESSION,
        "event_type": "CANDIDATE_DISCOVERED",
        "anchor_price": 100,
        "sector_name": "Pharma",
        "invalidation_price": 96,
    }
    old = legacy._mature_horizon(
        event,
        3,
        prices=prices,
        index_prices=indices,
        sector_map=sectors,
        transitions={},
    )
    new = _horizon(_evaluate(registry, path))
    assert new["return_pct"] == old["close_to_close_return_pct"] == 3.0
    for field in [
        "target_session_date",
        "maximum_favourable_excursion_pct",
        "maximum_adverse_excursion_pct",
        "days_to_2pct",
        "days_to_5pct",
        "days_to_stop",
        "benchmark_relative_return_pct",
        "sector_relative_return_pct",
    ]:
        assert new[field] == old[field], field


def test_unsupported_exchange_has_no_symbol_calendar_fallback(tmp_path):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    with duckdb.connect(str(path)) as conn:
        conn.execute("UPDATE _catalog SET exchange = ?", ["BSE"])
    outputs = _evaluate(registry, path, [_convergence_row(exchange="BSE")])
    assert _horizon(outputs)["data_quality_status"] == "INSUFFICIENT_PRICE_DATA"
    assert outputs["opportunity_convergence_primary_cohorts"] == []


def test_calendar_gap_shared_by_market_and_stock_is_not_a_missing_stock_session(
    tmp_path,
):
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control.duckdb")
    path = tmp_path / "ohlcv.duckdb"
    _ohlcv(path, sessions=25)
    non_session = SESSION + timedelta(days=2)
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "DELETE FROM _catalog WHERE CAST(timestamp AS DATE) = ?", [non_session]
        )
        conn.execute("DELETE FROM _index_catalog WHERE date = ?", [non_session])
    outcome = _horizon(_evaluate(registry, path))
    assert outcome["data_quality_status"] == "MATURED"
    assert outcome["target_session_date"] == SESSION + timedelta(days=4)
    assert outcome["return_pct"] == 4.0
