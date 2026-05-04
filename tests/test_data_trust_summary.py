from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import pandas as pd
from ai_trading_system.analytics.data_trust import load_data_trust_summary
from ai_trading_system.analytics.dq import DataQualityEngine
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.ingest.repair import _normalize_trade_frame
from ai_trading_system.pipeline.contracts import StageContext, StageResult


def _init_catalog_with_trust_columns(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                provider VARCHAR,
                provider_priority INTEGER,
                validation_status VARCHAR,
                validated_against VARCHAR,
                ingest_run_id VARCHAR,
                repair_batch_id VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog
            (symbol_id, security_id, exchange, timestamp, open, high, low, close, volume,
             provider, provider_priority, validation_status, validated_against, ingest_run_id, repair_batch_id)
            VALUES
            ('AAA', '101', 'NSE', '2026-04-08 15:30:00', 10, 11, 9, 10.5, 1000, 'unknown', 9, 'legacy_unverified', NULL, NULL, NULL),
            ('BBB', '102', 'NSE', '2026-04-08 15:30:00', 20, 21, 19, 20.5, 1000, 'unknown', 9, 'legacy_unverified', NULL, NULL, NULL)
            """
        )
    finally:
        conn.close()


def _insert_active_quarantine(
    db_path: Path,
    *,
    symbol_count: int,
    trade_date: str = "2026-04-08",
    symbol_ids: list[str] | None = None,
) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _catalog_quarantine (
                symbol_id VARCHAR,
                security_id VARCHAR,
                exchange VARCHAR,
                trade_date DATE,
                reason VARCHAR,
                status VARCHAR DEFAULT 'active',
                source_run_id VARCHAR,
                repair_batch_id VARCHAR,
                note VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP
            )
            """
        )
        symbols = symbol_ids or [f"Q{idx:04d}" for idx in range(symbol_count)]
        for idx, symbol_id in enumerate(symbols[:symbol_count]):
            conn.execute(
                """
                INSERT INTO _catalog_quarantine
                (symbol_id, security_id, exchange, trade_date, reason, status, source_run_id, note)
                VALUES (?, ?, 'NSE', CAST(? AS DATE), 'provider_unavailable', 'active', 'test-run', 'test quarantine')
                """,
                [symbol_id, str(1000 + idx), trade_date],
            )
    finally:
        conn.close()


def _insert_catalog_rows_for_date(
    db_path: Path,
    *,
    trade_date: str,
    row_count: int,
    provider: str = "nse_bhavcopy",
    validation_status: str = "trusted_primary",
) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        rows = [
            (
                f"S{idx:05d}",
                str(5000 + idx),
                "NSE",
                f"{trade_date} 15:30:00",
                100.0 + idx,
                101.0 + idx,
                99.0 + idx,
                100.5 + idx,
                1000 + idx,
                provider,
                1,
                validation_status,
                None,
                "test-run",
                None,
            )
            for idx in range(row_count)
        ]
        conn.executemany(
            """
            INSERT INTO _catalog
            (symbol_id, security_id, exchange, timestamp, open, high, low, close, volume,
             provider, provider_priority, validation_status, validated_against, ingest_run_id, repair_batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    finally:
        conn.close()


def test_load_data_trust_summary_flags_unknown_provider_as_degraded(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)

    summary = load_data_trust_summary(
        db_path,
        run_date="2026-04-08",
        blocked_quarantine_symbol_threshold=10,
        blocked_quarantine_ratio_threshold=0.6,
    )

    assert summary["latest_trade_date"] == "2026-04-08"
    assert summary["status"] == "degraded"
    assert float(summary["unknown_ratio_latest"]) == 1.0
    assert int(summary["latest_provider_stats"]["unknown_rows"]) == 2


def test_load_data_trust_summary_marks_small_latest_quarantine_as_degraded(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_active_quarantine(db_path, symbol_count=1)

    summary = load_data_trust_summary(
        db_path,
        run_date="2026-04-08",
        blocked_quarantine_symbol_threshold=10,
        blocked_quarantine_ratio_threshold=0.6,
    )

    assert summary["status"] == "degraded"
    assert int(summary["latest_quarantined_symbols"]) == 1
    assert float(summary["latest_quarantined_symbol_ratio"]) == 0.5


def test_load_data_trust_summary_blocks_large_latest_quarantine(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_active_quarantine(db_path, symbol_count=2, symbol_ids=["AAA", "BBB"])

    summary = load_data_trust_summary(
        db_path,
        run_date="2026-04-08",
        blocked_quarantine_symbol_threshold=1,
        blocked_quarantine_ratio_threshold=0.4,
    )

    assert summary["status"] == "blocked"
    assert int(summary["latest_quarantined_symbols"]) == 2
    assert float(summary["latest_quarantined_symbol_ratio"]) == 1.0


def test_load_data_trust_summary_blocks_when_critical_universe_threshold_crossed(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_catalog_rows_for_date(db_path, trade_date="2026-04-24", row_count=1574)
    _insert_active_quarantine(
        db_path,
        symbol_count=11,
        trade_date="2026-04-24",
        symbol_ids=[f"S{idx:05d}" for idx in range(600, 611)],
    )

    summary = load_data_trust_summary(
        db_path,
        run_date="2026-04-24",
        blocked_quarantine_symbol_threshold=10,
        blocked_quarantine_ratio_threshold=0.01,
    )

    assert summary["status"] == "blocked"
    assert int(summary["latest_quarantined_symbols"]) == 11
    assert int(summary["latest_critical_quarantined_symbols"]) == 11


def test_dq_provider_coverage_rule_fails_on_unknown_provider_rows(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-unknown-provider",
        run_date="2026-04-08",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={},
    )
    result = StageResult(
        metadata={
            "trust_summary": {
                "status": "trusted",
                "latest_provider_stats": {
                    "trade_date": "2026-04-08",
                    "total_rows": 25,
                    "primary_rows": 0,
                    "fallback_rows": 0,
                    "unknown_rows": 25,
                },
            }
        }
    )

    outcome = engine._rule_ingest_provider_coverage_low(context, result, "critical")

    assert outcome.status == "failed"
    assert outcome.failed_count == 1
    assert "unknown=" in outcome.message


def test_dq_unresolved_dates_rule_tolerates_small_unresolved_scope(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-unresolved-tolerated",
        run_date="2026-04-08",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={},
    )
    result = StageResult(
        metadata={
            "unresolved_dates": ["2026-04-08"],
            "unresolved_symbol_date_count": 3,
            "active_eligible_symbol_count": 996,
        }
    )

    outcome = engine._rule_ingest_unresolved_dates_present(context, result, "critical")

    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "unresolved_symbol_dates=3" in outcome.message


def test_dq_unresolved_dates_rule_blocks_when_threshold_crossed(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-unresolved-blocked",
        run_date="2026-04-08",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={
            "dq_max_unresolved_dates": 1,
            "dq_max_unresolved_symbol_dates": 10,
            "dq_max_unresolved_symbol_ratio_pct": 1.0,
        },
    )
    result = StageResult(
        metadata={
            "unresolved_dates": ["2026-04-07", "2026-04-08"],
            "unresolved_symbol_date_count": 50,
            "active_eligible_symbol_count": 996,
        }
    )

    outcome = engine._rule_ingest_unresolved_dates_present(context, result, "critical")

    assert outcome.status == "failed"
    assert outcome.failed_count == 1


def test_dq_unresolved_dates_rule_allows_multi_date_small_breadth(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-unresolved-multiday-tolerated",
        run_date="2026-04-10",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={
            "dq_max_unresolved_dates": 1,
            "dq_max_unresolved_symbol_dates": 10,
            "dq_max_unresolved_symbol_ratio_pct": 1.0,
        },
    )
    result = StageResult(
        metadata={
            "unresolved_dates": ["2026-04-08", "2026-04-09", "2026-04-10"],
            "unresolved_symbol_date_count": 9,
            "active_eligible_symbol_count": 996,
        }
    )

    outcome = engine._rule_ingest_unresolved_dates_present(context, result, "critical")

    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "Date threshold exceeded" in outcome.message


def test_dq_unresolved_dates_rule_uses_distinct_symbol_breadth_for_ratio(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-15-unresolved-distinct-breadth",
        run_date="2026-04-15",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={
            "dq_max_unresolved_dates": 1,
            "dq_max_unresolved_symbol_dates": 10,
            "dq_max_unresolved_symbol_ratio_pct": 1.0,
        },
    )
    result = StageResult(
        metadata={
            "unresolved_dates": ["2026-04-11", "2026-04-12", "2026-04-15"],
            "unresolved_symbol_date_count": 27,
            "unresolved_symbol_count": 9,
            "active_eligible_symbol_count": 996,
        }
    )

    outcome = engine._rule_ingest_unresolved_dates_present(context, result, "critical")

    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "unresolved_symbol_dates=9" in outcome.message
    assert "unresolved_symbol_date_pairs=27" in outcome.message


def test_dq_unresolved_dates_rule_scales_symbol_tolerance_with_ratio(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="pipeline-2026-04-24-unresolved-ratio-scaled",
        run_date="2026-04-24",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={
            "dq_max_unresolved_dates": 1,
            "dq_max_unresolved_symbol_dates": 10,
            "dq_max_unresolved_symbol_ratio_pct": 1.0,
        },
    )
    result = StageResult(
        metadata={
            "unresolved_dates": ["2026-04-24"],
            "unresolved_symbol_date_count": 11,
            "unresolved_symbol_count": 11,
            "active_eligible_symbol_count": 1623,
        }
    )

    outcome = engine._rule_ingest_unresolved_dates_present(context, result, "critical")

    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "effective_max_symbols=17" in outcome.message


def test_dq_features_quarantine_rule_tolerates_small_scope_when_thresholds_allow(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_active_quarantine(db_path, symbol_count=1)

    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="pipeline-2026-04-08-features-tolerated",
        run_date="2026-04-08",
        stage_name="features",
        attempt_number=1,
        registry=registry,
        params={
            "dq_features_max_quarantined_symbols": 10,
            "dq_features_max_quarantined_symbol_ratio_pct": 60.0,
        },
    )

    outcome = engine._rule_features_trust_quarantine_clear(context, StageResult(metadata={}), "critical")

    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "symbols=1" in outcome.message


def test_dq_features_quarantine_rule_blocks_when_threshold_crossed(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_active_quarantine(db_path, symbol_count=2, symbol_ids=["AAA", "BBB"])

    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="pipeline-2026-04-08-features-blocked",
        run_date="2026-04-08",
        stage_name="features",
        attempt_number=1,
        registry=registry,
        params={
            "dq_features_max_quarantined_symbols": 1,
            "dq_features_max_quarantined_symbol_ratio_pct": 0.1,
        },
    )

    outcome = engine._rule_features_trust_quarantine_clear(context, StageResult(metadata={}), "critical")

    assert outcome.status == "failed"
    assert outcome.failed_count == 1


def test_dq_ingest_latest_trade_date_quarantine_rule_blocks_with_trade_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_catalog_rows_for_date(db_path, trade_date="2026-04-24", row_count=1000)
    _insert_active_quarantine(db_path, symbol_count=2, trade_date="2026-04-23", symbol_ids=["S00600", "S00601"])
    _insert_active_quarantine(db_path, symbol_count=2, trade_date="2026-04-24", symbol_ids=["S00600", "S00601"])

    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="pipeline-2026-04-24-ingest-blocked",
        run_date="2026-04-24",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={
            "dq_features_max_quarantined_symbols": 1,
            "dq_features_max_quarantined_symbol_ratio_pct": 0.1,
        },
    )

    outcome = engine._rule_ingest_latest_trade_date_quarantine_clear(context, StageResult(metadata={}), "critical")

    assert outcome.status == "failed"
    assert outcome.failed_count == 1
    assert "trade_dates=2026-04-23, 2026-04-24" in outcome.message
    assert "latest_trade_date=2026-04-24" in outcome.message


def test_dq_features_quarantine_rule_blocks_when_critical_universe_threshold_crossed(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_catalog_rows_for_date(db_path, trade_date="2026-04-24", row_count=1574)
    _insert_active_quarantine(
        db_path,
        symbol_count=11,
        trade_date="2026-04-24",
        symbol_ids=[f"S{idx:05d}" for idx in range(600, 611)],
    )

    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="pipeline-2026-04-24-features-ratio-scaled",
        run_date="2026-04-24",
        stage_name="features",
        attempt_number=1,
        registry=registry,
        params={
            "dq_features_max_quarantined_symbols": 10,
            "dq_features_max_quarantined_symbol_ratio_pct": 1.0,
        },
    )

    outcome = engine._rule_features_trust_quarantine_clear(context, StageResult(metadata={}), "critical")

    assert outcome.status == "failed"
    assert outcome.failed_count == 1
    assert "effective_max_symbols=" in outcome.message


def test_dq_features_quarantine_rule_ignores_tail_only_quarantine_for_blocking(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _init_catalog_with_trust_columns(db_path)
    _insert_catalog_rows_for_date(db_path, trade_date="2026-04-24", row_count=1000)
    _insert_active_quarantine(db_path, symbol_count=50, trade_date="2026-04-24")

    registry = RegistryStore(tmp_path)
    engine = DataQualityEngine(registry)
    context = StageContext(
        project_root=tmp_path,
        db_path=db_path,
        run_id="pipeline-2026-04-24-features-tail-quarantine",
        run_date="2026-04-24",
        stage_name="features",
        attempt_number=1,
        registry=registry,
        params={
            "dq_features_max_quarantined_symbols": 1,
            "dq_features_max_quarantined_symbol_ratio_pct": 0.1,
        },
    )

    outcome = engine._rule_features_trust_quarantine_clear(context, StageResult(metadata={}), "critical")

    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "latest_critical_symbols=0" in outcome.message


def test_record_data_repair_run_updates_status_for_existing_run_id(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    registry.record_data_repair_run(
        repair_run_id="repair-2026-04-21",
        from_date="2026-04-13",
        to_date="2026-04-20",
        exchange="NSE",
        status="running",
        repaired_row_count=0,
        unresolved_symbol_count=41,
        unresolved_date_count=5,
        metadata={"source": "auto"},
    )
    registry.record_data_repair_run(
        repair_run_id="repair-2026-04-21",
        from_date="2026-04-13",
        to_date="2026-04-20",
        exchange="NSE",
        status="completed",
        repaired_row_count=37,
        unresolved_symbol_count=0,
        unresolved_date_count=0,
        report_uri="artifacts/data_repair/repair-2026-04-21.json",
        metadata={"source": "auto", "result": "ok"},
    )

    latest = registry.get_latest_data_repair_run("NSE")

    assert latest is not None
    assert latest["repair_run_id"] == "repair-2026-04-21"
    assert latest["status"] == "completed"
    assert latest["repaired_row_count"] == 37
    assert latest["unresolved_symbol_count"] == 0
    assert latest["unresolved_date_count"] == 0
    assert latest["report_uri"] == "artifacts/data_repair/repair-2026-04-21.json"
    assert latest["metadata"]["result"] == "ok"


def test_record_data_repair_run_repairs_legacy_index_before_upsert(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "control_plane.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE data_repair_run (
                repair_run_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                from_date DATE NOT NULL,
                to_date DATE NOT NULL,
                exchange VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                repaired_row_count BIGINT DEFAULT 0,
                unresolved_symbol_count BIGINT DEFAULT 0,
                unresolved_date_count BIGINT DEFAULT 0,
                report_uri VARCHAR,
                metadata_json VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_data_repair_run_created
            ON data_repair_run (created_at, exchange, status)
            """
        )
    finally:
        conn.close()

    registry = RegistryStore(tmp_path)
    registry.record_data_repair_run(
        repair_run_id="repair-legacy-2026-04-21",
        from_date="2026-04-13",
        to_date="2026-04-20",
        exchange="NSE",
        status="running",
    )
    registry.record_data_repair_run(
        repair_run_id="repair-legacy-2026-04-21",
        from_date="2026-04-13",
        to_date="2026-04-20",
        exchange="NSE",
        status="completed",
        repaired_row_count=12,
    )

    latest = registry.get_latest_data_repair_run("NSE")

    assert latest is not None
    assert latest["status"] == "completed"
    assert latest["repaired_row_count"] == 12

    conn = duckdb.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT sql FROM duckdb_indexes() WHERE index_name = ?",
            ["idx_data_repair_run_created"],
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "CREATE INDEX idx_data_repair_run_created ON data_repair_run(exchange, created_at);"


def test_normalize_trade_frame_avoids_futurewarning_on_trade_date_assignment() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-04-13 15:30:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10,
            }
        ]
    )

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        normalized = _normalize_trade_frame(frame)

    assert len(record) == 0
    assert normalized["trade_date"].tolist() == ["2026-04-13"]
