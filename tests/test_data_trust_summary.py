from __future__ import annotations

from pathlib import Path

import duckdb

from analytics.data_trust import load_data_trust_summary
from analytics.dq import DataQualityEngine
from analytics.registry import RegistryStore
from core.contracts import StageContext, StageResult


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


def _insert_active_quarantine(db_path: Path, *, symbol_count: int, trade_date: str = "2026-04-08") -> None:
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
        for idx in range(symbol_count):
            conn.execute(
                """
                INSERT INTO _catalog_quarantine
                (symbol_id, security_id, exchange, trade_date, reason, status, source_run_id, note)
                VALUES (?, ?, 'NSE', CAST(? AS DATE), 'provider_unavailable', 'active', 'test-run', 'test quarantine')
                """,
                [f"Q{idx:04d}", str(1000 + idx), trade_date],
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
    _insert_active_quarantine(db_path, symbol_count=2)

    summary = load_data_trust_summary(
        db_path,
        run_date="2026-04-08",
        blocked_quarantine_symbol_threshold=1,
        blocked_quarantine_ratio_threshold=0.4,
    )

    assert summary["status"] == "blocked"
    assert int(summary["latest_quarantined_symbols"]) == 2
    assert float(summary["latest_quarantined_symbol_ratio"]) == 1.0


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
    _insert_active_quarantine(db_path, symbol_count=2)

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
            "dq_features_max_quarantined_symbol_ratio_pct": 40.0,
        },
    )

    outcome = engine._rule_features_trust_quarantine_clear(context, StageResult(metadata={}), "critical")

    assert outcome.status == "failed"
    assert outcome.failed_count == 1
