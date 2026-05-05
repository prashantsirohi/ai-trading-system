"""Tests for DQ band classification and relax-mode behavior."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ai_trading_system.pipeline.contracts import (
    DataQualityCriticalError,
    DataQualityRepairableError,
    StageContext,
    StageResult,
)
from ai_trading_system.pipeline.dq.engine import (
    HARD_FLOOR_RULES,
    DataQualityEngine,
    DQRuleFailure,
)


def _engine() -> DataQualityEngine:
    registry = MagicMock()
    registry.get_rules_for_stage.return_value = []
    return DataQualityEngine(registry)


def _ctx(params: dict | None = None) -> StageContext:
    return StageContext(
        project_root=Path("."),
        db_path=Path("/tmp/dummy.duckdb"),
        run_id="test-run",
        run_date="2026-05-04",
        stage_name="ingest",
        attempt_number=1,
        params=params or {},
    )


def test_hard_floor_rule_failed_in_relaxed_mode_still_blocks():
    eng = _engine()
    outcome = DQRuleFailure(
        rule_id="ingest_catalog_not_empty",
        severity="critical",
        status="failed",
        failed_count=1,
        message="catalog empty",
    )
    out = eng._apply_relaxation(outcome, dq_mode="relaxed")
    assert out.band == "red_block"
    assert out.relaxed_from is None
    assert "[RELAXED]" not in out.message


def test_repairable_rule_in_strict_mode_stays_red_repairable():
    eng = _engine()
    outcome = DQRuleFailure(
        rule_id="ingest_provider_coverage_low",
        severity="critical",
        status="failed",
        failed_count=1,
        message="coverage low",
    )
    out = eng._apply_relaxation(outcome, dq_mode="strict")
    assert out.band == "red_repairable"
    assert out.relaxed_from is None


def test_repairable_rule_in_relaxed_mode_downgrades_to_amber():
    eng = _engine()
    outcome = DQRuleFailure(
        rule_id="ingest_latest_trade_date_quarantine_clear",
        severity="critical",
        status="failed",
        failed_count=1,
        message="11 stuck symbols",
    )
    out = eng._apply_relaxation(outcome, dq_mode="relaxed")
    assert out.band == "amber"
    assert out.relaxed_from == "red_repairable"
    assert out.message.startswith("[RELAXED]")


def test_high_severity_rule_becomes_amber_when_failed():
    eng = _engine()
    outcome = DQRuleFailure(
        rule_id="ingest_negative_volume",
        severity="high",
        status="failed",
        failed_count=1,
        message="negative volumes",
    )
    out = eng._apply_relaxation(outcome, dq_mode="relaxed")
    assert out.band == "amber"
    assert out.relaxed_from is None  # not relaxed — was already non-blocking


def test_passing_rule_is_untouched():
    eng = _engine()
    outcome = DQRuleFailure(
        rule_id="ingest_catalog_not_empty",
        severity="critical",
        status="passed",
        failed_count=0,
        message="ok",
    )
    out = eng._apply_relaxation(outcome, dq_mode="relaxed")
    assert out.band == ""
    assert out.relaxed_from is None


def test_evaluate_raises_critical_for_red_block():
    eng = _engine()

    class _StubRegistry:
        def get_rules_for_stage(self, _stage):
            return [{
                "rule_id": "ingest_catalog_not_empty",
                "stage_name": "ingest",
                "dataset_name": "_catalog",
                "severity": "critical",
                "rule_sql": None,
                "description": "catalog must not be empty",
                "owner": "pipeline",
            }]
        def record_dq_result(self, **kw): pass

    eng.registry = _StubRegistry()
    eng._rule_ingest_catalog_not_empty = lambda ctx, res, sev: DQRuleFailure(
        rule_id="ingest_catalog_not_empty", severity=sev, status="failed",
        failed_count=1, message="empty",
    )

    with pytest.raises(DataQualityCriticalError):
        eng.evaluate(_ctx({"dq_mode": "relaxed"}), StageResult())


def test_evaluate_raises_repairable_in_strict_mode_for_repairable_rule():
    eng = _engine()

    class _StubRegistry:
        def get_rules_for_stage(self, _stage):
            return [{
                "rule_id": "ingest_provider_coverage_low",
                "stage_name": "ingest",
                "dataset_name": "_catalog",
                "severity": "critical",
                "rule_sql": None,
                "description": "coverage low",
                "owner": "pipeline",
            }]
        def record_dq_result(self, **kw): pass

    eng.registry = _StubRegistry()
    eng._rule_ingest_provider_coverage_low = lambda ctx, res, sev: DQRuleFailure(
        rule_id="ingest_provider_coverage_low", severity=sev, status="failed",
        failed_count=1, message="primary 50%",
    )

    with pytest.raises(DataQualityRepairableError):
        eng.evaluate(_ctx({"dq_mode": "strict"}), StageResult())


def test_evaluate_does_not_raise_in_relaxed_mode_for_repairable_rule():
    eng = _engine()

    recorded = {}

    class _StubRegistry:
        def get_rules_for_stage(self, _stage):
            return [{
                "rule_id": "ingest_provider_coverage_low",
                "stage_name": "ingest",
                "dataset_name": "_catalog",
                "severity": "critical",
                "rule_sql": None,
                "description": "coverage low",
                "owner": "pipeline",
            }]
        def record_dq_result(self, **kw):
            recorded.update(kw)

    eng.registry = _StubRegistry()
    eng._rule_ingest_provider_coverage_low = lambda ctx, res, sev: DQRuleFailure(
        rule_id="ingest_provider_coverage_low", severity=sev, status="failed",
        failed_count=1, message="primary 50%",
    )

    out = eng.evaluate(_ctx({"dq_mode": "relaxed"}), StageResult())
    assert len(out) == 1
    assert out[0].band == "amber"
    assert out[0].relaxed_from == "red_repairable"
    assert recorded.get("relaxed_from") == "red_repairable"
    assert recorded.get("band") == "amber"


def test_hard_floor_rule_set_includes_critical_data_integrity_rules():
    # Lock the contract so a careless edit doesn't accidentally relax these.
    expected = {
        "ingest_catalog_not_empty",
        "ingest_required_fields_not_null",
        "ingest_ohlc_consistency",
        "ingest_duplicate_ohlcv_key",
        "features_snapshot_created",
        "features_registry_not_empty",
        "rank_artifact_not_empty",
    }
    assert expected.issubset(HARD_FLOOR_RULES)
