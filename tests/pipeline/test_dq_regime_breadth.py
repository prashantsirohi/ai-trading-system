"""Tests for the regime_breadth_confidence DQ rule."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from ai_trading_system.pipeline.contracts import StageContext, StageResult
from ai_trading_system.pipeline.dq.engine import DataQualityEngine


def _engine() -> DataQualityEngine:
    registry = MagicMock()
    registry.get_rules_for_stage.return_value = []
    return DataQualityEngine(registry)


def _ctx(params: dict | None = None) -> StageContext:
    return StageContext(
        project_root=Path("."),
        db_path=Path("/tmp/dummy.duckdb"),
        run_id="test-run",
        run_date="2024-06-01",
        stage_name="rank",
        attempt_number=1,
        params=params or {},
    )


def _result(snapshot: dict | None) -> StageResult:
    return StageResult(metadata={"market_regime": snapshot} if snapshot is not None else {})


def test_breadth_confidence_passes_when_above_threshold() -> None:
    eng = _engine()
    snapshot = {
        "breadth_confidence": 0.92,
        "eligible_200dma_count": 1490,
        "total_symbols_count": 1619,
    }
    outcome = eng._rule_regime_breadth_confidence(_ctx(), _result(snapshot), "critical")
    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "0.920" in outcome.message
    assert ">= 0.6" in outcome.message


def test_breadth_confidence_fails_when_below_threshold() -> None:
    eng = _engine()
    # Old-era day: 300/1500 symbols have 200-day history
    snapshot = {
        "breadth_confidence": 0.20,
        "eligible_200dma_count": 300,
        "total_symbols_count": 1500,
    }
    outcome = eng._rule_regime_breadth_confidence(_ctx(), _result(snapshot), "critical")
    assert outcome.status == "failed"
    assert outcome.failed_count == 1
    assert "structurally noisy" in outcome.message
    assert "300" in outcome.message


def test_threshold_is_configurable_via_params() -> None:
    eng = _engine()
    snapshot = {
        "breadth_confidence": 0.55,
        "eligible_200dma_count": 825,
        "total_symbols_count": 1500,
    }
    # Default 0.60 — fails
    out_default = eng._rule_regime_breadth_confidence(_ctx(), _result(snapshot), "critical")
    assert out_default.status == "failed"
    # Lowered to 0.50 — passes
    ctx_lo = _ctx({"regime_breadth_confidence_min": 0.50})
    out_relaxed = eng._rule_regime_breadth_confidence(ctx_lo, _result(snapshot), "critical")
    assert out_relaxed.status == "passed"


def test_missing_regime_snapshot_skips_gracefully() -> None:
    eng = _engine()
    outcome = eng._rule_regime_breadth_confidence(_ctx(), _result(None), "critical")
    assert outcome.status == "passed"
    assert outcome.failed_count == 0
    assert "skipped" in outcome.message.lower()


def test_snapshot_without_breadth_confidence_field_skips() -> None:
    """Older snapshots that predate this field shouldn't trigger the rule."""
    eng = _engine()
    legacy_snapshot = {"regime": "neutral", "pct_above_200dma": 0.45}
    outcome = eng._rule_regime_breadth_confidence(
        _ctx(), _result(legacy_snapshot), "critical"
    )
    assert outcome.status == "passed"
    assert "skipped" in outcome.message.lower()


def test_non_numeric_breadth_confidence_fails() -> None:
    eng = _engine()
    snapshot = {"breadth_confidence": "not-a-number"}
    outcome = eng._rule_regime_breadth_confidence(_ctx(), _result(snapshot), "critical")
    assert outcome.status == "failed"
    assert "non-numeric" in outcome.message


def test_rule_relaxes_in_relaxed_mode() -> None:
    """The new rule is NOT a hard floor — relaxed mode should downgrade it."""
    from ai_trading_system.pipeline.dq.engine import HARD_FLOOR_RULES

    assert "regime_breadth_confidence" not in HARD_FLOOR_RULES

    eng = _engine()
    snapshot = {
        "breadth_confidence": 0.20,
        "eligible_200dma_count": 300,
        "total_symbols_count": 1500,
    }
    outcome = eng._rule_regime_breadth_confidence(_ctx(), _result(snapshot), "critical")
    relaxed = eng._apply_relaxation(outcome, dq_mode="relaxed")
    assert relaxed.band == "amber"
    assert relaxed.relaxed_from == "red_repairable"
