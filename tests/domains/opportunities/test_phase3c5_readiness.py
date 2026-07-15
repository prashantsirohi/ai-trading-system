from __future__ import annotations

from ai_trading_system.domains.opportunities.calibration import (
    CalibrationConfig,
    CalibrationSampleRequirements,
    build_calibration_dataset,
)
from ai_trading_system.interfaces.cli.build_phase3c5_calibration import fixture_rows


def _ready_config(**overrides) -> CalibrationConfig:
    values = {
        "total_samples": CalibrationSampleRequirements(6, 3),
        "minimum_positive_samples": 1, "minimum_negative_samples": 1,
        "minimum_distinct_market_regimes": 3,
        "minimum_samples_per_outcome_horizon": 1,
        "minimum_samples_per_stage": 1,
        "minimum_samples_per_market_regime": 1,
        "minimum_samples_per_scan_tier": 1,
        "minimum_samples_per_setup_family": 1,
        "minimum_calendar_span_sessions": 1,
        "maximum_single_year_share": 1.0,
    }
    values.update(overrides)
    return CalibrationConfig(**values)


def _build(**kwargs):
    return build_calibration_dataset(
        fixture_rows("small_fixture", count=9), dataset_name="ready",
        dataset_purpose="entry", as_of="2026-07-15", config=_ready_config(),
        source_database_hashes={"fixture": "stable"}, **kwargs,
    )


def test_all_critical_checks_and_operational_evidence_pass_ready() -> None:
    result = _build(
        copied_realistic_performance_summary={"replay_equivalence": {"equivalent": True}},
        operator_migrations_applied=True, real_phase3b_history_present=True,
    )
    assert result.verdict.value == "READY"
    assert result.phase4_development_ready is True
    assert result.phase4_production_ready is True


def test_missing_realistic_baseline_is_ready_with_limitations() -> None:
    result = _build(operator_migrations_applied=True, real_phase3b_history_present=True)
    assert result.verdict.value == "READY_WITH_LIMITATIONS"
    assert "COPIED_REALISTIC_BASELINE_MISSING" in {item.limitation_id for item in result.limitations}


def test_operator_migrations_unapplied_is_development_ready_limitation() -> None:
    result = _build(
        copied_realistic_performance_summary={"replay_equivalence": {"equivalent": True}},
        real_phase3b_history_present=True,
    )
    assert result.phase4_development_ready is True
    assert result.phase4_production_ready is False
    assert "OPERATOR_MIGRATIONS_NOT_APPLIED" in {item.limitation_id for item in result.limitations}


def test_critical_sample_minimum_failure_is_not_ready() -> None:
    result = build_calibration_dataset(
        fixture_rows("small_fixture", count=2), dataset_name="small",
        dataset_purpose="entry", as_of="2026-07-15", config=_ready_config(),
    )
    assert result.verdict.value == "NOT_READY"


def test_limitation_sample_threshold_is_ready_with_limitations() -> None:
    config = _ready_config(total_samples=CalibrationSampleRequirements(10, 3))
    result = build_calibration_dataset(
        fixture_rows("small_fixture", count=6), dataset_name="limited",
        dataset_purpose="entry", as_of="2026-07-15", config=config,
        copied_realistic_performance_summary={"replay_equivalence": {"equivalent": True}},
        operator_migrations_applied=True, real_phase3b_history_present=True,
    )
    assert result.verdict.value == "READY_WITH_LIMITATIONS"


def test_class_imbalance_and_regime_sparsity_are_limitations() -> None:
    rows = fixture_rows("small_fixture", count=9)
    for row in rows:
        row["outcome_label"] = "positive"
        row["market_regime"] = "bull"
    result = build_calibration_dataset(
        rows, dataset_name="biased", dataset_purpose="entry", as_of="2026-07-15",
        config=_ready_config(), operator_migrations_applied=True,
        copied_realistic_performance_summary={"replay_equivalence": {"equivalent": True}},
        real_phase3b_history_present=True,
    )
    assert result.verdict.value == "NOT_READY"  # winner-only is a critical survivorship failure
    assert "REGIME_COVERAGE_SPARSE" in {item.limitation_id for item in result.limitations}


def test_lookahead_leak_is_not_ready() -> None:
    rows = fixture_rows("critical_leakage", count=9)
    result = build_calibration_dataset(rows, dataset_name="leak", dataset_purpose="entry", as_of="2026-07-15", config=_ready_config())
    assert result.verdict.value == "NOT_READY"


def test_unresolved_governance_is_quarantined_and_not_in_eligible_rows() -> None:
    rows = fixture_rows("small_fixture", count=9)
    rows[0]["correction_impact_status"] = "unresolved_legacy_ambiguous"
    result = build_calibration_dataset(rows, dataset_name="governed", dataset_purpose="entry", as_of="2026-07-15", config=_ready_config())
    assert result.quality_summary["quarantined_rows"] == 1
    assert all(row["entity_id"] != rows[0]["entity_id"] for row in result.eligible_rows)
