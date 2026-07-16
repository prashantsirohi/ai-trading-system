"""ADR-0006 A3: policy fingerprint determinism, sensitivity, and enforcement."""

from __future__ import annotations

import pytest

from ai_trading_system.domains.opportunities import coverage as coverage_module
from ai_trading_system.domains.opportunities.orchestration import contracts as orchestration_contracts
from ai_trading_system.domains.opportunities.orchestration.contracts import LIFECYCLE_RULE_VERSION
from ai_trading_system.domains.opportunities.policy_snapshot import (
    PolicyVersionContentMismatchError,
    compute_policy_snapshot,
    register_or_verify_policy_snapshots,
)
from ai_trading_system.pipeline.registry import RegistryStore


@pytest.fixture
def registry(tmp_path) -> RegistryStore:
    return RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")


def test_snapshot_is_deterministic() -> None:
    first = compute_policy_snapshot({})
    second = compute_policy_snapshot({})
    assert first.policy_snapshot_id == second.policy_snapshot_id
    assert dict(first.label_hashes) == dict(second.label_hashes)
    assert len(first.label_hashes) >= 7


@pytest.mark.parametrize(
    ("param", "value", "label"),
    [
        ("opportunity_rank_admission_percentile", 85.0, "admission-rules-v1"),
        ("opportunity_rank_velocity_floor", -3.0, "admission-rules-v1"),
        ("opportunity_rank_velocity_percentile_floor", 70.0, "admission-rules-v1"),
        ("opportunity_investigator_admission_score", 65.0, "admission-rules-v1"),
        ("opportunity_accumulation_admission_score", 70.0, "admission-rules-v1"),
        ("opportunity_pattern_admission_score", 75.0, "admission-rules-v1"),
        ("opportunity_breakout_admission_score", 75.0, "admission-rules-v1"),
        ("rank_deep_scan_limit", 100, "scan-routing-policy-v2"),
        ("stage_discovery_confidence_threshold", 60.0, "scan-routing-policy-v2"),
        ("minimum_sector_constituents", 8, "sector-stage-aggregation-v1"),
        ("minimum_sector_stage_coverage_ratio", 0.5, "sector-stage-aggregation-v1"),
    ],
)
def test_any_runtime_threshold_changes_owning_label_and_composite(param, value, label) -> None:
    baseline = compute_policy_snapshot({})
    changed = compute_policy_snapshot({param: value})
    assert changed.policy_snapshot_id != baseline.policy_snapshot_id
    assert changed.label_hashes[label] != baseline.label_hashes[label]
    unchanged = set(baseline.label_hashes) - {label}
    assert all(changed.label_hashes[other] == baseline.label_hashes[other] for other in unchanged)


def test_register_then_verify_then_mismatch(registry) -> None:
    snapshot = compute_policy_snapshot({})
    first = register_or_verify_policy_snapshots(registry, snapshot, run_id="run-1")
    assert first == {"registered": len(snapshot.label_hashes), "verified": 0}
    second = register_or_verify_policy_snapshots(registry, snapshot, run_id="run-2")
    assert second == {"registered": 0, "verified": len(snapshot.label_hashes)}

    drifted = compute_policy_snapshot({"opportunity_rank_admission_percentile": 85.0})
    with pytest.raises(PolicyVersionContentMismatchError) as excinfo:
        register_or_verify_policy_snapshots(registry, drifted, run_id="run-3")
    message = str(excinfo.value)
    assert "POLICY_VERSION_CONTENT_MISMATCH" in message
    assert "admission-rules-v1" in message
    assert "rank_admission_percentile" in message
    assert "90.0" in message and "85.0" in message


def test_mismatch_rolls_back_registrations_from_same_call(registry) -> None:
    snapshot = compute_policy_snapshot({})
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT INTO policy_version_registry
                   (version_label, policy_snapshot_id, content_json, first_registered_at, first_run_id)
               VALUES (?, 'bogus-hash', '{}', current_timestamp, 'seed-run')""",
            [LIFECYCLE_RULE_VERSION],
        )
    with pytest.raises(PolicyVersionContentMismatchError):
        register_or_verify_policy_snapshots(registry, snapshot, run_id="run-1")
    with registry._reader() as conn:  # noqa: SLF001
        rows = conn.execute("SELECT version_label FROM policy_version_registry").fetchall()
    assert [row[0] for row in rows] == [LIFECYCLE_RULE_VERSION]


def test_a2_patch_label_registers_beside_legacy_lifecycle_v1(registry) -> None:
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT INTO policy_version_registry
                   (version_label, policy_snapshot_id, content_json, first_registered_at, first_run_id)
               VALUES ('lifecycle-policy-v1', 'legacy-hash', '{}', current_timestamp, 'legacy-run')"""
        )
    result = register_or_verify_policy_snapshots(
        registry, compute_policy_snapshot({}), run_id="a2-run"
    )
    assert result["registered"] >= 1
    with registry._reader() as conn:  # noqa: SLF001
        labels = {row[0] for row in conn.execute(
            "SELECT version_label FROM policy_version_registry"
        ).fetchall()}
    assert {"lifecycle-policy-v1", LIFECYCLE_RULE_VERSION}.issubset(labels)


def test_code_constant_drift_is_caught_at_runtime(registry, monkeypatch) -> None:
    register_or_verify_policy_snapshots(registry, compute_policy_snapshot({}), run_id="run-1")
    monkeypatch.setitem(coverage_module.SECTOR_AGGREGATION_RULES, "stage_2_min_pct", 55.0)
    drifted = compute_policy_snapshot({})
    with pytest.raises(PolicyVersionContentMismatchError) as excinfo:
        register_or_verify_policy_snapshots(registry, drifted, run_id="run-2")
    message = str(excinfo.value)
    assert "sector-stage-aggregation-v1" in message
    assert "stage_2_min_pct" in message


def test_sector_gate_rule_drift_changes_lifecycle_fingerprint(registry, monkeypatch) -> None:
    baseline = compute_policy_snapshot({})
    register_or_verify_policy_snapshots(registry, baseline, run_id="run-1")
    monkeypatch.setitem(
        orchestration_contracts.SECTOR_GATE_RULES,
        "calibration_improving_velocity_floor_exclusive",
        0.1,
    )
    drifted = compute_policy_snapshot({})
    assert drifted.label_hashes[LIFECYCLE_RULE_VERSION] != baseline.label_hashes[LIFECYCLE_RULE_VERSION]
    with pytest.raises(PolicyVersionContentMismatchError, match=LIFECYCLE_RULE_VERSION):
        register_or_verify_policy_snapshots(registry, drifted, run_id="run-2")
