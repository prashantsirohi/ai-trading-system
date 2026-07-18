from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from ai_trading_system.research.pattern_lane_calibration.harness import (
    CalibrationResult,
    _calibration_metrics,
    _winner_recall,
    build_point_in_time_context,
    classify_lanes,
    scan_lane_patterns,
    run_calibration,
    write_calibration_result,
)
from ai_trading_system.research.pattern_lane_calibration.policy import (
    PATTERN_FAMILIES,
    default_r0_policy,
)


def _market(symbol: str, periods: int, *, start: str = "2025-01-01", base: float = 100.0) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=periods)
    rows = []
    for index, timestamp in enumerate(dates):
        close = base + index * 0.25
        rows.append(
            {
                "symbol_id": symbol,
                "exchange": "NSE",
                "timestamp": timestamp,
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 100_000.0,
            }
        )
    return pd.DataFrame(rows)


def test_policy_matrix_is_complete_and_excludes_continuation_families_from_base_lanes() -> None:
    policy = default_r0_policy()

    for row in policy.families.matrix.values():
        assert set(row) == set(PATTERN_FAMILIES)
    stage1 = policy.families.matrix["stage1_base:180_plus"]
    young = policy.families.matrix["young_listing_base:120_179"]
    for family in ("flag", "high_tight_flag", "pocket_pivot", "three_weeks_tight", "stage2_reclaim"):
        assert stage1[family] == "excluded"
        assert young[family] == "excluded"
    assert policy.families.matrix["ipo_early_base:35_49"]["ipo_base"] == "allowed"
    assert policy.families.matrix["stage2_continuation:180_plus"]["head_shoulders"] == "suppression_only"


def test_lane_classifier_applies_age_precedence_and_requires_valid_mature_stage2() -> None:
    context = pd.DataFrame(
        [
            {
                "symbol_id": "IPO", "exchange": "NSE", "as_of_date": "2026-01-01", "bar_count": 40,
                "early_ipo_liquidity_gate_passed": True, "standard_liquidity_gate_passed": False,
                "stage2_input_valid": False, "stage2_score": 100.0,
                "liquidity_policy_version": "ipo", "structure_observation_id": "ipo",
            },
            {
                "symbol_id": "YOUNG", "exchange": "NSE", "as_of_date": "2026-01-01", "bar_count": 150,
                "early_ipo_liquidity_gate_passed": False, "standard_liquidity_gate_passed": True,
                "stage2_input_valid": True, "stage2_score": 100.0,
                "liquidity_policy_version": "standard", "structure_observation_id": "young",
            },
            {
                "symbol_id": "MATURE_BAD", "exchange": "NSE", "as_of_date": "2026-01-01", "bar_count": 220,
                "early_ipo_liquidity_gate_passed": False, "standard_liquidity_gate_passed": True,
                "stage2_input_valid": False, "stage2_score": 100.0,
                "weekly_stage_is_fresh": False, "stage1_structure_checks_passed": False,
                "liquidity_policy_version": "standard", "structure_observation_id": "bad",
            },
            {
                "symbol_id": "MATURE_S2", "exchange": "NSE", "as_of_date": "2026-01-01", "bar_count": 220,
                "early_ipo_liquidity_gate_passed": False, "standard_liquidity_gate_passed": True,
                "stage2_input_valid": True, "stage2_score": 70.0,
                "weekly_stage_is_fresh": True, "weekly_stage": "S1", "stage1_structure_checks_passed": True,
                "liquidity_policy_version": "standard", "structure_observation_id": "s2",
            },
        ]
    )

    classified = classify_lanes(context).set_index("symbol_id")

    assert classified.loc["IPO", "scan_lane_as_of"] == "ipo_early_base"
    assert classified.loc["YOUNG", "scan_lane_as_of"] == "young_listing_base"
    assert classified.loc["MATURE_BAD", "scan_lane_as_of"] == "no_lane"
    assert classified.loc["MATURE_S2", "scan_lane_as_of"] == "stage2_continuation"


def test_stage1_gate_separates_current_stage_from_transition() -> None:
    base = {
        "exchange": "NSE", "as_of_date": "2026-01-01", "bar_count": 220,
        "early_ipo_liquidity_gate_passed": False, "standard_liquidity_gate_passed": True,
        "stage2_input_valid": False, "stage2_score": 0.0,
        "weekly_stage_is_fresh": True, "stage1_structure_checks_passed": True,
        "liquidity_policy_version": "standard",
    }
    context = pd.DataFrame([
        # current S1 -> admissible via label
        {**base, "symbol_id": "FRESH_S1", "weekly_stage": "S1",
         "weekly_stage_transition": "NONE", "structure_observation_id": "a"},
        # current S2 that just arrived from S1 -> admissible via transition only
        {**base, "symbol_id": "FRESH_S1_TO_S2", "weekly_stage": "S2",
         "weekly_stage_transition": "S1_TO_S2", "structure_observation_id": "b"},
        # plain current S2 with no transition -> NOT stage1-admissible
        {**base, "symbol_id": "PLAIN_S2", "weekly_stage": "S2",
         "weekly_stage_transition": "NONE", "structure_observation_id": "c"},
    ])

    classified = classify_lanes(context).set_index("symbol_id")

    assert classified.loc["FRESH_S1", "scan_lane_as_of"] == "stage1_base"
    assert "FRESH_WEEKLY_STAGE1" in classified.loc["FRESH_S1", "lane_assignment_reason_codes"]
    assert classified.loc["FRESH_S1_TO_S2", "scan_lane_as_of"] == "stage1_base"
    assert "FRESH_WEEKLY_STAGE1_TRANSITION" in classified.loc["FRESH_S1_TO_S2", "lane_assignment_reason_codes"]
    assert classified.loc["PLAIN_S2", "scan_lane_as_of"] == "no_lane"


def test_point_in_time_context_is_unchanged_by_future_market_and_stage_rows() -> None:
    market = _market("AAA", 240)
    as_of = market["timestamp"].iloc[219].date().isoformat()
    weekly = pd.DataFrame(
        [
            {"symbol": "AAA", "week_end_date": market["timestamp"].iloc[215], "stage_label": "S1"},
            {"symbol": "AAA", "week_end_date": market["timestamp"].iloc[230], "stage_label": "S2"},
        ]
    )

    with_future = build_point_in_time_context(market, as_of_date=as_of, weekly_stage_frame=weekly)
    without_future = build_point_in_time_context(
        market.iloc[:220], as_of_date=as_of, weekly_stage_frame=weekly.iloc[:1]
    )

    pd.testing.assert_frame_equal(with_future, without_future)
    assert with_future.iloc[0]["weekly_stage"] == "S1"
    assert pd.Timestamp(with_future.iloc[0]["as_of_date"]) <= pd.Timestamp(as_of)


def test_early_ipo_uses_separate_raw_history_liquidity_policy() -> None:
    market = _market("IPO", 40)
    as_of = market["timestamp"].iloc[-1].date().isoformat()

    context = build_point_in_time_context(market, as_of_date=as_of)
    classified = classify_lanes(context)

    assert bool(context.iloc[0]["early_ipo_liquidity_gate_passed"])
    assert not bool(context.iloc[0]["standard_liquidity_gate_passed"])
    assert context.iloc[0]["liquidity_policy_version"] == "ipo-early-liquidity-policy-v1"
    assert classified.iloc[0]["scan_lane_as_of"] == "ipo_early_base"


def test_short_history_dispatch_invokes_only_ipo_detector(monkeypatch) -> None:
    market = _market("IPO", 100)
    as_of = market["timestamp"].iloc[-1].date().isoformat()
    context = pd.DataFrame(
        [
            {
                "symbol_id": "IPO", "exchange": "NSE", "as_of_date": as_of, "bar_count": 100,
                "scan_lane_as_of": "young_listing_base", "history_band": "50_119",
                "stage2_score": 0.0, "stage2_label": "non_stage2",
                "lane_assignment_reason_codes": "[]", "liquidity_policy_version": "standard",
                "structure_observation_id": "obs",
            }
        ]
    )
    calls: list[str] = []

    def stub(*_args, **_kwargs):
        calls.append("ipo_base")
        return [], SimpleNamespace(candidate_count=0, confirmed_count=0, watchlist_count=0)

    monkeypatch.setattr(
        "ai_trading_system.research.pattern_lane_calibration.harness._detectors",
        lambda: {"ipo_base": stub},
    )

    signals, invocations = scan_lane_patterns(market, context, as_of_date=as_of)

    assert signals.empty
    assert calls == ["ipo_base"]
    assert invocations["pattern_family"].tolist() == ["ipo_base"]


def test_immutable_writer_rejects_production_trees_and_existing_bundle(tmp_path: Path) -> None:
    policy = default_r0_policy()
    result = CalibrationResult(
        context=pd.DataFrame(), detector_invocations=pd.DataFrame(), signals=pd.DataFrame(),
        outcomes=pd.DataFrame(), controls=pd.DataFrame(), metrics=pd.DataFrame(), winner_recall=pd.DataFrame(),
        runtime_diagnostics={},
        summary={"policy_hash": policy.content_hash},
        policy=policy, source_hashes={"market_frame": "abc", "weekly_stage_frame": "def"},
    )
    output = tmp_path / "research_bundle"

    paths = write_calibration_result(result, output)

    assert {path.name for path in paths} >= {"r0_pattern_manifest.json", "r0_pattern_policies.json"}
    with pytest.raises(FileExistsError):
        write_calibration_result(result, output)
    with pytest.raises(ValueError):
        write_calibration_result(result, tmp_path / "pipeline_runs" / "bad")


def test_metrics_and_winner_recall_keep_precision_and_recall_populations_separate() -> None:
    policy = default_r0_policy()
    signals = pd.DataFrame(
        [
            {
                "signal_id": "AAA-flat_base-watchlist-2026-01-05", "symbol_id": "AAA", "exchange": "NSE",
                "as_of_date": "2026-01-05", "signal_date": "2026-01-05", "scan_lane_as_of": "stage1_base",
                "pattern_family": "flat_base", "history_band": "180_plus", "pattern_state": "watchlist",
                "evidence_origin": "fresh", "market_regime": "unknown", "liquidity_cohort": 8,
            }
        ]
    )
    outcomes = pd.DataFrame(
        [
            {
                **signals.iloc[0].to_dict(), "horizon_sessions": 5, "outcome_window_complete": True,
                "forward_return": 0.10, "benchmark_relative_return": 0.08,
                "maximum_favourable_excursion": 0.15, "maximum_adverse_excursion": -0.03,
                "confirmed_breakout": True, "failed_breakout": False, "sessions_to_breakout": 2,
            }
        ]
    )
    winners = pd.DataFrame([{"symbol_id": "AAA", "first_guard_pass": "2026-01-10"}])

    metrics = _calibration_metrics(outcomes, signals, policy)
    recall = _winner_recall(signals, winners)

    assert metrics.iloc[0]["sample_size"] == 1
    assert metrics.iloc[0]["breakout_confirmation_rate"] == 1.0
    assert recall.iloc[0]["signal_before_first_guard_pass"]
    assert recall.iloc[0]["population_role"] == "recall_only_not_precision"


def test_parallel_run_reports_progress_and_resumes_completed_date(tmp_path: Path) -> None:
    market = pd.concat(
        [_market("IPO1", 40), _market("IPO2", 40, base=120.0), _market("UNIV_TOP1000_EW", 40, base=20_000.0)],
        ignore_index=True,
    )
    as_of = market["timestamp"].max().date().isoformat()
    checkpoints = tmp_path / "checkpoints"
    first_events: list[dict] = []

    first = run_calibration(
        market,
        as_of_dates=[as_of],
        workers=2,
        progress_callback=first_events.append,
        progress_every=1,
        checkpoint_dir=checkpoints,
    )

    assert any(event["event"] == "scan_progress" for event in first_events)
    assert any(event["event"] == "checkpoint_written" for event in first_events)
    assert first.summary["universe_rows"] == 2

    resumed_events: list[dict] = []
    resumed = run_calibration(
        market,
        as_of_dates=[as_of],
        workers=2,
        progress_callback=resumed_events.append,
        checkpoint_dir=checkpoints,
    )

    assert any(event["event"] == "checkpoint_loaded" for event in resumed_events)
    assert resumed.summary == first.summary
