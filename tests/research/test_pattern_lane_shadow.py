from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.research.pattern_lane_calibration.shadow import (
    EVIDENCE_CLASS,
    attach_evidence,
    build_parity_report,
    build_source_diagnostics,
    build_shadow_summary,
    classify_evidence,
    render_shadow_report_html,
    run_lane_shadow_scan,
    source_diagnostics_frame,
)

# Outcome/control/metric columns that R1a must never produce.
_OUTCOME_COLUMNS = {
    "horizon_sessions", "outcome_window_complete", "forward_return",
    "benchmark_return", "benchmark_relative_return", "sector_relative_return",
    "maximum_favourable_excursion", "maximum_adverse_excursion",
    "confirmed_breakout", "failed_breakout", "invalidated_setup",
    "sessions_to_breakout", "outcome_policy_version",
}


def _market(symbol: str, periods: int, *, start: str = "2025-01-01", base: float = 100.0) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=periods)
    rows = []
    for index, timestamp in enumerate(dates):
        close = base + index * 0.25
        rows.append({
            "symbol_id": symbol, "exchange": "NSE", "timestamp": timestamp,
            "open": close - 0.5, "high": close + 1.0, "low": close - 1.0,
            "close": close, "volume": 100_000.0,
        })
    return pd.DataFrame(rows)


def test_run_lane_shadow_scan_returns_evidence_without_outcome_columns() -> None:
    market = _market("YOUNG", 150)
    as_of = market["timestamp"].iloc[-1].date().isoformat()

    classified, signals, invocations, timings = run_lane_shadow_scan(
        market, as_of_date=as_of, weekly_stage_frame=None,
    )

    assert not classified.empty
    assert (classified["scan_lane_as_of"] == "young_listing_base").any()
    assert not invocations.empty  # detectors were invoked
    assert _OUTCOME_COLUMNS.isdisjoint(set(signals.columns))
    assert {"context_load_seconds", "classify_seconds", "scan_seconds", "total_seconds"} <= set(timings)


def test_classify_evidence_maps_every_lane_and_family_with_suppression_precedence() -> None:
    # Family precedence.
    assert classify_evidence("stage1_base", "flat_base") == "evidence_supported"
    assert classify_evidence("stage1_base", "vcp") == "evidence_supported_smaller_sample"
    assert classify_evidence("stage1_base", "flag") == "negative_evidence"
    assert classify_evidence("stage1_base", "high_tight_flag") == "insufficient_evidence"
    assert classify_evidence("stage1_base", "three_weeks_tight") == "insufficient_evidence"
    # head_shoulders suppression wins even in an otherwise evidence_supported lane.
    assert classify_evidence("stage1_base", "head_shoulders") == "suppression_only"
    # Lane fallback for families with no specific evidence entry.
    assert classify_evidence("stage1_base", "cup_handle") == "evidence_supported_low_volume"
    assert classify_evidence("young_listing_base", "cup_handle") == "observational"
    assert classify_evidence("ipo_early_base", "ipo_base") == "observational_non_promotable"
    # Default.
    assert classify_evidence("no_lane", "cup_handle") == EVIDENCE_CLASS["default"]


def _classified_row(**overrides) -> dict:
    row = {
        "symbol_id": "AAA", "exchange": "NSE", "scan_lane_as_of": "stage1_base",
        "weekly_stage_source": "governed_live", "weekly_stage_source_fallback_used": False,
        "weekly_stage_policy_version": "weekly-stage-v2",
        "weekly_stage_age_trading_days": 3, "weekly_stage_is_fresh": True,
    }
    row.update(overrides)
    return row


def test_build_source_diagnostics_counts_fallback_and_stale() -> None:
    context = pd.DataFrame([
        _classified_row(symbol_id="AAA", weekly_stage_source="governed_live"),
        _classified_row(
            symbol_id="BBB", weekly_stage_source="snapshot_fallback",
            weekly_stage_source_fallback_used=True,
            weekly_stage_policy_version="weekly_stage_snapshot:unversioned",
            weekly_stage_age_trading_days=5,
        ),
        _classified_row(symbol_id="CCC", weekly_stage_source="governed_backfill", weekly_stage_age_trading_days=8),
        _classified_row(symbol_id="DDD", weekly_stage_source=None, weekly_stage_is_fresh=False),
    ])

    diagnostics = build_source_diagnostics(context, require_stage_policy_version="weekly-stage-v2")

    assert diagnostics["rows_with_weekly_stage"] == 3
    assert diagnostics["rows_without_weekly_stage"] == 1
    assert diagnostics["fallback_rows"] == 1
    assert diagnostics["fallback_rate"] == round(1 / 3, 6)
    assert diagnostics["stale_admitted_as_fresh_count"] == 0
    assert diagnostics["policy_mismatch_count"] == 0
    assert diagnostics["source_counts"]["snapshot_fallback"] == 1


def test_build_source_diagnostics_flags_stale_admitted_as_fresh() -> None:
    # is_fresh True while age exceeds the freshness window (max 10 trading days).
    context = pd.DataFrame([_classified_row(weekly_stage_age_trading_days=25, weekly_stage_is_fresh=True)])

    diagnostics = build_source_diagnostics(context, require_stage_policy_version="weekly-stage-v2")

    assert diagnostics["stale_admitted_as_fresh_count"] == 1


def test_build_source_diagnostics_raises_on_governed_policy_mismatch() -> None:
    context = pd.DataFrame([
        _classified_row(weekly_stage_source="governed_live", weekly_stage_policy_version="weekly-stage-v1"),
    ])

    with pytest.raises(ValueError, match="policy mismatch"):
        build_source_diagnostics(context, require_stage_policy_version="weekly-stage-v2")


def test_source_diagnostics_frame_lists_each_source_plus_total() -> None:
    diagnostics = build_source_diagnostics(
        pd.DataFrame([
            _classified_row(symbol_id="AAA", weekly_stage_source="governed_live"),
            _classified_row(symbol_id="BBB", weekly_stage_source="governed_backfill"),
        ]),
        require_stage_policy_version="weekly-stage-v2",
    )
    frame = source_diagnostics_frame(diagnostics)
    assert set(frame["stage_source"]) >= {"governed_live", "governed_backfill", "snapshot_fallback", "TOTAL"}
    assert int(frame.loc[frame["stage_source"] == "TOTAL", "row_count"].iloc[0]) == 2


def test_build_parity_report_captures_legacy_hash_and_symbol_overlap(tmp_path: Path) -> None:
    legacy_path = tmp_path / "pattern_scan.csv"
    pd.DataFrame({"symbol_id": ["AAA", "BBB"]}).to_csv(legacy_path, index=False)
    legacy_artifact = StageArtifact.from_file("pattern_scan", legacy_path, row_count=2, attempt_number=1)
    lane_signals = pd.DataFrame({"symbol_id": ["BBB", "CCC"], "pattern_family": ["flat_base", "vcp"]})

    parity = build_parity_report(lane_signals, legacy_artifact)

    assert parity["legacy_pattern_scan"]["present"]
    assert parity["legacy_pattern_scan"]["content_hash"] == legacy_artifact.content_hash
    assert parity["legacy_pattern_scan"]["row_count"] == 2
    assert parity["overlap_count"] == 1
    assert parity["both"] == ["BBB"]
    assert parity["lane_only"] == ["CCC"]
    assert parity["legacy_only"] == ["AAA"]
    assert parity["operational_side_effects"] is False


def test_build_parity_report_handles_missing_legacy_artifact() -> None:
    parity = build_parity_report(pd.DataFrame({"symbol_id": ["AAA"]}), None)
    assert parity["legacy_pattern_scan"]["present"] is False
    assert parity["overlap_count"] == 0
    assert parity["lane_only"] == ["AAA"]


def test_attach_evidence_classifies_suppression_rows_and_preserves_provenance() -> None:
    # Regression guard: a head_shoulders suppression row must keep its as_of_date
    # and lane, and classify as suppression_only.
    signals = pd.DataFrame([{
        "symbol_id": "aaa", "exchange": "NSE", "pattern_family": "head_shoulders",
        "scan_lane_as_of": "stage1_base", "as_of_date": "2026-07-17",
        "signal_date": "2026-07-17", "evidence_origin": "fresh",
    }])
    classified = pd.DataFrame([{
        "symbol_id": "AAA", "exchange": "NSE", "scan_lane_as_of": "stage1_base",
        "weekly_stage_source": "governed_live", "weekly_stage_source_fallback_used": False,
        "weekly_stage_policy_version": "weekly-stage-v2",
        "weekly_stage_is_fresh": True, "weekly_stage_age_trading_days": 3,
        "weekly_stage_observation_id": "obs-1",
    }])

    enriched = attach_evidence(signals, classified)

    row = enriched.iloc[0]
    assert row["r1a_evidence_class"] == "suppression_only"
    assert row["stage_source"] == "governed_live"
    assert row["as_of_date"] == "2026-07-17"
    assert row["scan_lane_as_of"] == "stage1_base"
    assert row["evidence_origin"] == "fresh"


def test_render_shadow_report_html_carries_banner_and_no_action_language() -> None:
    summary = build_shadow_summary(
        pd.DataFrame({"scan_lane_as_of": ["stage1_base", "no_lane"]}),
        pd.DataFrame({"scan_lane_as_of": ["stage1_base"], "pattern_family": ["flat_base"],
                      "evidence_origin": ["fresh"], "r1a_evidence_class": ["evidence_supported"]}),
        diagnostics={"fallback_rate": 0.0, "stale_admitted_as_fresh_count": 0, "policy_mismatch_count": 0},
        parity={"legacy_pattern_scan": {"present": False}},
    )
    html = render_shadow_report_html(
        summary, {"source_counts": {}}, {"legacy_pattern_scan": {"present": False}},
        {"total_wall_seconds": 1.0}, run_date="2026-07-18",
    )
    assert "SHADOW — NON-ACTIONABLE" in html
    lowered = html.lower()
    for banned in ("buy now", "add to watchlist", "place order", "score:"):
        assert banned not in lowered
