from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.investigator import pattern_scan as pattern_scan_module
from ai_trading_system.domains.investigator.pattern_scan import (
    _classify_s1_state,
    build_investigator_pattern_scan,
)
from ai_trading_system.pipeline.contracts import StageContext


def _context(tmp_path: Path, params: dict[str, object] | None = None) -> StageContext:
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-1",
        run_date="2026-05-07",
        stage_name="investigator",
        attempt_number=1,
        params=params or {},
    )


def test_investigator_pattern_scan_normalizes_caps_and_disables_stage2_and_cache(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_load_pattern_frame(*args, **kwargs):
        captured["load_symbols"] = kwargs["symbols"]
        return pd.DataFrame(
            [
                {"symbol_id": "AAA", "timestamp": "2026-05-07", "close": 10},
                {"symbol_id": "BBB", "timestamp": "2026-05-07", "close": 20},
            ]
        )

    def fake_build_pattern_signals(*args, **kwargs):
        captured["build_symbols"] = kwargs["symbols"]
        captured["stage2_only"] = kwargs["stage2_only"]
        captured["write_pattern_cache"] = kwargs["write_pattern_cache"]
        return pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "pattern_family": "round_bottom",
                    "pattern_state": "watchlist",
                    "pattern_lifecycle_state": "watchlist",
                    "pattern_score": 58,
                    "setup_quality": 45,
                }
            ]
        )

    monkeypatch.setattr(pattern_scan_module, "load_pattern_frame", fake_load_pattern_frame)
    monkeypatch.setattr(pattern_scan_module, "build_pattern_signals", fake_build_pattern_signals)

    active = pd.DataFrame(
        [
            {"symbol_id": " aaa ", "status": "ACTIVE_RESEARCH", "verdict": "WATCH_ONLY", "final_score": 51},
            {"symbol_id": "AAA", "status": "ACTIVE_RESEARCH", "verdict": "WATCH_ONLY", "final_score": 51},
            {"symbol_id": "bbb", "status": "TRACKING", "verdict": "WATCH_ONLY", "final_score": 45},
            {"symbol_id": "CCC", "status": "TRACKING", "verdict": "WATCH_ONLY", "final_score": 44},
        ]
    )

    result = build_investigator_pattern_scan(
        context=_context(tmp_path, {"investigator_pattern_max_symbols": 2}),
        active_watchlist=active,
        ranked_df=pd.DataFrame({"symbol_id": ["BBB"]}),
    )

    assert captured["load_symbols"] == ["AAA", "BBB"]
    assert captured["build_symbols"] == ["AAA", "BBB"]
    assert captured["stage2_only"] is False
    assert captured["write_pattern_cache"] is False
    assert bool(result.iloc[0]["source_investigator"]) is True
    assert bool(result.iloc[0]["source_ranked"]) is False
    assert result.iloc[0]["investigator_status"] == "ACTIVE_RESEARCH"


def test_investigator_pattern_scan_empty_active_watchlist_returns_empty(tmp_path: Path) -> None:
    result = build_investigator_pattern_scan(
        context=_context(tmp_path),
        active_watchlist=pd.DataFrame(columns=["symbol_id"]),
        ranked_df=pd.DataFrame(),
    )

    assert result.empty
    assert "s1_promotion_state" in result.columns


def test_s1_classification_precedence() -> None:
    failed = _classify_s1_state(pd.Series({"pattern_lifecycle_state": "invalidated", "pattern_score": 95}))
    confirmed = _classify_s1_state(
        pd.Series(
            {
                "pattern_lifecycle_state": "confirmed",
                "pattern_state": "confirmed",
                "stage2_score": 75,
                "is_combined_volume_confirmation": True,
                "pattern_score": 80,
            }
        )
    )
    transition = _classify_s1_state(pd.Series({"pattern_score": 72, "breakout_volume_ratio": 1.3}))
    near = _classify_s1_state(pd.Series({"pattern_score": 66, "setup_quality": 20}))
    accumulation = _classify_s1_state(pd.Series({"pattern_score": 52, "delivery_pct": 58}))
    base = _classify_s1_state(pd.Series({"pattern_score": 38, "setup_quality": 20}))

    assert failed["s1_promotion_state"] == "FAILED_S1"
    assert confirmed["s1_promotion_state"] == "S2_CONFIRMED"
    assert transition["s1_promotion_state"] == "S1_TO_S2_TRANSITION"
    assert near["s1_promotion_state"] == "S1_NEAR_BREAKOUT"
    assert accumulation["s1_promotion_state"] == "S1_ACCUMULATION"
    assert base["s1_promotion_state"] == "S1_BASE_FORMING"


def test_s1_classification_ambiguity_matrix_cases() -> None:
    cases = [
        (
            "13.1 volume accumulation",
            {"pattern_score": 50, "volume_ratio_20": 1.3},
            "S1_ACCUMULATION",
        ),
        (
            "13.2 base forming",
            {"pattern_score": 50},
            "S1_BASE_FORMING",
        ),
        (
            "13.3 near breakout",
            {"pattern_score": 66},
            "S1_NEAR_BREAKOUT",
        ),
        (
            "13.4 transition",
            {"pattern_score": 72, "is_combined_volume_confirmation": True},
            "S1_TO_S2_TRANSITION",
        ),
        (
            "13.5 confirmed",
            {
                "stage2_score": 70,
                "pattern_state": "confirmed",
                "is_combined_volume_confirmation": True,
            },
            "S2_CONFIRMED",
        ),
        (
            "13.6 low delivery without fade",
            {"pattern_score": 50, "low_delivery_flag": True, "appearance_count_20d": 1, "price_progression_pct": 0},
            "S1_BASE_FORMING",
        ),
        (
            "13.7 low delivery failed follow-through",
            {"pattern_score": 50, "low_delivery_flag": True, "appearance_count_20d": 1, "price_progression_pct": -2},
            "FAILED_S1",
        ),
        (
            "13.8 hard trap",
            {"pattern_score": 50, "hard_trap_flag": True},
            "FAILED_S1",
        ),
        (
            "trigger accumulation weekly gainer",
            {"pattern_score": 50, "trigger_reason": "WEEKLY_GAINER"},
            "S1_ACCUMULATION",
        ),
        (
            "trigger accumulation stealth",
            {"pattern_score": 50, "trigger_reason": "STEALTH_ACCUMULATION"},
            "S1_ACCUMULATION",
        ),
    ]

    for label, row, expected in cases:
        classified = _classify_s1_state(pd.Series(row))
        assert classified["s1_promotion_state"] == expected, label
