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
