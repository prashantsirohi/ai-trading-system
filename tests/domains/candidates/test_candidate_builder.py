from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.candidates.builder import _first_available, build_final_candidates, build_final_candidates_from_files
from ai_trading_system.domains.candidates.contracts import FINAL_CANDIDATE_COLUMNS


def _ranked() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "name": "Alpha",
                "sector": "Capital Goods",
                "composite_score": 92,
                "relative_strength": 88,
                "near_52w_high_pct": 4,
                "stage2_label": "stage2",
            },
            {
                "symbol_id": "BBB",
                "name": "Beta",
                "sector": "IT",
                "composite_score": 89,
                "relative_strength": 82,
                "near_52w_high_pct": 8,
                "stage2_label": "stage2",
            },
            {
                "symbol_id": "CCC",
                "name": "Cycle",
                "sector": "Metals",
                "composite_score": 87,
                "relative_strength": 84,
                "near_52w_high_pct": 6,
                "stage2_label": "stage2",
            },
            {
                "symbol_id": "DDD",
                "name": "Delta",
                "sector": "FMCG",
                "composite_score": 86,
                "relative_strength": 80,
                "near_52w_high_pct": 5,
                "stage2_label": "stage2",
            },
            {
                "symbol_id": "BAD",
                "name": "Bad Co",
                "sector": "Capital Goods",
                "composite_score": 95,
                "relative_strength": 91,
                "near_52w_high_pct": 4,
                "stage2_label": "stage2",
            },
        ]
    )


def _breakout() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": "AAA", "breakout_score": 91, "qualified": True},
            {"symbol_id": "BAD", "breakout_score": 90, "qualified": True},
        ]
    )


def _pattern() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol_id": "BBB", "pattern_score": 86, "setup_quality": 80},
            {"symbol_id": "CCC", "pattern_score": 78, "setup_quality": 72},
        ]
    )


def _sector() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Sector": "Capital Goods", "Quadrant": "Leading", "RS_rank": 1, "Momentum": 0.3},
            {"Sector": "IT", "Quadrant": "Improving", "RS_rank": 7, "Momentum": 0.2},
            {"Sector": "Metals", "Quadrant": "Weakening", "RS_rank": 10, "Momentum": -0.1},
            {"Sector": "FMCG", "Quadrant": "Weakening", "RS_rank": 11, "Momentum": -0.2},
        ]
    )


def _watchlist() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "name": "Alpha",
                "industry_group": "Capital Goods",
                "fundamental_score": 81,
                "fundamental_tier": "A",
                "fundamental_trend_label": "STABLE_GOOD",
                "hard_red_flag": False,
                "final_watchlist_score": 88,
            },
            {
                "symbol": "BBB",
                "name": "Beta",
                "industry_group": "IT",
                "fundamental_score": 78,
                "fundamental_tier": "B",
                "fundamental_trend_label": "IMPROVING",
                "hard_red_flag": False,
                "final_watchlist_score": 86,
            },
            {
                "symbol": "CCC",
                "name": "Cycle",
                "industry_group": "Metals",
                "fundamental_score": 74,
                "fundamental_tier": "B",
                "fundamental_trend_label": "STABLE_GOOD",
                "hard_red_flag": False,
                "catalyst_score": 70,
                "catalyst_type": "RESULTS_BREAKOUT",
                "final_watchlist_score": 84,
            },
            {
                "symbol": "BAD",
                "name": "Bad Co",
                "industry_group": "Capital Goods",
                "fundamental_score": 25,
                "fundamental_tier": "Reject",
                "fundamental_trend_label": "DETERIORATING",
                "hard_red_flag": True,
                "red_flags": "pledged_pct > 10",
                "final_watchlist_score": 70,
            },
        ]
    )


def test_build_final_candidates_assigns_groups_and_rejects_red_flags() -> None:
    result, summary = build_final_candidates(
        ranked_signals=_ranked(),
        breakout_scan=_breakout(),
        pattern_scan=_pattern(),
        sector_dashboard=_sector(),
        watchlist_candidates=_watchlist(),
        max_candidates=25,
    )

    assert list(result.columns) == FINAL_CANDIDATE_COLUMNS
    by_symbol = result.set_index("symbol")
    assert by_symbol.loc["AAA", "candidate_group"] == "LEADING_SECTOR_BREAKOUT"
    assert by_symbol.loc["BBB", "candidate_group"] == "FUNDAMENTAL_IMPROVER"
    assert by_symbol.loc["CCC", "candidate_group"] == "RESULTS_OR_CATALYST_PENDING"
    assert by_symbol.loc["DDD", "candidate_group"] == "HIGH_RS_PULLBACK"
    assert by_symbol.loc["BAD", "candidate_group"] == "AVOID_RED_FLAG"
    assert "Rejected by fundamental red flag" in by_symbol.loc["BAD", "candidate_reason"]
    assert summary["rows_selected"] == len(result)
    assert summary["candidate_group_counts"]["AVOID_RED_FLAG"] == 1


def test_first_available_text_default_preserves_object_dtype() -> None:
    frame = pd.DataFrame({"industry_group": pd.Series([float("nan"), float("nan")], dtype="float64")})

    result = _first_available(frame, ["industry_group", "industry_group_fundamental"], "")

    assert result.dtype == object
    assert result.tolist() == ["", ""]


def test_build_final_candidates_requires_valid_setup_for_normal_candidates() -> None:
    ranked = pd.concat(
        [
            _ranked(),
            pd.DataFrame(
                [
                    {
                        "symbol_id": "NOSETUP",
                        "name": "No Setup",
                        "sector": "Capital Goods",
                        "composite_score": 99,
                        "relative_strength": 95,
                        "near_52w_high_pct": 30,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    result, _summary = build_final_candidates(
        ranked_signals=ranked,
        breakout_scan=_breakout(),
        pattern_scan=_pattern(),
        sector_dashboard=_sector(),
        watchlist_candidates=_watchlist(),
    )

    assert "NOSETUP" not in set(result["symbol"])


def test_build_final_candidates_from_files_writes_outputs(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    fund_dir = tmp_path / "fundamentals" / "attempt_1"
    output_dir = tmp_path / "candidates" / "attempt_1"
    rank_dir.mkdir(parents=True)
    fund_dir.mkdir(parents=True)
    _ranked().to_csv(rank_dir / "ranked_signals.csv", index=False)
    _breakout().to_csv(rank_dir / "breakout_scan.csv", index=False)
    _pattern().to_csv(rank_dir / "pattern_scan.csv", index=False)
    _sector().to_csv(rank_dir / "sector_dashboard.csv", index=False)
    _watchlist().to_csv(fund_dir / "watchlist_candidates.csv", index=False)

    result, summary = build_final_candidates_from_files(
        ranked_signals_path=rank_dir / "ranked_signals.csv",
        breakout_scan_path=rank_dir / "breakout_scan.csv",
        pattern_scan_path=rank_dir / "pattern_scan.csv",
        sector_dashboard_path=rank_dir / "sector_dashboard.csv",
        watchlist_candidates_path=fund_dir / "watchlist_candidates.csv",
        output_dir=output_dir,
    )

    assert (output_dir / "final_candidates.csv").exists()
    assert (output_dir / "candidate_summary.json").exists()
    persisted_summary = json.loads((output_dir / "candidate_summary.json").read_text(encoding="utf-8"))
    assert persisted_summary["rows_selected"] == len(result)
    assert summary["status"] == "completed"


def test_build_final_candidates_from_files_ignores_early_accumulation_scan(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    output_dir = tmp_path / "candidates" / "attempt_1"
    rank_dir.mkdir(parents=True)
    _ranked().to_csv(rank_dir / "ranked_signals.csv", index=False)
    _breakout().to_csv(rank_dir / "breakout_scan.csv", index=False)
    _pattern().to_csv(rank_dir / "pattern_scan.csv", index=False)
    _sector().to_csv(rank_dir / "sector_dashboard.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol_id": "EARLY",
                "early_accumulation_score": 99,
                "graduation_status": "pattern_confirmed",
            }
        ]
    ).to_csv(rank_dir / "early_accumulation_scan.csv", index=False)

    result, _summary = build_final_candidates_from_files(
        ranked_signals_path=rank_dir / "ranked_signals.csv",
        breakout_scan_path=rank_dir / "breakout_scan.csv",
        pattern_scan_path=rank_dir / "pattern_scan.csv",
        sector_dashboard_path=rank_dir / "sector_dashboard.csv",
        output_dir=output_dir,
    )

    assert "EARLY" not in set(result["symbol"])
