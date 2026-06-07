from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.candidate_tracker import CandidateTrackerConfig, run_candidate_tracker


def _config(tmp_path: Path, run_date: str, *, archive: bool = False) -> CandidateTrackerConfig:
    return CandidateTrackerConfig(
        db_path=tmp_path / "data" / "candidate_tracker.duckdb",
        run_date=run_date,
        run_id=f"run-{run_date}",
        archive_failures=archive,
    )


def _watchlist(symbol: str = "AAA") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "watchlist_bucket": "F4_ACTION_CANDIDATE",
                "fundamental_score": 82,
                "industry_group": "Capital Goods",
            }
        ]
    )


def _final(symbol: str = "AAA", *, close: float = 120.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "candidate_group": "FUND_VALUE_TECH_READY",
                "composite_score": 88,
                "rel_strength_score": 82,
                "sector": "Capital Goods",
                "close": close,
                "sma_50": 110,
                "sma_200": 100,
                "near_52w_high_pct": 4,
                "stage2_label": "stage2",
            }
        ]
    )


def test_f4_watchlist_candidate_creates_active_episode(tmp_path: Path) -> None:
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=pd.DataFrame(),
        watchlist_candidates=_watchlist(),
    )

    assert len(result.current) == 1
    row = result.current.iloc[0]
    assert row["symbol"] == "AAA"
    assert bool(row["active"]) is True
    assert row["latest_watchlist_bucket"] == "F4_ACTION_CANDIDATE"


def test_reseen_candidate_updates_existing_episode(tmp_path: Path) -> None:
    run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=_final(close=100),
        watchlist_candidates=_watchlist(),
    )
    result = run_candidate_tracker(
        config=_config(tmp_path, "2026-06-02"),
        final_candidates=_final(close=105),
        watchlist_candidates=_watchlist(),
    )

    assert result.summary["new_episodes"] == 0
    assert result.summary["updated_episodes"] == 1
    conn = duckdb.connect(str(tmp_path / "data" / "candidate_tracker.duckdb"), read_only=True)
    try:
        episodes = conn.execute("SELECT COUNT(*), MAX(last_seen_date)::TEXT FROM tracked_candidates").fetchone()
    finally:
        conn.close()
    assert episodes == (1, "2026-06-02")


def test_remove_from_tracking_stays_active_by_default_and_archives_when_enabled(tmp_path: Path) -> None:
    good_results = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "report_date": "2026-03-31",
                "available_at": "2026-06-01",
                "quarterly_result_score": 80,
                "quarterly_result_bucket": "GREAT_RESULT",
                "sales_yoy_pct": 25,
                "operating_profit_yoy_pct": 35,
                "profit_yoy_pct": 30,
                "opm_yoy_change_bps": 250,
            }
        ]
    )
    run_candidate_tracker(
        config=_config(tmp_path, "2026-06-01"),
        final_candidates=_final(close=120),
        quarterly_result_scores=good_results,
    )
    bad_final = _final(close=70)
    bad_final.loc[:, "sma_200"] = 100
    bad_final.loc[:, "rel_strength_score"] = 30
    bad_results = good_results.copy()
    bad_results.loc[:, "available_at"] = "2026-09-01"
    bad_results.loc[:, "report_date"] = "2026-06-30"
    bad_results.loc[:, "quarterly_result_score"] = 30
    bad_results.loc[:, "quarterly_result_bucket"] = "DETERIORATING"
    bad_results.loc[:, "profit_yoy_pct"] = -5
    bad_results.loc[:, "opm_yoy_change_bps"] = -300

    default_result = run_candidate_tracker(
        config=_config(tmp_path, "2026-09-01"),
        final_candidates=bad_final,
        quarterly_result_scores=bad_results,
    )

    assert default_result.current.iloc[0]["status"] == "REMOVE_FROM_TRACKING"
    assert bool(default_result.current.iloc[0]["active"]) is True

    archive_result = run_candidate_tracker(
        config=_config(tmp_path, "2026-09-02", archive=True),
        final_candidates=bad_final,
        quarterly_result_scores=bad_results,
    )

    assert archive_result.current.iloc[0]["status"] == "REMOVE_FROM_TRACKING"
    assert bool(archive_result.current.iloc[0]["active"]) is False
