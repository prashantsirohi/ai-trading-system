from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.fundamentals.enrich_rank import enrich_rank_artifacts


def _write_rank(rank_dir: Path, *, industries: dict[str, str]) -> None:
    rank_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 82},
            {"symbol_id": "BBB", "composite_score": 80},
            {"symbol_id": "CCC", "composite_score": 78},
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "breakout_score": 90, "candidate_tier": "A", "qualified": True},
            {"symbol_id": "BBB", "breakout_score": 88, "candidate_tier": "A", "qualified": True},
            {"symbol_id": "CCC", "breakout_score": 80, "candidate_tier": "A", "qualified": True},
        ]
    ).to_csv(rank_dir / "breakout_scan.csv", index=False)
    pd.DataFrame(
        [
            {"symbol_id": "AAA", "pattern_score": 80, "pattern_family": "vcp", "pattern_state": "confirmed"},
            {"symbol_id": "BBB", "pattern_score": 78, "pattern_family": "vcp", "pattern_state": "confirmed"},
            {"symbol_id": "CCC", "pattern_score": 70, "pattern_family": "vcp", "pattern_state": "confirmed"},
        ]
    ).to_csv(rank_dir / "pattern_scan.csv", index=False)


def _write_scores(path: Path, industries: dict[str, str]) -> None:
    pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": symbol,
                "industry_group": "x",
                "industry": industries[symbol],
                "quality_score": 80,
                "growth_score": 75,
                "balance_sheet_score": 90,
                "valuation_score": 65,
                "ownership_score": 85,
                "fundamental_score": 79,
                "fundamental_tier": "A",
                "red_flags": "",
                "hard_red_flag": False,
            }
            for symbol in ("AAA", "BBB", "CCC")
        ]
    ).to_csv(path, index=False)


def _write_industry_scores(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "industry": "Banks",
                "industry_key": "BANKS",
                "industry_growth_score": 80,
                "industry_quality_score": 80,
                "industry_valuation_score": 70,
                "industry_momentum_score": 60,
                "industry_fundamental_score": 76,
                "industry_fundamental_label": "QUALITY_GROWTH_LEADER",
                "industry_warning": "",
            },
            {
                "industry": "Pharma",
                "industry_key": "PHARMA",
                "industry_growth_score": 25,
                "industry_quality_score": 25,
                "industry_valuation_score": 40,
                "industry_momentum_score": 30,
                "industry_fundamental_score": 28,
                "industry_fundamental_label": "WEAK_FUNDAMENTALS",
                "industry_warning": "weak_roce",
            },
        ]
    ).to_csv(path, index=False)


def test_industry_columns_appear_in_watchlist(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    industries = {"AAA": "Banks", "BBB": "Pharma", "CCC": "Mystery"}
    _write_rank(rank_dir, industries=industries)
    scores = tmp_path / "scores.csv"
    _write_scores(scores, industries)
    industry_scores = tmp_path / "industry_scores.csv"
    _write_industry_scores(industry_scores)
    output = tmp_path / "watchlist.csv"

    result, metrics = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        industry_scores=industry_scores,
        output=output,
        return_metrics=True,
    )

    by_symbol = result.set_index("symbol")
    assert by_symbol.loc["AAA", "industry_fundamental_label"] == "QUALITY_GROWTH_LEADER"
    assert "industry backdrop supportive" in by_symbol.loc["AAA", "watchlist_reason"]
    # WEAK_FUNDAMENTALS downgrades ADD_TO_WATCHLIST -> STUDY_ONLY for BBB
    assert by_symbol.loc["BBB", "industry_fundamental_label"] == "WEAK_FUNDAMENTALS"
    assert by_symbol.loc["BBB", "watchlist_bucket"] == "STUDY_ONLY"
    assert "weak industry fundamentals" in by_symbol.loc["BBB", "watchlist_reason"]
    # Unmatched -> UNKNOWN with neutral 50
    assert by_symbol.loc["CCC", "industry_fundamental_label"] == "UNKNOWN"
    assert float(by_symbol.loc["CCC", "industry_fundamental_score"]) == 50.0

    assert metrics.matched_industry_rows == 2
    assert metrics.missing_industry_rows == 1
    assert "QUALITY_GROWTH_LEADER" in metrics.industry_label_counts


def test_industry_trend_label_appends_reason_and_metrics(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    industries = {"AAA": "Banks", "BBB": "Pharma", "CCC": "Mystery"}
    _write_rank(rank_dir, industries=industries)
    scores = tmp_path / "scores.csv"
    _write_scores(scores, industries)
    industry_scores = tmp_path / "industry_scores.csv"
    _write_industry_scores(industry_scores)

    industry_trends = tmp_path / "industry_trends.csv"
    pd.DataFrame(
        [
            {"industry_key": "BANKS",  "industry_fundamental_score_delta":  14.0, "industry_trend_label": "IMPROVING",     "industry_trend_reason": "rose"},
            {"industry_key": "PHARMA", "industry_fundamental_score_delta": -12.0, "industry_trend_label": "DETERIORATING", "industry_trend_reason": "fell"},
        ]
    ).to_csv(industry_trends, index=False)

    output = tmp_path / "watchlist.csv"
    result, metrics = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        industry_scores=industry_scores,
        industry_trends=industry_trends,
        output=output,
        return_metrics=True,
    )
    by_symbol = result.set_index("symbol")
    assert by_symbol.loc["AAA", "industry_trend_label"] == "IMPROVING"
    assert "industry trend improving" in by_symbol.loc["AAA", "watchlist_reason"]
    assert by_symbol.loc["BBB", "industry_trend_label"] == "DETERIORATING"
    assert "industry trend deteriorating" in by_symbol.loc["BBB", "watchlist_reason"]
    assert float(by_symbol.loc["AAA", "industry_score_delta"]) == 14.0
    assert by_symbol.loc["CCC", "industry_trend_label"] == "UNKNOWN"
    assert metrics.industry_trend_label_counts.get("IMPROVING", 0) >= 1


def test_missing_industry_scores_neutral(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    industries = {"AAA": "Banks", "BBB": "Pharma", "CCC": "Mystery"}
    _write_rank(rank_dir, industries=industries)
    scores = tmp_path / "scores.csv"
    _write_scores(scores, industries)
    output = tmp_path / "watchlist.csv"

    result = enrich_rank_artifacts(
        rank_dir=rank_dir,
        fundamental_scores=scores,
        fundamental_trends=None,
        industry_scores=tmp_path / "does_not_exist.csv",
        output=output,
    )
    assert (result["industry_fundamental_label"] == "UNKNOWN").all()
    assert (result["industry_fundamental_score"] == 50.0).all()
