from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.fundamentals.enrich_sector_dashboard import enrich_sector_dashboard


def _write_sector_dashboard(rank_dir: Path) -> None:
    rank_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"industry": "Banks", "rs_score": 80, "breadth": 0.6},
            {"industry": "Pharma", "rs_score": 60, "breadth": 0.4},
            {"industry": "Unknown", "rs_score": 50, "breadth": 0.5},
        ]
    ).to_csv(rank_dir / "sector_dashboard.csv", index=False)


def _write_industry_scores(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "industry": "Banks",
                "industry_key": "BANKS",
                "industry_fundamental_score": 76,
                "industry_growth_score": 80,
                "industry_quality_score": 80,
                "industry_valuation_score": 70,
                "industry_momentum_score": 60,
                "industry_fundamental_label": "QUALITY_GROWTH_LEADER",
                "industry_warning": "",
            },
            {
                "industry": "Pharma",
                "industry_key": "PHARMA",
                "industry_fundamental_score": 28,
                "industry_growth_score": 25,
                "industry_quality_score": 25,
                "industry_valuation_score": 40,
                "industry_momentum_score": 30,
                "industry_fundamental_label": "WEAK_FUNDAMENTALS",
                "industry_warning": "weak_roce",
            },
        ]
    ).to_csv(path, index=False)


def test_writes_enriched_csv_with_added_columns(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    _write_sector_dashboard(rank_dir)
    industry_scores = tmp_path / "industry_scores.csv"
    _write_industry_scores(industry_scores)

    enriched = enrich_sector_dashboard(rank_dir=rank_dir, industry_scores=industry_scores)

    output_path = rank_dir / "sector_dashboard_enriched.csv"
    assert output_path.exists()
    assert {"rs_score", "breadth", "industry_fundamental_score", "industry_fundamental_label"}.issubset(
        enriched.columns
    )
    assert len(enriched) == 3
    by_industry = enriched.set_index("industry")
    assert by_industry.loc["Banks", "industry_fundamental_label"] == "QUALITY_GROWTH_LEADER"
    assert by_industry.loc["Unknown", "industry_fundamental_label"] == "UNKNOWN"
    assert float(by_industry.loc["Unknown", "industry_fundamental_score"]) == 50.0


def test_missing_industry_scores_does_not_drop_rows(tmp_path: Path) -> None:
    rank_dir = tmp_path / "rank" / "attempt_1"
    _write_sector_dashboard(rank_dir)

    enriched = enrich_sector_dashboard(
        rank_dir=rank_dir, industry_scores=tmp_path / "does_not_exist.csv"
    )

    assert len(enriched) == 3
    assert (enriched["industry_fundamental_label"] == "UNKNOWN").all()
    assert (enriched["industry_fundamental_score"] == 50.0).all()
