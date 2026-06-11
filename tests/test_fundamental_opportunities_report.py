from __future__ import annotations

import json
import tomllib
from importlib import resources
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.publish.channels.fundamental_opportunities.builder import (
    build_fundamental_opportunity_report,
)
from ai_trading_system.domains.publish.channels.fundamental_opportunities.classifier import (
    classify_fundamental_opportunities,
    tracker_shortlist,
)
from ai_trading_system.domains.publish.channels.fundamental_opportunities.display import (
    clean_label,
    fmt_pct,
    score_band,
    score_width,
)
from ai_trading_system.domains.publish.channels.fundamental_opportunities.renderer import render_html
from ai_trading_system.domains.publish.channels.fundamental_opportunities.summary import build_report_summary


def test_assets_are_packaged() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    package_data = project["tool"]["setuptools"]["package-data"]["ai_trading_system"]

    assert "domains/publish/channels/fundamental_opportunities/templates/*.html" in package_data
    assert "domains/publish/channels/fundamental_opportunities/templates/partials/*.html" in package_data
    assert "domains/publish/channels/fundamental_opportunities/static/*.css" in package_data

    pkg = resources.files("ai_trading_system.domains.publish.channels.fundamental_opportunities")
    assert pkg.joinpath("templates", "fundamental_opportunities.html").is_file()
    assert pkg.joinpath("templates", "partials", "decision_row.html").is_file()
    assert pkg.joinpath("static", "report.css").is_file()


def test_classifier_covers_screenshot_buckets() -> None:
    rows = pd.DataFrame(
        [
            _row("QUAL", roce=24, debt_to_equity=0.2, opm=22, sales_growth_3y=18, profit_growth_3y=16, free_cash_flow_last_year=120, net_profit_cr=100),
            _row("HGRO", sales_growth_3y=38, profit_growth_3y=8, opm=9, opm_last_year=7),
            _row("CASH", sales_growth_3y=8, profit_growth_3y=7, opm=24, debt_to_equity=0.1, dividend_yield=3, free_cash_flow_last_year=130, net_profit_cr=100, roce=17),
            _row("TURN", sales_yoy_pct=12, profit_yoy_pct=5, opm=9, opm_last_year=6, quarterly_result_bucket="RESULT_ACCELERATION"),
            _row("VALU", roce=10, debt_to_equity=0.4, price_to_book=1.1, valuation_history_bucket="BELOW_OWN_MEDIAN"),
            _row("CYCL", industry_group="Metals", roce=14, debt_to_equity=0.8),
            _row("AVOID", roce=5, debt_to_equity=3),
        ]
    )

    out = classify_fundamental_opportunities(rows).set_index("symbol")

    assert out.loc["QUAL", "business_bucket"] == "QUALITY_COMPOUNDER"
    assert out.loc["HGRO", "business_bucket"] == "HIGH_GROWTH"
    assert out.loc["CASH", "business_bucket"] == "DIVIDEND_CASH_COW"
    assert out.loc["TURN", "business_bucket"] == "TURNAROUND_CANDIDATE"
    assert out.loc["VALU", "business_bucket"] == "DEEP_VALUE"
    assert out.loc["CYCL", "business_bucket"] == "CYCLICAL_COMMODITY"
    assert out.loc["AVOID", "business_bucket"] == "AVOID_WATCH"


def test_avoid_filter_runs_first_and_quality_wins_high_growth_overlap() -> None:
    rows = pd.DataFrame(
        [
            _row("BADGROWTH", roce=25, debt_to_equity=3, opm=23, sales_growth_3y=45, profit_growth_3y=30, free_cash_flow_last_year=120, net_profit_cr=100),
            _row("QUALITYGROWTH", roce=25, debt_to_equity=0.2, opm=24, sales_growth_3y=45, profit_growth_3y=30, free_cash_flow_last_year=130, net_profit_cr=100),
        ]
    )

    out = classify_fundamental_opportunities(rows).set_index("symbol")

    assert out.loc["BADGROWTH", "business_bucket"] == "AVOID_WATCH"
    assert out.loc["QUALITYGROWTH", "business_bucket"] == "QUALITY_COMPOUNDER"
    assert "High Growth" in out.loc["QUALITYGROWTH", "secondary_bucket_tags"]


def test_turnaround_is_trend_based_not_level_based() -> None:
    out = classify_fundamental_opportunities(
        pd.DataFrame(
            [
                _row("RISINGLOW", opm=9, opm_last_year=5, sales_yoy_pct=14, profit_yoy_pct=8, quarterly_result_bucket="MARGIN_EXPANSION"),
                _row("FALLINGHIGH", opm=18, opm_last_year=24, profit_growth_3y=-5),
            ]
        )
    ).set_index("symbol")

    assert out.loc["RISINGLOW", "business_bucket"] == "TURNAROUND_CANDIDATE"
    assert out.loc["FALLINGHIGH", "business_bucket"] == "AVOID_WATCH"


def test_tracker_shortlist_excludes_avoid_and_keeps_required_columns() -> None:
    classified = classify_fundamental_opportunities(
        pd.DataFrame(
            [
                _row("QUAL", roce=24, debt_to_equity=0.2, opm=22, sales_growth_3y=18, profit_growth_3y=16, free_cash_flow_last_year=120, net_profit_cr=100),
                _row("AVOID", roce=5, debt_to_equity=3),
            ]
        )
    )

    shortlist = tracker_shortlist(classified)

    assert shortlist["symbol"].tolist() == ["QUAL"]
    assert {
        "business_bucket",
        "secondary_bucket_tags",
        "opportunity_label",
        "bucket_reason",
        "manual_review_flag",
        "watchlist_bucket",
        "final_watchlist_score",
    }.issubset(shortlist.columns)


def test_summary_builder_sorts_top_opportunities_and_collapses_empty_buckets() -> None:
    classified = classify_fundamental_opportunities(
        pd.DataFrame(
            [
                _row("AAA", sales_growth_3y=38, profit_growth_3y=20, final_watchlist_score=65),
                _row("BBB", sales_yoy_pct=14, profit_yoy_pct=12, opm=8, opm_last_year=5, quarterly_result_bucket="RESULT_ACCELERATION"),
                _row("MISS", industry_group=pd.NA, industry=pd.NA, sector_name=pd.NA, valuation_history_bucket=pd.NA, sales_growth_3y=35),
                _row("LIMIT", valuation_history_bucket="INSUFFICIENT_HISTORY", industry_group="Metals", debt_to_equity=0.8, roce=12),
                _row("AVOID", roce=5, debt_to_equity=3, quarterly_result_bucket="DETERIORATING"),
            ]
        )
    )
    classified.loc[classified["symbol"].eq("AAA"), "final_watchlist_score"] = 91
    classified.loc[classified["symbol"].eq("BBB"), "final_watchlist_score"] = 75

    summary = build_report_summary(
        classified=classified,
        shortlist=tracker_shortlist(classified),
        as_of="2026-06-01",
        universe_id="UNIV_TEST",
        warnings=["sample warning"],
        limit_per_bucket=25,
    )

    assert summary["top_opportunities"][0]["symbol_display"] == "AAA"
    assert "Deep Value" in summary["no_candidate_buckets"]
    assert "AVOID_WATCH" not in summary["main_bucket_tables"]
    assert summary["appendix_bucket_counts"]["AVOID_WATCH"] >= 1
    assert summary["data_quality"]["missing_industry"] >= 1
    assert summary["data_quality"]["missing_valuation"] >= 1
    assert summary["data_quality"]["result_failures"] >= 1
    assert summary["data_quality"]["insufficient_history"] >= 1


def test_display_helpers_clean_raw_values_and_score_bands() -> None:
    assert clean_label(float("nan")) == "-"
    assert clean_label("IMPROVING_BELOW_AVERAGE") == "Improving, below own average"
    assert clean_label("DEEPLY_BELOW_HISTORY") == "Deep discount vs history"
    assert fmt_pct(float("nan")) == "N/A"
    assert score_band(72) == "Strong candidate"
    assert score_band(65) == "Watchlist candidate"
    assert score_band(55) == "Manual review"
    assert score_band(40) == "Weak / avoid"
    assert score_width(128) == 100


def test_renderer_is_decision_first_and_hides_avoid_main_section() -> None:
    classified = classify_fundamental_opportunities(
        pd.DataFrame(
            [
                _row("AAA", sales_growth_3y=38, profit_growth_3y=20),
                _row("AVOID", roce=5, debt_to_equity=3),
            ]
        )
    )
    classified.loc[classified["symbol"].eq("AAA"), "final_watchlist_score"] = 88
    summary = build_report_summary(
        classified=classified,
        shortlist=tracker_shortlist(classified),
        as_of="2026-06-01",
        universe_id="UNIV_TEST",
        warnings=[],
        limit_per_bucket=25,
    )
    html = render_html(_render_context(classified, summary))

    assert html.index("Executive Decision Page") < html.index("Methodology appendix")
    assert html.index("Top Opportunities Today") < html.index("Bucket Definitions")
    assert "No rows classified into this bucket" not in html
    assert "Sector Opportunity Map" in html
    assert "Data Quality" in html
    assert "AAA" in html
    assert "Avoid / Watch</h2>" not in html
    assert "Classification Flow" in html
    assert "Metric Definitions" in html
    assert "Bucket Matrix Appendix" in html


def test_build_report_from_temp_duckdbs_writes_manifest_and_shortlist(tmp_path: Path) -> None:
    fundamentals_db = tmp_path / "fundamentals.duckdb"
    ohlcv_db = tmp_path / "ohlcv.duckdb"
    scores_csv = tmp_path / "fundamental_scores_latest.csv"
    _seed_fundamentals_db(fundamentals_db)
    _seed_valuation_db(ohlcv_db)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "name": "AAA Ltd",
                "industry_group": "Capital Goods",
                "industry": "Industrial",
                "quality_score": 85,
                "growth_score": 75,
                "fundamental_score": 82,
                "hard_red_flag": False,
            }
        ]
    ).to_csv(scores_csv, index=False)

    manifest = build_fundamental_opportunity_report(
        as_of="2026-06-01",
        fundamentals_db_path=fundamentals_db,
        ohlcv_db_path=ohlcv_db,
        fundamental_scores_path=scores_csv,
        output_dir=tmp_path / "out",
        tracker_db_path=tmp_path / "missing_tracker.duckdb",
        universe_id="UNIV_TEST",
    )

    assert Path(manifest["html_path"]).exists()
    assert Path(manifest["shortlist_path"]).exists()
    assert Path(manifest["classified_path"]).exists()
    manifest_file = Path(manifest["manifest_path"])
    assert json.loads(manifest_file.read_text(encoding="utf-8"))["counts"]["total_rows"] >= 1
    parsed = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert "top_opportunities" in parsed
    assert "data_quality" in parsed
    assert "sector_map" in parsed
    assert "no_candidate_buckets" in parsed
    assert "main_report_bucket_counts" in parsed
    assert "appendix_bucket_counts" in parsed


def test_build_report_updates_tracker_only_when_requested(tmp_path: Path) -> None:
    fundamentals_db = tmp_path / "fundamentals.duckdb"
    ohlcv_db = tmp_path / "ohlcv.duckdb"
    scores_csv = tmp_path / "fundamental_scores_latest.csv"
    tracker_db = tmp_path / "candidate_tracker.duckdb"
    _seed_fundamentals_db(fundamentals_db)
    _seed_valuation_db(ohlcv_db)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "name": "AAA Ltd",
                "industry_group": "Capital Goods",
                "industry": "Industrial",
                "quality_score": 85,
                "growth_score": 75,
                "fundamental_score": 82,
                "hard_red_flag": False,
            }
        ]
    ).to_csv(scores_csv, index=False)

    report_only = build_fundamental_opportunity_report(
        as_of="2026-06-01",
        fundamentals_db_path=fundamentals_db,
        ohlcv_db_path=ohlcv_db,
        fundamental_scores_path=scores_csv,
        output_dir=tmp_path / "report_only",
        tracker_db_path=tracker_db,
        universe_id="UNIV_TEST",
    )
    assert "tracker_update" not in report_only
    assert not tracker_db.exists()

    updated = build_fundamental_opportunity_report(
        as_of="2026-06-01",
        fundamentals_db_path=fundamentals_db,
        ohlcv_db_path=ohlcv_db,
        fundamental_scores_path=scores_csv,
        output_dir=tmp_path / "updated",
        tracker_db_path=tracker_db,
        universe_id="UNIV_TEST",
        update_tracker=True,
    )
    assert updated["tracker_update"]["bucket_reviews"] >= 1
    assert tracker_db.exists()


def _render_context(classified: pd.DataFrame, summary: dict) -> dict:
    from ai_trading_system.domains.publish.channels.fundamental_opportunities.classifier import (
        BUCKET_CARDS,
        bucket_counts,
        bucket_matrix,
        metric_definitions,
    )
    from ai_trading_system.domains.publish.channels.fundamental_opportunities.summary import MAIN_BUCKETS

    return {
        "as_of": "2026-06-01",
        "universe_id": "UNIV_TEST",
        "cards": [card.__dict__ for card in BUCKET_CARDS],
        "metric_definitions": metric_definitions(),
        "bucket_matrix": bucket_matrix(),
        "bucket_counts": bucket_counts(classified),
        "bucket_tables": summary["main_bucket_tables"],
        "main_buckets": MAIN_BUCKETS,
        "total_rows": len(classified),
        "shortlist_rows": len(tracker_shortlist(classified)),
        "warnings": [],
        **summary,
    }


def _row(symbol: str, **overrides) -> dict:
    row = {
        "symbol": symbol,
        "industry_group": "Capital Goods",
        "industry": "Industrial",
        "roce": 12,
        "roe": 12,
        "debt_to_equity": 0.6,
        "opm": 12,
        "opm_last_year": 11,
        "sales_growth_3y": 10,
        "profit_growth_3y": 10,
        "sales_yoy_pct": 10,
        "profit_yoy_pct": 10,
        "quarterly_result_score": 65,
        "quarterly_result_bucket": "IGNORE",
        "valuation_history_score": 50,
        "valuation_history_bucket": "NEAR_OWN_MEDIAN",
        "free_cash_flow_last_year": 50,
        "net_profit_cr": 100,
        "hard_red_flag": False,
    }
    row.update(overrides)
    return row


def _seed_fundamentals_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE company_growth_features (
                symbol VARCHAR,
                report_date DATE,
                statement_basis VARCHAR,
                available_at DATE,
                sales_cr DOUBLE,
                net_profit_cr DOUBLE,
                operating_profit_cr DOUBLE,
                opm_pct DOUBLE,
                sales_yoy_growth DOUBLE,
                sales_qoq_growth DOUBLE,
                profit_yoy_growth DOUBLE,
                profit_qoq_growth DOUBLE,
                operating_profit_yoy_growth DOUBLE,
                operating_profit_qoq_growth DOUBLE,
                opm_yoy_change DOUBLE,
                opm_qoq_change DOUBLE,
                sales_8q_cagr DOUBLE,
                profit_8q_cagr DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO company_growth_features VALUES (
                'AAA', '2026-03-31', 'standalone', '2026-05-15',
                1000, 100, 180, 22,
                0.42, 0.05, 0.18, 0.04, 0.25, 0.08, 0.02, 0.01, 0.38, 0.16
            )
            """
        )
    finally:
        conn.close()


def _seed_valuation_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_valuation_bands (
                universe_id VARCHAR,
                date DATE,
                symbol VARCHAR,
                sector_name VARCHAR,
                pe_ttm DOUBLE,
                ps_ttm DOUBLE,
                pb DOUBLE,
                pe_pctile_5y DOUBLE,
                ps_pctile_5y DOUBLE,
                pb_pctile_5y DOUBLE,
                pe_vs_5y_median_pct DOUBLE,
                ps_vs_5y_median_pct DOUBLE,
                pb_vs_5y_median_pct DOUBLE,
                valuation_history_score DOUBLE,
                valuation_history_bucket VARCHAR,
                valuation_reason VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_valuation_bands VALUES (
                'UNIV_TEST', '2026-06-01', 'AAA', 'Capital Goods',
                18, 2, 1.2, 30, 35, 20, -12, -8, -15, 82, 'BELOW_OWN_MEDIAN', 'below own history'
            )
            """
        )
    finally:
        conn.close()
