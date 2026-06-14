from __future__ import annotations

import json
import tomllib
import warnings
from datetime import date
from datetime import timedelta
from importlib import resources
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import breadth, charts, history, metrics
from ai_trading_system.domains.publish.channels.weekly_pdf.builder import _safe_date, build_report
from ai_trading_system.domains.publish.channels.weekly_pdf.data_loader import load_report_data
from ai_trading_system.domains.publish.channels.weekly_pdf.renderer import render_html
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext


def test_weekly_pdf_assets_are_packaged() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    package_data = project["tool"]["setuptools"]["package-data"]["ai_trading_system"]

    assert "domains/publish/channels/weekly_pdf/templates/*.html" in package_data
    assert "domains/publish/channels/weekly_pdf/static/*.css" in package_data

    weekly_pdf_pkg = resources.files("ai_trading_system.domains.publish.channels.weekly_pdf")
    assert weekly_pdf_pkg.joinpath("templates", "weekly_report.html").is_file()
    assert weekly_pdf_pkg.joinpath("static", "report.css").is_file()


def test_weekly_pdf_renders_return_columns_as_percent_points() -> None:
    html = render_html(
        {
            "week_ending": "2026-04-29",
            "run_id": "pipeline-2026-04-29-abcdef12",
            "regime": {
                "trust_status": "trusted",
                "trust_confidence": None,
                "ml_status": None,
                "market_stage": "risk_on",
                "universe_count": 1,
                "stage2_count": 0,
                "sector_quadrant_counts": {},
            },
            "sectors": [],
            "top_ranked": [
                {
                    "symbol_id": "AAA",
                    "sector_name": "IT",
                    "composite_score": 91.0,
                    "rank_confidence": 1.0,
                    "stage2_label": "stage2",
                    "return_5": 6.5,
                    "return_20": 24.753149472250595,
                    "delivery_pct": 52.0,
                    "delivery_pct_imputed": False,
                }
            ],
            "volume_delivery": [],
            "weekly_price": [],
            "volume_shockers": [],
            "tier_a": [],
            "tier_b": [],
            "patterns": [],
            "prior_run_id": None,
            "prior_run_date": None,
            "rank_improvers": [],
            "rank_decliners": [],
            "sector_movers": [],
            "failed_breakouts": [],
            "breadth_latest": {},
            "breadth_rows": [],
            "charts": {},
        }
    )

    assert "24.8%" in html
    assert "2,475.3%" not in html


def test_weekly_pdf_renders_fundamental_sections() -> None:
    html = render_html(
        {
            "week_ending": "2026-05-07",
            "run_id": "pipeline-2026-05-07-abcdef12",
            "regime": {
                "trust_status": "trusted",
                "trust_confidence": None,
                "ml_status": None,
                "market_stage": "risk_on",
                "universe_count": 1,
                "stage2_count": 0,
                "sector_quadrant_counts": {},
            },
            "sectors": [],
            "top_ranked": [],
            "volume_delivery": [],
            "weekly_price": [],
            "volume_shockers": [],
            "tier_a": [],
            "tier_b": [],
            "patterns": [],
            "prior_run_id": None,
            "prior_run_date": None,
            "rank_improvers": [],
            "rank_decliners": [],
            "sector_movers": [],
            "failed_breakouts": [],
            "breadth_latest": {},
            "breadth_rows": [],
            "charts": {"valuation_cycle": None},
            "fundamental_universe": [{"pe_ttm": 24.1, "pe_200dma": 22.8, "pe_percentile_5y": 82, "valuation_zone": "expensive"}],
            "great_results": [{"symbol": "AAA", "insight_type": "great_result", "insight_score": 88}],
            "turnarounds": [{"symbol": "BBB", "insight_type": "turnaround_candidate", "insight_score": 81}],
            "compounders": [{"symbol": "CCC", "insight_type": "consistent_compounder", "insight_score": 78}],
            "sector_earnings": [{"sector_name": "IT", "sector_fundamental_score": 91, "great_result_count": 1, "turnaround_count": 0}],
            "valuation_cycle": [{"date": "2026-05-07", "entity_id": "UNIV_TOP500", "pe_ttm": 24.1, "pe_200dma": 22.8, "pe_percentile_5y": 82, "pe_zscore_5y": 1.2, "valuation_zone": "expensive"}],
        }
    )

    assert "Fundamental Insight Stories" in html
    assert "Valuation Cycle" in html
    assert "AAA" in html
    assert "Universe PE" in html


def test_weekly_move_metrics_capture_price_volume_delivery_and_shockers() -> None:
    no_return_5 = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "return_20": 24.75,
                "delivery_pct": 70.0,
                "composite_score": 90.0,
            }
        ]
    )
    assert metrics.volume_delivery_movers(no_return_5).empty

    with_return_5 = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "sector_name": "IT",
                "return_5": 6.5,
                "return_20": 24.75,
                "delivery_pct": 70.0,
                "volume_zscore_20": 2.5,
                "volume": 250,
                "vol_20_avg": 100,
                "composite_score": 90.0,
            },
            {
                "symbol_id": "BBB",
                "sector_name": "Bank",
                "return_5": 4.9,
                "return_20": 18.0,
                "delivery_pct": 75.0,
                "volume_zscore_20": 3.0,
                "composite_score": 95.0,
            },
        ]
    )

    movers = metrics.volume_delivery_movers(with_return_5)
    shockers = metrics.unusual_volume_shockers(with_return_5)
    price = metrics.weekly_price_movers(with_return_5, n=1)

    assert movers["symbol_id"].tolist() == ["AAA"]
    assert shockers["symbol_id"].tolist() == ["BBB", "AAA"]
    assert price["symbol_id"].tolist() == ["AAA"]


def test_find_prior_run_respects_tolerance_days(tmp_path: Path) -> None:
    _write_rank_attempt(tmp_path, "pipeline-2026-04-14-aaaabbbb")
    _write_rank_attempt(tmp_path, "pipeline-2026-04-21-ccccdddd")

    too_old = history.find_prior_run(
        tmp_path,
        current_run_id="pipeline-2026-05-02-eeeeffff",
        current_run_date=date(2026, 5, 2),
        target_days_back=7,
        tolerance_days=3,
    )
    assert too_old is None

    accepted = history.find_prior_run(
        tmp_path,
        current_run_id="pipeline-2026-04-28-eeeeffff",
        current_run_date=date(2026, 4, 28),
        target_days_back=7,
        tolerance_days=3,
    )
    assert accepted is not None
    assert accepted.run_id == "pipeline-2026-04-21-ccccdddd"


def test_rank_sector_movers_and_failed_breakouts() -> None:
    current = pd.DataFrame(
        [
            {"symbol_id": "AAA", "sector_name": "IT", "composite_score": 95.0, "close": 90.0},
            {"symbol_id": "BBB", "sector_name": "Bank", "composite_score": 80.0, "close": 210.0},
        ]
    )
    prior = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 70.0},
            {"symbol_id": "BBB", "composite_score": 99.0},
        ]
    )
    improvers, decliners = metrics.compute_rank_movers(current, prior, top_n=1)

    assert improvers.iloc[0]["symbol_id"] == "AAA"
    assert improvers.iloc[0]["rank_change"] == 1
    assert decliners.iloc[0]["symbol_id"] == "BBB"
    assert decliners.iloc[0]["rank_change"] == -1

    sectors = metrics.compute_sector_movers(
        pd.DataFrame(
            [
                {"Sector": "IT", "RS": 0.8, "RS_rank": 1, "Quadrant": "Leading"},
                {"Sector": "Bank", "RS": 0.4, "RS_rank": 2, "Quadrant": "Lagging"},
            ]
        ),
        pd.DataFrame(
            [
                {"Sector": "IT", "RS": 0.5, "RS_rank": 2},
                {"Sector": "Bank", "RS": 0.7, "RS_rank": 1},
            ]
        ),
    )
    assert sectors.iloc[0]["Sector"] == "IT"
    assert sectors.iloc[0]["rank_change"] == 1

    failed = metrics.detect_failed_breakouts(
        current_breakouts=pd.DataFrame(),
        prior_breakouts_per_run=[
            (
                "pipeline-2026-04-24-aaaabbbb",
                pd.DataFrame(
                    [
                        {
                            "symbol_id": "AAA",
                            "breakout_detected": True,
                            "prior_range_high": 100.0,
                            "candidate_tier": "A",
                        },
                    ]
                ),
            )
        ],
        current_ranked=current,
    )
    assert failed.iloc[0]["symbol_id"] == "AAA"
    assert failed.iloc[0]["drop_pct"] == -10.0


def test_stage2_cover_count_includes_strong_and_transition() -> None:
    ranked = pd.DataFrame(
        [
            {"symbol_id": "AAA", "stage2_label": "strong_stage2"},
            {"symbol_id": "BBB", "stage2_label": "stage1_to_stage2"},
            {"symbol_id": "CCC", "stage2_label": "Stage 2"},
            {"symbol_id": "DDD", "stage2_label": "base"},
        ]
    )

    summary = metrics.stage2_summary_for_report(ranked)
    empty_summary = metrics.stage2_summary_for_report(pd.DataFrame())

    assert summary["stage2_names"] == 3
    assert summary["strong_stage2"] == 1
    assert summary["transition_stage2"] == 1
    assert empty_summary["stage2_names"] == 0


def test_rank_movers_filter_by_sign() -> None:
    current = pd.DataFrame(
        [
            {"symbol_id": "AAA", "composite_score": 100.0},
            {"symbol_id": "BBB", "composite_score": 90.0},
            {"symbol_id": "CCC", "composite_score": 80.0},
        ]
    )
    prior = pd.DataFrame(
        [
            {"symbol_id": "CCC", "composite_score": 100.0},
            {"symbol_id": "BBB", "composite_score": 90.0},
            {"symbol_id": "AAA", "composite_score": 80.0},
        ]
    )

    improvers, decliners = metrics.compute_rank_movers(current, prior, top_n=10)

    assert improvers["symbol_id"].tolist() == ["AAA"]
    assert (improvers["rank_change"] > 0).all()
    assert decliners["symbol_id"].tolist() == ["CCC"]
    assert (decliners["rank_change"] < 0).all()


def test_signed_rank_format_never_outputs_plus_minus() -> None:
    assert metrics.fmt_signed_int(-1) == "-1"
    assert metrics.fmt_signed_int(1) == "+1"
    assert metrics.fmt_signed_int(0) == "0"
    assert "+-" not in metrics.fmt_signed_int(-1)


def test_best_patterns_by_symbol_collapses_duplicates() -> None:
    patterns = pd.DataFrame(
        [
            {"symbol_id": "ADANIGREEN", "pattern_family": "flag", "pattern_score": 70, "pattern_priority_score": 70},
            {"symbol_id": "ADANIGREEN", "pattern_family": "cup", "pattern_score": 92, "pattern_priority_score": 92},
            {"symbol_id": "BBB", "pattern_family": "base", "pattern_score": 80, "pattern_priority_score": 80},
        ]
    )

    result = metrics.best_patterns_by_symbol(patterns)
    adani = result[result["symbol_id"] == "ADANIGREEN"].iloc[0]

    assert result["symbol_id"].tolist().count("ADANIGREEN") == 1
    assert adani["pattern_family"] == "cup"
    assert adani["pattern_count"] == 2
    assert "flag" in adani["all_patterns"]
    assert "cup" in adani["all_patterns"]


def test_low_base_fundamentals_flagged() -> None:
    results = pd.DataFrame(
        [
            {"symbol": "SPIKE", "insight_score": 90, "profit_yoy_growth": 1200},
            {"symbol": "CLEAN", "insight_score": 80, "profit_yoy_growth": 25},
        ]
    )

    clean, caution = metrics.split_fundamental_results(results)

    assert caution["symbol"].tolist() == ["SPIKE"]
    assert clean["symbol"].tolist() == ["CLEAN"]
    assert "Profit growth" in caution.iloc[0]["quality_warning"]


def test_valuation_interpretation_high_pe_low_percentile() -> None:
    fair = metrics.valuation_cycle_interpretation(
        pd.DataFrame([{"pe_ttm": 59.3, "pe_percentile_5y": 32, "loss_mcap_pct": 0}])
    )
    extreme = metrics.valuation_cycle_interpretation(pd.DataFrame([{"pe_ttm": 200, "pe_percentile_5y": 32}]))
    lossy = metrics.valuation_cycle_interpretation(
        pd.DataFrame([{"pe_ttm": 40, "pe_percentile_5y": 32, "loss_mcap_pct": 30}])
    )

    assert fair["risk_label"] == "fair"
    assert "Absolute PE is high" in fair["detail"]
    assert extreme["risk_label"] == "unreliable"
    assert lossy["risk_label"] == "unreliable"


def test_fund_value_tech_overlap_from_watchlist_and_technical_only() -> None:
    watchlist = pd.DataFrame(
        [{"symbol": "AAA", "watchlist_bucket": "F4_ACTION_CANDIDATE", "quarterly_result_score": 80}]
    )
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "sector_name": "IT",
                "composite_score": 85,
                "stage2_label": "strong_stage2",
                "return_5": 4,
                "return_20": 12,
                "delivery_pct": 45,
            },
            {
                "symbol_id": "TECH",
                "sector_name": "IT",
                "composite_score": 90,
                "stage2_label": "stage2",
                "return_5": 3,
                "delivery_pct": 60,
            },
        ]
    )
    valuation = pd.DataFrame([{"symbol": "AAA", "valuation_history_score": 70, "valuation_history_bucket": "BELOW_OWN_MEDIAN"}])

    overlap = metrics.fund_value_tech_overlap(ranked=ranked, watchlist=watchlist, valuation=valuation)
    technical = metrics.fund_value_tech_overlap(ranked=ranked, watchlist=pd.DataFrame())

    assert overlap.iloc[0]["symbol"] == "AAA"
    assert overlap.iloc[0]["action"] == "ACTION_CANDIDATE"
    assert set(technical["action"]) == {"INFO_ONLY"}
    assert technical["technical_only"].all()


def test_executive_panel_fallback_and_avoid_rows() -> None:
    ranked = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "composite_score": 90,
                "stage2_label": "strong_stage2",
                "return_5": 2,
                "delivery_pct": 50,
            }
        ]
    )
    decliners = pd.DataFrame(
        [{"symbol_id": "BAD", "rank_change": -3, "return_5": -4, "composite_score": 60, "stage2_label": "weak"}]
    )

    panel = metrics.build_executive_decision_panel(
        ranked=ranked,
        rank_decliners=decliners,
        breadth_latest={"pct_above_sma20": 70, "pct_above_sma50": 70, "pct_above_sma200": 55},
    )

    assert panel["top_actionable"][0]["symbol"] == "AAA"
    assert panel["avoid_or_reduce"][0]["symbol"] == "BAD"
    assert panel["risk_label"] == "RISK_ON"


def test_candidate_tracker_view_and_sector_split() -> None:
    tracker = pd.DataFrame(
        [
            {"symbol": "GOOD", "current_status": "STRONG_IMPROVING", "tracking_health_score": 90},
            {"symbol": "BAD", "current_status": "DETERIORATING", "tracking_health_score": 20},
        ]
    )
    sectors = pd.DataFrame(
        [
            {"Sector": "Pharma", "Quadrant": "Leading", "RS_rank": 1, "RS": 0.9},
            {"Sector": "Aerospace", "Quadrant": "Weakening", "RS_rank": 3, "RS": 0.8},
        ]
    )

    tracker_view = metrics.candidate_tracker_weekly_view(tracker)
    sector_view = metrics.split_sector_leadership(sectors)

    assert tracker_view["strong_improving"]["symbol"].tolist() == ["GOOD"]
    assert tracker_view["deteriorating"]["symbol"].tolist() == ["BAD"]
    assert sector_view["fresh_leaders"]["Sector"].tolist() == ["Pharma"]
    assert sector_view["weakening_leaders"]["Sector"].tolist() == ["Aerospace"]


def test_candidate_tracker_view_accepts_real_status_column() -> None:
    tracker = pd.DataFrame(
        [
            {"symbol": "GOOD", "status": "IMPROVING", "tracking_health_score": 80},
            {"symbol": "BAD", "status": "DETERIORATING", "tracking_health_score": 20},
        ]
    )

    tracker_view = metrics.candidate_tracker_weekly_view(tracker)

    assert tracker_view["strong_improving"]["current_status"].tolist() == ["IMPROVING"]
    assert tracker_view["deteriorating"]["current_status"].tolist() == ["DETERIORATING"]


def test_sector_rotation_weekly_helpers_split_report_sections() -> None:
    sector_rotation = pd.DataFrame(
        [
            {"date": "2026-04-30", "industry": "Banks", "quadrant": "Leading", "rs_ratio": 104, "rs_momentum": 103, "alpha_20d": 0.05, "alpha_60d": 0.08, "outperformance_bucket": "Significant Outperformance"},
            {"date": "2026-04-30", "industry": "Auto", "quadrant": "Improving", "rs_ratio": 98, "rs_momentum": 102, "alpha_20d": 0.02, "alpha_60d": 0.03, "outperformance_bucket": "Minor Outperformance"},
        ]
    )
    stock_rotation = pd.DataFrame(
        [
            {"symbol": "LEAD", "industry": "Banks", "quadrant": "Leading", "rotation_adjusted_score": 90, "delivery_signal": "Accumulation"},
            {"symbol": "IMPR", "industry": "Auto", "quadrant": "Improving", "rotation_adjusted_score": 85, "delivery_signal": "Neutral"},
            {"symbol": "LAG", "industry": "IT", "quadrant": "Lagging", "rotation_adjusted_score": 50, "delivery_signal": "Neutral"},
            {"symbol": "WEAK", "industry": "Pharma", "quadrant": "Weakening", "rotation_adjusted_score": 45, "delivery_signal": "Distribution"},
        ]
    )
    accumulation = pd.DataFrame(
        [
            {"symbol": "LEAD", "delivery_signal": "Accumulation", "accumulation_score": 80, "delivery_pct_z20": 2.0},
            {"symbol": "WEAK", "delivery_signal": "Distribution", "accumulation_score": 72, "delivery_pct_z20": 1.5},
        ]
    )
    custom_indices = pd.DataFrame(
        [{"date": "2026-04-30", "industry": "Banks", "sector_index": 110, "weighting_method": "market_cap", "constituent_count": 12}]
    )

    info = metrics.sector_rotation_information(sector_rotation)
    stocks = metrics.split_stock_rotation(stock_rotation)
    delivery = metrics.accumulation_distribution_tables(accumulation)
    indices = metrics.custom_indices_summary(custom_indices, sector_rotation)

    assert info["industry"].tolist() == ["Banks", "Auto"]
    assert stocks["leading"]["symbol"].tolist() == ["LEAD"]
    assert stocks["improving"]["symbol"].tolist() == ["IMPR"]
    assert stocks["lagging"]["symbol"].tolist() == ["LAG"]
    assert stocks["weakening"]["symbol"].tolist() == ["WEAK"]
    assert delivery["accumulation"]["symbol"].tolist() == ["LEAD"]
    assert delivery["distribution"]["symbol"].tolist() == ["WEAK"]
    assert indices.iloc[0]["weighting_method"] == "market_cap"
    assert indices.iloc[0]["constituent_count"] == 12


def test_weekly_data_loader_accepts_sector_rotation_artifacts(tmp_path: Path) -> None:
    rank_dir = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-30-abcdef12" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    artifacts: dict[str, StageArtifact] = {}
    for artifact_type, body in {
        "pattern_scan": "symbol_id,pattern_family\nAAA,flag\n",
        "sector_rotation": "industry,quadrant,rs_ratio\nBanks,Leading,104\n",
        "stock_rotation": "symbol,quadrant,rotation_adjusted_score\nAAA,Leading,82\n",
        "accumulation_distribution": "symbol,delivery_signal,accumulation_score\nAAA,Accumulation,78\n",
        "sector_custom_indices": "date,industry,sector_index,weighting_method,constituent_count\n2026-04-30,Banks,110,market_cap,12\n",
    }.items():
        path = rank_dir / f"{artifact_type}.csv"
        path.write_text(body, encoding="utf-8")
        artifacts[artifact_type] = StageArtifact.from_file(artifact_type, path, row_count=1)
    summary_path = rank_dir / "rank_summary.json"
    summary_path.write_text(json.dumps({"data_trust_status": "trusted"}), encoding="utf-8")
    payload_path = rank_dir / "sector_rotation_payload.json"
    payload_path.write_text(json.dumps({"benchmark_name": "UNIV_TOP1000"}), encoding="utf-8")
    artifacts["rank_summary"] = StageArtifact.from_file("rank_summary", summary_path, row_count=1)
    artifacts["sector_rotation_payload"] = StageArtifact.from_file("sector_rotation_payload", payload_path, row_count=1)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-30-abcdef12",
        run_date="2026-04-30",
        stage_name="publish",
        attempt_number=1,
        params={"data_domain": "operational"},
        artifacts={"rank": artifacts},
    )

    data = load_report_data(context, {"ranked_signals": pd.DataFrame(), "breakout_scan": pd.DataFrame(), "sector_dashboard": pd.DataFrame()})

    assert data.sector_rotation["industry"].tolist() == ["Banks"]
    assert data.stock_rotation["symbol"].tolist() == ["AAA"]
    assert data.accumulation_distribution["delivery_signal"].tolist() == ["Accumulation"]
    assert data.sector_custom_indices["weighting_method"].tolist() == ["market_cap"]
    assert data.sector_rotation_payload["benchmark_name"] == "UNIV_TOP1000"


def test_candle_targets_dedupe_include_pattern_and_cap() -> None:
    ranked = pd.DataFrame([{"symbol_id": "AAA", "composite_score": 99}, {"symbol_id": "BBB", "composite_score": 98}])
    improvers = pd.DataFrame([{"symbol_id": "AAA"}, {"symbol_id": "CCC"}])
    patterns = pd.DataFrame([{"symbol_id": "PATTERN", "breakout_level": 123}])
    overlap = pd.DataFrame([{"symbol": "FVT"}])
    tracker = pd.DataFrame([{"symbol": "BAD", "current_status": "DETERIORATING"}])

    targets = charts.pick_candle_targets(
        ranked,
        improvers,
        pd.DataFrame(),
        patterns_best=patterns,
        fund_value_tech_overlap=overlap,
        candidate_tracker_current=tracker,
        n_each=3,
        cap=5,
    )
    symbols = [row["symbol_id"] for row in targets]

    assert len(symbols) == len(set(symbols))
    assert "PATTERN" in symbols
    assert len(symbols) == 5


def test_candle_targets_accept_status_alias_and_skip_invalid_symbols() -> None:
    ranked = pd.DataFrame([{"symbol_id": None, "composite_score": 99}, {"symbol_id": "nan", "composite_score": 98}])
    tracker = pd.DataFrame([{"symbol": "BAD", "status": "DETERIORATING"}, {"symbol": None, "status": "DETERIORATING"}])

    targets = charts.pick_candle_targets(
        ranked,
        pd.DataFrame([{"symbol_id": pd.NA}]),
        pd.DataFrame([{"candidate_tier": "A", "symbol_id": float("nan")}]),
        candidate_tracker_current=tracker,
        n_each=3,
        cap=5,
    )

    assert targets == [{"symbol_id": "BAD", "breakout_level": None, "source": "tracker_deteriorating"}]


def test_safe_date_parses_iso_string_for_stock_charts() -> None:
    assert _safe_date("2026-04-29") == date(2026, 4, 29)
    assert _safe_date("bad-date") is None


def test_market_breadth_date_normalization_has_no_chained_assignment_warning(tmp_path: Path) -> None:
    import duckdb

    db_path = tmp_path / "ohlcv.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        "CREATE TABLE _catalog(timestamp TIMESTAMP, symbol_id VARCHAR, close DOUBLE, exchange VARCHAR)"
    )
    start = date(2025, 1, 1)
    rows = []
    for idx in range(260):
        trade_date = start + timedelta(days=idx)
        rows.append((trade_date.isoformat(), "AAA", 100.0 + idx * 0.1, "NSE"))
        rows.append((trade_date.isoformat(), "BBB", 100.0 - idx * 0.05, "NSE"))
    con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?)", rows)
    con.close()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        result = breadth.compute_market_breadth(db_path, start + timedelta(days=259), weeks=4)

    assert not result.empty
    assert not any("ChainedAssignmentError" in str(w.message) for w in captured)


def test_weekly_market_breadth_uses_adjusted_close_and_252_eligibility(tmp_path: Path) -> None:
    import duckdb

    db_path = tmp_path / "ohlcv.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE _catalog(
            timestamp TIMESTAMP,
            symbol_id VARCHAR,
            close DOUBLE,
            adjusted_close DOUBLE,
            exchange VARCHAR
        )
        """
    )
    start = date(2025, 1, 1)
    rows = []
    for idx in range(260):
        trade_date = start + timedelta(days=idx)
        adjusted = 100.0 + idx
        raw_aaa = 10.0 if idx == 259 else adjusted
        rows.append((trade_date.isoformat(), "AAA", raw_aaa, adjusted, "NSE"))
        rows.append((trade_date.isoformat(), "BBB", adjusted, adjusted, "NSE"))
        if idx >= 250:
            rows.append((trade_date.isoformat(), "FRESH", 1000.0 + idx, 1000.0 + idx, "NSE"))
    con.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    con.close()

    result = breadth.compute_market_breadth(db_path, start + timedelta(days=259), weeks=2)
    latest = result.iloc[-1]

    assert latest["pct_above_sma200"] == 100.0
    assert latest["new_52w_highs"] == 2
    assert latest["symbols_252"] == 2
    assert latest["advancers"] == 3
    assert latest["decliners"] == 0
    assert latest["ad_pct"] == 1.0
    assert "ad_pct_sum63" in result.columns


def test_build_report_writes_html_manifest_and_tables(tmp_path: Path) -> None:
    rank_dir = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-29-abcdef12" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pattern_path = rank_dir / "pattern_scan.csv"
    summary_path = rank_dir / "rank_summary.json"
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "pattern_family": "flag",
                "pattern_score": 91.0,
                "pattern_state": "confirmed",
            }
        ]
    ).to_csv(pattern_path, index=False)
    summary_path.write_text(json.dumps({"data_trust_status": "trusted"}), encoding="utf-8")

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-29-abcdef12",
        run_date="2026-04-29",
        stage_name="publish",
        attempt_number=1,
        params={"data_domain": "operational"},
        artifacts={
            "rank": {
                "pattern_scan": StageArtifact.from_file("pattern_scan", pattern_path, row_count=1),
                "rank_summary": StageArtifact.from_file("rank_summary", summary_path, row_count=1),
            }
        },
    )
    datasets = {
        "ranked_signals": pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "sector_name": "IT",
                    "composite_score": 91.0,
                    "rank_confidence": 1.0,
                    "return_5": 6.5,
                    "return_20": 24.75,
                    "delivery_pct": 52.0,
                    "volume_zscore_20": 2.5,
                    "volume": 250,
                    "vol_20_avg": 100,
                    "delivery_pct_imputed": False,
                    "stage2_label": "stage2",
                }
            ]
        ),
        "breakout_scan": pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "candidate_tier": "A",
                    "breakout_score": 88.0,
                    "prior_range_high": 100.0,
                    "close": 105.0,
                }
            ]
        ),
        "sector_dashboard": pd.DataFrame(
            [{"Sector": "IT", "RS": 0.8, "RS_20": 0.7, "RS_50": 0.6, "Momentum": 0.2, "RS_rank": 1}]
        ),
        "sector_rotation": pd.DataFrame(
            [
                {"date": "2026-04-29", "industry": "Banks", "quadrant": "Leading", "rs_ratio": 104.0, "rs_momentum": 103.0, "alpha_20d": 0.05, "alpha_60d": 0.08, "outperformance_bucket": "Significant Outperformance"},
                {"date": "2026-04-29", "industry": "Auto", "quadrant": "Improving", "rs_ratio": 98.0, "rs_momentum": 102.0, "alpha_20d": 0.02, "alpha_60d": 0.03, "outperformance_bucket": "Minor Outperformance"},
            ]
        ),
        "stock_rotation": pd.DataFrame(
            [
                {"symbol": "AAA", "industry": "Banks", "quadrant": "Leading", "market_cap": 1000.0, "return_1w": 0.04, "return_1m": 0.12, "rotation_adjusted_score": 82.0, "delivery_signal": "Accumulation"},
                {"symbol": "BBB", "industry": "Auto", "quadrant": "Improving", "market_cap": 500.0, "return_1w": 0.03, "return_1m": 0.08, "rotation_adjusted_score": 78.0, "delivery_signal": "Neutral"},
            ]
        ),
        "accumulation_distribution": pd.DataFrame(
            [
                {"symbol": "AAA", "close": 105.0, "delivery_pct": 55.0, "delivery_pct_z20": 1.8, "volume_z20": 1.2, "price_return_5d": 0.04, "delivery_signal": "Accumulation", "accumulation_score": 78.0},
                {"symbol": "CCC", "close": 90.0, "delivery_pct": 60.0, "delivery_pct_z20": 1.4, "volume_z20": 0.8, "price_return_5d": -0.03, "delivery_signal": "Distribution", "accumulation_score": 70.0},
            ]
        ),
        "sector_custom_indices": pd.DataFrame(
            [{"date": "2026-04-29", "industry": "Banks", "sector_index": 110.0, "weighting_method": "market_cap", "constituent_count": 12}]
        ),
        "stock_scan": pd.DataFrame(),
        "dashboard_payload": {"summary": {"market_stage": "risk_on"}},
    }

    manifest = build_report(context, datasets, tmp_path / "report")

    assert Path(manifest["html_path"]).exists()
    assert Path(manifest["json_path"]).exists()
    assert Path(manifest["tables"]["weekly_ranked_top"]).exists()
    assert Path(manifest["tables"]["weekly_volume_delivery_movers"]).exists()
    assert Path(manifest["tables"]["weekly_price_movers"]).exists()
    assert Path(manifest["tables"]["weekly_unusual_volume_shockers"]).exists()
    assert Path(manifest["tables"]["weekly_patterns_best_by_symbol"]).exists()
    assert Path(manifest["tables"]["weekly_sector_rotation_summary"]).exists()
    assert Path(manifest["tables"]["weekly_stock_rotation_leading"]).exists()
    assert Path(manifest["tables"]["weekly_accumulation"]).exists()
    assert Path(manifest["tables"]["weekly_custom_indices"]).exists()
    assert "stage2_report_summary" in manifest
    assert "executive_panel" in manifest
    assert "empty_sections" in manifest
    assert "valuation_interpretation" in manifest
    assert manifest["counts"]["top_ranked"] == 1
    assert manifest["counts"]["volume_delivery"] == 1
    assert manifest["counts"]["weekly_price"] == 1
    assert manifest["counts"]["volume_shockers"] == 1
    assert manifest["counts"]["sector_rotation"] == 2
    assert manifest["counts"]["stock_rotation"] == 2
    assert manifest["counts"]["accumulation"] == 1
    assert manifest["counts"]["distribution"] == 1
    assert manifest["counts"]["custom_indices"] == 1
    html = Path(manifest["html_path"]).read_text(encoding="utf-8")
    assert "24.8%" in html
    assert "1. Executive Decision Panel" in html
    assert "3A. Sector Rotation" in html
    assert "Stock Rotation" in html
    assert "Accumulation vs Distribution" in html
    assert "Delivery Data Trends" in html
    assert "Custom Indices" in html
    assert "Significant Outperformance" in html
    assert "market_cap" in html
    assert "Market Moves Snapshot" in html
    assert 'class="pattern-table"' in html
    assert "Watchlist intersection is deferred to Phase 4" not in html


def test_empty_sections_added_to_manifest_and_html(tmp_path: Path) -> None:
    rank_dir = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-29-abcdef12" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pattern_path = rank_dir / "pattern_scan.csv"
    summary_path = rank_dir / "rank_summary.json"
    pd.DataFrame([{"symbol_id": "AAA", "pattern_family": "flag", "pattern_score": 91.0}]).to_csv(pattern_path, index=False)
    summary_path.write_text(json.dumps({"data_trust_status": "trusted"}), encoding="utf-8")

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-29-abcdef12",
        run_date="2026-04-29",
        stage_name="publish",
        attempt_number=1,
        params={"data_domain": "operational"},
        artifacts={
            "rank": {
                "pattern_scan": StageArtifact.from_file("pattern_scan", pattern_path, row_count=1),
                "rank_summary": StageArtifact.from_file("rank_summary", summary_path, row_count=1),
            }
        },
    )
    datasets = {
        "ranked_signals": pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "composite_score": 70.0,
                    "return_5": 1.0,
                    "delivery_pct": 10.0,
                    "stage2_label": "base",
                }
            ]
        ),
        "breakout_scan": pd.DataFrame(),
        "sector_dashboard": pd.DataFrame(),
        "stock_scan": pd.DataFrame(),
        "dashboard_payload": {},
    }

    manifest = build_report(context, datasets, tmp_path / "report")
    html = Path(manifest["html_path"]).read_text(encoding="utf-8")

    assert manifest["empty_sections"]["weekly_volume_delivery_movers"].startswith("No stock met return_5")
    assert "No stock met return_5 &gt;= 5%, delivery &gt;= 40%, and volume expansion rule." in html


def test_rendered_html_section_numbering_and_phase4_note_removed() -> None:
    html = render_html(
        {
            "week_ending": "2026-05-07",
            "run_date": "2026-05-07",
            "run_id": "pipeline-2026-05-07-abcdef12",
            "regime": {
                "trust_status": "trusted",
                "trust_confidence": None,
                "ml_status": None,
                "market_stage": "risk_on",
                "universe_count": 1,
                "stage2_count": 0,
                "sector_quadrant_counts": {},
            },
            "stage2_report_summary": {"stage2_names": 2, "strong_stage2": 1, "transition_stage2": 1},
            "executive_panel": {
                "risk_label": "UNKNOWN",
                "market_message": "Breadth unavailable.",
                "top_actionable": [],
                "track_next": [],
                "avoid_or_reduce": [],
            },
            "sectors": [],
            "sector_groups": {"fresh_leaders": [], "improving_sectors": [], "weakening_leaders": []},
            "top_ranked": [],
            "volume_delivery": [],
            "weekly_price": [],
            "volume_shockers": [],
            "tier_a": [],
            "tier_b": [],
            "patterns": [],
            "fund_value_tech_overlap": [],
            "prior_run_id": None,
            "prior_run_date": None,
            "rank_improvers": [],
            "rank_decliners": [],
            "sector_movers": [],
            "failed_breakouts": [],
            "breadth_latest": {},
            "breadth_rows": [],
            "events_of_week": {},
            "charts": {},
            "clean_great_results": [],
            "low_base_results": [],
            "turnarounds": [],
            "compounders": [],
            "sector_earnings": [],
            "valuation_cycle": [],
            "fundamental_universe": [],
            "valuation_interpretation": {"risk_label": "unknown", "headline": "Valuation data unavailable.", "detail": ""},
            "empty_sections": {},
            "candidate_tracker_enabled": False,
        }
    )

    headings = [line.strip() for line in html.splitlines() if line.strip().startswith("<h2>")]
    expected = [f"<h2>{idx}." for idx in range(1, 16)]

    assert all(any(heading.startswith(prefix) for heading in headings) for prefix in expected)
    assert sum(1 for heading in headings if heading.startswith("<h2>7.")) == 1
    assert "Watchlist intersection is deferred to Phase 4" not in html


def _write_rank_attempt(root: Path, run_id: str) -> None:
    rank_dir = root / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pd.DataFrame([{"symbol_id": "AAA", "composite_score": 1.0}]).to_csv(
        rank_dir / "ranked_signals.csv", index=False
    )
