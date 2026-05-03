from __future__ import annotations

import json
import tomllib
import warnings
from datetime import date
from datetime import timedelta
from importlib import resources
from pathlib import Path

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import breadth, history, metrics
from ai_trading_system.domains.publish.channels.weekly_pdf.builder import build_report
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
    assert manifest["counts"]["top_ranked"] == 1
    assert manifest["counts"]["volume_delivery"] == 1
    assert manifest["counts"]["weekly_price"] == 1
    assert manifest["counts"]["volume_shockers"] == 1
    assert "24.8%" in Path(manifest["html_path"]).read_text(encoding="utf-8")
    assert "Market Moves Snapshot" in Path(manifest["html_path"]).read_text(encoding="utf-8")
    assert 'class="pattern-table"' in Path(manifest["html_path"]).read_text(encoding="utf-8")


def _write_rank_attempt(root: Path, run_id: str) -> None:
    rank_dir = root / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True)
    pd.DataFrame([{"symbol_id": "AAA", "composite_score": 1.0}]).to_csv(
        rank_dir / "ranked_signals.csv", index=False
    )
