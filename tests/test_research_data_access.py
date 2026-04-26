from __future__ import annotations

from pathlib import Path

from ai_trading_system.interfaces.streamlit.research.data_access import load_drilldown_history_for_symbols, load_sector_history_for_sectors


def test_load_sector_history_for_sectors_reads_recent_sector_dashboards(tmp_path: Path) -> None:
    runs_dir = tmp_path / "data" / "pipeline_runs"
    run_a = runs_dir / "pipeline-2026-04-01-aaaa1111" / "rank" / "attempt_1"
    run_b = runs_dir / "pipeline-2026-04-02-bbbb2222" / "rank" / "attempt_1"
    run_a.mkdir(parents=True, exist_ok=True)
    run_b.mkdir(parents=True, exist_ok=True)

    (run_a / "sector_dashboard.csv").write_text(
        "Sector,RS,Momentum,RS_rank\nIndustrial,0.41,0.02,3\nBanks,0.55,0.04,1\n",
        encoding="utf-8",
    )
    (run_b / "sector_dashboard.csv").write_text(
        "Sector,RS,Momentum,RS_rank\nIndustrial,0.49,0.05,2\nBanks,0.53,0.01,1\n",
        encoding="utf-8",
    )

    history = load_sector_history_for_sectors(str(runs_dir), ["Industrial"], max_runs=10)

    assert len(history) == 2
    assert history["sector_name"].tolist() == ["Industrial", "Industrial"]
    assert history["rs_value"].round(2).tolist() == [0.41, 0.49]
    assert history["rank_position"].astype(int).tolist() == [3, 2]


def test_load_drilldown_history_for_symbols_aggregates_symbol_basket(tmp_path: Path) -> None:
    runs_dir = tmp_path / "data" / "pipeline_runs"
    run_a = runs_dir / "pipeline-2026-04-01-aaaa1111" / "rank" / "attempt_1"
    run_b = runs_dir / "pipeline-2026-04-02-bbbb2222" / "rank" / "attempt_1"
    run_a.mkdir(parents=True, exist_ok=True)
    run_b.mkdir(parents=True, exist_ok=True)

    (run_a / "ranked_signals.csv").write_text(
        "symbol_id,composite_score\nNAM-INDIA,70\nICICIAMC,68\nICRA,60\nOTHER,40\n",
        encoding="utf-8",
    )
    (run_b / "ranked_signals.csv").write_text(
        "symbol_id,composite_score\nICICIAMC,75\nNAM-INDIA,73\nICRA,64\nOTHER,30\n",
        encoding="utf-8",
    )

    history = load_drilldown_history_for_symbols(
        str(runs_dir),
        ["NAM-INDIA", "ICICIAMC", "ICRA"],
        max_runs=10,
    )

    assert len(history) == 2
    assert history["symbol_count"].tolist() == [3, 3]
    assert history["top_score"].tolist() == [70.0, 75.0]
    assert history["best_rank"].tolist() == [1.0, 1.0]
