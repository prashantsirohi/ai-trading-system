from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


def test_publish_stage_loads_investigator_datasets(tmp_path: Path) -> None:
    run_id = "pipeline-2026-05-07-pub"
    attempt_dir = tmp_path / "data" / "pipeline_runs" / run_id / "investigator" / "attempt_1"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    scores_path = attempt_dir / "investigator_scores.csv"
    repeat_path = attempt_dir / "repeat_tracker.csv"
    trap_path = attempt_dir / "trap_log.csv"
    active_path = attempt_dir / "active_watchlist.csv"
    archive_path = attempt_dir / "archived_investigator.csv"
    gate_path = attempt_dir / "final_3q_gate.csv"
    gainers_path = attempt_dir / "daily_gainer_log.csv"
    summary_path = attempt_dir / "investigator_summary.json"
    pd.DataFrame([{"symbol_id": "AAA", "verdict": "HIGH_CONVICTION", "final_score": 88}]).to_csv(scores_path, index=False)
    pd.DataFrame([{"symbol_id": "AAA", "repeat_score": 64}]).to_csv(repeat_path, index=False)
    pd.DataFrame([{"symbol_id": "TRAP", "verdict": "NOISE_TRAP"}]).to_csv(trap_path, index=False)
    pd.DataFrame([{"symbol_id": "AAA", "status": "HIGH_CONVICTION"}]).to_csv(active_path, index=False)
    pd.DataFrame([{"symbol_id": "XYZ", "drop_reason": "ONE_CANDLE_DRAMA"}]).to_csv(archive_path, index=False)
    pd.DataFrame([{"symbol_id": "AAA", "gate_status": "PENDING"}]).to_csv(gate_path, index=False)
    pd.DataFrame([{"symbol_id": "AAA", "daily_return_pct": 8.0}]).to_csv(gainers_path, index=False)
    summary_path.write_text(json.dumps({"active_count": 1}), encoding="utf-8")
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=run_id,
        run_date="2026-05-07",
        stage_name="publish",
        attempt_number=1,
        artifacts={
            "investigator": {
                "investigator_scores": StageArtifact.from_file("investigator_scores", scores_path, row_count=1),
                "repeat_tracker": StageArtifact.from_file("repeat_tracker", repeat_path, row_count=1),
                "trap_log": StageArtifact.from_file("trap_log", trap_path, row_count=1),
                "active_watchlist": StageArtifact.from_file("active_watchlist", active_path, row_count=1),
                "archived_investigator": StageArtifact.from_file("archived_investigator", archive_path, row_count=1),
                "final_3q_gate": StageArtifact.from_file("final_3q_gate", gate_path, row_count=1),
                "daily_gainer_log": StageArtifact.from_file("daily_gainer_log", gainers_path, row_count=1),
                "investigator_summary": StageArtifact.from_file("investigator_summary", summary_path, row_count=1),
            }
        },
    )
    datasets: dict[str, object] = {}

    PublishStage()._attach_investigator_datasets(context, datasets)  # noqa: SLF001

    assert datasets["investigator_summary"] == {"active_count": 1}
    assert datasets["investigator_scores"].iloc[0]["symbol_id"] == "AAA"
    assert datasets["investigator_high_conviction"].iloc[0]["symbol_id"] == "AAA"
    assert datasets["investigator_archive"].iloc[0]["drop_reason"] == "ONE_CANDLE_DRAMA"
