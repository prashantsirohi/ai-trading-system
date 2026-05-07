from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


class _FakeDeliveryManager:
    def deliver(self, context, channel, artifact, sender):
        payload = sender() or {}
        return {
            "channel": channel,
            "status": "completed",
            **(payload if isinstance(payload, dict) else {}),
        }


def _publish_context(tmp_path: Path, *, with_fundamentals: bool) -> StageContext:
    rank_dir = tmp_path / "data" / "pipeline_runs" / "run-pub" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    ranked_path = rank_dir / "ranked_signals.csv"
    pd.DataFrame([{"symbol_id": "AAA", "composite_score": 90}]).to_csv(ranked_path, index=False)
    artifacts = {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", ranked_path, row_count=1),
        }
    }
    if with_fundamentals:
        fund_dir = tmp_path / "data" / "pipeline_runs" / "run-pub" / "fundamentals" / "attempt_1"
        fund_dir.mkdir(parents=True, exist_ok=True)
        watchlist_path = fund_dir / "watchlist_candidates.csv"
        summary_path = fund_dir / "fundamental_summary.json"
        pd.DataFrame(
            [{"symbol": "AAA", "watchlist_bucket": "ADD_TO_WATCHLIST", "final_watchlist_score": 82}]
        ).to_csv(watchlist_path, index=False)
        summary_path.write_text('{"status": "completed"}', encoding="utf-8")
        artifacts["fundamentals"] = {
            "watchlist_candidates": StageArtifact.from_file("watchlist_candidates", watchlist_path, row_count=1),
            "fundamental_summary": StageArtifact.from_file("fundamental_summary", summary_path, row_count=1),
        }
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="run-pub",
        run_date="2026-05-07",
        stage_name="publish",
        attempt_number=1,
        params={"local_publish": True},
        artifacts=artifacts,
    )


def test_publish_succeeds_without_fundamentals_artifact(tmp_path: Path) -> None:
    metadata = PublishStage(delivery_manager=_FakeDeliveryManager())._run_default(
        _publish_context(tmp_path, with_fundamentals=False)
    )

    assert metadata["targets"][0]["channel"] == "local_summary"
    assert "fundamentals_top_add_to_watchlist" not in metadata


def test_publish_includes_fundamentals_watchlist_when_present(tmp_path: Path) -> None:
    metadata = PublishStage(delivery_manager=_FakeDeliveryManager())._run_default(
        _publish_context(tmp_path, with_fundamentals=True)
    )

    assert metadata["fundamentals_top_add_to_watchlist"] == ["AAA"]
    assert metadata["fundamental_summary_uri"].endswith("fundamental_summary.json")
