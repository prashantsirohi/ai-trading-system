from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage, _has_fundamental_tracking_watchlist


class _FakeDeliveryManager:
    def deliver(self, context, channel, artifact, sender):
        try:
            payload = sender() or {}
        except Exception as exc:  # noqa: BLE001
            return {
                "channel": channel,
                "status": "failed",
                "error_message": str(exc),
            }
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
            [
                {
                    "symbol": "AAA",
                    "watchlist_bucket": "F1_FUNDAMENTAL_WATCH",
                    "final_watchlist_score": 82,
                    "quarterly_result_score": 88,
                    "valuation_history_score": 50,
                }
            ]
        ).to_csv(watchlist_path, index=False)
        summary_path.write_text('{"status": "completed"}', encoding="utf-8")
        great_path = fund_dir / "great_results.csv"
        great_latest_path = fund_dir / "great_results_latest.csv"
        turn_path = fund_dir / "turnaround_candidates.csv"
        turn_latest_path = fund_dir / "turnaround_candidates_latest.csv"
        comp_path = fund_dir / "compounder_candidates.csv"
        comp_latest_path = fund_dir / "compounder_candidates_latest.csv"
        sector_path = fund_dir / "sector_earnings_leadership.csv"
        sector_latest_path = fund_dir / "sector_earnings_latest.csv"
        universe_path = fund_dir / "universe_valuation_daily.csv"
        universe_latest_path = fund_dir / "universe_valuation_latest.csv"
        payload_path = fund_dir / "fundamental_dashboard_payload.json"
        pd.DataFrame([{"symbol": "AAA", "insight_score": 88}, {"symbol": "AAA", "insight_score": 70}]).to_csv(great_path, index=False)
        pd.DataFrame([{"symbol": "AAA", "insight_score": 88}]).to_csv(great_latest_path, index=False)
        pd.DataFrame([{"symbol": "BBB", "insight_score": 81}, {"symbol": "BBB", "insight_score": 70}]).to_csv(turn_path, index=False)
        pd.DataFrame([{"symbol": "BBB", "insight_score": 81}]).to_csv(turn_latest_path, index=False)
        pd.DataFrame([{"symbol": "CCC", "insight_score": 78}, {"symbol": "CCC", "insight_score": 70}]).to_csv(comp_path, index=False)
        pd.DataFrame([{"symbol": "CCC", "insight_score": 78}]).to_csv(comp_latest_path, index=False)
        sector_frame = pd.DataFrame([{"sector_name": "Capital Goods", "report_date": "2026-05-07", "sector_fundamental_score": 92}])
        sector_frame.to_csv(
            sector_path, index=False
        )
        sector_frame.to_csv(sector_latest_path, index=False)
        universe_frame = pd.DataFrame([{"universe_id": "UNIV_TOP500", "date": "2026-05-07", "pe_ttm": 24.1, "valuation_zone": "expensive"}])
        universe_frame.to_csv(
            universe_path, index=False
        )
        universe_frame.to_csv(universe_latest_path, index=False)
        payload_path.write_text(
            '{"run_date":"2026-05-07","summary":{"great_results_count":1},"universe":{"pe_ttm":24.1,"valuation_zone":"expensive"},"top_great_results":[{"symbol":"AAA"}],"top_turnarounds":[],"top_compounders":[],"sector_earnings_leadership":[],"valuation_chart":[]}',
            encoding="utf-8",
        )
        artifacts["fundamentals"] = {
            "watchlist_candidates": StageArtifact.from_file("watchlist_candidates", watchlist_path, row_count=1),
            "fundamental_summary": StageArtifact.from_file("fundamental_summary", summary_path, row_count=1),
            "great_results": StageArtifact.from_file("great_results", great_path, row_count=1),
            "great_results_latest": StageArtifact.from_file("great_results_latest", great_latest_path, row_count=1),
            "turnaround_candidates": StageArtifact.from_file("turnaround_candidates", turn_path, row_count=1),
            "turnaround_candidates_latest": StageArtifact.from_file("turnaround_candidates_latest", turn_latest_path, row_count=1),
            "compounder_candidates": StageArtifact.from_file("compounder_candidates", comp_path, row_count=1),
            "compounder_candidates_latest": StageArtifact.from_file("compounder_candidates_latest", comp_latest_path, row_count=1),
            "sector_earnings_leadership": StageArtifact.from_file("sector_earnings_leadership", sector_path, row_count=1),
            "sector_earnings_latest": StageArtifact.from_file("sector_earnings_latest", sector_latest_path, row_count=1),
            "universe_valuation_daily": StageArtifact.from_file("universe_valuation_daily", universe_path, row_count=1),
            "universe_valuation_latest": StageArtifact.from_file("universe_valuation_latest", universe_latest_path, row_count=1),
            "fundamental_dashboard_payload": StageArtifact.from_file("fundamental_dashboard_payload", payload_path, row_count=1),
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

    assert metadata["fundamentals_top_add_to_watchlist"] == []
    assert metadata["fundamentals_top_tracking_watchlist"] == ["AAA"]
    assert metadata["fundamental_summary_uri"].endswith("fundamental_summary.json")
    assert metadata["fundamentals"]["great_results_count"] == 1
    assert metadata["fundamentals"]["turnaround_count"] == 1
    assert metadata["fundamentals"]["compounder_count"] == 1
    assert metadata["fundamentals"]["top_earnings_sector"] == "Capital Goods"
    assert metadata["fundamentals"]["universe_pe"] == 24.1
    assert metadata["fundamentals"]["valuation_zone"] == "expensive"


def test_fundamental_watchlist_channel_ignores_downturn_only_rows() -> None:
    watchlist = pd.DataFrame(
        [
            {
                "symbol": "THERMAX",
                "watchlist_bucket": "D1_RESULT_DOWNTURN",
                "final_watchlist_score": 64.13,
                "quarterly_result_bucket": "DETERIORATING",
            }
        ]
    )

    assert _has_fundamental_tracking_watchlist(watchlist) is False


def test_publish_fundamental_sheet_failure_is_non_blocking(tmp_path: Path) -> None:
    called_channels = []

    def _fundamentals_fail(_context, _artifact, _datasets):
        called_channels.append("google_sheets_fundamentals")
        raise RuntimeError("sheet formatting failed")

    def _telegram_ok(_context, _artifact, _datasets):
        called_channels.append("telegram_summary")
        return {"message_id": "telegram-ok"}

    metadata = PublishStage(
        channel_handlers={
            "google_sheets_fundamentals": _fundamentals_fail,
            "telegram_summary": _telegram_ok,
        },
        delivery_manager=_FakeDeliveryManager(),
    )._run_default(_publish_context(tmp_path, with_fundamentals=True))

    assert PublishStage.CHANNEL_ROLES["google_sheets_fundamentals"] == "publish_optional"
    assert called_channels == ["google_sheets_fundamentals", "telegram_summary"]
    assert any("google_sheets_fundamentals" in msg for msg in metadata["non_blocking_failures"])
    assert "failures" not in metadata
