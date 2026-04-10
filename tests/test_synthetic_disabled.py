from __future__ import annotations

from pathlib import Path

import pytest

from collectors import daily_update_runner
from run.stages import FeaturesStage, IngestStage, PublishStage, RankStage
from core.contracts import StageContext


def _context(tmp_path: Path) -> StageContext:
    return StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "ohlcv.duckdb",
        run_id="test-run",
        run_date="2026-04-06",
        stage_name="ingest",
        attempt_number=1,
        params={"smoke": True, "data_domain": "operational"},
    )


def test_daily_update_runner_refuses_missing_live_dhan_for_bulk(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyCollector:
        def __init__(self, *args, **kwargs) -> None:
            self.use_api = False
            self.dhan = None
            self.token_manager = type(
                "DummyTokenManager",
                (),
                {"ensure_valid_token": staticmethod(lambda hours_before_expiry=1: None), "client_id": "", "api_key": ""},
            )()
            self.client_id = ""
            self.api_key = ""
            self.access_token = ""

        def _ensure_valid_token(self) -> bool:
            return False

    monkeypatch.setattr(daily_update_runner, "DhanCollector", DummyCollector)

    with pytest.raises(RuntimeError, match="authenticated Dhan access"):
        daily_update_runner.run(
            symbols_only=True,
            features_only=False,
            batch_size=10,
            bulk=True,
            data_domain="operational",
        )


@pytest.mark.parametrize(
    ("stage", "stage_name"),
    [
        (IngestStage(operation=lambda context: {}), "ingest"),
        (FeaturesStage(operation=lambda context: {}), "features"),
        (RankStage(operation=lambda context: {}), "rank"),
        (PublishStage(operation=lambda context: {}), "publish"),
    ],
)
def test_pipeline_stages_reject_smoke_mode(tmp_path: Path, stage, stage_name: str) -> None:
    context = _context(tmp_path)
    context.stage_name = stage_name

    with pytest.raises(RuntimeError, match="Smoke mode is disabled"):
        stage.run(context)
