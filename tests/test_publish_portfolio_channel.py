from __future__ import annotations

from pathlib import Path

import json
import pytest

from ai_trading_system.domains.publish.channels.google_sheets_manager import GoogleSheetsManager
from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.publish import PublishStage


def test_publish_portfolio_raises_when_analysis_reports_failure(monkeypatch, tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-publish-portfolio",
        run_date="2026-04-10",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )
    artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="hash",
        row_count=1,
    )

    monkeypatch.setattr(
        "ai_trading_system.pipeline.daily_pipeline.run_portfolio_analysis",
        lambda: {"ok": False, "error": "Google Sheets authentication failed"},
    )

    with pytest.raises(RuntimeError, match="Google Sheets authentication failed"):
        PublishStage()._publish_portfolio(context, artifact, {})


def test_publish_portfolio_returns_report_metadata_on_success(monkeypatch, tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "operational" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-10-publish-portfolio-success",
        run_date="2026-04-10",
        stage_name="publish",
        attempt_number=1,
        registry=registry,
        params={"data_domain": "operational"},
    )
    artifact = StageArtifact(
        artifact_type="ranked_signals",
        uri=str(tmp_path / "ranked_signals.csv"),
        content_hash="hash",
        row_count=1,
    )

    monkeypatch.setattr(
        "ai_trading_system.pipeline.daily_pipeline.run_portfolio_analysis",
        lambda: {"ok": True, "positions": 7},
    )

    result = PublishStage()._publish_portfolio(context, artifact, {})

    assert result["report_id"] == "portfolio_sheet"
    assert result["positions"] == 7


def test_google_sheets_manager_preserves_token_refresh_error(monkeypatch, tmp_path: Path) -> None:
    token_path = tmp_path / "token.json"
    token_path.write_text(
        json.dumps(
            {
                "token": "expired-token",
                "refresh_token": "refresh-token",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "scopes": GoogleSheetsManager.SCOPES,
                "expiry": "2026-04-09T10:50:45.246981Z",
            }
        ),
        encoding="utf-8",
    )

    class _FakeCreds:
        valid = False
        refresh_token = "refresh-token"

        def refresh(self, _request) -> None:
            raise RuntimeError("DNS resolve failed for oauth2.googleapis.com")

    monkeypatch.setattr(
        "ai_trading_system.domains.publish.channels.google_sheets_manager.OAuthCredentials.from_authorized_user_file",
        lambda *_args, **_kwargs: _FakeCreds(),
    )

    manager = GoogleSheetsManager(
        credentials_path=tmp_path / "client_secret.json",
        token_path=token_path,
        spreadsheet_id="sheet-id",
    )

    assert manager.client is None
    assert manager.last_error is not None
    assert "Token auth failed" in manager.last_error
    assert "oauth2.googleapis.com" in manager.last_error
