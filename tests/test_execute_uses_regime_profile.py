from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.execute import ExecuteStage


def test_execute_uses_regime_profile(monkeypatch, tmp_path: Path) -> None:
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame(
        [{"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 90.0}]
    ).to_csv(ranked_path, index=False)

    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps(
            {
                "summary": {"data_trust_status": "trusted"},
                "market_regime": {"regime": "bull", "raw_regime": "bull"},
                "regime_profile": {
                    "name": "profile_C_cash_only",
                    "regime": "bull",
                    "max_exposure": 0.85,
                    "max_positions": 10,
                    "max_sector_exposure": 0.32,
                    "max_single_stock_weight": 0.10,
                    "atr_stop_mult": 2.6,
                },
            }
        ),
        encoding="utf-8",
    )

    captured = {}

    def fake_run(self, **kwargs):
        captured.update(kwargs)
        return {
            "actions": [],
            "executions": [],
            "positions_before": [],
            "positions_after": [],
            "status": "completed",
        }

    monkeypatch.setattr("ai_trading_system.domains.execution.autotrader.AutoTrader.run", fake_run)
    monkeypatch.setattr(
        "ai_trading_system.analytics.regime_detector.RegimeDetector.get_market_regime",
        lambda self: {"market_regime": "TREND"},
    )

    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-05-13-execute",
        run_date="2026-05-13",
        stage_name="execute",
        attempt_number=1,
        params={
            "data_domain": "operational",
            "execution_capital": 1000000,
            "execution_preview": True,
            "execution_enabled": False,
        },
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact("ranked_signals", str(ranked_path)),
                "dashboard_payload": StageArtifact("dashboard_payload", str(dashboard_path)),
            }
        },
    )

    result = ExecuteStage().run(context)

    assert captured["capital"] == 850000
    assert captured["target_position_count"] == 10
    assert captured["max_positions"] == 10
    assert captured["max_sector_exposure"] == 0.32
    assert captured["max_single_stock_weight"] == 0.10
    assert captured["exit_atr_multiple"] == 2.6
    assert captured["use_portfolio_constraints"] is True
    assert captured["regime"] == "bull"
    assert result.metadata["effective_execution_capital"] == 850000
