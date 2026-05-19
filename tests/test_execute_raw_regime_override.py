"""Phase-3 raw-regime override: execute stage opt-in safety gate.

When raw_regime collapses to risk_off but confirmed_regime still says
bull/strong_bull, the lagging confirmed signal would otherwise size up
fresh entries. The opt-in
``execution_raw_regime_overrides_on_disagreement`` param zeroes entry
capital and max_positions while leaving existing positions untouched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.execute import ExecuteStage


def _dashboard(market_regime: dict, regime_profile: dict, disagreement: dict) -> str:
    return json.dumps(
        {
            "summary": {"data_trust_status": "trusted"},
            "market_regime": market_regime,
            "market_regime_disagreement": disagreement,
            "regime_profile": regime_profile,
        }
    )


def _setup(monkeypatch, tmp_path: Path, params: dict, dashboard_json: str):
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame(
        [{"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 90.0}]
    ).to_csv(ranked_path, index=False)

    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(dashboard_json, encoding="utf-8")

    captured: dict = {}

    def fake_run(self, **kwargs):
        captured.update(kwargs)
        return {
            "actions": [],
            "executions": [],
            "positions_before": [],
            "positions_after": [],
            "status": "completed",
        }

    monkeypatch.setattr(
        "ai_trading_system.domains.execution.autotrader.AutoTrader.run", fake_run
    )
    monkeypatch.setattr(
        "ai_trading_system.analytics.regime_detector.RegimeDetector.get_market_regime",
        lambda self: {"market_regime": "TREND"},
    )

    base_params = {
        "data_domain": "operational",
        "execution_capital": 1000000,
        "execution_preview": True,
        "execution_enabled": False,
    }
    base_params.update(params)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-05-13-execute",
        run_date="2026-05-13",
        stage_name="execute",
        attempt_number=1,
        params=base_params,
        artifacts={
            "rank": {
                "ranked_signals": StageArtifact("ranked_signals", str(ranked_path)),
                "dashboard_payload": StageArtifact("dashboard_payload", str(dashboard_path)),
            }
        },
    )
    return context, captured


def test_override_off_by_default_lets_bull_profile_through(monkeypatch, tmp_path: Path):
    """Without the opt-in param, dangerous disagreement is exposed but does
    not change execution behavior — operator stays in confirmed-bull mode."""
    dashboard = _dashboard(
        market_regime={"regime": "bull", "raw_regime": "risk_off"},
        regime_profile={
            "name": "profile_C_cash_only",
            "regime": "bull",
            "max_exposure": 0.85,
            "max_positions": 10,
            "max_sector_exposure": 0.32,
            "max_single_stock_weight": 0.10,
            "atr_stop_mult": 2.6,
        },
        disagreement={
            "present": True,
            "dangerous": True,
            "direction": "raw_worse",
            "confirmed": "bull",
            "raw": "risk_off",
        },
    )
    context, captured = _setup(monkeypatch, tmp_path, params={}, dashboard_json=dashboard)
    result = ExecuteStage().run(context)
    # Bull profile applied as usual (max_exposure 0.85 of 1M)
    assert captured["capital"] == 850000
    assert captured["max_positions"] == 10
    # Metadata still surfaces the disagreement for visibility
    assert result.metadata["market_regime_disagreement"]["dangerous"] is True
    assert result.metadata["raw_regime_override_active"] is False


def test_override_on_zeroes_entries_when_dangerous(monkeypatch, tmp_path: Path):
    dashboard = _dashboard(
        market_regime={"regime": "bull", "raw_regime": "risk_off"},
        regime_profile={
            "name": "profile_C_cash_only",
            "regime": "bull",
            "max_exposure": 0.85,
            "max_positions": 10,
            "max_sector_exposure": 0.32,
            "max_single_stock_weight": 0.10,
            "atr_stop_mult": 2.6,
        },
        disagreement={
            "present": True,
            "dangerous": True,
            "direction": "raw_worse",
            "confirmed": "bull",
            "raw": "risk_off",
        },
    )
    context, captured = _setup(
        monkeypatch,
        tmp_path,
        params={"execution_raw_regime_overrides_on_disagreement": True},
        dashboard_json=dashboard,
    )
    result = ExecuteStage().run(context)
    # Override forces entries to zero even though confirmed profile says bull
    assert captured["capital"] == 0
    assert captured["max_positions"] == 0
    assert captured["target_position_count"] == 0
    assert result.metadata["raw_regime_override_active"] is True


def test_override_on_no_op_when_not_dangerous(monkeypatch, tmp_path: Path):
    """raw=neutral while confirmed=bull is a softening but not 'collapse'.
    The override should NOT fire."""
    dashboard = _dashboard(
        market_regime={"regime": "bull", "raw_regime": "neutral"},
        regime_profile={
            "name": "profile_C_cash_only",
            "regime": "bull",
            "max_exposure": 0.85,
            "max_positions": 10,
            "max_sector_exposure": 0.32,
            "max_single_stock_weight": 0.10,
            "atr_stop_mult": 2.6,
        },
        disagreement={
            "present": True,
            "dangerous": False,
            "direction": "raw_worse",
            "confirmed": "bull",
            "raw": "neutral",
        },
    )
    context, captured = _setup(
        monkeypatch,
        tmp_path,
        params={"execution_raw_regime_overrides_on_disagreement": True},
        dashboard_json=dashboard,
    )
    result = ExecuteStage().run(context)
    # Bull profile applied as usual
    assert captured["capital"] == 850000
    assert captured["max_positions"] == 10
    assert result.metadata["raw_regime_override_active"] is False


def test_override_on_no_op_when_no_disagreement(monkeypatch, tmp_path: Path):
    dashboard = _dashboard(
        market_regime={"regime": "bull", "raw_regime": "bull"},
        regime_profile={
            "name": "profile_C_cash_only",
            "regime": "bull",
            "max_exposure": 0.85,
            "max_positions": 10,
            "max_sector_exposure": 0.32,
            "max_single_stock_weight": 0.10,
            "atr_stop_mult": 2.6,
        },
        disagreement={
            "present": False,
            "dangerous": False,
            "direction": "same",
            "confirmed": "bull",
            "raw": "bull",
        },
    )
    context, captured = _setup(
        monkeypatch,
        tmp_path,
        params={"execution_raw_regime_overrides_on_disagreement": True},
        dashboard_json=dashboard,
    )
    result = ExecuteStage().run(context)
    assert captured["capital"] == 850000
    assert result.metadata["raw_regime_override_active"] is False
