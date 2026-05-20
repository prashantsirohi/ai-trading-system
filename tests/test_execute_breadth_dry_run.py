"""Phase 8: execute writes a breadth-impulse dry-run audit block when the
active risk matrix symlink is present, and skips it otherwise.

Live sizing must NOT change: ``capital``, ``max_positions`` etc. flow from
the legacy 1-D profile regardless of the matrix.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.execute import ExecuteStage


REPO_ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = REPO_ROOT / "config" / "strategies" / "regime" / "risk_matrix.yaml"


def _make_context(tmp_path: Path) -> StageContext:
    ranked_path = tmp_path / "ranked_signals.csv"
    pd.DataFrame(
        [{"symbol_id": "AAA", "exchange": "NSE", "close": 100.0, "composite_score": 90.0}]
    ).to_csv(ranked_path, index=False)

    dashboard_path = tmp_path / "dashboard_payload.json"
    dashboard_path.write_text(
        json.dumps(
            {
                "summary": {"data_trust_status": "trusted"},
                "market_regime": {
                    "regime": "strong_bull",
                    "raw_regime": "strong_bull",
                    "breadth_velocity_bucket": "very_negative",
                    "regime_age_days": 35,
                },
                "regime_profile": {
                    "name": "profile_C_cash_only",
                    "regime": "strong_bull",
                    "max_exposure": 1.00,
                    "max_positions": 14,
                    "max_sector_exposure": 0.38,
                    "max_single_stock_weight": 0.12,
                    "atr_stop_mult": 3.0,
                },
            }
        ),
        encoding="utf-8",
    )
    return StageContext(
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


def _stub_autotrader(monkeypatch) -> None:
    def fake_run(self, **kwargs):
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


def test_execute_dry_run_present_when_matrix_active(monkeypatch, tmp_path: Path) -> None:
    _stub_autotrader(monkeypatch)
    # Activate matrix via symlink under tmp project_root.
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "active_risk_matrix.yaml").symlink_to(MATRIX_PATH)

    result = ExecuteStage().run(_make_context(tmp_path))

    dry = result.metadata["breadth_impulse_dry_run"]
    assert dry is not None
    assert dry["matrix_name"] == "risk_matrix_v1"
    assert dry["regime"] == "strong_bull"
    assert dry["velocity_bucket"] == "very_negative"
    assert dry["regime_age_days"] == 35
    # age 35 → multiplier 0.85
    assert dry["age_multiplier"] == 0.85
    # strong_bull × very_negative cell exposure = 0.35 (per shipped matrix)
    # proposed = 0.35 * 0.85 = 0.2975
    assert abs(dry["proposed_gross_exposure"] - 0.2975) < 1e-6
    # legacy = 1.00 (strong_bull max_exposure from regime_profile)
    assert dry["legacy_gross_exposure"] == 1.00
    assert dry["delta"] == round(0.2975 - 1.00, 6)
    assert dry["applied_live"] is False
    # Cell payload is fully populated for downstream audit.
    assert dry["cell"]["allow_new_buys"] is False
    assert dry["cell"]["action"] == "defensive_trim"

    # Live sizing untouched — legacy profile still drives effective capital.
    assert result.metadata["effective_execution_capital"] == 1_000_000


def test_execute_dry_run_absent_when_no_symlink(monkeypatch, tmp_path: Path) -> None:
    _stub_autotrader(monkeypatch)
    # No symlink created.
    result = ExecuteStage().run(_make_context(tmp_path))
    assert result.metadata["breadth_impulse_dry_run"] is None
