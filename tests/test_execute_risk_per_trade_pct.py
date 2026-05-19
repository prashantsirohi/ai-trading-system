"""Phase 6 — risk_per_trade_pct flows from regime profile through execute.

The regime profile YAML declares per-regime per-trade risk caps; the
execute stage extracts them and the autotrader uses the profile value as
the override for the signal-level field.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ai_trading_system.analytics.regime.profiles import RegimeProfile
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.stages.execute import ExecuteStage


# ── RegimeProfile dataclass ──────────────────────────────────────────────


def test_profile_from_mapping_reads_risk_per_trade_pct() -> None:
    payload = {
        "min_score": 64,
        "rank_top_n": 40,
        "max_exposure": 0.85,
        "max_positions": 10,
        "max_sector_exposure": 0.32,
        "max_single_stock_weight": 0.10,
        "atr_stop_mult": 2.6,
        "breakout_mode": "normal",
        "allow_pyramiding": True,
        "risk_per_trade_pct": 0.75,
    }
    p = RegimeProfile.from_mapping("bull", payload, name="profile_C_cash_only")
    assert p.risk_per_trade_pct == 0.75


def test_profile_omitting_risk_returns_none() -> None:
    payload = {
        "min_score": 64,
        "rank_top_n": 40,
        "max_exposure": 0.85,
        "max_positions": 10,
        "max_sector_exposure": 0.32,
        "max_single_stock_weight": 0.10,
        "atr_stop_mult": 2.6,
        "breakout_mode": "normal",
        "allow_pyramiding": True,
    }
    p = RegimeProfile.from_mapping("bull", payload)
    assert p.risk_per_trade_pct is None


def test_profile_invalid_risk_pct_returns_none() -> None:
    payload = {
        "min_score": 64,
        "rank_top_n": 40,
        "max_exposure": 0.85,
        "max_positions": 10,
        "max_sector_exposure": 0.32,
        "max_single_stock_weight": 0.10,
        "atr_stop_mult": 2.6,
        "breakout_mode": "normal",
        "allow_pyramiding": True,
        "risk_per_trade_pct": "high",
    }
    p = RegimeProfile.from_mapping("bull", payload)
    assert p.risk_per_trade_pct is None


# ── Execute stage integration ────────────────────────────────────────────


def _setup(monkeypatch, tmp_path: Path, *, profile_block: dict):
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
                "regime_profile": profile_block,
            }
        ),
        encoding="utf-8",
    )

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
    return context, captured


def test_execute_forwards_profile_risk_per_trade_pct(monkeypatch, tmp_path: Path):
    profile = {
        "name": "profile_C_cash_only",
        "regime": "bull",
        "max_exposure": 0.85,
        "max_positions": 10,
        "max_sector_exposure": 0.32,
        "max_single_stock_weight": 0.10,
        "atr_stop_mult": 2.6,
        "risk_per_trade_pct": 0.75,
    }
    context, captured = _setup(monkeypatch, tmp_path, profile_block=profile)
    result = ExecuteStage().run(context)
    assert captured["risk_per_trade_pct"] == 0.75
    assert result.metadata["effective_risk_per_trade_pct"] == 0.75


def test_execute_passes_none_when_profile_omits_risk_pct(monkeypatch, tmp_path: Path):
    """Legacy profile without the new field: autotrader receives None,
    falls back to signal-payload value."""
    profile = {
        "name": "legacy_profile",
        "regime": "bull",
        "max_exposure": 0.85,
        "max_positions": 10,
        "max_sector_exposure": 0.32,
        "max_single_stock_weight": 0.10,
        "atr_stop_mult": 2.6,
    }
    context, captured = _setup(monkeypatch, tmp_path, profile_block=profile)
    result = ExecuteStage().run(context)
    assert captured["risk_per_trade_pct"] is None
    assert result.metadata["effective_risk_per_trade_pct"] is None


# ── Shipped profile YAMLs include the field ──────────────────────────────


def test_profile_C_cash_only_yaml_includes_risk_per_trade_pct() -> None:
    """The shipped profile_C YAML must declare risk_per_trade_pct for
    every regime so live runs always get a profile-driven value."""
    import yaml

    repo_root = Path(__file__).resolve().parents[1]
    yaml_path = repo_root / "config" / "strategies" / "regime" / "profile_C_cash_only.yaml"
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    for regime in ("risk_off", "neutral", "cautious_bull", "bull", "strong_bull"):
        block = payload[regime]
        assert "risk_per_trade_pct" in block, f"{regime} missing risk_per_trade_pct"
        assert isinstance(block["risk_per_trade_pct"], (int, float))


def test_profile_D_margin_research_yaml_includes_risk_per_trade_pct() -> None:
    import yaml

    repo_root = Path(__file__).resolve().parents[1]
    yaml_path = repo_root / "config" / "strategies" / "regime" / "profile_D_margin_research.yaml"
    payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    for regime in ("risk_off", "neutral", "cautious_bull", "bull", "strong_bull"):
        block = payload[regime]
        assert "risk_per_trade_pct" in block, f"{regime} missing risk_per_trade_pct"
