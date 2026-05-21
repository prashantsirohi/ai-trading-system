from __future__ import annotations

from pathlib import Path


FRONTEND_ROOT = Path(__file__).resolve().parents[2] / "web" / "execution-console-v2" / "ai-trading-dashboard-starter" / "src"


def test_workspace_mapper_and_control_tower_surface_market_direction() -> None:
    workspace = (FRONTEND_ROOT / "lib/api/workspace.ts").read_text(encoding="utf-8")
    page = (FRONTEND_ROOT / "pages/ControlTowerPage.tsx").read_text(encoding="utf-8")
    card = (FRONTEND_ROOT / "components/control-tower/MarketDirectionCard.tsx").read_text(encoding="utf-8")

    assert "directionBias: asString(safe.direction_bias)" in workspace
    assert "allowedExposure: asNumber(safe.allowed_exposure)" in workspace
    assert "breadthVelocityBucket: asString(safe.breadth_velocity_bucket)" in workspace
    assert "regimePhaseLabel: asString(safe.regime_phase_label)" in workspace
    assert "regimePhaseS2Pct: asNumber(safe.regime_phase_s2_pct)" in workspace
    assert "MarketDirectionCard" in page
    assert "Market Direction" in card
    assert "phaseText" in card
    assert "S2 Breadth" in card
    assert "Phase Velocity" in card
    assert "Required Setup" in card
