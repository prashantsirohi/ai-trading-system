from __future__ import annotations

from ai_trading_system.analytics.regime.regime_phase import compute_regime_phase
from ai_trading_system.domains.ranking.payloads import (
    attach_market_regime_phase_to_payload,
)


def test_attach_market_regime_phase_to_payload_adds_summary_fields():
    payload = {"summary": {"run_id": "test-run"}}
    phase = compute_regime_phase(
        market_stage="MIXED",
        regime="neutral",
        breadth_velocity_bucket="positive",
        s2_pct=0.20,
    )

    updated = attach_market_regime_phase_to_payload(payload, phase.to_dict())

    assert updated["market_regime_phase"]["regime_phase"] == "base_forming_stage1"
    assert updated["summary"]["regime_phase"] == "base_forming_stage1"
    assert updated["summary"]["regime_phase_label"] == "Base forming (S1)"
    assert updated["summary"]["regime_phase_s2_pct"] == 0.2
