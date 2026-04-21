from __future__ import annotations

from ai_trading_system.domains.publish.publish_payloads import apply_trust_overlay


def test_apply_trust_overlay_sets_warning_for_non_trusted_status() -> None:
    trusted = apply_trust_overlay({"symbol_id": "AAA"}, "trusted")
    degraded = apply_trust_overlay({"symbol_id": "AAA"}, "degraded")

    assert trusted["trust_status"] == "trusted"
    assert trusted["trust_warning"] is None
    assert degraded["trust_status"] == "degraded"
    assert "Trust status is degraded" in degraded["trust_warning"]
