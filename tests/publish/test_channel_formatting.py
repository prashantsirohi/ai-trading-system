from __future__ import annotations

from ai_trading_system.domains.publish.publish_payloads import (
    attach_publish_confidence,
    attach_publish_explainability,
    format_rows_for_channel,
)


def test_format_rows_for_channel_applies_channel_density() -> None:
    rows = [{"symbol_id": f"S{i}"} for i in range(15)]

    telegram = format_rows_for_channel(rows, "telegram")
    sheets = format_rows_for_channel(rows, "sheets")
    dashboard = format_rows_for_channel(rows, "dashboard")

    assert telegram["mode"] == "concise"
    assert len(telegram["rows"]) == 10
    assert sheets["mode"] == "full"
    assert len(sheets["rows"]) == 15
    assert dashboard["mode"] == "structured_json"


def test_publish_explainability_and_confidence_are_attached_additively() -> None:
    row = {"top_factors": ["relative_strength"], "rank_confidence": 0.82}
    enriched = attach_publish_confidence(attach_publish_explainability(row))

    assert enriched["why_selected"] == ["relative_strength"]
    assert enriched["key_factors"] == ["relative_strength"]
    assert enriched["publish_confidence"] == 0.82
