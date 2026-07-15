from __future__ import annotations

from datetime import datetime, timezone

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage
from ai_trading_system.domains.opportunities.routing import decide_scan_route
from ai_trading_system.platform.telemetry import compare_semantic_outputs


def test_instrumented_replay_preserves_routing_identity() -> None:
    fixed = datetime(2026, 7, 15, tzinfo=timezone.utc)
    kwargs = {
        "symbol_id": "RELIANCE", "rank_selected": True, "rank_position": 1,
        "stage_discovery": True, "stock_stage": WeinsteinStage.STAGE_1,
        "sector_stage": WeinsteinStage.STAGE_2, "decided_at": fixed,
    }
    first = decide_scan_route(**kwargs)
    replay = decide_scan_route(**kwargs)
    comparison = compare_semantic_outputs(
        {"routing_decision_ids": [first.routing_decision_id], "routing_input_hashes": [first.routing_input_hash]},
        {"routing_decision_ids": [replay.routing_decision_id], "routing_input_hashes": [replay.routing_input_hash]},
    )
    assert comparison["equivalent"] is True
    assert comparison["decision_identity_matches"] is True
