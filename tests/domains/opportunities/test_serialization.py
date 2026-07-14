from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    ActionEligibility,
    CandidateAction,
    CandidateSnapshot,
    CandidateState,
    EvidenceSnapshot,
    EvidenceVerdict,
    FollowthroughStatus,
    OpportunitySnapshot,
    ProgressStatus,
    RiskLevel,
)
from ai_trading_system.domains.opportunities.serialization import from_dict, to_dict, to_json


NOW = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)


def test_nested_candidate_round_trip_is_deterministic(stage_factory, sector_factory) -> None:
    opportunity = OpportunitySnapshot(
        85,
        2,
        99,
        -3,
        ProgressStatus.IMPROVING,
        {"trend": 80, "relative_strength": 90},
        "rank-v1",
        NOW,
    )
    evidence = EvidenceSnapshot(
        88,
        EvidenceVerdict.HIGH_CONVICTION,
        80,
        85,
        90,
        82,
        76,
        85,
        80,
        RiskLevel.LOW,
        RiskLevel.LOW,
        ("volume expansion",),
        (),
        ("fundamental refresh",),
        "investigator-v1",
        NOW,
    )
    candidate = CandidateSnapshot(
        "candidate-1",
        "setup-1",
        "AAA",
        "NSE",
        NOW,
        opportunity,
        evidence,
        CandidateState.DISCOVERED,
        FollowthroughStatus.NOT_APPLICABLE,
        stage_factory(),
        sector_factory(),
        "bull",
        "leading",
        2,
        1,
        False,
        CandidateAction.WATCH,
        ActionEligibility.NOT_APPLICABLE,
    )

    payload = to_dict(candidate)
    restored = from_dict(CandidateSnapshot, json.loads(json.dumps(payload)))
    assert restored == candidate
    assert to_json(restored) == to_json(candidate)
    assert '"market_regime":"bull"' in to_json(candidate)
    assert payload["as_of"].endswith("+00:00")
    with pytest.raises(TypeError):
        candidate.opportunity.factor_scores["trend"] = 10
