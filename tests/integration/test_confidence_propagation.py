from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.execution.candidate_builder import attach_execution_weight
from ai_trading_system.domains.publish.publish_payloads import attach_publish_metadata
from ai_trading_system.domains.ranking.service import attach_rank_confidence_from_features


def test_feature_confidence_flows_into_rank_and_execute_and_publish() -> None:
    frame = pd.DataFrame(
        [
            {"symbol_id": "AAA", "feature_confidence": 0.82},
            {"symbol_id": "BBB", "feature_confidence": 0.40},
        ]
    )

    ranked = attach_rank_confidence_from_features(frame)
    execution_ready = attach_execution_weight(ranked)
    publish_row = attach_publish_metadata(execution_ready.iloc[0].to_dict(), trust_status="trusted")

    assert ranked["rank_confidence"].tolist() == [0.82, 0.40]
    assert execution_ready["execution_weight"].tolist() == [0.82, 0.40]
    assert publish_row["trust_status"] == "trusted"
    assert publish_row["publish_confidence"] == 0.82
