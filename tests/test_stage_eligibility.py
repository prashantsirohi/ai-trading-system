"""Tests for stage_eligibility filter/annotate helpers."""
from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.stage_eligibility import (
    annotate_with_stage,
    filter_by_stage,
)


def _candidates():
    return pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "score": [0.9, 0.8, 0.7, 0.6],
    })


def _snapshot():
    return pd.DataFrame({
        "symbol": ["A", "B", "C"],
        "stage_label": ["S2", "S1", "S2"],
        "stage_confidence": [0.8, 0.9, 0.4],   # C below threshold
        "stage_transition": ["NONE", "NONE", "NONE"],
    })


def test_filter_keeps_only_allowed_stage_above_confidence():
    out = filter_by_stage(_candidates(), _snapshot(),
                          allowed_stages={"S2"}, min_confidence=0.6,
                          require_snapshot=True)
    assert list(out["symbol"]) == ["A"]


def test_filter_passes_unsnapshotted_when_not_required():
    # D has no snapshot row; default require_snapshot=False keeps it.
    out = filter_by_stage(_candidates(), _snapshot(),
                          allowed_stages={"S2"}, min_confidence=0.6)
    assert set(out["symbol"]) == {"A", "D"}


def test_filter_drops_unsnapshotted_when_required():
    out = filter_by_stage(_candidates(), _snapshot(),
                          allowed_stages={"S2"}, min_confidence=0.6,
                          require_snapshot=True)
    assert "D" not in set(out["symbol"])


def test_filter_empty_snapshot_passthrough():
    out = filter_by_stage(_candidates(), pd.DataFrame())
    assert list(out["symbol"]) == ["A", "B", "C", "D"]


def test_filter_empty_snapshot_required_drops_all():
    out = filter_by_stage(_candidates(), pd.DataFrame(), require_snapshot=True)
    assert out.empty


def test_annotate_left_joins_stage_columns():
    out = annotate_with_stage(_candidates(), _snapshot())
    assert set(out.columns) >= {"stage_label", "stage_confidence", "stage_transition"}
    assert out.set_index("symbol").loc["D", "stage_label"] is None or \
           pd.isna(out.set_index("symbol").loc["D", "stage_label"])
