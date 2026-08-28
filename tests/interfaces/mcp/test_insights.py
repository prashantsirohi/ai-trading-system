"""Tests for bounded MCP v2.1 explanations and aggregate orientation."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.context import StoreBusyError
from ai_trading_system.interfaces.mcp.envelope import AS_OF_EXACT
from ai_trading_system.interfaces.mcp.tools.insights import (
    compare_symbols,
    explain_symbol,
    get_market_snapshot,
    summarize_universe,
)


def test_universe_summary_uses_complete_match_set(ctx: McpContext) -> None:
    response = summarize_universe(ctx, scope="full_universe")
    assert response["data"]["matched_count"] == 3
    assert response["data"]["full_universe_size"] == 3
    assert response["data"]["shortlist_size"] == 2
    assert response["data"]["fundamental_thesis_counts"] == {"QUALITY_COMPOUNDER": 1}
    assert response["data"]["evidence_missing_counts"]["fundamental_thesis"] == 2
    assert response["meta"]["aggregation_complete"] is True


def test_universe_summary_preserves_filters(ctx: McpContext) -> None:
    response = summarize_universe(ctx, min_composite_score=60)
    assert response["data"]["matched_count"] == 1
    assert response["meta"]["filters"]["min_composite_score"] == 60


def test_explain_symbol_reports_same_date_shortlist_membership(
    ctx: McpContext,
) -> None:
    response = explain_symbol(ctx, "AAA")
    assert response["data"]["selection"] == {
        "in_full_universe": True,
        "in_shortlist": True,
        "reasons": [],
    }
    assert response["data"]["full_universe_rank"]["rank_position"] == 8
    assert (
        response["data"]["fundamental_thesis"]["classification"]["primary_thesis"]
        == "QUALITY_COMPOUNDER"
    )
    assert response["meta"]["evidence_only"] is True


def test_explain_symbol_derives_recorded_policy_exclusions(ctx: McpContext) -> None:
    response = explain_symbol(ctx, "CCC")
    selection = response["data"]["selection"]
    assert selection["in_full_universe"] is True
    assert selection["in_shortlist"] is False
    assert set(selection["reasons"]) == {
        "BELOW_EFFECTIVE_TOP_N",
        "BELOW_EFFECTIVE_MIN_SCORE",
    }


def test_explain_symbol_is_point_in_time(ctx: McpContext) -> None:
    response = explain_symbol(ctx, "AAA", as_of="2026-01-06")
    assert response["meta"]["as_of_status"] == AS_OF_EXACT
    assert response["data"]["decision_date"] == "2026-01-06"
    assert response["data"]["quote"]["date"] == "2026-01-06"


def test_explain_symbol_retains_non_price_evidence_during_writer_lock(
    ctx: McpContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.interfaces.mcp.tools import insights

    monkeypatch.setattr(
        insights.profile_tool,
        "get_symbol_profile",
        lambda *args, **kwargs: (_ for _ in ()).throw(StoreBusyError("locked")),
    )
    response = explain_symbol(ctx, "AAA")
    assert response["data"]["quote"] is None
    assert response["data"]["stage"]["stage_label"] == "transition_1_to_2"
    assert response["data"]["selection"]["in_shortlist"] is True
    assert any("writer lock" in note for note in response["meta"]["notes"])


def test_compare_symbols_is_bounded_and_rank_ordered(ctx: McpContext) -> None:
    response = compare_symbols(ctx, ["CCC", "AAA", "AAA"])
    assert [row["symbol_id"] for row in response["data"]] == ["AAA", "CCC"]
    assert response["meta"]["compared_count"] == 2
    with pytest.raises(ValueError, match="at most 10"):
        compare_symbols(ctx, [f"S{index}" for index in range(11)])


def test_market_snapshot_keeps_latest_only_block_empty_historically(
    ctx: McpContext,
) -> None:
    response = get_market_snapshot(ctx, as_of="2026-01-06")
    assert response["data"]["universe"]["matched_count"] == 2
    assert response["data"]["sector_leadership"] == []
    assert (
        response["meta"]["blocks"]["sector_leadership"]["as_of_status"]
        == "AS_OF_UNSUPPORTED"
    )
    assert response["meta"]["evidence_only"] is True
