"""Tests for the composed profile and the cross-sectional tools."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_NO_DATA,
)
from ai_trading_system.interfaces.mcp.tools.profile import get_symbol_profile
from ai_trading_system.interfaces.mcp.tools.screen import screen_universe
from ai_trading_system.interfaces.mcp.tools.sectors import (
    get_sector_constituents,
    get_sector_overview,
)


# ---------------------------------------------------------------------------
# get_symbol_profile
# ---------------------------------------------------------------------------


def test_profile_returns_all_blocks(ctx: McpContext) -> None:
    data = get_symbol_profile(ctx, "AAA")["data"]
    assert set(data) == {"identity", "quote", "stage", "rank", "fundamentals"}
    assert data["identity"]["symbol_id"] == "AAA"
    assert data["quote"]["date"] == "2026-01-09"
    assert data["stage"]["stage_label"] == "transition_1_to_2"
    assert data["rank"]["position"]["rank_position"] == 8


def test_profile_quote_uses_the_adjusted_basis(ctx: McpContext) -> None:
    """Composed from get_ohlcv, so it inherits the correct price basis."""

    response = get_symbol_profile(ctx, "AAA", as_of="2026-01-05")
    assert response["data"]["quote"]["close"] == 100.0
    assert response["meta"]["blocks"]["quote"]["source"].endswith(
        "_catalog_feature_source"
    )


def test_profile_is_exchange_aware(ctx: McpContext) -> None:
    """The BSE listing has its own prices and no rank."""

    bse = get_symbol_profile(ctx, "AAA", exchange="BSE")
    assert bse["data"]["quote"]["close"] == 119.5
    assert bse["data"]["rank"] is None
    assert bse["meta"]["exchange"] == "BSE"


def test_every_block_carries_its_own_date_and_source(ctx: McpContext) -> None:
    blocks = get_symbol_profile(ctx, "AAA")["meta"]["blocks"]
    assert set(blocks) == {"identity", "quote", "stage", "rank", "fundamentals"}
    for name, meta in blocks.items():
        assert set(meta) == {"as_of_status", "as_of_effective", "source", "notes"}, name
    assert blocks["quote"]["as_of_effective"] == "2026-01-09"
    assert blocks["rank"]["source"] == "control_plane.duckdb:rank_history"


def test_a_block_without_data_is_empty_not_backfilled(ctx: McpContext) -> None:
    """The core failure mode of wrapping get_stock_detail."""

    response = get_symbol_profile(ctx, "AAA", as_of="2026-01-02")
    assert response["data"]["stage"]["stage_label"] == "stage_1_basing"
    # Fundamentals were not published until 2025-11-15, which is before this
    # date, so they are present; rank starts 2026-01-05, so it is not.
    assert response["data"]["rank"] is None
    assert response["meta"]["blocks"]["rank"]["as_of_status"] == AS_OF_NO_DATA
    assert response["meta"]["alignment"] == "INCOMPLETE"
    assert any("left\nempty" in n or "left empty" in n for n in response["meta"]["notes"])


def test_alignment_is_reported(ctx: McpContext) -> None:
    response = get_symbol_profile(ctx, "AAA")
    assert response["meta"]["alignment"] in {
        "ALIGNED",
        "PARTIALLY_STALE",
        "STALE",
        "INCOMPLETE",
    }
    assert "max_block_spread_days" in response["meta"]


def test_profile_before_everything_is_empty(ctx: McpContext) -> None:
    response = get_symbol_profile(ctx, "AAA", as_of="2019-01-01")
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA
    assert all(
        response["data"][block] is None
        for block in ("quote", "stage", "rank", "fundamentals")
    )


def test_unknown_symbol_profile_explains_itself(ctx: McpContext) -> None:
    response = get_symbol_profile(ctx, "ZZZ")
    assert response["data"]["identity"] is None
    assert response["meta"]["blocks"]["identity"]["notes"]


# ---------------------------------------------------------------------------
# screen_universe
# ---------------------------------------------------------------------------


def test_screen_returns_the_ranked_cross_section(ctx: McpContext) -> None:
    response = screen_universe(ctx)
    symbols = [row["symbol_id"] for row in response["data"]]
    assert symbols == ["AAA", "BBB"]
    assert response["meta"]["universe_size"] == 2


def test_screen_joins_stage_and_sector(ctx: McpContext) -> None:
    row = next(r for r in screen_universe(ctx)["data"] if r["symbol_id"] == "AAA")
    assert row["stage_label"] == "transition_1_to_2"
    assert row["stage_family"] == "stage_1"
    assert row["sector_name"] == "Capital Goods"


def test_screen_filters_by_exact_stage_in_either_vocabulary(ctx: McpContext) -> None:
    canonical = screen_universe(ctx, stage_label="transition_1_to_2")
    assert [row["symbol_id"] for row in canonical["data"]] == ["AAA"]

    # A legacy S2 filter must not accidentally match a transition.
    legacy = screen_universe(ctx, stage_label="S2")
    assert legacy["data"] == []


def test_screen_family_filter_admits_transitions(ctx: McpContext) -> None:
    """stage_family is how you ask the looser structural question."""

    response = screen_universe(ctx, stage_family_filter="stage_1")
    assert [row["symbol_id"] for row in response["data"]] == ["AAA"]


def test_screen_filters_by_sector(ctx: McpContext) -> None:
    response = screen_universe(ctx, sector="capital goods")
    assert [row["symbol_id"] for row in response["data"]] == ["AAA"]


def test_screen_filters_compose(ctx: McpContext) -> None:
    response = screen_universe(
        ctx, sector="Capital Goods", min_composite_score=70, max_rank_position=20
    )
    assert [row["symbol_id"] for row in response["data"]] == ["AAA"]
    assert response["meta"]["filters"]["min_composite_score"] == 70


def test_screen_score_filter_excludes(ctx: McpContext) -> None:
    assert screen_universe(ctx, min_composite_score=99)["data"] == []


def test_screen_sorting(ctx: McpContext) -> None:
    by_score = screen_universe(ctx, sort_by="composite_score")
    assert [row["symbol_id"] for row in by_score["data"]] == ["AAA", "BBB"]
    assert by_score["meta"]["sort_by"] == "composite_score"


def test_screen_rejects_unknown_sort(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="Unknown sort_by"):
        screen_universe(ctx, sort_by="pe_ratio")


def test_screen_limit_is_capped_and_reported(ctx: McpContext) -> None:
    response = screen_universe(ctx, limit=1)
    assert len(response["data"]) == 1
    assert response["meta"]["matched_count"] == 2
    assert response["meta"]["truncated"] is True
    assert any("matched; showing" in n for n in response["meta"]["notes"])


def test_screen_as_of_is_point_in_time(ctx: McpContext) -> None:
    response = screen_universe(ctx, as_of="2026-01-06")
    assert response["meta"]["as_of_status"] == AS_OF_EXACT
    assert all(row["trade_date"] == "2026-01-06" for row in response["data"])
    row = next(r for r in response["data"] if r["symbol_id"] == "AAA")
    assert row["rank_position"] == 11
    # The governed stage store only had stage_1_basing by then.
    assert row["stage_label"] == "stage_1_basing"


def test_screen_before_coverage_is_empty(ctx: McpContext) -> None:
    response = screen_universe(ctx, as_of="2020-01-01")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA


def test_screen_unknown_universe_is_empty(ctx: McpContext) -> None:
    response = screen_universe(ctx, universe_id="NO_SUCH_UNIVERSE")
    assert response["data"] == []


# ---------------------------------------------------------------------------
# sectors
# ---------------------------------------------------------------------------


def test_sector_overview_reports_stage_distribution(ctx: McpContext) -> None:
    response = get_sector_overview(ctx)
    row = next(r for r in response["data"] if r["sector_name"] == "Capital Goods")
    assert row["constituents_observed"] == 1
    assert row["stage_1_count"] == 1
    assert row["stage_1_pct"] == 100.0
    assert row["in_transition"] == 1


def test_sector_overview_is_point_in_time(ctx: McpContext) -> None:
    response = get_sector_overview(ctx, as_of="2026-01-02")
    assert response["meta"]["as_of_status"] == AS_OF_EXACT
    assert response["meta"]["as_of_effective"] == "2026-01-02"
    row = response["data"][0]
    assert row["in_transition"] == 0


def test_sector_overview_before_coverage_is_empty(ctx: McpContext) -> None:
    response = get_sector_overview(ctx, as_of="2020-01-01")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA
    assert any("No governed stage observations" in n for n in response["meta"]["notes"])


def test_sector_overview_states_what_it_does_not_include(ctx: McpContext) -> None:
    """Rank-artifact sector RS is latest-only and deliberately absent."""

    response = get_sector_overview(ctx)
    assert any("latest-only" in note for note in response["meta"]["notes"])


def test_sector_constituents_merge_stage_master_and_rank(ctx: McpContext) -> None:
    response = get_sector_constituents(ctx, "Capital Goods")
    row = response["data"][0]
    assert row["symbol_id"] == "AAA"
    assert row["symbol_name"] == "Alpha Industries Ltd"
    assert row["stage_label"] == "transition_1_to_2"
    assert row["rank_position"] == 8
    assert row["mcap"] == 125000.0


def test_sector_constituents_are_point_in_time(ctx: McpContext) -> None:
    response = get_sector_constituents(ctx, "Capital Goods", as_of="2026-01-06")
    row = response["data"][0]
    assert row["rank_position"] == 11
    assert row["stage_label"] == "stage_1_basing"


def test_sector_constituents_case_insensitive(ctx: McpContext) -> None:
    assert get_sector_constituents(ctx, "capital goods")["data"]


def test_unknown_sector_lists_the_known_ones(ctx: McpContext) -> None:
    response = get_sector_constituents(ctx, "Widgets")
    assert response["data"] == []
    assert any("Known sectors" in note for note in response["meta"]["notes"])


def test_blank_sector_is_rejected(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="sector is required"):
        get_sector_constituents(ctx, "  ")


def test_sector_tools_report_the_stage_vocabulary(ctx: McpContext) -> None:
    assert get_sector_overview(ctx)["meta"]["stage_vocabulary"] == "WeinsteinStage"
    assert (
        get_sector_constituents(ctx, "Metals")["meta"]["stage_vocabulary"]
        == "WeinsteinStage"
    )
