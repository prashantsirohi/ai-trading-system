"""Tests for get_stage_history across the three stage stores."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
)
from ai_trading_system.interfaces.mcp.tools.stage import (
    GRANULARITIES,
    get_stage_history,
)


def test_defaults_to_the_governed_store(ctx: McpContext) -> None:
    """The default must be the store with live coverage, not the stale one."""

    response = get_stage_history(ctx, "AAA")
    assert response["meta"]["granularity"] == "weekly_governed"
    assert response["meta"]["source"] == (
        "control_plane.duckdb:weekly_stock_stage_history"
    )
    assert len(response["data"]) == 2


@pytest.mark.parametrize("granularity", GRANULARITIES)
def test_every_granularity_names_its_store_and_coverage(
    ctx: McpContext, granularity: str
) -> None:
    response = get_stage_history(ctx, "AAA", granularity=granularity)
    assert response["meta"]["granularity"] == granularity
    assert ":" in response["meta"]["source"]
    assert set(response["meta"]["coverage"]) == {"first", "last"}
    assert response["data"], "each fixture store holds at least one row"


def test_rejects_an_unknown_granularity(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="Unknown granularity"):
        get_stage_history(ctx, "AAA", granularity="hourly")


# ---------------------------------------------------------------------------
# Stage vocabulary on the wire
# ---------------------------------------------------------------------------


def test_rows_carry_both_spellings(ctx: McpContext) -> None:
    """Neither a canonical nor a legacy filter should miss rows."""

    row = get_stage_history(ctx, "AAA", granularity="daily")["data"][-1]
    assert row["stage_label"] == "stage_2_advancing"
    assert row["stage_label_legacy"] == "S2"
    assert row["stage_family"] == "stage_2"
    assert row["is_transition"] is False


def test_a_transition_has_no_legacy_code_but_keeps_a_family(ctx: McpContext) -> None:
    """The case the legacy vocabulary cannot express at all."""

    row = get_stage_history(ctx, "AAA")["data"][-1]
    assert row["stage_label"] == "transition_1_to_2"
    assert row["stage_label_legacy"] is None
    assert row["stage_family"] == "stage_1"
    assert row["is_transition"] is True


def test_legacy_store_rows_are_translated_forward(ctx: McpContext) -> None:
    """The legacy store speaks S1..S4; output is still canonical."""

    row = get_stage_history(ctx, "AAA", granularity="weekly_legacy")["data"][-1]
    assert row["stage_label"] == "stage_2_advancing"
    assert row["stage_label_legacy"] == "S2"
    assert row["bars_in_stage"] == 4
    assert row["stage_transition"] == "S1_TO_S2"


def test_vocabulary_is_declared_in_meta(ctx: McpContext) -> None:
    assert get_stage_history(ctx, "AAA")["meta"]["stage_vocabulary"] == "WeinsteinStage"


# ---------------------------------------------------------------------------
# Point-in-time
# ---------------------------------------------------------------------------


def test_as_of_cuts_off_governed_rows(ctx: McpContext) -> None:
    response = get_stage_history(ctx, "AAA", as_of="2026-01-02")
    assert response["meta"]["as_of_status"] == AS_OF_EXACT
    assert [row["observation_date"] for row in response["data"]] == ["2026-01-02"]
    assert response["data"][0]["stage_label"] == "stage_1_basing"


def test_as_of_before_coverage_returns_no_rows_and_says_why(ctx: McpContext) -> None:
    response = get_stage_history(ctx, "AAA", as_of="2025-06-01")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA
    assert any("does not include" in note for note in response["meta"]["notes"])


def test_no_as_of_reports_latest(ctx: McpContext) -> None:
    response = get_stage_history(ctx, "AAA")
    assert response["meta"]["as_of_status"] == AS_OF_LATEST
    assert response["meta"]["as_of_effective"] == "2026-01-09"


def test_the_stale_legacy_store_is_visibly_stale(ctx: McpContext) -> None:
    """The trap: legacy coverage stops before the governed store starts."""

    legacy = get_stage_history(ctx, "AAA", granularity="weekly_legacy")
    governed = get_stage_history(ctx, "AAA")
    assert legacy["meta"]["coverage"]["last"] == "2025-12-26"
    assert governed["meta"]["coverage"]["last"] == "2026-01-09"

    # Asking the legacy store about January returns nothing, with an
    # explanation rather than a bare empty list.
    january = get_stage_history(
        ctx, "AAA", granularity="weekly_legacy", from_date="2026-01-01"
    )
    assert january["data"] == []
    assert january["meta"]["notes"]


def test_legacy_store_flags_its_missing_exchange_column(ctx: McpContext) -> None:
    response = get_stage_history(ctx, "AAA", granularity="weekly_legacy")
    assert any("no exchange column" in note for note in response["meta"]["notes"])


def test_date_window_applies(ctx: McpContext) -> None:
    response = get_stage_history(
        ctx, "AAA", granularity="daily", from_date="2026-01-07", to_date="2026-01-08"
    )
    assert [row["observation_date"] for row in response["data"]] == [
        "2026-01-07",
        "2026-01-08",
    ]


def test_limit_keeps_the_most_recent_rows(ctx: McpContext) -> None:
    """A long history must not hide the present behind an ascending LIMIT."""

    response = get_stage_history(ctx, "AAA", granularity="daily", limit=2)
    assert [row["observation_date"] for row in response["data"]] == [
        "2026-01-08",
        "2026-01-09",
    ]


def test_exchange_is_respected(ctx: McpContext) -> None:
    response = get_stage_history(ctx, "AAA", exchange="BSE")
    assert response["data"] == []
    assert response["meta"]["exchange"] == "BSE"


def test_unknown_symbol_explains_itself(ctx: McpContext) -> None:
    response = get_stage_history(ctx, "ZZZ")
    assert response["data"] == []
    assert any("holds no stage rows" in note for note in response["meta"]["notes"])
