"""Tests for get_ohlcv, including the price-basis and point-in-time contracts."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
)
from ai_trading_system.interfaces.mcp.tools.prices import get_ohlcv


def test_defaults_to_the_adjusted_basis(ctx: McpContext) -> None:
    """The default basis must match what the feature store is computed on."""

    response = get_ohlcv(ctx, "AAA")
    assert response["meta"]["price_basis"] == "adjusted"
    first = response["data"][0]
    assert first["date"] == "2026-01-05"
    assert first["close"] == 100.0


def test_raw_basis_is_available_and_labelled(ctx: McpContext) -> None:
    """Raw prices stay reachable, but never silently."""

    response = get_ohlcv(ctx, "AAA", adjusted=False)
    assert response["meta"]["price_basis"] == "raw"
    assert response["data"][0]["close"] == 200.0
    assert response["meta"]["source"].endswith(":_catalog")


def test_the_two_bases_actually_differ_before_the_split(ctx: McpContext) -> None:
    """Guards against the view silently collapsing to raw prices."""

    adjusted = {row["date"]: row["close"] for row in get_ohlcv(ctx, "AAA")["data"]}
    raw = {
        row["date"]: row["close"]
        for row in get_ohlcv(ctx, "AAA", adjusted=False)["data"]
    }
    assert adjusted["2026-01-05"] == 100.0 and raw["2026-01-05"] == 200.0
    # After the corporate action the two bases converge.
    assert adjusted["2026-01-09"] == raw["2026-01-09"] == 120.0


def test_symbol_is_normalized(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "  aaa  ")
    assert response["meta"]["symbol"] == "AAA"
    assert response["data"]


def test_exchange_is_part_of_identity(ctx: McpContext) -> None:
    """A BSE listing must not leak into an NSE answer."""

    nse = get_ohlcv(ctx, "AAA", exchange="NSE")
    bse = get_ohlcv(ctx, "AAA", exchange="BSE")
    assert len(nse["data"]) == 5
    assert len(bse["data"]) == 2
    assert bse["data"][-1]["close"] == 119.5
    assert nse["data"][-1]["close"] == 120.0


def test_rejects_an_unknown_exchange(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="Unsupported exchange"):
        get_ohlcv(ctx, "AAA", exchange="NYSE")


def test_delivery_is_joined(ctx: McpContext) -> None:
    assert get_ohlcv(ctx, "AAA")["data"][0]["delivery_pct"] == 45.5


def test_no_as_of_reports_latest(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA")
    assert response["meta"]["as_of_status"] == AS_OF_LATEST
    assert response["meta"]["as_of_requested"] is None
    assert response["meta"]["as_of_effective"] == "2026-01-09"


def test_as_of_cuts_off_and_never_returns_later_rows(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA", as_of="2026-01-07")
    assert response["meta"]["as_of_status"] == AS_OF_EXACT
    assert response["meta"]["as_of_effective"] == "2026-01-07"
    assert [row["date"] for row in response["data"]] == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
    ]


def test_as_of_before_any_data_returns_no_rows(ctx: McpContext) -> None:
    """Never the present as a substitute for the requested past."""

    response = get_ohlcv(ctx, "AAA", as_of="2020-01-01")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA
    assert response["meta"]["as_of_effective"] is None
    # Coverage still shows data exists, just not that far back.
    assert response["meta"]["coverage"]["first"] == "2026-01-05"


def test_as_of_tightens_to_date(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA", to_date="2026-01-09", as_of="2026-01-06")
    assert [row["date"] for row in response["data"]] == ["2026-01-05", "2026-01-06"]


def test_from_date_window(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA", from_date="2026-01-08")
    assert [row["date"] for row in response["data"]] == ["2026-01-08", "2026-01-09"]


def test_limit_keeps_the_most_recent_rows_in_order(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA", limit=2)
    assert [row["date"] for row in response["data"]] == ["2026-01-08", "2026-01-09"]
    assert response["meta"]["truncated"] is True
    assert response["meta"]["notes"]


def test_limit_is_clamped_not_trusted(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA", limit=10_000_000)
    assert response["meta"]["truncated"] is False
    assert response["meta"]["row_count"] == 5


def test_unknown_symbol_is_an_empty_answer_with_a_reason(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "ZZZ")
    assert response["data"] == []
    assert response["meta"]["coverage"] == {"first": None, "last": None}
    assert any("No OHLCV is stored" in note for note in response["meta"]["notes"])


def test_meta_names_the_file_and_table(ctx: McpContext) -> None:
    response = get_ohlcv(ctx, "AAA")
    assert response["meta"]["source"] == "ohlcv.duckdb:_catalog_feature_source"
    assert response["meta"]["data_domain"] == "operational"
