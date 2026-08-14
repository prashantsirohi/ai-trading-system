"""Tests for the technicals, rank and fundamentals tools."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
)
from ai_trading_system.interfaces.mcp.tools.fundamentals import get_fundamentals
from ai_trading_system.interfaces.mcp.tools.rank import get_rank_detail, get_rank_history
from ai_trading_system.interfaces.mcp.tools.technicals import get_technical_features


# ---------------------------------------------------------------------------
# technicals
# ---------------------------------------------------------------------------


def test_merges_families_into_one_row_per_session(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA")
    assert len(response["data"]) == 5
    row = response["data"][-1]
    assert row["date"] == "2026-01-09"
    assert row["rsi_14"] == 67.0
    assert row["sma_200"] == 82.0
    assert row["adx_14"] == 30.0


def test_phase1_features_are_included(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA")
    assert response["meta"]["phase1_included"] is True
    row = response["data"][-1]
    assert row["beta_to_nifty_60"] == 1.12
    assert row["liquidity_score"] == 0.81
    assert row["delivery_trend_score"] == 0.62


def test_phase1_can_be_excluded(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA", include_phase1=False)
    assert response["meta"]["phase1_included"] is False
    assert "beta_to_nifty_60" not in response["data"][-1]


def test_family_selection_is_reported(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA", families=["rsi", "sma"])
    assert set(response["meta"]["families"]) == {"rsi", "sma"}
    assert "adx_14" not in response["data"][-1]
    assert any("were skipped" not in n for n in response["meta"]["notes"] or [""])


def test_declares_the_adjusted_price_basis(ctx: McpContext) -> None:
    """Indicators are computed on adjusted prices; say so explicitly."""

    assert get_technical_features(ctx, "AAA")["meta"]["price_basis"] == "adjusted"


def test_technicals_as_of_cutoff(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA", as_of="2026-01-06")
    assert [row["date"] for row in response["data"]] == ["2026-01-05", "2026-01-06"]
    assert response["meta"]["as_of_status"] == AS_OF_EXACT


def test_technicals_before_coverage_is_empty(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA", as_of="2020-01-01")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA


def test_symbol_without_partitions_explains_itself(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "BBB")
    assert response["data"] == []
    assert any("No technical feature partitions" in n for n in response["meta"]["notes"])


def test_technicals_limit_keeps_the_tail(ctx: McpContext) -> None:
    response = get_technical_features(ctx, "AAA", limit=2)
    assert [row["date"] for row in response["data"]] == ["2026-01-08", "2026-01-09"]
    assert response["meta"]["truncated"] is True


# ---------------------------------------------------------------------------
# rank
# ---------------------------------------------------------------------------


def test_rank_detail_groups_the_factor_breakdown(ctx: McpContext) -> None:
    data = get_rank_detail(ctx, "AAA")["data"]
    assert set(data) == {"identity", "position", "factors", "provenance", "other"}
    assert data["position"]["rank_position"] == 8
    assert data["position"]["composite_score"] == 74.0
    assert data["factors"]["rs_score"] == 80.0
    assert data["factors"]["trend_score"] == 70.0
    assert data["provenance"]["rank_model_version"] == "v1"


def test_rank_detail_takes_the_newest_row_not_the_oldest(ctx: McpContext) -> None:
    """The bug an ascending ORDER BY + LIMIT would introduce."""

    data = get_rank_detail(ctx, "AAA")["data"]
    assert data["identity"]["trade_date"] == "2026-01-09"
    assert data["position"]["rank_position"] == 8


def test_rank_detail_as_of_cutoff(ctx: McpContext) -> None:
    response = get_rank_detail(ctx, "AAA", as_of="2026-01-06")
    assert response["data"]["identity"]["trade_date"] == "2026-01-06"
    assert response["data"]["position"]["rank_position"] == 11
    assert response["meta"]["as_of_status"] == AS_OF_EXACT


def test_rank_detail_before_coverage_returns_nothing(ctx: McpContext) -> None:
    response = get_rank_detail(ctx, "AAA", as_of="2020-01-01")
    assert response["data"] is None
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA
    assert any("does not include" in n for n in response["meta"]["notes"])


def test_symbol_that_rotated_out_still_returns_its_last_ranking(
    ctx: McpContext,
) -> None:
    """A rotating top-N universe must not look like 'never ranked'.

    ROTATOR was ranked through 2026-01-08 but not on the latest session. The
    effective date is resolved per symbol, so its last known ranking is
    returned with an honest as_of_effective rather than an empty answer.
    """

    response = get_rank_detail(ctx, "ROTATOR")
    assert response["data"] is not None
    assert response["data"]["identity"]["trade_date"] == "2026-01-08"
    assert response["data"]["position"]["rank_position"] == 44
    assert response["meta"]["as_of_effective"] == "2026-01-08"
    # ... while a genuinely current symbol still reports the latest session.
    assert get_rank_detail(ctx, "AAA")["meta"]["as_of_effective"] == "2026-01-09"


def test_never_ranked_symbol_says_so(ctx: McpContext) -> None:
    response = get_rank_detail(ctx, "CCC", exchange="BSE")
    assert response["data"] is None
    assert any("never been ranked" in n for n in response["meta"]["notes"])


def test_rank_history_is_ascending_and_bounded(ctx: McpContext) -> None:
    response = get_rank_history(ctx, "AAA")
    dates = [row["trade_date"] for row in response["data"]]
    assert dates == sorted(dates)
    assert dates[-1] == "2026-01-09"
    assert response["meta"]["as_of_status"] == AS_OF_LATEST


def test_rank_history_limit_keeps_the_most_recent(ctx: McpContext) -> None:
    response = get_rank_history(ctx, "AAA", limit=2)
    assert [row["trade_date"] for row in response["data"]] == [
        "2026-01-08",
        "2026-01-09",
    ]


def test_rank_history_as_of_cutoff(ctx: McpContext) -> None:
    response = get_rank_history(ctx, "AAA", as_of="2026-01-07")
    assert [row["trade_date"] for row in response["data"]] == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
    ]


def test_rank_is_exchange_aware(ctx: McpContext) -> None:
    assert get_rank_detail(ctx, "AAA", exchange="BSE")["data"] is None


def test_rank_source_names_the_history_table(ctx: McpContext) -> None:
    """Not the CSV artifact, whose promotion state would need checking."""

    assert get_rank_detail(ctx, "AAA")["meta"]["source"] == (
        "control_plane.duckdb:rank_history"
    )


# ---------------------------------------------------------------------------
# fundamentals
# ---------------------------------------------------------------------------


def test_all_blocks_are_present(ctx: McpContext) -> None:
    data = get_fundamentals(ctx, "AAA")["data"]
    assert set(data) == {
        "company",
        "scores",
        "snapshot",
        "growth",
        "financials",
        "valuation",
    }


def test_cutoff_is_publication_date_not_fiscal_period(ctx: McpContext) -> None:
    """The Dec-2025 quarter was published in Feb 2026 and must not appear."""

    january = get_fundamentals(ctx, "AAA", as_of="2026-01-09")["data"]
    assert {row["report_date"] for row in january["financials"]} == {"2025-09-30"}
    assert {row["report_date"] for row in january["growth"]} == {"2025-09-30"}
    assert january["scores"]["fundamental_tier"] == "B"
    assert january["snapshot"]["pe"] == 28.0
    assert january["company"]["market_cap_cr"] == 118000.0


def test_after_publication_the_newer_period_appears(ctx: McpContext) -> None:
    march = get_fundamentals(ctx, "AAA", as_of="2026-03-01")["data"]
    assert {row["report_date"] for row in march["financials"]} == {
        "2025-09-30",
        "2025-12-31",
    }
    assert march["scores"]["fundamental_tier"] == "A"
    assert march["snapshot"]["pe"] == 30.5


def test_statement_basis_is_echoed_and_honoured(ctx: McpContext) -> None:
    response = get_fundamentals(ctx, "AAA", statement_basis="consolidated")
    assert response["meta"]["statement_basis"] == "consolidated"
    assert response["meta"]["available_statement_bases"] == [
        "consolidated",
        "standalone",
    ]
    sales = [
        row for row in response["data"]["financials"] if row["metric_id"] == "sales"
    ]
    assert all(row["statement_basis"] == "consolidated" for row in sales)
    assert sales[-1]["value"] == 1400.0


def test_default_basis_is_standalone(ctx: McpContext) -> None:
    assert get_fundamentals(ctx, "AAA")["meta"]["statement_basis"] == "standalone"


def test_as_of_basis_is_declared_per_block(ctx: McpContext) -> None:
    """The agent must be able to see which column each cutoff used."""

    basis = get_fundamentals(ctx, "AAA")["meta"]["as_of_basis"]
    assert basis["financials"] == "screener_financials.available_at"
    assert "export date" in basis["scores"]


def test_before_any_publication_returns_empty_blocks(ctx: McpContext) -> None:
    response = get_fundamentals(ctx, "AAA", as_of="2025-01-01")
    assert response["meta"]["as_of_status"] == AS_OF_NO_DATA
    assert response["data"]["financials"] == []
    assert response["data"]["scores"] is None
    assert any("No fundamental evidence" in n for n in response["meta"]["notes"])


def test_unsupported_basis_is_rejected(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="Unsupported statement basis"):
        get_fundamentals(ctx, "AAA", statement_basis="proforma")


def test_growth_rows_carry_both_dates(ctx: McpContext) -> None:
    row = get_fundamentals(ctx, "AAA", as_of="2026-03-01")["data"]["growth"][-1]
    assert row["report_date"] == "2025-12-31"
    assert row["available_at"] == "2026-02-14"
    assert row["profit_yoy_growth"] == 22.0
