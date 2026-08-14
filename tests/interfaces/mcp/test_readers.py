"""Tests for the MCP-owned readers that replace read-write dependencies."""

from __future__ import annotations

import pytest

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.readers import featurestore, master, screener


# ---------------------------------------------------------------------------
# master
# ---------------------------------------------------------------------------


def test_exact_symbol_match_wins(ctx: McpContext) -> None:
    candidates = master.search_symbols(ctx, "AAA")
    assert [row["match_type"] for row in candidates] == [master.MATCH_EXACT_SYMBOL]
    assert candidates[0]["exchange"] == "NSE"


def test_search_carries_the_identity_fields_symbolmaster_lacks(ctx: McpContext) -> None:
    """exchange, security_id and mcap are exactly what dual listings need."""

    row = master.search_symbols(ctx, "AAA")[0]
    assert row["exchange"] == "NSE"
    assert row["security_id"] == "SECAAA"
    assert row["mcap"] == 125000.0
    assert row["isin"] == "INE000A01001"


def test_isin_resolves(ctx: McpContext) -> None:
    candidates = master.search_symbols(ctx, "INE000B01002")
    assert candidates[0]["symbol_id"] == "BBB"
    assert candidates[0]["match_type"] == master.MATCH_EXACT_ISIN


def test_security_id_resolves(ctx: McpContext) -> None:
    candidates = master.search_symbols(ctx, "SECCCC")
    assert candidates[0]["symbol_id"] == "CCC"
    assert candidates[0]["match_type"] == master.MATCH_EXACT_SECURITY_ID


def test_name_search_falls_through_to_the_last_tier(ctx: McpContext) -> None:
    candidates = master.search_symbols(ctx, "Metals")
    assert candidates[0]["symbol_id"] == "BBB"
    assert candidates[0]["match_type"] == master.MATCH_NAME_CONTAINS


def test_search_is_case_and_whitespace_insensitive(ctx: McpContext) -> None:
    assert master.search_symbols(ctx, "  aaa ")[0]["symbol_id"] == "AAA"


def test_blank_query_returns_nothing(ctx: McpContext) -> None:
    assert master.search_symbols(ctx, "   ") == []


def test_search_query_cannot_inject_sql(ctx: McpContext) -> None:
    """Agent input is bound, never interpolated."""

    assert master.search_symbols(ctx, "'; DROP TABLE symbols; --") == []
    with ctx.sqlite(ctx.master_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0] == 3


def test_get_symbol_record_is_exchange_aware(ctx: McpContext) -> None:
    assert master.get_symbol_record(ctx, "CCC", "BSE")["symbol_id"] == "CCC"
    assert master.get_symbol_record(ctx, "CCC", "NSE") is None


def test_sector_members_and_listing(ctx: McpContext) -> None:
    members = master.sector_members(ctx, "capital goods")
    assert [row["symbol_id"] for row in members] == ["AAA"]
    assert master.list_sectors(ctx) == ["Capital Goods", "Metals"]
    assert master.sector_by_symbol(ctx)["AAA"] == "Capital Goods"


# ---------------------------------------------------------------------------
# featurestore
# ---------------------------------------------------------------------------


def test_reads_and_merges_families(ctx: McpContext) -> None:
    frame, present = featurestore.read_symbol_features(ctx, "AAA")
    assert set(present) == {"rsi", "sma", "adx"}
    assert {"rsi_14", "sma_200", "adx_14"} <= set(frame.columns)
    assert len(frame) == 5
    # Identity columns appear exactly once despite three merged families.
    assert list(frame.columns).count("symbol_id") == 1


def test_family_selection_is_validated(ctx: McpContext) -> None:
    frame, present = featurestore.read_symbol_features(ctx, "AAA", families=["rsi"])
    assert present == ["rsi"]
    assert "sma_200" not in frame.columns

    with pytest.raises(featurestore.FeaturePathError, match="Unknown feature families"):
        featurestore.read_symbol_features(ctx, "AAA", families=["../../etc"])


def test_date_window_is_applied(ctx: McpContext) -> None:
    frame, _ = featurestore.read_symbol_features(
        ctx, "AAA", from_date="2026-01-07", to_date="2026-01-08"
    )
    assert len(frame) == 2


def test_missing_partitions_are_skipped_not_fatal(ctx: McpContext) -> None:
    frame, present = featurestore.read_symbol_features(ctx, "BBB")
    assert present == []
    assert frame.empty


def test_path_traversal_is_rejected(ctx: McpContext) -> None:
    with pytest.raises(featurestore.FeaturePathError, match="escapes"):
        featurestore.contained_path(ctx, "..", "..", "etc", "passwd")


def test_cross_section_takes_the_latest_row_per_symbol(ctx: McpContext) -> None:
    frame = featurestore.read_latest_cross_section(ctx, "rsi")
    assert list(frame["symbol_id"]) == ["AAA"]
    assert frame.iloc[0]["rsi_14"] == 67.0


def test_cross_section_respects_the_cutoff(ctx: McpContext) -> None:
    frame = featurestore.read_latest_cross_section(ctx, "rsi", cutoff="2026-01-06")
    assert frame.iloc[0]["rsi_14"] == 58.0


def test_cross_section_of_an_absent_exchange_is_empty(ctx: McpContext) -> None:
    assert featurestore.read_latest_cross_section(ctx, "rsi", exchange="BSE").empty


# ---------------------------------------------------------------------------
# screener
# ---------------------------------------------------------------------------


def test_financials_cut_off_on_publication_not_period(ctx: McpContext) -> None:
    """The Dec-2025 quarter was published in Feb 2026, so Jan cannot see it."""

    january = screener.financials(ctx, "AAA", as_of="2026-01-09")
    periods = {row["report_date"] for row in january}
    assert periods == {"2025-09-30"}

    march = screener.financials(ctx, "AAA", as_of="2026-03-01")
    assert {row["report_date"] for row in march} == {"2025-09-30", "2025-12-31"}


def test_financials_never_leak_a_future_publication(ctx: McpContext) -> None:
    rows = screener.financials(ctx, "AAA", as_of="2026-01-09")
    assert all(row["available_at"] <= "2026-01-09" for row in rows)


def test_statement_bases_are_not_blended(ctx: McpContext) -> None:
    standalone = screener.financials(ctx, "AAA", as_of="2026-03-01")
    consolidated = screener.financials(
        ctx, "AAA", statement_basis="consolidated", as_of="2026-03-01"
    )
    assert all(row["statement_basis"] == "standalone" for row in standalone)
    assert all(row["statement_basis"] == "consolidated" for row in consolidated)

    sales = {
        row["report_date"]: row["value"]
        for row in standalone
        if row["metric_id"] == "sales"
    }
    consolidated_sales = {
        row["report_date"]: row["value"]
        for row in consolidated
        if row["metric_id"] == "sales"
    }
    assert sales["2025-12-31"] == 1000.0
    assert consolidated_sales["2025-12-31"] == 1400.0


def test_available_bases_reports_what_is_stored(ctx: McpContext) -> None:
    assert screener.available_bases(ctx, "AAA") == ["consolidated", "standalone"]


def test_unsupported_basis_is_rejected(ctx: McpContext) -> None:
    with pytest.raises(ValueError, match="Unsupported statement basis"):
        screener.financials(ctx, "AAA", statement_basis="proforma")


def test_company_snapshot_respects_the_cutoff(ctx: McpContext) -> None:
    early = screener.company_snapshot(ctx, "AAA", as_of="2026-01-09")
    assert early["as_of_date"] == "2025-11-15"
    assert early["market_cap_cr"] == 118000.0

    later = screener.company_snapshot(ctx, "AAA")
    assert later["as_of_date"] == "2026-02-14"


def test_market_valuation_respects_the_cutoff(ctx: McpContext) -> None:
    assert screener.market_valuation(ctx, "AAA", as_of="2026-01-05")["pe"] == 28.0
    assert screener.market_valuation(ctx, "AAA")["pe"] == 30.5


def test_metric_names_are_joined_from_the_catalog(ctx: McpContext) -> None:
    rows = screener.financials(ctx, "AAA", as_of="2026-03-01")
    names = {row["metric_id"]: row["metric_name"] for row in rows}
    assert names["net_profit"] == "Net Profit"


def test_unknown_symbol_reads_empty(ctx: McpContext) -> None:
    assert screener.financials(ctx, "ZZZ") == []
    assert screener.company_snapshot(ctx, "ZZZ") is None
    assert screener.market_valuation(ctx, "ZZZ") is None
