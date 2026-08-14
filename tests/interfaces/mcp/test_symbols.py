"""Tests for resolve_symbol's identity contract."""

from __future__ import annotations

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import AS_OF_LATEST, AS_OF_UNSUPPORTED
from ai_trading_system.interfaces.mcp.tools.symbols import resolve_symbol


def test_returns_a_candidate_list_not_a_single_row(ctx: McpContext) -> None:
    response = resolve_symbol(ctx, "AAA")
    assert isinstance(response["data"], list)
    assert response["meta"]["candidate_count"] == 1
    assert response["meta"]["ambiguous"] is False


def test_candidates_carry_full_identity(ctx: McpContext) -> None:
    """exchange + security_id + mcap are what disambiguate a listing."""

    row = resolve_symbol(ctx, "AAA")["data"][0]
    for field in (
        "symbol_id",
        "exchange",
        "security_id",
        "isin",
        "symbol_name",
        "sector",
        "industry",
        "mcap",
        "match_type",
    ):
        assert field in row, field
    assert row["exchange"] == "NSE"


def test_name_lookup_works(ctx: McpContext) -> None:
    response = resolve_symbol(ctx, "Beta Metals")
    assert response["data"][0]["symbol_id"] == "BBB"
    assert response["meta"]["best_match_type"] == "name_contains"


def test_isin_lookup_works(ctx: McpContext) -> None:
    assert resolve_symbol(ctx, "INE000C01003")["data"][0]["symbol_id"] == "CCC"


def test_bse_only_listing_is_found_with_its_exchange(ctx: McpContext) -> None:
    row = resolve_symbol(ctx, "CCC")["data"][0]
    assert row["exchange"] == "BSE"


def test_unknown_query_returns_an_explained_empty_result(ctx: McpContext) -> None:
    response = resolve_symbol(ctx, "NOSUCHTHING")
    assert response["data"] == []
    assert response["meta"]["candidate_count"] == 0
    assert any("No master row matches" in note for note in response["meta"]["notes"])


def test_as_of_is_refused_rather_than_answered_with_the_present(
    ctx: McpContext,
) -> None:
    """The master has no history, so a historical read must return nothing."""

    response = resolve_symbol(ctx, "AAA", as_of="2020-01-01")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == AS_OF_UNSUPPORTED
    assert response["meta"]["as_of_effective"] is None
    assert any("current state only" in note for note in response["meta"]["notes"])


def test_without_as_of_the_status_is_latest(ctx: McpContext) -> None:
    assert resolve_symbol(ctx, "AAA")["meta"]["as_of_status"] == AS_OF_LATEST


def test_limit_is_clamped(ctx: McpContext) -> None:
    response = resolve_symbol(ctx, "A", limit=10_000)
    assert response["meta"]["candidate_count"] <= 100


def test_meta_names_the_master_store(ctx: McpContext) -> None:
    assert resolve_symbol(ctx, "AAA")["meta"]["source"] == "masterdata.db:symbols"


def test_ambiguity_is_surfaced_when_listings_tie(
    ctx: McpContext, connection_guard, data_root
) -> None:
    """A dual listing must never be silently collapsed to one exchange."""

    import sqlite3

    with connection_guard.paused():
        conn = sqlite3.connect(str(data_root / "masterdata.db"))
        conn.execute(
            "INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "DUAL", "SECDUAL1", "Dual Listed Ltd", "NSE", "EQUITY",
                "INE000D01004", 1, 0.05, 0, "Chemicals", "Specialty",
                "DUAL", "500004", 7000.0, "2026-01-09",
            ),
        )
        conn.commit()
        conn.close()

    response = resolve_symbol(ctx, "INE000D01004")
    assert response["meta"]["candidate_count"] == 1

    # Now make the ISIN genuinely ambiguous across exchanges.
    with connection_guard.paused():
        conn = sqlite3.connect(str(data_root / "masterdata.db"))
        conn.execute(
            "INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "DUALB", "SECDUAL2", "Dual Listed Ltd", "BSE", "EQUITY",
                "INE000D01004", 1, 0.05, 0, "Chemicals", "Specialty",
                None, "500004", 7000.0, "2026-01-09",
            ),
        )
        conn.commit()
        conn.close()

    response = resolve_symbol(ctx, "INE000D01004")
    assert response["meta"]["candidate_count"] == 2
    assert response["meta"]["ambiguous"] is True
    assert {row["exchange"] for row in response["data"]} == {"NSE", "BSE"}
    assert any("Pass an explicit 'exchange'" in n for n in response["meta"]["notes"])
