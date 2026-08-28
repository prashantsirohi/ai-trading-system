"""Contract and transport tests for the private journal MCP."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ai_trading_system.domains.trade_journal.store import TradeJournalStore
from ai_trading_system.interfaces.journal_mcp import server, tools
from ai_trading_system.interfaces.journal_mcp.context import JournalMcpContext
from ai_trading_system.interfaces.mcp.context import McpProfile


@pytest.fixture
def journal_context(tmp_path: Path) -> JournalMcpContext:
    db_path = tmp_path / "trade_journal.duckdb"
    store = TradeJournalStore(tmp_path, db_path=db_path)
    store.migrate(apply=True)
    with store.writer() as conn:
        conn.execute(
            "INSERT INTO instrument_identity VALUES (?,?,?)",
            ["instrument-a", "INE012345678", datetime(2026, 1, 2)],
        )
        conn.execute(
            "INSERT INTO instrument_alias VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ["alias-a", "instrument-a", "AAA", "INE012345678", "NSE", "EQ", "EQ",
             date(2020, 1, 1), None, "import-a", datetime(2026, 1, 2)],
        )
        conn.execute(
            "INSERT INTO journal_analysis_run VALUES (?,?,?,?,?,?,?,?,?,?)",
            ["reconstruction-a", "account-a", "reconstruction", "COMPLETED", "journal-v1",
             "hash-r", None, datetime(2026, 1, 8), datetime(2026, 1, 9), None],
        )
        conn.execute(
            """INSERT INTO portfolio_reconstruction
               (analysis_run_id,account_ref,instrument_id,as_of_at,quantity,fifo_cost,
                weighted_average_cost,trust_status,source_snapshot_json,generated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            ["reconstruction-a", "account-a", "instrument-a", datetime(2026, 1, 7, 10),
             Decimal("6"), Decimal("601.50"), Decimal("100.25"), "TRUSTED", "{}",
             datetime(2026, 1, 8)],
        )
        conn.execute(
            """INSERT INTO journal_fill
               (fill_id,import_id,account_ref,instrument_id,symbol,isin,exchange,segment,series,
                trade_date,executed_at,side,auction,quantity,price,trade_id,order_id,
                economics_hash,trust_status,raw_row_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ["fill-a", "import-a", "account-a", "instrument-a", "AAA", "INE012345678",
             "NSE", "EQ", "EQ", date(2026, 1, 6), datetime(2026, 1, 6, 10), "buy",
             False, Decimal("10"), Decimal("100.25"), "trade-a", "order-a", "econ-a",
             "TRUSTED", "raw-a"],
        )
        conn.execute(
            "INSERT INTO trade_episode VALUES (?,?,?,?,?,?,?,?,?,?)",
            ["episode-a", "reconstruction-a", "account-a", "instrument-a",
             datetime(2026, 1, 6, 10), None, "OPEN", Decimal("39"), "TRUSTED",
             datetime(2026, 1, 8)],
        )
        conn.execute(
            "INSERT INTO episode_fill_link VALUES (?,?,?,?,?)",
            ["episode-a", "reconstruction-a", "fill-a", "INITIAL_ENTRY", Decimal("10")],
        )
        conn.execute(
            "INSERT INTO journal_annotation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ["annotation-a", "episode-a", 1, "private thesis", "breakout", Decimal("95"),
             Decimal("120"), None, "wait for confirmation", '["review"]', "operator-name",
             datetime(2026, 1, 8)],
        )
        conn.execute(
            "INSERT INTO journal_analysis_run VALUES (?,?,?,?,?,?,?,?,?,?)",
            ["analysis-a", "account-a", "point_in_time_analysis", "COMPLETED", "journal-v1",
             "hash-a", None, datetime(2026, 1, 9), datetime(2026, 1, 10), None],
        )
        conn.execute(
            "INSERT INTO trade_evaluation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ["evaluation-a", "analysis-a", "episode-a", "fill-a", "ENTRY_PROCESS",
             Decimal("82"), "SCORED", '{"coverage":"1"}', "BREAKOUT", "HIGH",
             "journal-v1", datetime(2026, 1, 9)],
        )
        conn.execute(
            "INSERT INTO portfolio_risk_snapshot VALUES (?,?,?,?,?,?,?,?)",
            ["risk-a", "analysis-a", "account-a", date(2026, 1, 7), "holdings_only",
             '{"gross_market_value":"720"}', "journal-v1", datetime(2026, 1, 9)],
        )
        conn.execute(
            "INSERT INTO portfolio_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ["snapshot-a", "import-a", "account-a", date(2026, 1, 7), None, "eod",
             "reconciliation_only", "TRUSTED", None, None, None, True, None,
             datetime(2026, 1, 8)],
        )
        conn.execute(
            "INSERT INTO portfolio_reconciliation VALUES (?,?,?,?,?,?,?,?,?,?)",
            ["reconciliation-a", "analysis-a", "snapshot-a", "account-a",
             datetime(2026, 1, 7, 23, 59), "MATCHED", 1, 0, "TRUSTED",
             datetime(2026, 1, 10)],
        )
        conn.execute(
            "INSERT INTO portfolio_reconciliation_item VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ["reconciliation-a", "instrument-a", "AAA", "MATCHED", Decimal("6"),
             Decimal("6"), Decimal("601.50"), Decimal("601.50"), Decimal("601.50"),
             Decimal("0"), Decimal("0"), "{}"],
        )
        conn.execute(
            """INSERT INTO journal_dq_issue
               (issue_id,account_ref,severity,issue_type,entity_type,entity_id,evidence_json,
                lifecycle_status,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            ["dq-a", "account-a", "WARNING", "TEST_WARNING", "episode", "episode-a",
             '{"private":"payload"}', "OPEN", datetime(2026, 1, 8)],
        )
        conn.execute(
            "INSERT INTO trade_episode VALUES (?,?,?,?,?,?,?,?,?,?)",
            ["episode-b", "reconstruction-a", "account-b", "instrument-a",
             datetime(2026, 1, 6), None, "OPEN", None, "TRUSTED", datetime(2026, 1, 8)],
        )
    return JournalMcpContext(
        profile=McpProfile.FIXTURE,
        project_root=tmp_path,
        db_path=db_path,
        account_ref="account-a",
    )


def _listed_tools(built: Any) -> list[Any]:
    return asyncio.run(built.list_tools())


def test_context_is_read_only_and_uses_opaque_scope(journal_context: JournalMcpContext) -> None:
    assert journal_context.account_scope.startswith("account_")
    assert "account-a" not in journal_context.account_scope
    with journal_context.reader() as conn:
        with pytest.raises(duckdb.InvalidInputException):
            conn.execute("DELETE FROM trade_episode")


def test_core_tools_are_account_scoped_and_sanitized(journal_context: JournalMcpContext) -> None:
    positions = tools.get_journal_positions(journal_context)
    assert positions["data"][0]["symbol"] == "AAA"
    episodes = tools.list_trade_episodes(journal_context)
    assert [row["episode_id"] for row in episodes["data"]] == ["episode-a"]
    detail = tools.get_trade_episode(
        journal_context, "episode-a", include_annotations=True
    )
    assert detail["data"]["annotations"][0]["thesis"] == "private thesis"
    assert "author" not in detail["data"]["annotations"][0]
    assert tools.get_trade_episode(journal_context, "episode-b")["data"] == {}
    dq = tools.get_journal_data_quality(journal_context)
    assert dq["data"][0]["evidence_available"] is True
    assert "evidence_json" not in dq["data"][0]


def test_temporal_cutoffs_fail_closed(journal_context: JournalMcpContext) -> None:
    assert tools.get_journal_positions(
        journal_context, known_at="2026-01-08"
    )["data"] == []
    assert tools.get_journal_positions(
        journal_context, portfolio_as_of="2026-01-06"
    )["data"] == []
    assert tools.get_journal_reconciliation(
        journal_context, known_at="2026-01-09"
    )["data"] == {}


def test_all_tools_register_without_account_parameter(journal_context: JournalMcpContext) -> None:
    registered = _listed_tools(server.build_server(journal_context))
    assert {tool.name for tool in registered} == {
        name for name, _, _ in server._tool_specs()
    }
    for tool in registered:
        schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", {})
        assert "account_ref" not in schema.get("properties", {})


def test_real_stdio_client_discovers_and_calls_journal_tool(
    journal_context: JournalMcpContext,
) -> None:
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    async def exercise() -> None:
        environment = dict(os.environ)
        environment["DATA_ROOT"] = str(journal_context.db_path.parent)
        environment["AI_TRADING_JOURNAL_MCP_ACCOUNT_REF"] = "account-a"
        environment["PYTHONPATH"] = str(server.__file__).split("/ai_trading_system/")[0]
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "ai_trading_system.interfaces.journal_mcp.server", "--profile", "fixture"],
            env=environment,
        )
        async with stdio_client(parameters) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                discovered = await session.list_tools()
                assert "get_journal_overview" in {item.name for item in discovered.tools}
                result = await session.call_tool("get_journal_positions", {})
                assert not result.is_error
                assert result.content

    asyncio.run(exercise())
