"""Tests for the schema catalog, the tool catalog, and the CLI surface."""

from __future__ import annotations

import json
from typing import Any

import pytest

from ai_trading_system.domains.opportunities.contracts import WeinsteinStage
from ai_trading_system.interfaces.mcp import server
from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.schema_catalog import (
    SURFACE_NAMES,
    describe_schema,
)
from ai_trading_system.interfaces.mcp.tools.fundamentals import get_fundamentals
from ai_trading_system.interfaces.mcp.tools.prices import get_ohlcv
from ai_trading_system.interfaces.mcp.tools.rank import get_rank_detail
from ai_trading_system.interfaces.mcp.tools.sectors import get_sector_overview
from ai_trading_system.interfaces.mcp.tools.stage import get_stage_history
from ai_trading_system.interfaces.mcp.tools.technicals import get_technical_features


# ---------------------------------------------------------------------------
# schema catalog
# ---------------------------------------------------------------------------


def test_index_lists_every_surface() -> None:
    index = describe_schema()
    assert {row["surface"] for row in index["surfaces"]} == set(SURFACE_NAMES)


@pytest.mark.parametrize("surface", SURFACE_NAMES)
def test_each_surface_is_fully_described(surface: str) -> None:
    spec = describe_schema(surface)
    for field in ("surface", "tool", "store", "tables", "grain", "as_of_support"):
        assert spec[field], f"{surface} missing {field}"
    assert spec["columns"]
    for column in spec["columns"]:
        assert set(column) == {"name", "type", "meaning", "units"}
        assert column["name"] and column["type"] and column["meaning"]


def test_unknown_surface_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown surface"):
        describe_schema("options_chain")


def test_stage_surface_publishes_the_full_vocabulary_mapping() -> None:
    """The mapping is the thing an agent otherwise has to grep for."""

    vocabulary = describe_schema("stage")["vocabulary"]
    assert set(vocabulary["canonical"]) == {stage.value for stage in WeinsteinStage}
    assert set(vocabulary["legacy"]) == {"S1", "S2", "S3", "S4", "UNDEFINED"}

    transitions = [row for row in vocabulary["mapping"] if row["is_transition"]]
    assert len(transitions) == 4
    assert all(row["legacy"] is None for row in transitions)
    assert all(row["family"].startswith("stage_") for row in transitions)


# ---------------------------------------------------------------------------
# The catalog must stay honest as schemas move
# ---------------------------------------------------------------------------


def _documented(surface: str) -> set[str]:
    return {column["name"] for column in describe_schema(surface)["columns"]}


def test_ohlcv_columns_match_the_tool_output(ctx: McpContext) -> None:
    produced = set(get_ohlcv(ctx, "AAA")["data"][0])
    assert produced <= _documented("ohlcv"), produced - _documented("ohlcv")


def test_technicals_columns_are_documented(ctx: McpContext) -> None:
    produced = set(get_technical_features(ctx, "AAA")["data"][0])
    undocumented = produced - _documented("technicals")
    assert not undocumented, f"undocumented technical columns: {undocumented}"


def test_stage_columns_are_documented(ctx: McpContext) -> None:
    """Every field the stage tool emits should be explainable."""

    produced = set(get_stage_history(ctx, "AAA", granularity="weekly_legacy")["data"][0])
    documented = _documented("stage")
    # Provenance columns are self-describing and intentionally not catalogued.
    ignorable = {"run_id", "stage_transition", "support_level", "resistance_level",
                 "ma10w", "ma40w", "weekly_volume_ratio", "source_week_start",
                 "source_week_end", "sector_id", "sector_name",
                 "classifier_version"}
    assert not (produced - documented - ignorable)


def test_rank_columns_are_documented(ctx: McpContext) -> None:
    data = get_rank_detail(ctx, "AAA")["data"]
    produced = set(data["position"]) | set(data["factors"]) | set(data["provenance"])
    produced |= {"trade_date", "universe_id"}
    undocumented = produced - _documented("rank")
    assert not undocumented, f"undocumented rank columns: {undocumented}"


def test_sector_columns_are_documented(ctx: McpContext) -> None:
    produced = set(get_sector_overview(ctx)["data"][0])
    documented = _documented("sector")
    derived = {"unknown_count", "unknown_pct", "stage_1_pct", "stage_3_pct",
               "stage_4_pct", "stage_2_share_pct"}
    assert not (produced - documented - derived)


def test_fundamental_score_columns_are_documented(ctx: McpContext) -> None:
    scores = get_fundamentals(ctx, "AAA")["data"]["scores"]
    documented = _documented("fundamentals")
    identity = {"snapshot_date", "symbol", "name", "industry_group", "industry",
                "red_flags", "screener_snapshot_date"}
    assert not (set(scores) - documented - identity)


# ---------------------------------------------------------------------------
# tool catalog / CLI
# ---------------------------------------------------------------------------


def test_tool_names_are_unique_and_described() -> None:
    specs = server._tool_specs()
    names = [name for name, _, _ in specs]
    assert len(names) == len(set(names))
    assert len(names) == 12
    for name, function, description in specs:
        assert callable(function), name
        assert len(description) > 40, name


def test_every_expected_tool_is_registered() -> None:
    names = {name for name, _, _ in server._tool_specs()}
    assert names == {
        "describe_schema",
        "resolve_symbol",
        "get_symbol_profile",
        "get_ohlcv",
        "get_technical_features",
        "get_stage_history",
        "get_rank_detail",
        "get_rank_history",
        "screen_universe",
        "get_sector_overview",
        "get_sector_constituents",
        "get_fundamentals",
    }


def test_instructions_explain_the_as_of_contract() -> None:
    for token in ("AS_OF_UNSUPPORTED", "NO_DATA_AS_OF", "EXACT", "price_basis"):
        assert token in server.INSTRUCTIONS


def test_binding_hides_the_context_parameter(ctx: McpContext) -> None:
    """FastMCP derives the input schema from the signature, so ctx must go."""

    import inspect

    bound = server._bind(get_ohlcv, ctx)
    parameters = inspect.signature(bound).parameters
    assert "ctx" not in parameters
    assert "symbol" in parameters and "adjusted" in parameters
    assert bound("AAA")["meta"]["symbol"] == "AAA"


def test_list_tools_needs_no_store(capsys: pytest.CaptureFixture[str]) -> None:
    assert server.main(["--list-tools"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["server"] == "ai-trading-system"
    assert len(payload["tools"]) == 12
    assert set(payload["surfaces"]) == set(SURFACE_NAMES)


def test_operator_profile_failure_is_reported_not_raised(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("DATA_ROOT", raising=False)
    assert server.main([]) == 2
    assert "Configuration error" in capsys.readouterr().err


def test_self_test_runs_every_probe(
    ctx: McpContext, monkeypatch: pytest.MonkeyPatch, data_root, capsys
) -> None:
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    exit_code = server.main(
        ["--profile", "fixture", "--self-test", "--self-test-as-of", "2026-01-06"]
    )
    output = capsys.readouterr().out
    assert "probes passed" in output
    # RELIANCE is absent from the fixture, so every probe should still answer
    # cleanly with empty data rather than raising.
    assert exit_code == 0, output


def test_parser_defaults_to_the_operator_profile() -> None:
    args = server.build_parser().parse_args([])
    assert args.profile == "operator"
    assert args.data_domain == "operational"


# ---------------------------------------------------------------------------
# End-to-end through the MCP SDK
# ---------------------------------------------------------------------------

mcp_sdk = pytest.importorskip(
    "mcp", reason="the MCP SDK is an optional runtime dependency"
)


def _list_tools(built: Any) -> list[Any]:
    import asyncio

    return asyncio.run(built.list_tools())


def test_server_registers_every_tool_with_the_sdk(ctx: McpContext) -> None:
    tools = _list_tools(server.build_server(ctx))
    assert {tool.name for tool in tools} == {
        name for name, _, _ in server._tool_specs()
    }


def test_registered_schemas_do_not_expose_the_context(ctx: McpContext) -> None:
    """A leaked ctx parameter would be unfillable by a model."""

    for tool in _list_tools(server.build_server(ctx)):
        schema = getattr(tool, "input_schema", None) or getattr(
            tool, "inputSchema", {}
        )
        properties = set((schema or {}).get("properties", {}))
        assert "ctx" not in properties and "context" not in properties, tool.name


def test_key_parameters_survive_binding(ctx: McpContext) -> None:
    tools = {tool.name: tool for tool in _list_tools(server.build_server(ctx))}
    schema = getattr(tools["get_ohlcv"], "input_schema", None) or getattr(
        tools["get_ohlcv"], "inputSchema", {}
    )
    properties = set(schema.get("properties", {}))
    assert {"symbol", "exchange", "as_of", "adjusted", "limit"} <= properties


def test_calling_a_tool_through_the_server_returns_the_envelope(
    ctx: McpContext,
) -> None:
    import asyncio

    built = server.build_server(ctx)
    result = asyncio.run(built.call_tool("get_ohlcv", {"symbol": "AAA", "limit": 2}))

    # The SDK has moved this shape around between versions: a (content,
    # payload) tuple in some, a CallToolResult in others.
    if isinstance(result, tuple):
        payload = result[1]
    else:
        payload = getattr(result, "structured_content", None) or result
        assert not getattr(result, "is_error", False)

    assert payload["meta"]["symbol"] == "AAA"
    assert payload["meta"]["price_basis"] == "adjusted"
    assert len(payload["data"]) == 2
