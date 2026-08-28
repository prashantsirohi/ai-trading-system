"""Standalone stdio MCP server for private, read-only journal evidence."""

from __future__ import annotations

import argparse
import functools
import inspect
import json
import logging
import sys
import typing
from typing import Any, Callable

from ai_trading_system.interfaces.journal_mcp.context import (
    JournalMcpContext,
    JournalSchemaError,
)
from ai_trading_system.interfaces.journal_mcp.schema_catalog import describe_schema
from ai_trading_system.interfaces.journal_mcp import tools
from ai_trading_system.interfaces.mcp.context import McpConfigurationError, McpProfile

SERVER_NAME = "ai-trading-journal"

INSTRUCTIONS = """\
Private read-only access to the operator's actual trading journal. This server
is pinned to exactly one account at startup; tools never enumerate or accept an
account identifier. Every response uses the standard {"data": ..., "meta": ...}
envelope and includes an opaque account_scope.

portfolio_as_of is the economic date being described. known_at is the latest
recording or analysis-completion timestamp the answer may use. Never describe
gross, holdings_only, or securities_only results as NAV, net return, TWR, or
XIRR. Missing, partial, blocked, or untrusted evidence remains explicit.

Start with describe_schema, then get_journal_overview. No tool imports files,
runs analysis, approves governance, changes annotations, or touches a broker.
"""


def _describe_schema(ctx: JournalMcpContext, surface: str | None = None) -> dict[str, Any]:
    del ctx
    return describe_schema(surface)


def _tool_specs() -> list[tuple[str, Callable[..., Any], str]]:
    return [
        ("describe_schema", _describe_schema, "Describe the private journal evidence contract, temporal semantics, scope labels, trust fields, and owning tables."),
        ("get_journal_overview", tools.get_journal_overview, "Summarize the configured account's latest knowable reconstruction, analysis, positions, open episodes, and unresolved DQ counts."),
        ("get_journal_positions", tools.get_journal_positions, "Read trusted reconstructed security positions from one completed version, with separate economic and recording-time cutoffs."),
        ("list_trade_episodes", tools.list_trade_episodes, "List zero-to-zero trade episodes for the configured account, optionally filtered by symbol, status, and economic date."),
        ("get_trade_episode", tools.get_trade_episode, "Read one account-owned episode and optionally its accepted fills and sanitized private annotations; annotation authors are never returned."),
        ("get_trade_evaluations", tools.get_trade_evaluations, "Read version-pinned entry, add, and exit process evaluations without turning insufficient evidence into failed scores."),
        ("get_portfolio_journal_series", tools.get_portfolio_journal_series, "Read holdings-only portfolio risk observations; these are not NAV, cash-aware, charge-adjusted, TWR, or XIRR returns."),
        ("get_journal_reconciliation", tools.get_journal_reconciliation, "Read one account-owned or latest eligible holdings reconciliation and its sanitized discrepancy items."),
        ("get_journal_data_quality", tools.get_journal_data_quality, "Read sanitized account-scoped journal DQ status without raw upload rows or evidence payloads."),
    ]


def _bind(function: Callable[..., Any], context: JournalMcpContext) -> Callable[..., Any]:
    signature = inspect.signature(function)
    try:
        hints = typing.get_type_hints(function)
    except Exception:  # pragma: no cover
        hints = {}
    remaining = []
    for name, parameter in signature.parameters.items():
        if name in {"ctx", "context"}:
            continue
        if name in hints:
            parameter = parameter.replace(annotation=hints[name])
        remaining.append(parameter)

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return function(context, *args, **kwargs)

    wrapper.__signature__ = signature.replace(  # type: ignore[attr-defined]
        parameters=remaining,
        return_annotation=hints.get("return", signature.return_annotation),
    )
    wrapper.__annotations__ = {
        name: hint for name, hint in hints.items() if name not in {"ctx", "context"}
    }
    return wrapper


def build_server(context: JournalMcpContext):
    try:
        from mcp.server.mcpserver import MCPServer as _Server
    except ModuleNotFoundError:
        try:
            from mcp.server.fastmcp import FastMCP as _Server
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise SystemExit("The 'mcp' package is not installed; run 'uv sync'.") from exc
    server = _Server(SERVER_NAME, instructions=INSTRUCTIONS)
    for name, function, description in _tool_specs():
        server.add_tool(_bind(function, context), name=name, description=description)
    return server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ai-trading-journal-mcp",
        description="Private account-scoped read-only trading-journal MCP server.",
    )
    parser.add_argument(
        "--profile",
        choices=[profile.value for profile in McpProfile],
        default=McpProfile.OPERATOR.value,
    )
    parser.add_argument("--list-tools", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    if args.list_tools:
        print(json.dumps({
            "server": SERVER_NAME,
            "tools": [
                {"name": name, "description": description}
                for name, _, description in _tool_specs()
            ],
        }, indent=2))
        return 0
    try:
        context = JournalMcpContext.from_env(args.profile)
    except (McpConfigurationError, JournalSchemaError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    build_server(context).run()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
