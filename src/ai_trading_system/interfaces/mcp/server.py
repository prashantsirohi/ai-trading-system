"""stdio MCP server exposing the trading system's read surfaces.

This is the only module that touches the ``mcp`` package, and it imports it
lazily inside ``build_server`` so the tool layer stays importable and testable
without the SDK installed.

Strictly read-only: no pipeline triggers, no broker or execution imports, and
every store handle is opened read-only by ``McpContext``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Callable

from ai_trading_system.interfaces.mcp.context import (
    McpConfigurationError,
    McpContext,
    McpProfile,
)
from ai_trading_system.interfaces.mcp.schema_catalog import (
    SURFACE_NAMES,
    describe_schema,
)
from ai_trading_system.interfaces.mcp.tools import fundamentals as fundamentals_tool
from ai_trading_system.interfaces.mcp.tools import fundamental_discovery as fundamental_discovery_tool
from ai_trading_system.interfaces.mcp.tools import patterns as patterns_tool
from ai_trading_system.interfaces.mcp.tools import governance as governance_tool
from ai_trading_system.interfaces.mcp.tools import lifecycle as lifecycle_tool
from ai_trading_system.interfaces.mcp.tools import sector_leadership as sector_leadership_tool
from ai_trading_system.interfaces.mcp.tools import prices as prices_tool
from ai_trading_system.interfaces.mcp.tools import profile as profile_tool
from ai_trading_system.interfaces.mcp.tools import rank as rank_tool
from ai_trading_system.interfaces.mcp.tools import screen as screen_tool
from ai_trading_system.interfaces.mcp.tools import sectors as sectors_tool
from ai_trading_system.interfaces.mcp.tools import stage as stage_tool
from ai_trading_system.interfaces.mcp.tools import symbols as symbols_tool
from ai_trading_system.interfaces.mcp.tools import technicals as technicals_tool

logger = logging.getLogger(__name__)

SERVER_NAME = "ai-trading-system"

INSTRUCTIONS = """\
Read-only access to an Indian equity trading system (NSE primary, BSE for
mastered symbols). Every response is {"data": ..., "meta": {...}}.

Start with describe_schema to learn what a column means and which store owns
it, and resolve_symbol to pin down identity — a symbol is (symbol_id,
exchange), and dual listings are returned as separate candidates rather than
guessed.

meta.as_of_status tells you what a response is: LATEST (no cutoff requested),
EXACT (every row is at or before your as_of), NO_DATA_AS_OF (nothing existed
by then), AS_OF_UNSUPPORTED (that surface is latest-only and returns no rows
rather than substituting the present). Historical answers never contain data
published after the requested date.

Prices default to the split-adjusted basis, which is what the technical
features are computed on; meta.price_basis states which basis you got.

Stages appear in two vocabularies. Every stage row carries stage_label
(canonical, always set), stage_label_legacy (null for the four transition
states), stage_family, and is_transition.
"""


def _tool_specs() -> list[tuple[str, Callable[..., Any], str]]:
    """Name, callable and description for every exposed tool."""

    return [
        (
            "describe_schema",
            _describe_schema_tool,
            "Column dictionary for a surface (ohlcv, technicals, stage, rank, "
            "pattern, sector, fundamentals, fundamental_discovery): type, meaning, units, owning store, and "
            "the stage-vocabulary mapping. Call this before interpreting "
            "unfamiliar columns. Omit 'surface' for an index of all surfaces.",
        ),
        (
            "resolve_symbol",
            symbols_tool.resolve_symbol,
            "Resolve a ticker, company name, ISIN or security id to symbol "
            "master candidates. Returns every matching listing with its "
            "exchange; meta.ambiguous flags a dual listing. The master is "
            "current-state, so 'as_of' returns no rows.",
        ),
        (
            "get_symbol_profile",
            profile_tool.get_symbol_profile,
            "One-call overview of a symbol: identity, latest quote, weekly "
            "stage, rank position with factor breakdown, and fundamentals. "
            "Every block carries its own date and source; a block with no data "
            "at 'as_of' is left empty rather than filled from another date.",
        ),
        (
            "get_ohlcv",
            prices_tool.get_ohlcv,
            "Daily candles plus delivery percentage. 'adjusted' defaults to "
            "true (split/bonus adjusted, the basis technical features use); "
            "set it false for raw exchange prices.",
        ),
        (
            "get_technical_features",
            technicals_tool.get_technical_features,
            "Technical indicator history: rsi, adx, sma, ema, macd, atr, bb, "
            "roc, supertrend, plus Phase 1 risk and liquidity features "
            "(realized volatility, beta, drawdown, liquidity score, delivery "
            "trend).",
        ),
        (
            "get_stage_history",
            stage_tool.get_stage_history,
            "Weinstein stage observations. 'granularity' selects the store: "
            "weekly_governed (default, exchange-aware, current coverage), "
            "weekly_legacy (original weekly store, often stale, no exchange "
            "column), or daily.",
        ),
        (
            "get_rank_detail",
            rank_tool.get_rank_detail,
            "The newest ranked row for a symbol at or before 'as_of', grouped "
            "into identity, position, factor scores, and provenance.",
        ),
        (
            "get_rank_history",
            rank_tool.get_rank_history,
            "Rank position and composite score over time for one symbol, "
            "oldest first.",
        ),
        (
            "get_pattern_detail",
            patterns_tool.get_pattern_detail,
            "Operational pattern observations for a symbol on the newest model-pinned session at or before the cutoff. Shadow pattern-lane evidence is never blended in.",
        ),
        (
            "get_pattern_history",
            patterns_tool.get_pattern_history,
            "Operational pattern lifecycle history for a symbol, including family, state, score, setup quality, pivot distance, breakout state and model provenance.",
        ),
        (
            "screen_universe",
            screen_tool.screen_universe,
            "Filter the ranked universe cross-section: by stage (either "
            "vocabulary), stage family, sector, minimum composite score and "
            "maximum rank position. Use this instead of pulling every symbol.",
        ),
        (
            "get_sector_overview",
            sectors_tool.get_sector_overview,
            "Stage distribution per sector at a point in time, derived from "
            "governed weekly stage observations.",
        ),
        (
            "get_sector_constituents",
            sectors_tool.get_sector_constituents,
            "Symbols in a sector with their stage, rank position and market "
            "cap.",
        ),
        (
            "get_sector_leadership",
            sector_leadership_tool.get_sector_leadership,
            "Latest-only sector relative-strength, momentum, rotation quadrant, earnings-leadership and valuation-cycle evidence from promoted artifacts and analytical stores.",
        ),
        (
            "get_fundamentals",
            fundamentals_tool.get_fundamentals,
            "Fundamental evidence in five blocks: company snapshot, scores, "
            "valuation snapshot, quarterly growth, and raw financial line "
            "items. Cutoffs use the publication date, not the fiscal period. "
            "'statement_basis' is standalone or consolidated, never blended.",
        ),
        (
            "get_fundamental_thesis",
            fundamental_discovery_tool.get_fundamental_thesis,
            "Point-in-time fundamental-discovery thesis classification and daily shadow projection for one listing, kept separate from generic fundamental scores.",
        ),
        (
            "get_fundamental_thesis_history",
            fundamental_discovery_tool.get_fundamental_thesis_history,
            "Historical fundamental-discovery projections joined to their exact immutable classifications, with evaluations, blockers, policy versions and change evidence.",
        ),
        (
            "screen_fundamental_theses",
            fundamental_discovery_tool.screen_fundamental_theses,
            "Screen one pinned fundamental-discovery projection date by thesis, classification status, eligibility, blocker or statement basis.",
        ),
        (
            "get_fundamental_lane_overview",
            fundamental_discovery_tool.get_fundamental_lane_overview,
            "Aggregate the pinned fundamental-discovery cross-section by seven-family thesis vocabulary, status, eligibility and blockers.",
        ),
        (
            "get_pipeline_run",
            governance_tool.get_pipeline_run,
            "Inspect pipeline run status, timing, errors, metadata, stage-attempt count, artifact count and data-quality issue count without triggering pipeline work.",
        ),
        (
            "get_data_quality_status",
            governance_tool.get_data_quality_status,
            "Read persisted data-quality rule outcomes by run, stage, severity and cutoff, including failure counts, messages and evidence links.",
        ),
        (
            "get_artifact_lineage",
            governance_tool.get_artifact_lineage,
            "Read artifacts only from their exact completed producer attempts, including content hashes, row counts, lifecycle status and producer completion evidence.",
        ),
        (
            "get_data_freshness",
            governance_tool.get_data_freshness,
            "Summarize latest knowable dates and explicit CURRENT, STALE or MISSING status for rank, pattern, weekly-stage and fundamental-discovery surfaces.",
        ),
        (
            "get_candidate_status",
            lifecycle_tool.get_candidate_status,
            "Latest point-in-time canonical candidate episode and snapshot for one listing; this is read-only shadow lifecycle context, not execution state.",
        ),
        (
            "get_candidate_history",
            lifecycle_tool.get_candidate_history,
            "Historically reconstruct canonical candidate episodes with their snapshots and transitions for a symbol or candidate id.",
        ),
        (
            "get_investigator_evidence",
            lifecycle_tool.get_investigator_evidence,
            "Point-in-time Investigator evidence observations attached to canonical opportunity episodes, retaining verdict, positive, negative and missing evidence.",
        ),
        (
            "get_opportunity_episode",
            lifecycle_tool.get_opportunity_episode,
            "Aggregate one canonical opportunity episode with snapshots, transitions, structural, Investigator, rank and fundamental observations without mutation.",
        ),
    ]


def _describe_schema_tool(ctx: McpContext, surface: str | None = None) -> dict[str, Any]:
    """Context-taking wrapper so every tool shares one call signature."""

    return describe_schema(surface)


def build_server(context: McpContext):
    """Construct the FastMCP server with every tool bound to ``context``."""

    # The SDK renamed FastMCP to MCPServer in 1.27; both expose the same
    # add_tool/run surface, so support whichever is installed.
    try:
        from mcp.server.mcpserver import MCPServer as _Server
    except ModuleNotFoundError:
        try:
            from mcp.server.fastmcp import FastMCP as _Server
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on install
            raise SystemExit(
                "The 'mcp' package is not installed in this environment. "
                "Run 'uv sync' (or 'uv pip install mcp') and try again."
            ) from exc

    server = _Server(SERVER_NAME, instructions=INSTRUCTIONS)

    for name, function, description in _tool_specs():
        server.add_tool(
            _bind(function, context),
            name=name,
            description=description,
        )
    return server


def _bind(function: Callable[..., Any], context: McpContext) -> Callable[..., Any]:
    """Partially apply ``context`` while keeping the remaining signature.

    The SDK derives each tool's input schema from the wrapper's signature, so
    the context parameter is dropped rather than exposed to the model.

    Tool modules use ``from __future__ import annotations``, which leaves
    annotations as strings. Those are resolved here against the *defining*
    module's namespace and attached as real objects, so the schema builder
    never has to resolve a name (such as ``date``) that happens not to be
    importable from this module.
    """

    import functools
    import inspect
    import typing

    signature = inspect.signature(function)
    try:
        hints = typing.get_type_hints(function)
    except Exception:  # pragma: no cover - defensive
        hints = {}

    remaining: list[inspect.Parameter] = []
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


def run_self_test(context: McpContext, *, historical_as_of: str) -> int:
    """Call every tool twice — latest and historical — and report the envelopes.

    Exits non-zero if any tool raises. ``envelope`` already refuses to return a
    row dated after the requested ``as_of``, so a leak surfaces here as a
    ``FutureDataError`` rather than a silently wrong answer.
    """

    probes: list[tuple[str, Callable[[], Any]]] = [
        ("describe_schema", lambda: describe_schema("stage")),
        ("resolve_symbol", lambda: symbols_tool.resolve_symbol(context, "RELIANCE")),
    ]

    symbol = "RELIANCE"
    for as_of in (None, historical_as_of):
        label = "latest" if as_of is None else f"as_of={historical_as_of}"
        probes.extend(
            [
                (
                    f"get_symbol_profile ({label})",
                    lambda a=as_of: profile_tool.get_symbol_profile(
                        context, symbol, as_of=a
                    ),
                ),
                (
                    f"get_ohlcv ({label})",
                    lambda a=as_of: prices_tool.get_ohlcv(
                        context, symbol, as_of=a, limit=5
                    ),
                ),
                (
                    f"get_technical_features ({label})",
                    lambda a=as_of: technicals_tool.get_technical_features(
                        context, symbol, as_of=a, limit=5
                    ),
                ),
                (
                    f"get_stage_history ({label})",
                    lambda a=as_of: stage_tool.get_stage_history(
                        context, symbol, as_of=a, limit=5
                    ),
                ),
                (
                    f"get_rank_detail ({label})",
                    lambda a=as_of: rank_tool.get_rank_detail(context, symbol, as_of=a),
                ),
                (
                    f"get_rank_history ({label})",
                    lambda a=as_of: rank_tool.get_rank_history(
                        context, symbol, as_of=a, limit=5
                    ),
                ),
                (
                    f"get_pattern_detail ({label})",
                    lambda a=as_of: patterns_tool.get_pattern_detail(context, symbol, as_of=a),
                ),
                (
                    f"get_pattern_history ({label})",
                    lambda a=as_of: patterns_tool.get_pattern_history(context, symbol, as_of=a, limit=5),
                ),
                (
                    f"screen_universe ({label})",
                    lambda a=as_of: screen_tool.screen_universe(
                        context, as_of=a, limit=5
                    ),
                ),
                (
                    f"screen_universe full_universe ({label})",
                    lambda a=as_of: screen_tool.screen_universe(
                        context, scope="full_universe", as_of=a, limit=5
                    ),
                ),
                (
                    f"get_sector_overview ({label})",
                    lambda a=as_of: sectors_tool.get_sector_overview(
                        context, as_of=a, limit=5
                    ),
                ),
                (
                    f"get_sector_constituents ({label})",
                    lambda a=as_of: sectors_tool.get_sector_constituents(context, "Capital Goods", as_of=a, limit=5),
                ),
                (
                    f"get_sector_leadership ({label})",
                    lambda a=as_of: sector_leadership_tool.get_sector_leadership(context, as_of=a, limit=5),
                ),
                (
                    f"get_fundamentals ({label})",
                    lambda a=as_of: fundamentals_tool.get_fundamentals(
                        context, symbol, as_of=a
                    ),
                ),
                (
                    f"get_fundamental_thesis ({label})",
                    lambda a=as_of: fundamental_discovery_tool.get_fundamental_thesis(context, symbol, as_of=a),
                ),
                (
                    f"get_fundamental_thesis_history ({label})",
                    lambda a=as_of: fundamental_discovery_tool.get_fundamental_thesis_history(context, symbol, as_of=a, limit=5),
                ),
                (
                    f"screen_fundamental_theses ({label})",
                    lambda a=as_of: fundamental_discovery_tool.screen_fundamental_theses(context, as_of=a, limit=5),
                ),
                (
                    f"get_fundamental_lane_overview ({label})",
                    lambda a=as_of: fundamental_discovery_tool.get_fundamental_lane_overview(context, as_of=a),
                ),
                (
                    f"get_pipeline_run ({label})",
                    lambda a=as_of: governance_tool.get_pipeline_run(context, as_of=a, limit=5),
                ),
                (
                    f"get_data_quality_status ({label})",
                    lambda a=as_of: governance_tool.get_data_quality_status(context, as_of=a, limit=5),
                ),
                (
                    f"get_artifact_lineage ({label})",
                    lambda a=as_of: governance_tool.get_artifact_lineage(context, as_of=a, limit=5),
                ),
                (
                    f"get_data_freshness ({label})",
                    lambda a=as_of: governance_tool.get_data_freshness(context, as_of=a),
                ),
                (
                    f"get_candidate_status ({label})",
                    lambda a=as_of: lifecycle_tool.get_candidate_status(context, symbol, as_of=a),
                ),
                (
                    f"get_candidate_history ({label})",
                    lambda a=as_of: lifecycle_tool.get_candidate_history(context, symbol=symbol, as_of=a, limit=5),
                ),
                (
                    f"get_investigator_evidence ({label})",
                    lambda a=as_of: lifecycle_tool.get_investigator_evidence(context, symbol=symbol, as_of=a, limit=5),
                ),
                (
                    f"get_opportunity_episode ({label})",
                    lambda a=as_of: lifecycle_tool.get_opportunity_episode(context, "self-test-missing", as_of=a, limit=5),
                ),
            ]
        )

    failures = 0
    for name, call in probes:
        try:
            result = call()
        except Exception as exc:  # noqa: BLE001 - the self-test reports everything
            failures += 1
            print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
            continue
        meta = result.get("meta") if isinstance(result, dict) else None
        if meta is None:
            print(f"ok    {name}")
            continue
        print(
            f"ok    {name}: status={meta.get('as_of_status')} "
            f"effective={meta.get('as_of_effective')} "
            f"rows={meta.get('row_count')} source={meta.get('source')}"
        )

    print(
        f"\n{len(probes) - failures}/{len(probes)} probes passed "
        f"(profile={context.profile.value}, data_root={context.paths.root_dir})"
    )
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ai-trading-mcp",
        description="Read-only MCP server over the trading system's data stores.",
    )
    parser.add_argument(
        "--profile",
        choices=[profile.value for profile in McpProfile],
        default=McpProfile.OPERATOR.value,
        help=(
            "operator (default) requires an explicit external DATA_ROOT; "
            "fixture permits a temporary or repo-local root for testing."
        ),
    )
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Data domain to read (default: operational).",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Call every tool once at latest and once historically, print the "
            "response metadata, and exit non-zero on any failure."
        ),
    )
    parser.add_argument(
        "--self-test-as-of",
        default="2026-01-02",
        help="Historical date used by --self-test (default: 2026-01-02).",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Print the tool catalog as JSON and exit; does not open a store.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    if args.list_tools:
        print(
            json.dumps(
                {
                    "server": SERVER_NAME,
                    "surfaces": list(SURFACE_NAMES),
                    "tools": [
                        {"name": name, "description": description}
                        for name, _, description in _tool_specs()
                    ],
                },
                indent=2,
            )
        )
        return 0

    try:
        context = McpContext.from_env(args.profile, data_domain=args.data_domain)
    except McpConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.self_test:
        return run_self_test(context, historical_as_of=args.self_test_as_of)

    build_server(context).run()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
