"""MCP v2 pattern, fundamental-discovery and full-universe contracts."""

from __future__ import annotations

import duckdb

from ai_trading_system.interfaces.mcp.context import McpContext, McpProfile
from ai_trading_system.interfaces.mcp.tools.fundamental_discovery import (
    get_fundamental_lane_overview,
    get_fundamental_thesis,
    get_fundamental_thesis_history,
    screen_fundamental_theses,
)
from ai_trading_system.interfaces.mcp.tools.patterns import (
    get_pattern_detail,
    get_pattern_history,
)
from ai_trading_system.interfaces.mcp.tools.screen import screen_universe
from ai_trading_system.interfaces.mcp.tools.governance import (
    get_artifact_lineage, get_data_freshness, get_data_quality_status, get_pipeline_run,
)
from ai_trading_system.interfaces.mcp.tools.lifecycle import (
    get_candidate_history, get_candidate_status, get_investigator_evidence,
    get_opportunity_episode,
)
from ai_trading_system.interfaces.mcp.tools.sector_leadership import get_sector_leadership


def test_pattern_detail_and_history_are_operational_and_point_in_time(ctx: McpContext) -> None:
    detail = get_pattern_detail(ctx, "AAA", as_of="2026-01-06")
    assert detail["data"][0]["trade_date"] == "2026-01-06"
    assert detail["meta"]["actionable"] is True
    assert detail["meta"]["shadow_pattern_lane_included"] is False
    history = get_pattern_history(ctx, "AAA", as_of="2026-01-07")
    assert [row["trade_date"] for row in history["data"]] == [
        "2026-01-05", "2026-01-06", "2026-01-07"
    ]


def test_fundamental_thesis_is_separate_and_has_all_seven_evaluations(ctx: McpContext) -> None:
    response = get_fundamental_thesis(ctx, "AAA", as_of="2026-01-09")
    assert response["data"]["classification"]["primary_thesis"] == "QUALITY_COMPOUNDER"
    assert response["data"]["projection"]["admission_eligible"] is True
    assert len(response["data"]["evaluations"]) == 7
    assert response["data"]["change"]["previous_source_data_hash"] == "hash-old"
    assert response["meta"]["lane"] == "fundamental_discovery"


def test_fundamental_history_and_cross_section_pin_the_cutoff(ctx: McpContext) -> None:
    history = get_fundamental_thesis_history(ctx, "AAA", as_of="2026-01-07")
    assert [row["projection"]["projection_date"] for row in history["data"]] == [
        "2026-01-05", "2026-01-06", "2026-01-07"
    ]
    screened = screen_fundamental_theses(
        ctx, as_of="2026-01-07", primary_thesis="QUALITY_COMPOUNDER",
        admission_eligible=True,
    )
    assert screened["meta"]["projection_date"] == "2026-01-07"
    assert [row["symbol_id"] for row in screened["data"]] == ["AAA"]
    overview = get_fundamental_lane_overview(ctx, as_of="2026-01-07")
    assert overview["data"]["primary_thesis_counts"]["QUALITY_COMPOUNDER"] == 1


def test_fundamental_future_created_or_available_rows_are_excluded(ctx: McpContext, connection_guard) -> None:
    with connection_guard.paused():
        with duckdb.connect(str(ctx.fundamentals_db)) as conn:
            conn.execute(
                """INSERT INTO fundamental_thesis_classification VALUES (
                    'future','AAA','NSE',CAST('2026-01-06' AS DATE),'hash-future','standalone',
                    CAST('2025-12-31' AS DATE),CAST('2026-01-08' AS DATE),
                    'CAPITAL_RETURN_INCOME','[]','QUALIFIED','[]','{}','taxonomy-v1','rules-v1',
                    'future',CAST('2026-01-08' AS TIMESTAMP))"""
            )
            conn.execute(
                """INSERT INTO fundamental_thesis_projection VALUES (
                    'future-p','AAA','NSE',CAST('2026-01-06' AS DATE),'hash-future',
                    'CAPITAL_RETURN_INCOME','[]','stage_2_advancing',TRUE,'[]','{}',
                    'taxonomy-v1','rules-v1','admission-v1','future',CAST('2026-01-08' AS TIMESTAMP))"""
            )
    response = get_fundamental_thesis(ctx, "AAA", as_of="2026-01-06")
    assert response["data"]["classification"]["source_data_hash"] == "hash-new"


def test_missing_fundamental_tables_fail_closed_without_creating_schema(
    tmp_path, monkeypatch, connection_guard
) -> None:
    root = tmp_path / "empty-data"
    root.mkdir()
    db_path = root / "fundamentals.duckdb"
    with connection_guard.paused():
        duckdb.connect(str(db_path)).close()
    monkeypatch.setenv("DATA_ROOT", str(root))
    empty_ctx = McpContext.from_env(McpProfile.FIXTURE)
    response = get_fundamental_thesis(empty_ctx, "AAA")
    assert response["data"] is None
    with connection_guard.paused():
        with duckdb.connect(str(db_path), read_only=True) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables"
            ).fetchone()[0] == 0


def test_full_universe_scope_is_additive_and_shortlist_default_is_unchanged(ctx: McpContext) -> None:
    shortlist = screen_universe(ctx)
    full = screen_universe(ctx, scope="full_universe")
    assert [row["symbol_id"] for row in shortlist["data"]] == ["AAA", "BBB"]
    assert [row["symbol_id"] for row in full["data"]] == ["AAA", "BBB", "CCC"]
    assert full["meta"]["scope"] == "full_universe"
    assert full["meta"]["effective_top_n"] == 20
    assert full["meta"]["regime_freshness_status"] == "ALIGNED"


def test_screen_pattern_and_fundamental_filters_compose(ctx: McpContext) -> None:
    response = screen_universe(
        ctx, pattern_family="cup_handle", min_pattern_score=60,
        primary_thesis="QUALITY_COMPOUNDER", admission_eligible=True,
    )
    assert [row["symbol_id"] for row in response["data"]] == ["AAA"]


def test_latest_only_sector_leadership_never_substitutes_current_for_history(ctx: McpContext) -> None:
    response = get_sector_leadership(ctx, as_of="2026-01-07")
    assert response["data"] == []
    assert response["meta"]["as_of_status"] == "AS_OF_UNSUPPORTED"


def test_governance_and_lifecycle_tools_fail_closed_on_missing_optional_tables(ctx: McpContext) -> None:
    assert get_pipeline_run(ctx)["data"] == []
    assert get_data_quality_status(ctx)["data"] == []
    assert get_artifact_lineage(ctx)["data"] == []
    freshness = get_data_freshness(ctx, as_of="2026-01-09")
    assert {row["surface"] for row in freshness["data"]} >= {
        "rank_shortlist", "rank_full_universe", "operational_pattern",
        "weekly_stage", "fundamental_discovery",
    }
    assert get_candidate_status(ctx, "AAA")["data"] is None
    assert get_candidate_history(ctx, symbol="AAA")["data"] == []
    assert get_investigator_evidence(ctx, symbol="AAA")["data"] == []
    assert get_opportunity_episode(ctx, "missing")["data"] is None
