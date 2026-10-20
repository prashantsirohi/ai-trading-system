from copy import deepcopy
import json

from ai_trading_system.domains.opportunities.daily_review import (
    Source,
    SOURCE_TYPES,
    build_daily_review,
)


def source_rows(**tables):
    sources = {name: Source() for name in SOURCE_TYPES}
    for name, rows in tables.items():
        sources[name] = Source(rows, {"session": "2026-10-19", "content_hash": name})
    return sources


def build(sources, **kwargs):
    return build_daily_review(
        session="2026-10-19",
        sources=sources,
        final_summary={"status": "complete", "calendar": {"status": "PASS"}},
        execution_summary={},
        **kwargs,
    )


def listing(symbol, **kwargs):
    return {"symbol_id": symbol, "exchange": "NSE", **kwargs}


def test_union_keeps_all_lanes_and_positions_without_manufactured_rank():
    sources = source_rows(
        rank=[listing("BOTH", composite_score=90), listing("RANK", composite_score=80)],
        shortlist=[listing("RANK"), listing("BOTH")],
        investigator=[listing("INV", trigger_reason="WEEKLY_GAINER", final_score=75)],
        investigator_active=[listing("WATCH", status="TRACKING", final_score=52)],
        fundamental=[
            listing(
                "FUND",
                classification_status="QUALIFIED",
                admission_blockers='["SECTOR_BLOCKED"]',
            )
        ],
        patterns=[
            listing(
                "BOTH",
                pattern_evaluation_state="KNOWN",
                lane_freshness="FRESH",
                pattern_member=True,
            )
        ],
        positions=[listing("OLD", quantity=2)],
        final_universe=[
            {"exchange": "NSE", "symbol": "OLD", "stage": "S4", "selected": False}
        ],
        technical=[listing("TECH_ONLY", entry_confirmed_state="MET")],
    )
    before = deepcopy(sources)
    rows, _, summary = build(sources)
    by_symbol = {r["symbol"]: r for r in rows}
    assert set(by_symbol) == {"BOTH", "RANK", "INV", "WATCH", "FUND", "OLD"}
    assert rows[0]["symbol"] == "OLD" and rows[0]["queue"] == "POSITION_ATTENTION"
    assert by_symbol["OLD"]["stage"] == "S4"
    assert by_symbol["OLD"]["position_mode"] == "PAPER"
    assert by_symbol["INV"]["rank_score"] is None
    assert by_symbol["INV"]["final_review_selected"] is False
    assert json.loads(by_symbol["BOTH"]["sources_json"]) == ["RANK", "PATTERN"]
    assert "SECTOR_BLOCKED" in by_symbol["FUND"]["blockers_json"]
    assert summary["rows"] == 6 and sources == before


def test_final_review_priority_is_preserved_and_risk_precedes_entry():
    review = {
        "exchange": "NSE",
        "symbol": "AAA",
        "readiness": "SETUP_REVIEW",
        "review_priority": 7,
    }
    sources = source_rows(final_universe=[review], final_list=[review])
    rows, _, _ = build(sources)
    assert rows[0]["queue"] == "ENTRY_REVIEW" and rows[0]["final_review_priority"] == 7
    sources["technical"].rows = [listing("AAA", sma20_break_state="MET")]
    rows, _, _ = build(sources)
    assert rows[0]["queue"] == "INVESTIGATE"
    assert rows[0]["final_review_selected"] is True


def test_changes_ignore_daily_prices_rank_and_missing_feeds_never_remove():
    sources = source_rows(
        rank=[listing("AAA", composite_score=70)], shortlist=[listing("AAA")]
    )
    previous, _, summary = build(sources)
    sources["rank"].rows[0]["composite_score"] = 71
    rows, changes, _ = build(sources, previous=previous, previous_summary=summary)
    assert rows[0]["change"] == "UNCHANGED" and changes == []
    sources["shortlist"] = Source(issue="SOURCE_MISSING")
    rows, changes, current = build(sources, previous=previous, previous_summary=summary)
    assert not rows and changes[0]["change"] == "UNCONFIRMED_ABSENCE"
    assert not current["membership_coverage_complete"]
    sources["shortlist"] = Source()
    _, changes, _ = build(sources, previous=previous, previous_summary=summary)
    assert changes[0]["change"] == "REMOVED"


def test_change_queue_and_new_member_and_policy_boundary():
    sources = source_rows(shortlist=[listing("AAA")])
    previous, _, summary = build(sources)
    sources["positions"].rows = [listing("AAA", quantity=1)]
    sources["shortlist"].rows.append(listing("BBB"))
    rows, changes, _ = build(sources, previous=previous, previous_summary=summary)
    assert {r["symbol"]: r["change"] for r in rows} == {"AAA": "CHANGED", "BBB": "NEW"}
    assert changes[0]["previous_queue"] == "INVESTIGATE"
    summary["policy_hash"] = "legacy"
    rows, _, _ = build(sources, previous=previous, previous_summary=summary)
    assert {r["change"] for r in rows} == {"BASELINE"}


def test_tracker_identity_ambiguity_future_and_duplicates_are_visible():
    sources = source_rows(
        rank=[listing("AAA"), {"exchange": "BSE", "symbol_id": "AAA"}],
        tracker=[{"symbol": "AAA", "active": True, "episode_id": "tracking-1"}],
        shortlist=[
            listing("DUP"),
            listing("DUP"),
            listing("FUTURE", trade_date="2026-10-20"),
        ],
    )
    rows, _, summary = build(sources)
    assert len(rows) == 1 and rows[0]["symbol"] == "DUP"
    assert rows[0]["evidence_status"] == "CONFLICT"
    assert len(summary["identity_exceptions"]) == 2
    assert (
        summary["status"] == "degraded" and not summary["membership_coverage_complete"]
    )


def test_multiple_tracker_episodes_keep_one_listing_and_unknown_labels():
    sources = source_rows(
        rank=[listing("AAA")],
        tracker=[
            {
                "symbol": "AAA",
                "active": True,
                "episode_id": "one",
                "status": "IMPROVING",
            },
            {
                "symbol": "AAA",
                "active": True,
                "episode_id": "two",
                "status": "TECHNICAL_FAILURE",
            },
        ],
    )
    rows, _, _ = build(sources)
    assert len(rows) == 1 and json.loads(rows[0]["episode_ids_json"]) == ["one", "two"]
    assert rows[0]["above_sma20_state"] == "UNKNOWN"
    assert rows[0]["queue"] == "INVESTIGATE"


def test_position_quantity_changes_and_missing_feed_retains_unverified_risk():
    sources = source_rows(positions=[listing("AAA", quantity=1)])
    prior, _, summary = build(sources)
    sources["positions"].rows[0]["quantity"] = 2
    rows, _, _ = build(sources, previous=prior, previous_summary=summary)
    assert rows[0]["change"] == "CHANGED"
    sources["positions"] = Source(issue="SOURCE_MISSING")
    rows, _, summary2 = build(sources, previous=prior, previous_summary=summary)
    assert (
        rows[0]["position_status"] == "UNVERIFIED_PRIOR"
        and rows[0]["has_position"] is None
    )
    assert rows[0]["queue"] == "POSITION_ATTENTION" and rows[0]["change"] == "UNKNOWN"
    rows2, _, _ = build(sources, previous=rows, previous_summary=summary2)
    assert len(rows2) == 1 and rows2[0]["position_status"] == "UNVERIFIED_PRIOR"
