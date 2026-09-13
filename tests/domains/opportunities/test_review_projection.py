"""Isolated source-contract fixtures; never operational market evidence."""

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from ai_trading_system.domains.opportunities.review_projection import (
    build_review_projection,
    calendar_context,
)
from ai_trading_system.domains.opportunities.review_sources import (
    ReviewSource,
    ReviewSourceBundle,
)

SESSION = date(2026, 9, 11)
CUTOFF = datetime(2026, 9, 11, 20, tzinfo=timezone.utc)


def source(rows):
    return ReviewSource(
        pd.DataFrame(rows),
        {
            "available_at": CUTOFF - timedelta(hours=1),
            "content_hash": "a" * 64,
            "run_id": "fixture",
            "attempt": 1,
            "session": SESSION,
        },
        None,
    )


@pytest.fixture(scope="module")
def bundle():
    holidays = (date(2025, 1, 1), date(2026, 1, 1))
    dates = [
        d for d in pd.bdate_range(end=SESSION, periods=260) if d.date() not in holidays
    ]
    market = pd.DataFrame(
        [
            {
                "exchange": "NSE",
                "symbol_id": f"TEST{i}",
                "timestamp": d,
                "open": 100.0,
                "high": 101.0 if j >= len(dates) - 20 else 105.0,
                "low": 99.0 if j >= len(dates) - 20 else 95.0,
                "close": 100.0,
                "volume": 50 if j >= len(dates) - 20 else 100,
                "adjusted_open": 100.0,
                "adjusted_high": 101.0 if j >= len(dates) - 20 else 105.0,
                "adjusted_low": 99.0 if j >= len(dates) - 20 else 95.0,
                "adjusted_close": 100.0,
                "adjustment_factor": 1.0,
                "adjusted_at": None,
                "ingestion_ts": None,
                "provider": "nse_bhavcopy",
                "validation_status": "trusted_primary",
                "provider_discrepancy_flag": False,
            }
            for i in range(100)
            for j, d in enumerate(dates)
        ]
    )
    stage = {
        "symbol_id": "TEST0",
        "exchange": "NSE",
        "source_week_end": str(SESSION),
        "effective_stage": "stage_1_basing",
        "stage_status": "locked",
        "stage_transition": "none",
        "classifier_version": "weekly-stage-v2",
        "source_artifact_hash": "stagehash",
    }
    assessment = {
        "exchange": "NSE",
        "symbol_id": "TEST0",
        "session_date": str(SESSION),
        "source_policy_version": "pattern-lane-r0-policy-v1",
        "pattern_member": True,
        "pattern_evaluation_state": "KNOWN",
        "lane_freshness": "FRESH",
        "signal_ids_json": '["positive"]',
        "signal_count": 1,
    }
    signal = {
        "exchange": "NSE",
        "symbol_id": "TEST0",
        "as_of_date": str(SESSION),
        "signal_date": str(SESSION),
        "pattern_start": str(dates[-30].date()),
        "lane_policy_version": "pattern-lane-r0-policy-v1",
        "signal_id": "positive",
        "pattern_family": "flat_base",
        "pattern_state": "confirmed",
        "signal_direction": "bullish",
        "r1a_evidence_class": "evidence_supported",
        "breakout_level": 99.0,
        "watchlist_trigger_level": 99.0,
        "invalidation_price": 90.0,
        "pattern_score": 50.0,
    }
    fundamental = {
        "exchange": "NSE",
        "symbol_id": "TEST0",
        "as_of": str(SESSION),
        "admission_version": "fundamental-thesis-admission-v1.1",
        "classification_status": "QUALIFIED",
        "primary_thesis": "QUALITY_COMPOUNDER",
        "admission_eligible": "True",
        "admission_blockers_json": "[]",
        "statement_basis": "consolidated",
        "source_available_at": "2026-08-01",
        "source_data_hash": "fh",
    }
    sources = {
        "stage": source([stage]),
        "pattern": source([assessment]),
        "signals": source([signal]),
        "fundamental": source([fundamental]),
        "rank": source(
            [
                {
                    "exchange": "NSE",
                    "symbol_id": "TEST0",
                    "timestamp": str(SESSION),
                    "composite_score": 70.0,
                    "composite_score_adjusted": 75.0,
                }
            ]
        ),
        "positions": source(
            [
                {
                    "exchange": "NSE",
                    "symbol_id": "TEST99",
                    "as_of": str(SESSION),
                    "position_cycle_id": "position-99",
                }
            ]
        ),
    }
    master = pd.DataFrame(
        [
            {
                "symbol_id": f"TEST{i}",
                "exchange": "NSE",
                "isin": "INE000000001",
                "instrument_type": "EQ",
                "last_updated": "2026-01-01",
            }
            for i in range(100)
        ]
    )
    return ReviewSourceBundle(
        SESSION,
        CUTOFF,
        sources,
        market,
        master,
        holidays,
        pd.DataFrame({"date": dates}),
        pd.DataFrame(columns=["symbol_id", "exchange", "trade_date", "status"]),
        {("NSE", "TEST0"): stage},
        None,
        [
            {
                "stage_name": "ingest",
                "rule_id": "required",
                "status": "passed",
                "band": "green",
            }
        ],
        {"market": "market-hash"},
    )


def changed_source(bundle, key, edit):
    rows = bundle.sources[key].frame.to_dict("records")
    edit(rows)
    return replace(bundle, sources={**bundle.sources, key: source(rows)})


def test_full_projection_late_base_and_position_denominators(bundle):
    p = build_review_projection(bundle)
    assert len(p.universe) == 100 and len(p.ordered) == 1
    row = p.ordered[0]
    assert row["symbol"] == "TEST0" and row["combination"] == "P+F"
    assert row["readiness"] == "SETUP_REVIEW" and row["review_priority"] == 1
    assert row["rank_score"] == 75.0 and row["setup_signal_id"] == "positive"
    assert (
        p.summary["position_expected_cycles"]
        == p.summary["position_represented_cycles"]
        == 1
    )
    assert p.summary["position_missing_cycles"] == []
    assert next(r for r in p.universe if r["symbol"] == "TEST99")[
        "position_attention_required"
    ]


def test_retries_order_and_hash_are_deterministic(bundle):
    first = build_review_projection(bundle)
    shuffled = replace(
        bundle,
        market=bundle.market.sample(frac=1, random_state=3),
        master=bundle.master.iloc[::-1],
    )
    again = build_review_projection(shuffled)
    assert first.ordered == again.ordered
    assert (
        first.summary["decision_content_hash"] == again.summary["decision_content_hash"]
    )


def test_missing_lane_does_not_remove_other_lane_or_denominators(bundle):
    p = build_review_projection(
        replace(bundle, sources={**bundle.sources, "fundamental": ReviewSource()})
    )
    assert len(p.universe) == 100 and p.ordered[0]["combination"] == "P_ONLY"
    assert p.summary["source_issues"]["fundamental"] == "SOURCE_MISSING"
    assert p.summary["status"] == "degraded"


@pytest.mark.parametrize(
    "key,edit",
    [
        ("pattern", lambda rows: rows.append(dict(rows[0]))),
        ("signals", lambda rows: rows[0].update(signal_id="unlinked")),
        ("signals", lambda rows: rows[0].update(signal_date="2026-09-12")),
        ("signals", lambda rows: rows.append(dict(rows[0]))),
    ],
)
def test_pattern_receipts_fail_closed(bundle, key, edit):
    p = build_review_projection(changed_source(bundle, key, edit))
    row = next(r for r in p.universe if r["symbol"] == "TEST0")
    assert not row["pattern_qualified"]
    assert row["fundamental_qualified"]
    assert row["readiness"] == "EVIDENCE_EXCEPTION"
    assert not p.ordered


def test_stale_rank_is_not_relabelled_current(bundle):
    p = build_review_projection(
        changed_source(
            bundle, "rank", lambda rows: rows[0].update(timestamp="2026-09-10")
        )
    )
    assert p.ordered[0]["rank_score"] is None
    assert "RANK_STALE_EVIDENCE" in p.ordered[0]["reasons_json"]


def test_bearish_signal_never_becomes_setup_and_remains_a_blocker(bundle):
    b = changed_source(
        bundle,
        "signals",
        lambda rows: rows.append(
            {
                **rows[0],
                "signal_id": "bear",
                "pattern_family": "head_shoulders",
                "signal_direction": "bearish",
                "r1a_evidence_class": "suppression_only",
            }
        ),
    )
    b = changed_source(
        b,
        "pattern",
        lambda rows: rows[0].update(
            signal_ids_json='["positive","bear"]', signal_count=2
        ),
    )
    row = build_review_projection(b).ordered[0]
    assert row["setup_signal_id"] == "positive" and row["readiness"] == "DEFER"
    assert "BEARISH_SUPPRESSION_PRESENT" in row["reasons_json"]


def test_unknown_fundamental_blockers_and_old_policy_do_not_qualify(bundle):
    for edit in (
        lambda rows: rows[0].update(admission_blockers_json="not-json"),
        lambda rows: rows[0].update(
            admission_version="fundamental-thesis-admission-v1"
        ),
    ):
        p = build_review_projection(changed_source(bundle, "fundamental", edit))
        assert p.ordered[0]["combination"] == "P_ONLY"


def test_governance_conflict_cannot_be_upgraded_by_pattern(bundle):
    b = replace(
        bundle, governed={("NSE", "TEST0"): {"source_artifact_hash": "different"}}
    )
    p = build_review_projection(b)
    assert not p.ordered
    assert (
        "GOVERNANCE_NOT_VERIFIED"
        in next(r for r in p.universe if r["symbol"] == "TEST0")["reasons_json"]
    )


def test_calendar_holes_are_not_skipped(bundle):
    b = replace(
        bundle,
        indices=bundle.indices.loc[
            ~pd.to_datetime(bundle.indices.date).dt.date.eq(date(2026, 9, 8))
        ],
    )
    calendar, report = calendar_context(b)
    assert calendar == () and report["status"] == "FAILED"
    assert "2026-09-08" in report["missing_sessions"]
    assert not build_review_projection(b).ordered


def test_future_market_version_and_missing_bars_fail_closed(bundle):
    market = bundle.market.copy()
    market.loc[
        (market.symbol_id == "TEST0") & (market.timestamp == market.timestamp.max()),
        "adjusted_at",
    ] = "2026-09-12"
    assert not build_review_projection(replace(bundle, market=market)).ordered
    market = bundle.market.loc[
        ~(
            (bundle.market.symbol_id == "TEST0")
            & (bundle.market.timestamp == bundle.market.timestamp.max())
        )
    ]
    assert not build_review_projection(replace(bundle, market=market)).ordered


def test_replay_keeps_early_stage1_out_of_final_list(bundle):
    market = bundle.market.copy()
    market.loc[market.symbol_id == "TEST0", "high"] = 110.0
    market.loc[market.symbol_id == "TEST0", "adjusted_high"] = 110.0
    market.loc[market.symbol_id == "TEST0", "low"] = 95.0
    market.loc[market.symbol_id == "TEST0", "adjusted_low"] = 95.0
    p = build_review_projection(replace(bundle, market=market))
    assert not p.ordered
    row = next(r for r in p.universe if r["symbol"] == "TEST0")
    assert row["universe_state"] == "EXCLUDED"
    assert "LATE_BASE_FAILED_RANGE_CONTRACTION_RATIO" in row["reasons_json"]
