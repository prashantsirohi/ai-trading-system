"""Isolated contract examples, not forward market or performance evidence."""

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json

import pytest

from ai_trading_system.domains.opportunities.review_policy import (
    POLICY,
    EvidenceStamp,
    FundamentalEvidence,
    LateBaseMetrics,
    PatternEvidence,
    PatternSignal,
    RankEvidence,
    ReviewInput,
    SetupEvidence,
    UniverseEvidence,
    evaluate_review,
    ordered_review_list,
)

SESSION = date(2026, 9, 11)
NOW = datetime(2026, 9, 11, 15, tzinfo=timezone.utc)


def stamp(policy="test-source-v1", symbol="TEST", exchange="NSE"):
    return EvidenceStamp(
        exchange, symbol, SESSION, NOW - timedelta(hours=1), "test-hash", policy
    )


def sample(symbol="TEST", exchange="NSE", stage="S2"):
    def evidence(policy="test-source-v1"):
        return stamp(policy, symbol, exchange)

    return ReviewInput(
        exchange,
        symbol,
        SESSION,
        NOW,
        UniverseEvidence(
            evidence(),
            evidence(POLICY.stage_policy),
            stage,
            0,
            250,
            100.0,
            0.5,
            True,
            True,
            True,
            stage_status="locked",
            late_base=LateBaseMetrics(1.0, 0.0, 20.0, 0.8, 5.0, 1.0, 0.8, 1.0, 0.0),
        ),
        pattern=PatternEvidence(
            evidence(POLICY.pattern_policy),
            "KNOWN",
            "FRESH",
            (
                PatternSignal(
                    "signal-1",
                    "flat_base",
                    "confirmed",
                    "bullish",
                    "evidence_supported",
                ),
            ),
        ),
        fundamental=FundamentalEvidence(
            evidence(POLICY.fundamental_policy),
            "QUALIFIED",
            "QUALITY_COMPOUNDER",
            True,
            (),
            "consolidated",
            SESSION - timedelta(days=20),
            "accounting-hash",
        ),
        setup=SetupEvidence(evidence(), "confirmed", 99.0, 90.0, "LOW"),
        rank=RankEvidence(evidence(), 75.0),
    )


def test_qualifying_stage2_and_late_stage1():
    for stage, reason in (
        ("S2", "GOVERNED_STAGE2"),
        ("S1", "LATE_STAGE1_STRUCTURE_PASS"),
    ):
        result = evaluate_review(sample(stage=stage))
        assert result.selected and result.combination == "P+F"
        assert result.readiness == "SETUP_REVIEW"
        assert reason in result.reasons
        assert result.policy_hash == POLICY.content_hash


@pytest.mark.parametrize("stage", ["S3", "S4"])
def test_stage_exclusion_preserves_position_attention(stage):
    result = evaluate_review(replace(sample(stage=stage), has_open_position=True))
    assert not result.selected
    assert result.position_attention_required
    assert result.universe_state == "EXCLUDED"


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("identity_verified", False, "EVIDENCE_EXCEPTION"),
        ("market_trusted", None, "EVIDENCE_EXCEPTION"),
        ("market_trusted", "True", "EVIDENCE_EXCEPTION"),
        ("governance_resolved", False, "EVIDENCE_EXCEPTION"),
        ("stage_age_sessions", -1, "EVIDENCE_EXCEPTION"),
        ("stage_age_sessions", 11, "EVIDENCE_EXCEPTION"),
        ("stage_age_sessions", True, "EVIDENCE_EXCEPTION"),
        ("history_bars", 179, "EXCLUDED"),
        ("close", 19.99, "EXCLUDED"),
        ("close", float("nan"), "EVIDENCE_EXCEPTION"),
        ("liquidity_percentile", 0.199, "EXCLUDED"),
        ("liquidity_percentile", float("inf"), "EVIDENCE_EXCEPTION"),
        ("liquidity_percentile", 1.01, "EVIDENCE_EXCEPTION"),
        ("stage", "UNKNOWN", "EVIDENCE_EXCEPTION"),
    ],
)
def test_universe_fail_closed(field, value, expected):
    row = sample()
    result = evaluate_review(
        replace(row, universe=replace(row.universe, **{field: value}))
    )
    assert result.universe_state == expected
    assert not result.selected


def test_inclusive_universe_boundaries():
    row = sample()
    row = replace(
        row,
        universe=replace(
            row.universe,
            history_bars=180,
            close=20.0,
            liquidity_percentile=0.2,
            stage_age_sessions=10,
        ),
    )
    assert evaluate_review(row).universe_state == "ELIGIBLE"


@pytest.mark.parametrize("field,low,high", POLICY.late_base_bounds)
def test_every_late_base_check_is_required(field, low, high):
    row = sample(stage="S1")
    missing = replace(row.universe.late_base, **{field: None})
    assert (
        evaluate_review(
            replace(row, universe=replace(row.universe, late_base=missing))
        ).universe_state
        == "EVIDENCE_EXCEPTION"
    )
    failing = replace(row.universe.late_base, **{field: low - 0.01})
    assert (
        evaluate_review(
            replace(row, universe=replace(row.universe, late_base=failing))
        ).universe_state
        == "EXCLUDED"
    )
    boundary = replace(row.universe.late_base, **{field: low})
    assert (
        evaluate_review(
            replace(row, universe=replace(row.universe, late_base=boundary))
        ).universe_state
        == "ELIGIBLE"
    )


def test_transition_does_not_override_current_stage():
    row = sample()
    assert evaluate_review(
        replace(row, universe=replace(row.universe, transition="S1_TO_S2"))
    ).selected
    row = sample(stage="S4")
    result = evaluate_review(
        replace(
            row,
            universe=replace(
                row.universe, transition="S1_TO_S2", technical_stage2=True
            ),
        )
    )
    assert not result.selected
    assert "STAGE_TRANSITION_CONFLICT" in result.reasons


@pytest.mark.parametrize("source", ["pattern", "fundamental", "setup", "rank"])
def test_post_decision_or_cross_listing_evidence_never_qualifies(source):
    row = sample()
    for altered_stamp in (
        replace(getattr(row, source).stamp, available_at=NOW + timedelta(seconds=1)),
        replace(getattr(row, source).stamp, symbol="OTHER"),
        replace(getattr(row, source).stamp, exchange="BSE"),
        replace(getattr(row, source).stamp, session=SESSION - timedelta(days=1)),
    ):
        result = evaluate_review(
            replace(row, **{source: replace(getattr(row, source), stamp=altered_stamp)})
        )
        if source in ("pattern", "fundamental"):
            assert not getattr(result, source).qualified
        elif source == "setup":
            assert result.readiness == "EVIDENCE_EXCEPTION"
        else:
            assert result.rank_score is None


def test_future_stage_cannot_claim_zero_age():
    row = sample()
    stage_stamp = replace(row.universe.stage_stamp, session=SESSION + timedelta(days=1))
    assert not evaluate_review(
        replace(row, universe=replace(row.universe, stage_stamp=stage_stamp))
    ).selected


def test_lane_or_and_missing_distinct_from_evaluated_none():
    row = sample()
    missing = evaluate_review(replace(row, fundamental=None))
    none = evaluate_review(
        replace(
            row,
            fundamental=replace(
                row.fundamental,
                classification="UNCLASSIFIED_FUNDAMENTAL",
                primary_thesis=None,
                admission_eligible=False,
            ),
        )
    )
    assert missing.selected and none.selected
    assert missing.combination == none.combination == "P_ONLY"
    assert missing.fundamental.state != none.fundamental.state
    assert evaluate_review(replace(row, pattern=None)).combination == "F_ONLY"
    assert not evaluate_review(replace(row, pattern=None, fundamental=None)).selected


def test_suppression_primary_does_not_hide_secondary_bullish_setup():
    row = sample()
    bearish = PatternSignal(
        "head", "head_shoulders", "confirmed", "bearish", "suppression_only"
    )
    only = replace(row.pattern, signals=(bearish,))
    assert not evaluate_review(replace(row, pattern=only)).pattern.qualified
    both = replace(row.pattern, signals=(bearish, *row.pattern.signals))
    assert evaluate_review(replace(row, pattern=both)).pattern.qualified


@pytest.mark.parametrize(
    "change",
    [
        {"signals": ()},
        {"evaluation_state": "NONE"},
        {"freshness": "STALE"},
        {
            "signals": (
                PatternSignal(
                    "x", "flat_base", "invalidated", "bullish", "evidence_supported"
                ),
            )
        },
        {
            "signals": (
                PatternSignal(
                    "x", "head_shoulders", "confirmed", "bullish", "suppression_only"
                ),
            )
        },
    ],
)
def test_pattern_membership_is_not_positive_qualification(change):
    row = sample()
    assert not evaluate_review(
        replace(row, pattern=replace(row.pattern, **change))
    ).pattern.qualified


@pytest.mark.parametrize(
    "changes,state",
    [
        ({"source_available_date": SESSION + timedelta(days=1)}, "FUTURE_EVIDENCE"),
        ({"source_available_date": SESSION - timedelta(days=551)}, "STALE_SOURCE"),
        ({"source_available_date": None}, "MISSING_SOURCE_DATE"),
        ({"statement_basis": "mixed"}, "INVALID_SOURCE"),
        ({"admission_eligible": "True"}, "DAILY_CONTEXT_BLOCKED"),
        ({"blockers": ("DAILY_CONTEXT_INCOMPLETE",)}, "DAILY_CONTEXT_BLOCKED"),
        ({"stamp": stamp("fundamental-thesis-admission-v1")}, "UNSUPPORTED_POLICY"),
    ],
)
def test_fundamental_daily_contract(changes, state):
    row = sample()
    result = evaluate_review(
        replace(row, fundamental=replace(row.fundamental, **changes))
    )
    assert not result.fundamental.qualified and result.fundamental.state == state


@pytest.mark.parametrize(
    "changes,readiness",
    [
        ({"state": "NONE"}, "DEVELOPING_WATCH"),
        ({"state": "watchlist"}, "DEVELOPING_WATCH"),
        ({"trigger": 101.0}, "DEVELOPING_WATCH"),
        ({"extension_risk": "HIGH"}, "DEFER"),
        ({"extension_risk": "MEDIUM"}, "DEFER"),
        ({"extension_risk": "UNKNOWN"}, "EVIDENCE_EXCEPTION"),
        ({"invalidation": 100.5, "trigger": 101.0}, "DEFER"),
        ({"invalidation": None}, "EVIDENCE_EXCEPTION"),
        ({"invalidation": 101.0}, "EVIDENCE_EXCEPTION"),
        ({"trigger": float("inf")}, "EVIDENCE_EXCEPTION"),
        ({"blockers": ("SECTOR_BLOCKED",)}, "DEFER"),
    ],
)
def test_readiness(changes, readiness):
    row = sample()
    result = evaluate_review(
        replace(row, setup=replace(row.setup, **changes), pattern=None)
    )
    assert result.combination == "F_ONLY"
    assert result.readiness == readiness


def test_technical_disagreement_defers_governed_stage2():
    row = sample()
    result = evaluate_review(
        replace(row, universe=replace(row.universe, technical_stage2=False))
    )
    assert result.selected and result.readiness == "DEFER"


def test_order_is_readiness_then_score_then_identity_without_overlap_bonus():
    a, b, c, d = (sample(s) for s in ("AAA", "BBB", "CCC", "DDD"))
    a = replace(a, fundamental=None, rank=replace(a.rank, score=50.0))
    b = replace(b, rank=replace(b.rank, score=40.0))
    c = replace(
        c, setup=replace(c.setup, state="watchlist"), rank=replace(c.rank, score=99.0)
    )
    d = replace(d, rank=None)
    assert [r.symbol for r in ordered_review_list((c, d, b, a))] == [
        "AAA",
        "BBB",
        "DDD",
        "CCC",
    ]
    assert ordered_review_list((a, b, c, d)) == ordered_review_list((d, c, b, a))
    assert not ordered_review_list((sample(stage="S4"),))
    assert not ordered_review_list((replace(a, setup=None),))
    assert [
        r.exchange
        for r in ordered_review_list((sample(exchange="NSE"), sample(exchange="BSE")))
    ] == ["BSE", "NSE"]


def test_duplicate_and_mixed_session_batches_rejected():
    row = sample()
    with pytest.raises(ValueError, match="Duplicate"):
        ordered_review_list((row, row))
    other = replace(sample("OTHER"), session=SESSION - timedelta(days=1))
    with pytest.raises(ValueError, match="mixed"):
        ordered_review_list((row, other))


def test_naive_decision_timestamp_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate_review(replace(sample(), decision_at=NOW.replace(tzinfo=None)))


def test_policy_is_frozen_and_content_bound():
    from dataclasses import FrozenInstanceError, asdict

    with pytest.raises(FrozenInstanceError):
        POLICY.min_close = 0
    assert POLICY.content_hash != replace(POLICY, min_close=21.0).content_hash
    assert (
        json.loads(json.dumps(asdict(POLICY)))["version"] == "stage-universe-review-v1"
    )


def test_mixed_cutoffs_rejected_even_in_one_session():
    first = sample("FIRST")
    second = replace(sample("SECOND"), decision_at=NOW + timedelta(minutes=1))
    with pytest.raises(ValueError, match="cutoffs"):
        ordered_review_list((first, second))


def test_stage_status_and_transition_are_preserved():
    row = sample()
    row = replace(
        row,
        universe=replace(
            row.universe, stage_status="provisional", transition="S1_TO_S2"
        ),
    )
    result = evaluate_review(row)
    assert (result.stage, result.stage_status, result.transition) == (
        "S2",
        "provisional",
        "S1_TO_S2",
    )
    unknown = replace(row, universe=replace(row.universe, stage_status="UNKNOWN"))
    assert not evaluate_review(unknown).selected


def test_unknown_pattern_class_cannot_be_called_positive():
    row = sample()
    unknown = replace(row.pattern.signals[0], evidence_class="")
    result = evaluate_review(
        replace(row, pattern=replace(row.pattern, signals=(unknown,)))
    )
    assert not result.pattern.qualified


def test_negative_research_evidence_remains_visible():
    row = sample()
    flag = replace(
        row.pattern.signals[0], family="flag", evidence_class="negative_evidence"
    )
    result = evaluate_review(
        replace(row, pattern=replace(row.pattern, signals=(flag,)))
    )
    assert result.pattern.qualified
    assert "PATTERN_EVIDENCE_CLASS_NEGATIVE_EVIDENCE" in result.pattern.reasons
