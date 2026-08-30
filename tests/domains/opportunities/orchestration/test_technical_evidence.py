from __future__ import annotations

from datetime import date, datetime, timezone

from ai_trading_system.domains.opportunities.contracts import InvestigatorContext
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    OpportunityShadowConfig,
    OpportunitySourceBundle,
    SourceDescriptor,
    TechnicalEvidenceState,
)
from ai_trading_system.domains.opportunities.orchestration.admission import (
    evaluate_admission,
)
from ai_trading_system.domains.opportunities.orchestration.service import (
    _technical_evidence_lineage,
)
from ai_trading_system.domains.opportunities.orchestration.technical_evidence import (
    TechnicalMarketMetrics,
    classify_symbol_technical_evidence,
)


AS_OF = datetime(2026, 8, 28, tzinfo=timezone.utc)
SESSION = date(2026, 8, 28)


def _classify(context, previous=None):
    return classify_symbol_technical_evidence(
        symbol_id="ABC",
        exchange="NSE",
        as_of=AS_OF,
        observed_session=SESSION,
        context=context,
        previous=previous,
    )


def test_boundary_values_confirm_entry_without_admission_authority():
    evidence = _classify(
        InvestigatorContext(
            price=90.0,
            sma20=90.0,
            high_52w=100.0,
            trigger_reason="WEEKLY_GAINER",
            context_as_of=AS_OF,
        )
    )

    assert evidence.near_52w_high_10 is TechnicalEvidenceState.MET
    assert evidence.above_sma20 is TechnicalEvidenceState.MET
    assert evidence.entry_confirmed is TechnicalEvidenceState.MET
    assert evidence.weekly_gainer is TechnicalEvidenceState.MET
    assert evidence.distance_from_52w_high_pct == -10.0


def test_first_close_below_sma20_is_a_separate_break_label():
    previous = _classify(
        InvestigatorContext(
            price=101.0,
            sma20=100.0,
            high_52w=105.0,
            trigger_reason="DAILY_GAINER",
            context_as_of=AS_OF,
        )
    )
    current = classify_symbol_technical_evidence(
        symbol_id="ABC",
        exchange="NSE",
        as_of=datetime(2026, 8, 29, tzinfo=timezone.utc),
        observed_session=date(2026, 8, 29),
        context=InvestigatorContext(
            price=99.0,
            sma20=100.0,
            high_52w=105.0,
            trigger_reason="DAILY_GAINER",
            context_as_of=datetime(2026, 8, 29, tzinfo=timezone.utc),
        ),
        previous=previous,
    )

    assert current.above_sma20 is TechnicalEvidenceState.NOT_MET
    assert current.sma20_break is TechnicalEvidenceState.MET


def test_missing_context_is_explicit_unknown():
    evidence = _classify(None)

    assert evidence.entry_confirmed is TechnicalEvidenceState.UNKNOWN
    assert evidence.sma20_break is TechnicalEvidenceState.NOT_APPLICABLE
    assert "investigator_context_unavailable" in evidence.missing_reasons


def test_market_metrics_keep_technical_labels_separate_from_investigator():
    evidence = classify_symbol_technical_evidence(
        symbol_id="ABC",
        exchange="NSE",
        as_of=AS_OF,
        observed_session=SESSION,
        context=None,
        market_metrics=TechnicalMarketMetrics(
            price=95.0,
            sma20=90.0,
            high_52w=100.0,
            observed_sessions=252,
        ),
    )

    assert evidence.weekly_gainer is TechnicalEvidenceState.UNKNOWN
    assert evidence.entry_confirmed is TechnicalEvidenceState.MET
    assert evidence.price_basis == "ADJUSTED_CLOSE"


def test_technical_labels_do_not_change_investigator_admission():
    context = InvestigatorContext(
        price=95.0,
        sma20=90.0,
        high_52w=100.0,
        trigger_reason="WEEKLY_GAINER",
        review_eligible=True,
        context_as_of=AS_OF,
    )
    evidence = _classify(context)
    base = OpportunitySourceBundle(
        symbol_id="ABC",
        exchange="NSE",
        as_of=AS_OF,
        investigator_context=context,
    )
    labeled = OpportunitySourceBundle(
        symbol_id="ABC",
        exchange="NSE",
        as_of=AS_OF,
        investigator_context=context,
        technical_evidence=evidence,
        technical_evidence_observation_id="technical_evidence_test",
    )

    assert evaluate_admission(base, OpportunityShadowConfig()) == evaluate_admission(
        labeled, OpportunityShadowConfig()
    )


def test_technical_lineage_is_stable_across_opportunity_retries():
    source = SourceDescriptor(
        stage_name="rank",
        artifact_type="ranked_signals",
        artifact_path="/archive/rank.csv",
        artifact_hash="rank-hash",
        run_id="run-1",
        stage_attempt=1,
    )
    bundle = OpportunitySourceBundle(
        symbol_id="ABC",
        exchange="NSE",
        as_of=AS_OF,
        source_lineage=(source,),
    )
    metrics = TechnicalMarketMetrics(95.0, 90.0, 100.0, 252)

    first = _technical_evidence_lineage(
        bundle,
        market_metrics=metrics,
        ohlcv_db_path=None,
        run_id="run-1",
        stage_attempt=2,
        policy_snapshot_id=None,
    )
    retry = _technical_evidence_lineage(
        bundle,
        market_metrics=metrics,
        ohlcv_db_path=None,
        run_id="run-1",
        stage_attempt=3,
        policy_snapshot_id=None,
    )

    assert first == retry
    assert first.stage_attempt == 1
