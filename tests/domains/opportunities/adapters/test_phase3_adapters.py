from __future__ import annotations

from datetime import datetime, timezone

from ai_trading_system.domains.opportunities.adapters import (
    adapt_investigator_rows,
    adapt_ranking_rows,
    adapt_sector_stage_rows,
    adapt_stock_stage_rows,
)
from ai_trading_system.domains.opportunities.contracts import StageStatus, WeinsteinStage
from ai_trading_system.domains.opportunities.orchestration.contracts import SourceDescriptor


NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)
SOURCE = SourceDescriptor("rank", "fixture", "/tmp/fixture.csv", "abc", "run-1", 1, 1)


def test_ranking_derives_position_percentile_and_preserves_missing_velocity():
    result = adapt_ranking_rows(
        [{"symbol": "abc", "exchange": "nse", "composite_score": "88", "relative_strength_score": "91"}],
        source=SOURCE,
        as_of=NOW,
    )
    snapshot = result.records[0].value
    assert snapshot.rank_position == 1
    assert snapshot.rank_percentile == 100
    assert snapshot.rank_velocity is None
    assert snapshot.factor_scores["relative_strength_score"] == 91


def test_investigator_keeps_unavailable_components_null():
    result = adapt_investigator_rows(
        [{"symbol_id": "ABC", "final_score": "82", "verdict": "HIGH_CONVICTION", "volume_delivery_score": "18"}],
        source=SOURCE,
        as_of=NOW,
    )
    snapshot = result.records[0].value
    assert snapshot.evidence_score == 82
    assert snapshot.volume_quality == 18
    assert snapshot.delivery_quality is None
    assert snapshot.market_alignment is None


def test_stock_stage_separates_provisional_and_locked():
    provisional = adapt_stock_stage_rows(
        [{"symbol_id": "ABC", "weekly_stage_label": "S1_TO_S2", "weekly_stage_confidence": "0.8", "week_end_date": "2026-07-14"}],
        source=SOURCE,
        as_of=NOW,
    ).records[0].value
    assert provisional.stage_status is StageStatus.PROVISIONAL
    assert provisional.provisional_stage is WeinsteinStage.TRANSITION_1_TO_2
    assert provisional.locked_stage is WeinsteinStage.UNKNOWN
    assert provisional.confidence_score == 80

    locked = adapt_stock_stage_rows(
        [{"symbol_id": "ABC", "stage_label": "S2", "stage_confidence": "0.75", "week_end_date": "2026-07-10", "created_at": "2026-07-10T12:00:00+00:00"}],
        source=SOURCE,
        as_of=NOW,
    ).records[0].value
    assert locked.stage_status is StageStatus.LOCKED
    assert locked.locked_stage is WeinsteinStage.STAGE_2
    assert locked.stage_locked_at is not None


def test_sector_rotation_does_not_imply_structural_stage():
    result = adapt_sector_stage_rows(
        [{"Sector": "Capital Goods", "RS_rank_pct": "95", "Quadrant": "Leading"}],
        source=SOURCE,
        as_of=NOW,
    )
    snapshot = result.records[0].value
    assert snapshot.stage_snapshot.effective_stage is WeinsteinStage.UNKNOWN
    assert snapshot.sector_rotation_state == "Leading"
    assert any(item.code == "sector_structural_stage_unavailable" for item in result.warnings)


def test_locked_unknown_stage_remains_canonical_unknown():
    result = adapt_sector_stage_rows(
        [{
            "sector_name": "Capital Goods", "effective_stage": "unknown",
            "stage_status": "locked", "stage_confidence_score": 0,
            "source_week_end": "2026-07-10", "as_of": "2026-07-10T12:00:00+00:00",
        }],
        source=SOURCE,
        as_of=NOW,
    )
    stage = result.records[0].value.stage_snapshot
    assert stage.stage_status is StageStatus.UNKNOWN
    assert stage.effective_stage is WeinsteinStage.UNKNOWN
