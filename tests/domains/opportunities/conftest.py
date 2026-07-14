from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from ai_trading_system.domains.opportunities.contracts import (
    SectorStageSnapshot,
    StageConfidenceComponents,
    StageSnapshot,
    StageStatus,
    StageTransitionReason,
    WeinsteinStage,
)
from ai_trading_system.domains.opportunities.validation import confidence_band_for_score, derive_monitoring_stage


NOW = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def stage_factory():
    def build(
        *,
        status: StageStatus = StageStatus.LOCKED,
        provisional: WeinsteinStage = WeinsteinStage.UNKNOWN,
        locked: WeinsteinStage = WeinsteinStage.STAGE_2,
        confidence: float = 80.0,
    ) -> StageSnapshot:
        return StageSnapshot(
            provisional_stage=provisional,
            locked_stage=locked,
            effective_stage=derive_monitoring_stage(provisional, locked),
            stage_status=status,
            confidence_score=confidence,
            confidence_band=confidence_band_for_score(confidence),
            confidence_components=StageConfidenceComponents(80, 80, 80, 80, 80, 80),
            stage_as_of=NOW,
            stage_locked_at=NOW if status is StageStatus.LOCKED else None,
            source_week_start=date(2026, 7, 6),
            source_week_end=date(2026, 7, 10),
            previous_locked_stage=WeinsteinStage.STAGE_1,
            weeks_in_locked_stage=2,
            provisional_persistence_days=1,
            transition_reason=StageTransitionReason.TRANSITION_PERSISTED,
            classifier_version="weekly-stage-v1",
        )

    return build

@pytest.fixture
def sector_factory(stage_factory):
    def build(*, stage=None, relative_strength: str = "Improving") -> SectorStageSnapshot:
        return SectorStageSnapshot(
            sector_id="capital-goods",
            sector_name="Capital Goods",
            stage_snapshot=stage or stage_factory(),
            sector_relative_strength_state=relative_strength,
            sector_rotation_state="Leading",
        )

    return build
