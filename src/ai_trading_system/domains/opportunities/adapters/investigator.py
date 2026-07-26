"""Investigator evidence adapter."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any, Mapping, Sequence

from ai_trading_system.domains.opportunities.compatibility import (
    map_legacy_evidence_verdict,
)
from ai_trading_system.domains.opportunities.contracts import (
    EvidenceSnapshot,
    InvestigatorContext,
)
from ai_trading_system.domains.opportunities.orchestration.contracts import (
    AdaptedRecord,
    AdapterResult,
    AdapterWarning,
    INVESTIGATOR_ACTIVE_REVIEW_SCORE,
    INVESTIGATOR_PRIMARY_LANE,
    RejectedSourceRow,
    SourceDescriptor,
)

from .common import (
    as_float,
    first,
    normalize_exchange,
    normalize_symbol,
    risk_level,
    row_identity,
    text_tuple,
)


def adapt_investigator_rows(
    rows: Sequence[Mapping[str, Any]], *, source: SourceDescriptor, as_of: datetime
) -> AdapterResult[AdaptedRecord[EvidenceSnapshot]]:
    records: list[AdaptedRecord[EvidenceSnapshot]] = []
    warnings: list[AdapterWarning] = []
    rejected: list[RejectedSourceRow] = []
    for row in rows:
        identity = row_identity(row)
        symbol = normalize_symbol(first(row, "symbol_id", "symbol", "ticker"))
        exchange = normalize_exchange(first(row, "exchange", "exchange_code"))
        score = as_float(
            first(row, "final_score", "investigator_score", "evidence_score")
        )
        if not symbol or score is None or not 0 <= score <= 100:
            rejected.append(
                RejectedSourceRow(
                    source.artifact_type,
                    identity,
                    "missing Investigator evaluation",
                    ("symbol_id", "final_score"),
                )
            )
            continue
        verdict = map_legacy_evidence_verdict(
            first(row, "verdict", "investigator_verdict")
        )
        warnings.extend(
            AdapterWarning(source.artifact_type, identity, "legacy_verdict", message)
            for message in verdict.warnings
        )
        combined_volume = as_float(first(row, "volume_delivery_score"))
        snapshot = EvidenceSnapshot(
            evidence_score=score,
            investigator_verdict=verdict.value,
            accumulation_score=as_float(
                first(row, "early_accumulation_score", "accumulation_score")
            ),
            pattern_score=as_float(
                first(row, "pattern_score", "base_pattern_freshness_score")
            ),
            breakout_quality=as_float(
                first(
                    row, "breakout_quality", "breakout_score", "trigger_quality_score"
                )
            ),
            volume_quality=as_float(
                first(
                    row,
                    "volume_quality",
                    "volume_confirmation_score",
                    "volume_score",
                    "volume_delivery_score",
                )
            ),
            delivery_quality=as_float(
                first(row, "delivery_quality", "delivery_accumulation_score")
            ),
            sector_alignment=as_float(
                first(row, "sector_alignment", "sector_support_score")
            ),
            market_alignment=as_float(
                first(row, "market_alignment", "market_support_score")
            ),
            extension_risk=risk_level(
                first(row, "extension_risk", "extension_risk_level")
            ),
            failure_risk=risk_level(first(row, "failure_risk", "failure_risk_level")),
            positive_evidence=text_tuple(
                first(row, "positive_evidence", "positive_evidence_json")
            ),
            negative_evidence=text_tuple(
                first(row, "negative_evidence", "negative_evidence_json")
            ),
            missing_evidence=text_tuple(
                first(row, "missing_evidence", "missing_evidence_json")
            ),
            evidence_model_version=str(
                first(
                    row,
                    "evidence_model_version",
                    "investigator_model_version",
                    "model_version",
                )
                or "investigator-unknown"
            ),
            evaluated_at=as_of,
        )
        if combined_volume is not None and snapshot.delivery_quality is None:
            warnings.append(
                AdapterWarning(
                    source.artifact_type,
                    identity,
                    "combined_volume_delivery",
                    "combined volume/delivery score retained as volume quality; delivery quality remains unavailable",
                )
            )
        pattern_family = _text(first(row, "pattern_family"))
        pattern_state = _text(first(row, "pattern_state"))
        pattern_score = as_float(
            first(row, "pattern_score", "base_pattern_freshness_score")
        )
        setup_quality_score = as_float(first(row, "setup_quality"))
        breakout_type = _text(first(row, "breakout_type", "setup_family"))
        breakout_tier = _tier(
            first(row, "breakout_tier", "candidate_tier", "pattern_operational_tier")
        )
        move_tag = _text(first(row, "move_tag"))
        trigger_reason = _text(first(row, "trigger_reason"))
        if move_tag == "UNKNOWN" and trigger_reason == "WEEKLY_GAINER":
            move_tag = "WEEKLY_MOMENTUM"
        review_eligible = (
            move_tag == INVESTIGATOR_PRIMARY_LANE
            and score >= INVESTIGATOR_ACTIVE_REVIEW_SCORE
        )
        price = as_float(first(row, "close", "price", "latest_close"))
        sma50 = as_float(first(row, "sma50", "sma_50", "ma50", "dma_50"))
        high_52w = as_float(first(row, "high_52w", "52w_high", "week_52_high"))
        breakout_level = as_float(
            first(row, "breakout_level", "trigger_price", "pivot_price")
        )
        context = InvestigatorContext(
            stage_label=_text(first(row, "stage_label")),
            stage_confidence=_confidence(
                first(
                    row,
                    "stage_confidence",
                    "stage_score",
                    "stage1_score_confidence",
                )
            ),
            pattern_family=pattern_family,
            pattern_state=pattern_state,
            pattern_score=pattern_score,
            setup_quality_score=setup_quality_score,
            setup_quality_bucket=_setup_bucket(
                first(row, "setup_quality_bucket", "setup_quality")
            ),
            breakout_type=breakout_type,
            candidate_tier=breakout_tier,
            breakout_tier=breakout_tier,
            qualified_breakout=_optional_bool(first(row, "qualified_breakout")),
            move_tag=move_tag,
            trigger_reason=trigger_reason,
            final_score=score,
            attribution_score=score,
            review_lane=(
                "PRIMARY"
                if review_eligible
                else "CONDITIONAL"
                if trigger_reason == "DAILY_GAINER"
                else "RESEARCH_ONLY"
            ),
            review_eligible=review_eligible,
            confirmed_regime=_text(
                first(row, "confirmed_regime", "market_regime", "regime")
            ),
            raw_regime=_text(first(row, "raw_regime")),
            regime_confidence=_ratio_confidence(first(row, "regime_confidence")),
            breadth_velocity_bucket=_text(first(row, "breadth_velocity_bucket")),
            breadth_velocity_quantile=_text(
                first(row, "breadth_velocity_quantile")
            ),
            regime_score_chg_5d=as_float(first(row, "regime_score_chg_5d")),
            sector_relative_strength_bucket=_text(
                first(
                    row,
                    "sector_relative_strength_bucket",
                    "sector_rs_bucket",
                )
            ),
            sector_leadership=_text(
                first(row, "sector_leadership", "sector_quadrant")
            ),
            price=price,
            volume=as_float(first(row, "volume", "latest_volume")),
            sma20=as_float(first(row, "sma20", "sma_20", "ma20", "dma_20")),
            sma50=sma50,
            sma200=as_float(first(row, "sma200", "sma_200", "ma200", "dma_200")),
            high_52w=high_52w,
            breakout_level=breakout_level,
            invalidation_price=as_float(
                first(
                    row,
                    "invalidation_price",
                    "pattern_invalidation_price",
                    "invalidation_level",
                )
            ),
            distance_from_breakout_pct=_distance_pct(price, breakout_level),
            distance_from_sma50_pct=_distance_pct(price, sma50),
            distance_from_52w_high_pct=_distance_pct(price, high_52w),
            context_as_of=as_of,
            source_run_id=source.run_id,
            source_artifact_hashes=(source.artifact_hash,),
            classifier_versions=tuple(
                value
                for value in (
                    str(
                        first(row, "stage1_model_version", "stage_classifier_version")
                        or ""
                    ).strip(),
                    str(
                        first(
                            row,
                            "evidence_model_version",
                            "investigator_model_version",
                            "model_version",
                        )
                        or ""
                    ).strip(),
                )
                if value
            ),
            attribution_mode="OBSERVED_AT_DECISION",
            source_lineage=(
                {
                    "artifact_type": source.artifact_type,
                    "run_id": source.run_id,
                    "stage_attempt": source.stage_attempt,
                    "observed_at": as_of,
                    "artifact_hash": source.artifact_hash,
                },
            ),
            evaluation_states={
                "stage": _evaluation_state(
                    first(row, "stage_evaluation_state"), _text(first(row, "stage_label"))
                ),
                "pattern_attempted": _evaluation_state(
                    first(row, "pattern_evaluation_state"),
                    pattern_family,
                    known_default="KNOWN" if pattern_score is not None else "UNKNOWN",
                ),
                "pattern": _evaluation_state(
                    first(row, "pattern_classification_state"), pattern_family
                ),
                "setup_quality": _evaluation_state(
                    first(row, "setup_quality_state"),
                    setup_quality_score,
                ),
                "breakout": _evaluation_state(
                    first(row, "breakout_evaluation_state"), breakout_type
                ),
                "regime": _evaluation_state(
                    first(row, "regime_evaluation_state"),
                    _text(first(row, "confirmed_regime", "market_regime", "regime")),
                ),
                "breadth": _evaluation_state(
                    first(row, "breadth_evaluation_state"),
                    _text(first(row, "breadth_velocity_bucket")),
                ),
                "sector": _evaluation_state(
                    first(row, "sector_evaluation_state"),
                    _text(first(row, "sector_relative_strength_bucket", "sector_rs_bucket")),
                ),
                "lineage": "KNOWN" if source.artifact_hash and source.run_id else "UNKNOWN",
            },
        )
        records.append(
            AdaptedRecord(
                exchange,
                symbol,
                replace(snapshot, investigator_context=context),
                identity,
                source,
            )
        )
    return AdapterResult(tuple(records), tuple(warnings), tuple(rejected), source)


def _text(value: Any) -> str:
    text = str(value or "").strip().upper().replace(" ", "_").replace("-", "_")
    return text if text and text not in {"NAN", "<NA>"} else "UNKNOWN"


def _confidence(value: Any) -> float | None:
    parsed = as_float(value)
    if parsed is None:
        return None
    return max(0.0, min(100.0, parsed * 100.0 if parsed <= 1.0 else parsed))


def _ratio_confidence(value: Any) -> float | None:
    parsed = as_float(value)
    if parsed is None:
        return None
    return max(0.0, min(1.0, parsed / 100.0 if parsed > 1.0 else parsed))


def _setup_bucket(value: Any) -> str:
    text = _text(value)
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    parsed = as_float(value)
    if parsed is None:
        return "UNKNOWN"
    return "HIGH" if parsed >= 70 else "MEDIUM" if parsed >= 45 else "LOW"


def _tier(value: Any) -> str:
    text = _text(value)
    return text if text in {"A", "B", "C", "D"} else "UNKNOWN"


def _optional_bool(value: Any) -> bool | None:
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes", "y", "qualified"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _distance_pct(value: float | None, reference: float | None) -> float | None:
    if value is None or reference in (None, 0.0):
        return None
    return round(((float(value) / float(reference)) - 1.0) * 100.0, 6)


def _evaluation_state(
    explicit: Any,
    value: Any,
    *,
    known_default: str = "KNOWN",
) -> str:
    allowed = {
        "KNOWN",
        "NONE",
        "NOT_ELIGIBLE",
        "NOT_EVALUATED",
        "ERROR",
        "UNKNOWN",
    }
    normalized = _text(explicit)
    if explicit not in (None, "") and normalized in allowed:
        return normalized
    normalized_value = _text(value)
    if normalized_value == "NONE":
        return "NONE"
    if normalized_value == "UNKNOWN":
        return "UNKNOWN"
    return known_default
