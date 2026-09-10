"""Read-only Investigator/fundamental/pattern convergence projection."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from ai_trading_system.domains.opportunities.registry import stable_digest

from .contracts import (
    INVESTIGATOR_ACTIVE_REVIEW_SCORE,
    INVESTIGATOR_PRIMARY_TRIGGER,
)

CONVERGENCE_POLICY_VERSION = "opportunity-convergence-v1.3"
CONVERGENCE_COHORTS: tuple[str, ...] = (
    "I_ONLY",
    "F_ONLY",
    "P_ONLY",
    "I_F",
    "I_P",
    "F_P",
    "I_F_P",
    "NONE",
)


class LaneEvaluationState(str, Enum):
    KNOWN = "KNOWN"
    NONE = "NONE"
    NOT_ELIGIBLE = "NOT_ELIGIBLE"
    NOT_EVALUATED = "NOT_EVALUATED"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class LaneFreshness(str, Enum):
    FRESH = "FRESH"
    STALE = "STALE"
    FUTURE = "FUTURE"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True, slots=True)
class LaneAssessment:
    """One normalized lane result at the convergence decision session."""

    state: LaneEvaluationState
    lane_state: str
    member: bool
    freshness: LaneFreshness
    evidence_hash: str | None
    artifact_hashes: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    details: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.member and self.state is not LaneEvaluationState.KNOWN:
            raise ValueError("lane membership requires KNOWN evaluation state")
        if self.member and self.freshness is not LaneFreshness.FRESH:
            raise ValueError("lane membership requires fresh evidence")
        object.__setattr__(
            self,
            "details",
            MappingProxyType(dict(self.details or {})),
        )


def build_convergence_view(
    *,
    session_date: date,
    policy_snapshot_id: str | None,
    universe_keys: Iterable[tuple[str, str]],
    investigator_rows: list[dict[str, Any]],
    investigator_receipts: list[dict[str, Any]],
    fundamental_rows: list[dict[str, Any]],
    pattern_rows: list[dict[str, Any]],
    investigator_artifact_hash: str | None,
    investigator_receipt_artifact_hash: str | None,
    fundamental_artifact_hash: str | None,
    pattern_artifact_hash: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build a deterministic, mutually exclusive I/F/P operator projection."""

    investigator = _group(investigator_rows)
    receipts = _group(investigator_receipts)
    fundamental = _group(fundamental_rows)
    pattern = _group(pattern_rows)
    keys = {
        _normal_key(exchange, symbol)
        for exchange, symbol in universe_keys
        if str(symbol or "").strip()
    }
    keys.update(investigator)
    keys.update(receipts)
    keys.update(fundamental)
    keys.update(pattern)

    rows: list[dict[str, Any]] = []
    for exchange, symbol in sorted(keys):
        i_assessment = _investigator_assessment(
            session_date=session_date,
            rows=investigator.get((exchange, symbol), ()),
            receipts=receipts.get((exchange, symbol), ()),
            artifact_hash=investigator_artifact_hash,
            receipt_artifact_hash=investigator_receipt_artifact_hash,
        )
        f_assessment = _fundamental_assessment(
            session_date=session_date,
            rows=fundamental.get((exchange, symbol), ()),
            artifact_hash=fundamental_artifact_hash,
        )
        p_assessment = _pattern_assessment(
            session_date=session_date,
            rows=pattern.get((exchange, symbol), ()),
            artifact_hash=pattern_artifact_hash,
        )
        cohort = _cohort(
            i_assessment.member,
            f_assessment.member,
            p_assessment.member,
        )
        row = {
            "exchange": exchange,
            "symbol_id": symbol,
            "session_date": session_date.isoformat(),
            "policy_snapshot_id": policy_snapshot_id,
            "convergence_policy_version": CONVERGENCE_POLICY_VERSION,
            "convergence_cohort": cohort,
        }
        _add_lane(row, "investigator", i_assessment)
        _add_lane(row, "fundamental", f_assessment)
        _add_lane(row, "pattern", p_assessment)
        sector_name, sector_state, sector_reasons = _sector_context(
            i_assessment,
            f_assessment,
            p_assessment,
        )
        row["sector_name"] = sector_name
        row["sector_evaluation_state"] = sector_state
        row["sector_reason_codes_json"] = json.dumps(
            sector_reasons, separators=(",", ":")
        )
        row["evidence_hash"] = stable_digest(
            {
                "exchange": exchange,
                "symbol_id": symbol,
                "session_date": session_date,
                "policy_snapshot_id": policy_snapshot_id,
                "cohort": cohort,
                "investigator_evidence_hash": i_assessment.evidence_hash,
                "fundamental_evidence_hash": f_assessment.evidence_hash,
                "pattern_evidence_hash": p_assessment.evidence_hash,
                "sector_name": sector_name,
                "sector_evaluation_state": sector_state,
            }
        )
        rows.append(row)

    return rows, convergence_readiness_inputs(
        rows,
        expected_count=len(keys),
        source_presence={
            "investigator": bool(
                investigator_artifact_hash and investigator_receipt_artifact_hash
            ),
            "fundamental": bool(fundamental_artifact_hash),
            "pattern": bool(pattern_artifact_hash),
        },
        policy_snapshot_id=policy_snapshot_id,
    )


def convergence_readiness_inputs(
    rows: list[dict[str, Any]],
    *,
    expected_count: int,
    source_presence: Mapping[str, bool],
    policy_snapshot_id: str | None,
) -> list[dict[str, Any]]:
    """Return P1.5 checks in the existing Phase 3C-5 readiness row shape."""

    valid_states = {state.value for state in LaneEvaluationState}
    valid_cohorts = set(CONVERGENCE_COHORTS)
    keys = {
        (
            row.get("exchange"),
            row.get("symbol_id"),
            row.get("session_date"),
            row.get("policy_snapshot_id"),
        )
        for row in rows
    }
    state_cells = [
        row.get(f"{lane}_evaluation_state")
        for row in rows
        for lane in ("investigator", "fundamental", "pattern")
    ]
    cohort_matches = sum(
        row.get("convergence_cohort")
        == _cohort(
            bool(row.get("investigator_member")),
            bool(row.get("fundamental_member")),
            bool(row.get("pattern_member")),
        )
        and row.get("convergence_cohort") in valid_cohorts
        for row in rows
    )
    source_backed = [
        (row, lane)
        for row in rows
        for lane in ("investigator", "fundamental", "pattern")
        if row.get(f"{lane}_evaluation_state")
        in {
            LaneEvaluationState.KNOWN.value,
            LaneEvaluationState.NONE.value,
            LaneEvaluationState.NOT_ELIGIBLE.value,
            LaneEvaluationState.ERROR.value,
        }
    ]
    hash_covered = sum(
        bool(row.get(f"{lane}_evidence_hash")) for row, lane in source_backed
    )
    active_evidence = [
        (row, lane)
        for row in rows
        for lane in ("investigator", "fundamental", "pattern")
        if bool(
            row.get(
                {
                    "investigator": "investigator_primary_review_eligible",
                    "fundamental": "fundamental_admission_eligible",
                    "pattern": "pattern_evidence_present",
                }[lane]
            )
        )
    ]
    fresh_active_evidence = sum(
        row.get(f"{lane}_freshness") == LaneFreshness.FRESH.value
        for row, lane in active_evidence
    )
    unknown_count = sum(
        value == LaneEvaluationState.UNKNOWN.value for value in state_cells
    )
    error_count = sum(value == LaneEvaluationState.ERROR.value for value in state_cells)
    error_count += sum(
        row.get("sector_evaluation_state") == LaneEvaluationState.ERROR.value
        for row in rows
    )
    source_count = sum(bool(value) for value in source_presence.values())

    checks = (
        (
            "OPPORTUNITY_CONVERGENCE_LANE_SOURCES",
            source_count,
            3,
            "PASS" if source_count == 3 else "PENDING",
            "all three immutable lane sources are present",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_CARDINALITY",
            len(rows),
            expected_count,
            "PASS" if len(rows) == expected_count else "FAIL",
            "one row per exchange, symbol, session, and policy snapshot",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_UNIQUE_KEYS",
            len(keys),
            len(rows),
            "PASS" if len(keys) == len(rows) else "FAIL",
            "convergence keys are unique",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_EXPLICIT_STATES",
            sum(value in valid_states for value in state_cells),
            len(state_cells),
            "PASS" if all(value in valid_states for value in state_cells) else "FAIL",
            "every lane uses the frozen explicit evaluation-state vocabulary",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_COHORT_EXCLUSIVITY",
            cohort_matches,
            len(rows),
            "PASS" if cohort_matches == len(rows) else "FAIL",
            "each row maps to exactly one I/F/P membership cohort",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_EVIDENCE_HASHES",
            hash_covered,
            len(source_backed),
            "PASS" if hash_covered == len(source_backed) else "FAIL",
            "every source-backed lane state carries immutable evidence lineage",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_ACTIVE_FRESHNESS",
            fresh_active_evidence,
            len(active_evidence),
            "PASS" if fresh_active_evidence == len(active_evidence) else "FAIL",
            "active cohort membership requires current-session evidence",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_UNEXPLAINED_UNKNOWN",
            unknown_count,
            0,
            "PASS" if unknown_count == 0 else "FAIL",
            "UNKNOWN is reserved for unexplained source gaps",
        ),
        (
            "OPPORTUNITY_CONVERGENCE_SOURCE_ERRORS",
            error_count,
            0,
            "PASS" if error_count == 0 else "FAIL",
            "duplicate, malformed, or future-dated lane evidence fails readiness",
        ),
    )
    return [
        {
            "check_id": check_id,
            "category": "opportunity_convergence",
            "status": status,
            "observed": observed,
            "expected": expected,
            "production_blocking": True,
            "policy_version": CONVERGENCE_POLICY_VERSION,
            "policy_snapshot_id": policy_snapshot_id,
            "details": details,
        }
        for check_id, observed, expected, status, details in checks
    ]


def _investigator_assessment(
    *,
    session_date: date,
    rows: Iterable[dict[str, Any]],
    receipts: Iterable[dict[str, Any]],
    artifact_hash: str | None,
    receipt_artifact_hash: str | None,
) -> LaneAssessment:
    source_rows = list(rows)
    receipt_rows = list(receipts)
    hashes = tuple(value for value in (artifact_hash, receipt_artifact_hash) if value)
    if not artifact_hash or not receipt_artifact_hash:
        return _not_evaluated(hashes, "INVESTIGATOR_SOURCE_ABSENT")
    if len(source_rows) > 1 or len(receipt_rows) > 1:
        return _error(hashes, "DUPLICATE_INVESTIGATOR_ROWS", source_rows, receipt_rows)
    if not receipt_rows:
        return LaneAssessment(
            LaneEvaluationState.UNKNOWN,
            "MISSING_INTAKE_RECEIPT",
            False,
            LaneFreshness.UNKNOWN,
            stable_digest({"artifact_hashes": hashes, "source_rows": source_rows}),
            hashes,
            ("MISSING_INTAKE_RECEIPT",),
        )
    receipt = receipt_rows[0]
    evidence_hash = stable_digest(
        {
            "artifact_hashes": hashes,
            "receipt": _select(
                receipt,
                "trade_date",
                "tracked",
                "selected_trigger_reason",
                "decision_state",
                "reason_codes",
            ),
            "score": _select(
                source_rows[0] if source_rows else {},
                "trade_date",
                "trigger_reason",
                "move_tag",
                "final_score",
                "verdict",
                "sector",
                "sector_name",
                "invalidation_price",
            ),
        }
    )
    freshness = _combined_freshness(
        session_date,
        [receipt.get("trade_date")]
        + ([source_rows[0].get("trade_date")] if source_rows else []),
    )
    if freshness is LaneFreshness.FUTURE:
        return LaneAssessment(
            LaneEvaluationState.ERROR,
            "FUTURE_DATED_EVIDENCE",
            False,
            freshness,
            evidence_hash,
            hashes,
            ("FUTURE_DATED_EVIDENCE",),
        )
    tracked = _bool(receipt.get("tracked"))
    if tracked and not source_rows:
        return LaneAssessment(
            LaneEvaluationState.ERROR,
            "TRACKED_ROW_MISSING_SCORE",
            False,
            freshness,
            evidence_hash,
            hashes,
            ("TRACKED_ROW_MISSING_SCORE",),
        )
    if not tracked:
        return LaneAssessment(
            LaneEvaluationState.NOT_ELIGIBLE,
            str(receipt.get("decision_state") or "EXCLUDED"),
            False,
            freshness,
            evidence_hash,
            hashes,
            _reason_codes(receipt.get("reason_codes")),
        )
    row = source_rows[0]
    trigger = str(
        row.get("trigger_reason") or receipt.get("selected_trigger_reason") or "UNKNOWN"
    ).upper()
    score = _float(row.get("final_score"))
    primary_eligible = (
        trigger == INVESTIGATOR_PRIMARY_TRIGGER
        and score is not None
        and score >= INVESTIGATOR_ACTIVE_REVIEW_SCORE
    )
    member = primary_eligible and freshness is LaneFreshness.FRESH
    return LaneAssessment(
        LaneEvaluationState.KNOWN,
        str(row.get("move_tag") or trigger or "TRACKED").upper(),
        member,
        freshness,
        evidence_hash,
        hashes,
        () if primary_eligible else ("NOT_PRIMARY_REVIEW_ELIGIBLE",),
        {
            "tracked": True,
            "trigger_reason": trigger,
            "final_score": score,
            "primary_review_eligible": primary_eligible,
            "sector_name": _first_text(row, "sector_name", "sector"),
            "invalidation_price": _float(row.get("invalidation_price")),
        },
    )


def _fundamental_assessment(
    *,
    session_date: date,
    rows: Iterable[dict[str, Any]],
    artifact_hash: str | None,
) -> LaneAssessment:
    source_rows = list(rows)
    hashes = (artifact_hash,) if artifact_hash else ()
    if not artifact_hash:
        return _not_evaluated(hashes, "FUNDAMENTAL_SOURCE_ABSENT")
    if not source_rows:
        return _not_evaluated(hashes, "OUTSIDE_FUNDAMENTAL_EVALUATION_UNIVERSE")
    if len(source_rows) > 1:
        return _error(hashes, "DUPLICATE_FUNDAMENTAL_ROWS", source_rows)
    row = source_rows[0]
    evidence_hash = stable_digest(
        {
            "artifact_hash": artifact_hash,
            "row": _select(
                row,
                "as_of",
                "primary_thesis",
                "classification_status",
                "admission_eligible",
                "admission_blockers",
                "source_data_hash",
                "taxonomy_version",
                "rule_version",
                "admission_version",
                "sector",
                "sector_name",
            ),
        }
    )
    freshness = _combined_freshness(session_date, [row.get("as_of")])
    if freshness is LaneFreshness.FUTURE:
        return LaneAssessment(
            LaneEvaluationState.ERROR,
            "FUTURE_DATED_EVIDENCE",
            False,
            freshness,
            evidence_hash,
            hashes,
            ("FUTURE_DATED_EVIDENCE",),
        )
    primary = str(row.get("primary_thesis") or "").strip()
    classification = str(row.get("classification_status") or "").strip().upper()
    if not classification:
        return LaneAssessment(
            LaneEvaluationState.ERROR,
            "MISSING_CLASSIFICATION_STATUS",
            False,
            freshness,
            evidence_hash,
            hashes,
            ("MISSING_CLASSIFICATION_STATUS",),
        )
    eligible = _bool(row.get("admission_eligible"))
    state = LaneEvaluationState.KNOWN if eligible else LaneEvaluationState.NOT_ELIGIBLE
    member = eligible and freshness is LaneFreshness.FRESH
    return LaneAssessment(
        state,
        primary or classification,
        member,
        freshness,
        evidence_hash,
        hashes,
        () if eligible else _reason_codes(row.get("admission_blockers")),
        {
            "admission_eligible": eligible,
            "primary_thesis": primary or None,
            "classification_status": classification,
            "sector_name": _first_text(row, "sector_name", "sector"),
        },
    )


def _pattern_assessment(
    *,
    session_date: date,
    rows: Iterable[dict[str, Any]],
    artifact_hash: str | None,
) -> LaneAssessment:
    source_rows = list(rows)
    hashes = (artifact_hash,) if artifact_hash else ()
    if not artifact_hash:
        return _not_evaluated(hashes, "PATTERN_ASSESSMENT_SOURCE_ABSENT")
    if not source_rows:
        return LaneAssessment(
            LaneEvaluationState.UNKNOWN,
            "MISSING_PATTERN_ASSESSMENT",
            False,
            LaneFreshness.UNKNOWN,
            stable_digest({"artifact_hash": artifact_hash, "rows": []}),
            hashes,
            ("MISSING_PATTERN_ASSESSMENT",),
        )
    if len(source_rows) > 1:
        return _error(hashes, "DUPLICATE_PATTERN_ASSESSMENTS", source_rows)
    row = source_rows[0]
    evidence_hash = str(row.get("evidence_hash") or "") or stable_digest(
        {"artifact_hash": artifact_hash, "row": row}
    )
    date_freshness = _combined_freshness(
        session_date, [row.get("session_date") or row.get("as_of_date")]
    )
    # A producer's FRESH label cannot override a stale or missing session.
    freshness = (
        date_freshness
        if date_freshness is not LaneFreshness.FRESH
        else _freshness_value(row.get("lane_freshness")) or date_freshness
    )
    if freshness is LaneFreshness.FUTURE:
        return LaneAssessment(
            LaneEvaluationState.ERROR,
            "FUTURE_DATED_EVIDENCE",
            False,
            freshness,
            evidence_hash,
            hashes,
            ("FUTURE_DATED_EVIDENCE",),
        )
    try:
        state = LaneEvaluationState(
            str(row.get("pattern_evaluation_state") or "UNKNOWN").upper()
        )
    except ValueError:
        state = LaneEvaluationState.ERROR
    raw_member = _bool(row.get("pattern_member") or row.get("pattern_evidence_present"))
    member = (
        raw_member
        and state is LaneEvaluationState.KNOWN
        and freshness is LaneFreshness.FRESH
    )
    reason_codes = _reason_codes(row.get("reason_codes_json"))
    if state is LaneEvaluationState.ERROR and not reason_codes:
        reason_codes = ("INVALID_PATTERN_EVALUATION_STATE",)
    return LaneAssessment(
        state,
        str(
            row.get("primary_pattern_state")
            or row.get("pattern_state")
            or row.get("scan_lane")
            or state.value
        ).upper(),
        member,
        freshness,
        evidence_hash,
        hashes,
        reason_codes,
        {
            "evidence_present": raw_member,
            "scan_lane": row.get("scan_lane"),
            "signal_count": int(_float(row.get("signal_count")) or 0),
            "primary_signal_id": row.get("primary_signal_id"),
            "primary_pattern_family": row.get("primary_pattern_family"),
            "primary_evidence_class": row.get("primary_evidence_class"),
            "evidence_origin": row.get("evidence_origin"),
            "sector_name": _first_text(row, "sector_name", "sector"),
        },
    )


def _sector_context(
    *assessments: LaneAssessment,
) -> tuple[str | None, str, tuple[str, ...]]:
    values = {
        str(assessment.details.get("sector_name") or "").strip()
        for assessment in assessments
        if assessment.details
        and str(assessment.details.get("sector_name") or "").strip()
    }
    normalized = {" ".join(value.lower().split()) for value in values}
    if not values:
        return (
            None,
            LaneEvaluationState.NOT_EVALUATED.value,
            ("SECTOR_CONTEXT_NOT_AVAILABLE",),
        )
    if len(normalized) > 1:
        return None, LaneEvaluationState.ERROR.value, ("CONFLICTING_SECTOR_CONTEXT",)
    return sorted(values)[0], LaneEvaluationState.KNOWN.value, ()


def _add_lane(row: dict[str, Any], prefix: str, assessment: LaneAssessment) -> None:
    row[f"{prefix}_evaluation_state"] = assessment.state.value
    row[f"{prefix}_state"] = assessment.lane_state
    row[f"{prefix}_member"] = assessment.member
    row[f"{prefix}_freshness"] = assessment.freshness.value
    row[f"{prefix}_evidence_hash"] = assessment.evidence_hash
    row[f"{prefix}_artifact_hashes_json"] = json.dumps(
        assessment.artifact_hashes, separators=(",", ":")
    )
    row[f"{prefix}_reason_codes_json"] = json.dumps(
        assessment.reason_codes, separators=(",", ":")
    )
    for key, value in dict(assessment.details or {}).items():
        row[f"{prefix}_{key}"] = value


def _group(
    rows: Iterable[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = row.get("symbol_id") or row.get("symbol")
        if not str(symbol or "").strip():
            continue
        grouped[_normal_key(str(row.get("exchange") or "NSE"), str(symbol))].append(row)
    return grouped


def _normal_key(exchange: str, symbol: str) -> tuple[str, str]:
    return str(exchange or "NSE").strip().upper(), str(symbol).strip().upper()


def _not_evaluated(artifact_hashes: tuple[str, ...], reason: str) -> LaneAssessment:
    return LaneAssessment(
        LaneEvaluationState.NOT_EVALUATED,
        reason,
        False,
        LaneFreshness.NOT_APPLICABLE,
        None,
        artifact_hashes,
        (reason,),
    )


def _error(artifact_hashes: tuple[str, ...], reason: str, *rows: Any) -> LaneAssessment:
    return LaneAssessment(
        LaneEvaluationState.ERROR,
        reason,
        False,
        LaneFreshness.UNKNOWN,
        stable_digest({"artifact_hashes": artifact_hashes, "rows": rows}),
        artifact_hashes,
        (reason,),
    )


def _cohort(investigator: bool, fundamental: bool, pattern: bool) -> str:
    return {
        (True, False, False): "I_ONLY",
        (False, True, False): "F_ONLY",
        (False, False, True): "P_ONLY",
        (True, True, False): "I_F",
        (True, False, True): "I_P",
        (False, True, True): "F_P",
        (True, True, True): "I_F_P",
        (False, False, False): "NONE",
    }[(investigator, fundamental, pattern)]


def _combined_freshness(session_date: date, values: Iterable[Any]) -> LaneFreshness:
    dates = [_date_value(value) for value in values]
    if not dates or any(value is None for value in dates):
        return LaneFreshness.UNKNOWN
    if any(value > session_date for value in dates if value is not None):
        return LaneFreshness.FUTURE
    if any(value < session_date for value in dates if value is not None):
        return LaneFreshness.STALE
    return LaneFreshness.FRESH


def _freshness_value(value: Any) -> LaneFreshness | None:
    try:
        return LaneFreshness(str(value or "").strip().upper())
    except ValueError:
        return None


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if parsed != parsed else parsed


def _select(row: Mapping[str, Any], *fields: str) -> dict[str, Any]:
    return {field: row.get(field) for field in fields}


def _first_text(row: Mapping[str, Any], *fields: str) -> str | None:
    for field in fields:
        value = str(row.get(field) or "").strip()
        if value:
            return value
    return None


def _reason_codes(value: Any) -> tuple[str, ...]:
    if isinstance(value, (tuple, list)):
        return tuple(str(item) for item in value if str(item).strip())
    text = str(value or "").strip()
    if not text:
        return ()
    if text.startswith("["):
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, list):
            return tuple(str(item) for item in decoded if str(item).strip())
    return tuple(item for item in text.split("|") if item)
