from __future__ import annotations

from datetime import datetime
from typing import Any


CLAIM_TYPES = {
    "CAPEX_ANNOUNCED", "CAPEX_AMOUNT", "CAPEX_UNDER_CONSTRUCTION", "CAPEX_COMPLETED",
    "PLANT_COMMISSIONED", "COMMERCIAL_PRODUCTION_STARTED", "CAPACITY_OLD",
    "CAPACITY_NEW", "CAPACITY_INCREMENT", "UTILISATION_REPORTED",
    "CUSTOMER_QUALIFIED", "COMMERCIAL_SUPPLY_STARTED", "DEMAND_PATH_REPORTED",
    "EXPECTED_INCREMENTAL_REVENUE", "CAPEX_DELAYED", "CAPEX_CANCELLED",
}
EVIDENCE_STATES = {"PRESENT", "NOT_DISCLOSED"}
REVIEW_DECISIONS = {"ACCEPT", "REJECT", "AMBIGUOUS"}


class JCurveClaimContract:
    """Deterministic validation around announcement claim extraction."""

    schema_version = "jcurve-claim-v1"

    def validate_batch(self, payload: dict[str, Any]) -> tuple[str, ...]:
        if set(payload) != {"claims"} or not isinstance(payload.get("claims"), list):
            return ("CLAIM_BATCH_SHAPE_INVALID",)
        if len(payload["claims"]) > 20:
            return ("CLAIM_BATCH_TOO_LARGE",)
        errors: list[str] = []
        for index, claim in enumerate(payload["claims"]):
            errors.extend(f"{index}:{error}" for error in self.validate_claim(claim))
        return tuple(sorted(set(errors)))

    def validate_claim(self, claim: dict[str, Any]) -> tuple[str, ...]:
        expected = {
            "claim_type", "evidence_state", "numeric_value", "boolean_value",
            "text_value", "unit", "project_name", "project_location",
            "exact_excerpt", "page", "effective_at", "confidence",
        }
        errors: list[str] = []
        if set(claim) != expected:
            errors.append("CLAIM_FIELDS_INVALID")
        if claim.get("claim_type") not in CLAIM_TYPES:
            errors.append("CLAIM_TYPE_INVALID")
        state = claim.get("evidence_state")
        if state not in EVIDENCE_STATES:
            errors.append("EVIDENCE_STATE_INVALID")
        confidence = claim.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            errors.append("CONFIDENCE_INVALID")
        if claim.get("numeric_value") is not None and not isinstance(
            claim.get("numeric_value"), (int, float)
        ):
            errors.append("NUMERIC_VALUE_INVALID")
        if claim.get("numeric_value") is not None and not str(claim.get("unit") or "").strip():
            errors.append("NUMERIC_UNIT_MISSING")
        if state == "PRESENT":
            if not isinstance(claim.get("page"), int) or claim.get("page", 0) < 1:
                errors.append("PAGE_REQUIRED")
            if len(str(claim.get("exact_excerpt") or "").strip()) < 10:
                errors.append("EXCERPT_REQUIRED")
        elif any(
            claim.get(field) is not None
            for field in ("numeric_value", "boolean_value", "text_value", "unit", "exact_excerpt", "page")
        ):
            errors.append("NOT_DISCLOSED_MUST_BE_NULL")
        effective = claim.get("effective_at")
        if effective is not None:
            try:
                parsed = datetime.fromisoformat(str(effective).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    errors.append("EFFECTIVE_TIMEZONE_MISSING")
            except ValueError:
                errors.append("EFFECTIVE_AT_INVALID")
        return tuple(sorted(set(errors)))

    def validate_review_batch(
        self, payload: dict[str, Any], *, claim_count: int
    ) -> tuple[str, ...]:
        if set(payload) != {"reviews"} or not isinstance(payload.get("reviews"), list):
            return ("REVIEW_BATCH_SHAPE_INVALID",)
        errors: list[str] = []
        seen: set[int] = set()
        for review in payload["reviews"]:
            if set(review) != {"claim_index", "decision", "supported_excerpt", "supported_page", "issue_codes"}:
                errors.append("REVIEW_FIELDS_INVALID")
                continue
            index = review.get("claim_index")
            if not isinstance(index, int) or not 0 <= index < claim_count or index in seen:
                errors.append("REVIEW_INDEX_INVALID")
            else:
                seen.add(index)
            if review.get("decision") not in REVIEW_DECISIONS:
                errors.append("REVIEW_DECISION_INVALID")
            if review.get("decision") == "ACCEPT":
                if len(str(review.get("supported_excerpt") or "").strip()) < 10:
                    errors.append("REVIEW_EXCERPT_REQUIRED")
                if not isinstance(review.get("supported_page"), int):
                    errors.append("REVIEW_PAGE_REQUIRED")
            if not isinstance(review.get("issue_codes"), list):
                errors.append("REVIEW_ISSUES_INVALID")
        if seen != set(range(claim_count)):
            errors.append("REVIEW_COVERAGE_INCOMPLETE")
        return tuple(sorted(set(errors)))
