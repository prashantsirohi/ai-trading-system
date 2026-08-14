from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ISIN_PATTERN = re.compile(r"^INE[A-Z0-9]{8}[0-9]$")
ALLOWED_COMPANY_TYPES = {
    "INDUSTRIAL",
    "BANK",
    "FINANCIAL_INSTITUTION",
    "MARKET_INFRASTRUCTURE",
}
ALLOWED_SCOPES = {"standalone", "consolidated", "not_applicable"}


@dataclass(frozen=True)
class ClaimPolicyResult:
    status: str
    reason_codes: tuple[str, ...]


class QualitativeClaimContract:
    """Deterministic gate around agent-proposed qualitative claims.

    Agents may propose and verify claims, but they cannot override provenance,
    cutoff, company-type, scope, or high-impact escalation rules.
    """

    def __init__(self, contract: dict[str, Any], kpi_contracts: dict[str, Any]):
        self.contract = contract
        self.kpi_contracts = kpi_contracts

    @classmethod
    def from_files(cls, contract_path: str | Path, kpi_path: str | Path):
        return cls(
            json.loads(Path(contract_path).read_text(encoding="utf-8")),
            json.loads(Path(kpi_path).read_text(encoding="utf-8")),
        )

    def validate_claim(
        self, claim: dict[str, Any], *, as_of_date: date
    ) -> tuple[str, ...]:
        errors: list[str] = []
        missing = [
            field
            for field in self.contract["required_claim_fields"]
            if self._blank(claim.get(field))
        ]
        if missing:
            errors.append("CLAIM_REQUIRED_FIELDS_MISSING:" + ",".join(sorted(missing)))
        if claim.get("schema_version") != self.contract["claim_schema_version"]:
            errors.append("CLAIM_SCHEMA_VERSION_MISMATCH")
        policy = self.contract["low_cost_agent_policy"]
        if claim.get("extraction_model") != policy["primary_model"]:
            errors.append("CLAIM_EXTRACTION_MODEL_MISMATCH")
        if (
            claim.get("extraction_prompt_version")
            != policy["extraction_prompt_version"]
        ):
            errors.append("CLAIM_EXTRACTION_PROMPT_MISMATCH")
        if claim.get("topic") not in self.contract["allowed_topics"]:
            errors.append("CLAIM_TOPIC_INVALID")
        if claim.get("claim_kind") not in self.contract["claim_kinds"]:
            errors.append("CLAIM_KIND_INVALID")
        if claim.get("materiality") not in self.contract["materiality_levels"]:
            errors.append("CLAIM_MATERIALITY_INVALID")
        if claim.get("company_type") not in ALLOWED_COMPANY_TYPES:
            errors.append("CLAIM_COMPANY_TYPE_INVALID")
        if claim.get("statement_scope") not in ALLOWED_SCOPES:
            errors.append("CLAIM_STATEMENT_SCOPE_INVALID")
        if not ISIN_PATTERN.fullmatch(str(claim.get("isin") or "")):
            errors.append("CLAIM_ISIN_INVALID")
        if not HASH_PATTERN.fullmatch(str(claim.get("source_content_hash") or "")):
            errors.append("CLAIM_SOURCE_HASH_INVALID")
        if not isinstance(claim.get("page"), int) or claim.get("page", 0) < 1:
            errors.append("CLAIM_PAGE_INVALID")
        if len(str(claim.get("exact_excerpt") or "").strip()) < 10:
            errors.append("CLAIM_EXCERPT_INSUFFICIENT")
        if len(str(claim.get("claim_text") or "").strip()) < 5:
            errors.append("CLAIM_TEXT_INSUFFICIENT")
        published = self._datetime(claim.get("published_at"))
        if published is None:
            errors.append("CLAIM_PUBLICATION_TIMESTAMP_INVALID")
        elif published.tzinfo is None or published.utcoffset() is None:
            errors.append("CLAIM_PUBLICATION_TIMEZONE_MISSING")
        elif published.date() > as_of_date:
            errors.append("CLAIM_POST_CUTOFF")
        if claim.get("value") is not None and self._blank(claim.get("unit")):
            errors.append("CLAIM_VALUE_UNIT_MISSING")
        errors.extend(self._metric_routing_errors(claim))
        return tuple(sorted(set(errors)))

    def validate_review(
        self, review: dict[str, Any], *, claim_id: str, expected_role: str
    ) -> tuple[str, ...]:
        errors: list[str] = []
        required = {
            "review_id",
            "schema_version",
            "claim_id",
            "reviewer_role",
            "decision",
            "reviewer_model",
            "prompt_version",
            "independent_context",
            "normalized_claim_hash",
            "issue_codes",
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "request_id",
            "batch_id",
        }
        missing = [field for field in required if self._blank(review.get(field))]
        if missing:
            errors.append("REVIEW_REQUIRED_FIELDS_MISSING:" + ",".join(sorted(missing)))
        if review.get("schema_version") != self.contract["review_schema_version"]:
            errors.append("REVIEW_SCHEMA_VERSION_MISMATCH")
        if review.get("claim_id") != claim_id:
            errors.append("REVIEW_CLAIM_ID_MISMATCH")
        if review.get("reviewer_role") != expected_role:
            errors.append("REVIEW_ROLE_MISMATCH")
        if review.get("decision") not in self.contract["review_decisions"]:
            errors.append("REVIEW_DECISION_INVALID")
        policy = self.contract["low_cost_agent_policy"]
        if review.get("reviewer_model") != policy["primary_model"]:
            errors.append("REVIEW_MODEL_MISMATCH")
        expected_prompt = (
            policy["extraction_prompt_version"]
            if expected_role == "EXTRACTION"
            else policy["verification_prompt_version"]
        )
        if review.get("prompt_version") != expected_prompt:
            errors.append("REVIEW_PROMPT_VERSION_MISMATCH")
        if (
            expected_role == "VERIFICATION"
            and review.get("independent_context") is not True
        ):
            errors.append("VERIFIER_CONTEXT_NOT_INDEPENDENT")
        for field in ("input_tokens", "cached_input_tokens", "output_tokens"):
            if not isinstance(review.get(field), int) or review.get(field, -1) < 0:
                errors.append(f"REVIEW_{field.upper()}_INVALID")
        if not isinstance(review.get("issue_codes"), list):
            errors.append("REVIEW_ISSUE_CODES_INVALID")
        if not HASH_PATTERN.fullmatch(str(review.get("normalized_claim_hash") or "")):
            errors.append("REVIEW_NORMALIZED_CLAIM_HASH_INVALID")
        return tuple(sorted(set(errors)))

    def decide(
        self,
        claim: dict[str, Any],
        *,
        as_of_date: date,
        extraction_review: dict[str, Any] | None,
        verification_review: dict[str, Any] | None,
        conflict_detected: bool = False,
    ) -> ClaimPolicyResult:
        claim_errors = self.validate_claim(claim, as_of_date=as_of_date)
        if claim_errors:
            return ClaimPolicyResult("AGENT_REJECTED", claim_errors)
        if extraction_review is None:
            return ClaimPolicyResult("AGENT_EXTRACTED", ("EXTRACTION_REVIEW_PENDING",))
        extraction_errors = self.validate_review(
            extraction_review,
            claim_id=claim["claim_id"],
            expected_role="EXTRACTION",
        )
        if extraction_errors or extraction_review.get("decision") == "REJECT":
            return ClaimPolicyResult(
                "AGENT_REJECTED",
                extraction_errors or ("EXTRACTION_AGENT_REJECTED",),
            )
        if verification_review is None:
            return ClaimPolicyResult(
                "AGENT_EXTRACTED", ("VERIFICATION_REVIEW_PENDING",)
            )
        verification_errors = self.validate_review(
            verification_review,
            claim_id=claim["claim_id"],
            expected_role="VERIFICATION",
        )
        if verification_errors or verification_review.get("decision") == "REJECT":
            return ClaimPolicyResult(
                "AGENT_REJECTED",
                verification_errors or ("VERIFICATION_AGENT_REJECTED",),
            )
        if extraction_review["request_id"] == verification_review["request_id"]:
            return ClaimPolicyResult(
                "AGENT_REJECTED", ("VERIFIER_REQUEST_NOT_INDEPENDENT",)
            )
        if conflict_detected:
            return ClaimPolicyResult("CONFLICT_DETECTED", ("DETERMINISTIC_CONFLICT",))
        if "AMBIGUOUS" in {
            extraction_review.get("decision"),
            verification_review.get("decision"),
        }:
            return ClaimPolicyResult("HUMAN_REVIEW_REQUIRED", ("AGENT_AMBIGUITY",))
        if (
            extraction_review["normalized_claim_hash"]
            != verification_review["normalized_claim_hash"]
        ):
            return ClaimPolicyResult(
                "HUMAN_REVIEW_REQUIRED", ("AGENT_NORMALIZATION_DISAGREEMENT",)
            )
        if claim["materiality"] == "HIGH":
            return ClaimPolicyResult("HUMAN_REVIEW_REQUIRED", ("HIGH_MATERIALITY",))
        if claim["topic"] in self.contract["high_impact_topics"]:
            return ClaimPolicyResult("HUMAN_REVIEW_REQUIRED", ("HIGH_IMPACT_TOPIC",))
        if claim["claim_kind"] in self.contract["high_impact_claim_kinds"]:
            return ClaimPolicyResult(
                "HUMAN_REVIEW_REQUIRED", ("HIGH_IMPACT_CLAIM_KIND",)
            )
        return ClaimPolicyResult("AGENT_VERIFIED", ("TWO_AGENT_AGREEMENT",))

    def _metric_routing_errors(self, claim: dict[str, Any]) -> list[str]:
        topic = claim.get("topic")
        if topic in {"governance", "shareholding", "management_guidance"}:
            return []
        company_type = claim.get("company_type")
        route = self.kpi_contracts.get("routing", {}).get(company_type)
        contract = self.kpi_contracts.get("contracts", {}).get(route, {})
        metric = claim.get("metric_name")
        if not route:
            return ["CLAIM_KPI_ROUTE_UNRESOLVED"]
        if metric in set(contract.get("not_applicable", [])):
            return ["CLAIM_METRIC_NOT_APPLICABLE"]
        if metric not in set(contract.get("quarterly_kpis", [])):
            return ["CLAIM_METRIC_OUTSIDE_COMPANY_CONTRACT"]
        return []

    @staticmethod
    def _blank(value: Any) -> bool:
        return value is None or (isinstance(value, str) and not value.strip())

    @staticmethod
    def _datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
