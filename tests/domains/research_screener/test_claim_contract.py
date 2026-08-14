from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.domains.research_screener.claim_contract import (
    QualitativeClaimContract,
)
from ai_trading_system.domains.research_screener.store import ResearchScreenerStore


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _contract() -> QualitativeClaimContract:
    return QualitativeClaimContract.from_files(
        PROJECT_ROOT / "configs/research_screener/qualitative_claim_contract.json",
        PROJECT_ROOT / "configs/research_screener/kpi_contracts.json",
    )


def _claim(**changes):
    claim = {
        "claim_id": "claim:001",
        "schema_version": "qualitative-claim-v1",
        "research_run_id": "research:test",
        "document_id": "research-document:test",
        "company_id": "company:INE001B01026",
        "security_id": "security:INE001B01026",
        "isin": "INE001B01026",
        "company_type": "INDUSTRIAL",
        "topic": "order_book",
        "metric_name": "order_book",
        "claim_kind": "ACTUAL",
        "claim_text": "The order book was INR 4,200 crore at year end.",
        "exact_excerpt": "The executable order book stood at INR 4,200 crore as at March 31, 2026.",
        "page": 47,
        "source_artifact_id": "artifact:annual-report:test",
        "source_content_hash": "a" * 64,
        "published_at": "2026-07-15T10:00:00+05:30",
        "fiscal_period": "FY2025-26",
        "statement_scope": "consolidated",
        "value": 4200.0,
        "unit": "INR_CRORE",
        "currency": "INR",
        "materiality": "MEDIUM",
        "extraction_model": "gpt-4o-mini-2024-07-18",
        "extraction_prompt_version": "qualitative-extractor-v1",
    }
    claim.update(changes)
    return claim


def _review(role: str, **changes):
    review = {
        "review_id": f"review:{role.lower()}:001",
        "schema_version": "qualitative-claim-review-v1",
        "claim_id": "claim:001",
        "reviewer_role": role,
        "decision": "ACCEPT",
        "reviewer_model": "gpt-4o-mini-2024-07-18",
        "prompt_version": (
            "qualitative-extractor-v1"
            if role == "EXTRACTION"
            else "qualitative-verifier-v1"
        ),
        "independent_context": role == "VERIFICATION",
        "normalized_claim_hash": "b" * 64,
        "issue_codes": [],
        "input_tokens": 900,
        "cached_input_tokens": 0,
        "output_tokens": 140,
        "request_id": f"req_{role.lower()}",
        "batch_id": "batch_001",
    }
    review.update(changes)
    return review


def test_low_cost_policy_is_pinned_batched_and_bounded():
    raw = json.loads(
        (
            PROJECT_ROOT / "configs/research_screener/qualitative_claim_contract.json"
        ).read_text()
    )
    policy = raw["low_cost_agent_policy"]
    assert policy["transport"] == "OPENAI_BATCH_API"
    assert policy["primary_model"] == "gpt-4o-mini-2024-07-18"
    assert policy["structured_outputs_required"] is True
    assert policy["full_document_prompting_forbidden"] is True
    assert policy["max_context_characters"] == 18000
    assert policy["calibration_company_count"] == 25
    assert policy["max_retries_per_request"] == 1
    assert policy["halt_new_requests_at_budget_fraction"] == 0.9
    assert policy["verification_only_for_deterministically_valid_claims"] is True
    assert policy["model_escalation_enabled"] is False

    for schema_path in raw["structured_output_schemas"].values():
        schema = json.loads((PROJECT_ROOT / schema_path).read_text())
        assert schema["additionalProperties"] is False
        assert schema["required"]


def test_two_independent_agents_can_verify_low_impact_claim():
    result = _contract().decide(
        _claim(),
        as_of_date=date(2026, 8, 12),
        extraction_review=_review("EXTRACTION"),
        verification_review=_review("VERIFICATION"),
    )
    assert result.status == "AGENT_VERIFIED"
    assert result.reason_codes == ("TWO_AGENT_AGREEMENT",)


def test_guidance_and_high_materiality_require_human_review():
    guidance = _claim(
        topic="management_guidance",
        metric_name="guidance",
        claim_kind="GUIDANCE",
    )
    result = _contract().decide(
        guidance,
        as_of_date=date(2026, 8, 12),
        extraction_review=_review("EXTRACTION"),
        verification_review=_review("VERIFICATION"),
    )
    assert result.status == "HUMAN_REVIEW_REQUIRED"
    assert result.reason_codes == ("HIGH_IMPACT_TOPIC",)

    result = _contract().decide(
        _claim(materiality="HIGH"),
        as_of_date=date(2026, 8, 12),
        extraction_review=_review("EXTRACTION"),
        verification_review=_review("VERIFICATION"),
    )
    assert result.reason_codes == ("HIGH_MATERIALITY",)


def test_disagreement_and_non_independent_verifier_never_auto_accept():
    result = _contract().decide(
        _claim(),
        as_of_date=date(2026, 8, 12),
        extraction_review=_review("EXTRACTION"),
        verification_review=_review("VERIFICATION", normalized_claim_hash="c" * 64),
    )
    assert result.status == "HUMAN_REVIEW_REQUIRED"
    assert result.reason_codes == ("AGENT_NORMALIZATION_DISAGREEMENT",)

    result = _contract().decide(
        _claim(),
        as_of_date=date(2026, 8, 12),
        extraction_review=_review("EXTRACTION"),
        verification_review=_review("VERIFICATION", independent_context=False),
    )
    assert result.status == "AGENT_REJECTED"
    assert "VERIFIER_CONTEXT_NOT_INDEPENDENT" in result.reason_codes

    result = _contract().decide(
        _claim(),
        as_of_date=date(2026, 8, 12),
        extraction_review=_review("EXTRACTION"),
        verification_review=_review("VERIFICATION", request_id="req_extraction"),
    )
    assert result.reason_codes == ("VERIFIER_REQUEST_NOT_INDEPENDENT",)


def test_deterministic_provenance_cutoff_and_company_contract_fail_closed():
    contract = _contract()
    errors = contract.validate_claim(
        _claim(source_content_hash="bad", published_at="2026-08-13T00:00:00+05:30"),
        as_of_date=date(2026, 8, 12),
    )
    assert "CLAIM_SOURCE_HASH_INVALID" in errors
    assert "CLAIM_POST_CUTOFF" in errors

    errors = contract.validate_claim(
        _claim(published_at="2026-08-12T10:00:00"),
        as_of_date=date(2026, 8, 12),
    )
    assert "CLAIM_PUBLICATION_TIMEZONE_MISSING" in errors

    errors = contract.validate_claim(
        _claim(company_type="BANK", metric_name="order_book"),
        as_of_date=date(2026, 8, 12),
    )
    assert "CLAIM_METRIC_OUTSIDE_COMPANY_CONTRACT" in errors


def test_not_disclosed_is_evidence_state_not_claim_kind():
    errors = _contract().validate_claim(
        _claim(claim_kind="NOT_DISCLOSED", exact_excerpt="", page=None),
        as_of_date=date(2026, 8, 12),
    )
    assert "CLAIM_KIND_INVALID" in errors
    assert "CLAIM_EXCERPT_INSUFFICIENT" in errors


def test_model_and_prompt_versions_are_pinned():
    contract = _contract()
    errors = contract.validate_claim(
        _claim(extraction_model="unapproved-model"),
        as_of_date=date(2026, 8, 12),
    )
    assert "CLAIM_EXTRACTION_MODEL_MISMATCH" in errors

    errors = contract.validate_review(
        _review("VERIFICATION", prompt_version="qualitative-extractor-v1"),
        claim_id="claim:001",
        expected_role="VERIFICATION",
    )
    assert "REVIEW_PROMPT_VERSION_MISMATCH" in errors


def test_migration_adds_claim_review_and_policy_tables(tmp_path):
    store = ResearchScreenerStore(tmp_path / "research-screener.duckdb")
    with store.writer() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                ["main"],
            ).fetchall()
        }
    assert {
        "qualitative_claim",
        "qualitative_claim_review",
        "qualitative_claim_policy_decision",
    }.issubset(tables)
