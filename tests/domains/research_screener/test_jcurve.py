from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from ai_trading_system.domains.research_screener.jcurve import service as jcurve_service
from ai_trading_system.domains.research_screener.jcurve.cohort import BaselineCohort
from ai_trading_system.domains.research_screener.jcurve.market_intel_adapter import (
    JCURVE_FILTER_SIGNALS,
    MarketIntelAnnouncementAdapter,
)
from ai_trading_system.domains.research_screener.jcurve.model_router import (
    OpenRouterModelRouter,
    RouterPolicy,
)
from ai_trading_system.domains.research_screener.jcurve.models import (
    EvidencePacket,
    EvidencePage,
    ModelRoute,
)
from ai_trading_system.domains.research_screener.jcurve.policy import (
    CapexStagePolicy,
    MaterialityInputs,
)
from ai_trading_system.domains.research_screener.jcurve.service import JCurveImportService
from ai_trading_system.domains.research_screener.jcurve.store import JCurveStore


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class _Response:
    def __init__(self, payload: dict):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_import_manifest_serializes_upstream_coverage_timestamps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_from = datetime(2026, 8, 17)
    requested_to = datetime(2026, 8, 23, 23, 59, 59)

    class _Adapter:
        def __init__(self, **_kwargs):
            pass

        def read(self, **_kwargs):
            return []

        def coverage_receipts(self, **_kwargs):
            return {
                "policy_version": "market-intel-high-value-filter-v1",
                "requested_from": requested_from,
                "requested_to": requested_to,
                "coverage_proven": False,
                "covered_sources": [],
                "missing_sources": ["bse_corp", "nse_api"],
                "receipts": [{
                    "collection_run_id": "fixture", "source": "nse_api",
                    "requested_from": requested_from, "requested_to": requested_to,
                }],
            }

    monkeypatch.setattr(jcurve_service, "MarketIntelAnnouncementAdapter", _Adapter)
    output_root = tmp_path / "runs"
    result = JCurveImportService(
        store_path=tmp_path / "research.duckdb", output_root=output_root,
    ).run(
        market_intel_db=tmp_path / "market.duckdb",
        published_from=date(2026, 8, 17), as_of_date=date(2026, 8, 23),
        upstream_filter_policy="market-intel-high-value-filter-v1",
    )

    manifest = json.loads(
        (Path(result["output_dir"]) / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["upstream_coverage"]["requested_from"] == "2026-08-17 00:00:00"
    assert manifest["upstream_coverage"]["receipts"][0]["requested_to"] == (
        "2026-08-23 23:59:59"
    )


def test_import_coverage_sources_follow_frozen_primary_listings() -> None:
    dual_and_nse = type("Cohort", (), {"members": (
        {"company_id": "dual", "nse_symbol": "DUAL", "bse_code": "500001"},
        {"company_id": "nse", "nse_symbol": "NSEONLY", "bse_code": None},
    )})()
    with_bse_only = type("Cohort", (), {"members": (
        {"company_id": "nse", "nse_symbol": "NSEONLY", "bse_code": None},
        {"company_id": "bse", "nse_symbol": None, "bse_code": "500002"},
    )})()

    assert JCurveImportService._required_coverage_sources(dual_and_nse) == ("nse_api",)
    assert JCurveImportService._required_coverage_sources(with_bse_only) == (
        "nse_api", "bse_corp",
    )
    assert JCurveImportService._required_coverage_sources(None) == (
        "nse_api", "bse_corp",
    )


def test_coverage_receipts_accept_nse_only_cohort_scope(tmp_path: Path) -> None:
    market_db = tmp_path / "market.duckdb"
    conn = duckdb.connect(str(market_db))
    conn.execute("""CREATE TABLE announcement_collection_run (
        collection_run_id VARCHAR, source VARCHAR, requested_from TIMESTAMP,
        requested_to TIMESTAMP, status VARCHAR, pages_complete BOOLEAN,
        item_count BIGINT, failure_count BIGINT, policy_version VARCHAR,
        policy_hash VARCHAR)""")
    conn.execute("""CREATE TABLE announcement_filter_decision (
        raw_event_id BIGINT, policy_version VARCHAR)""")
    conn.execute(
        "INSERT INTO announcement_collection_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ["nse-complete", "nse_api", datetime(2024, 8, 23),
         datetime(2026, 8, 23, 23, 59, 59, 999999), "COMPLETED", True,
         100, 0, "market-intel-high-value-filter-v1", "hash"],
    )
    conn.close()

    coverage = MarketIntelAnnouncementAdapter(
        market_intel_db=market_db, screener_db=tmp_path / "unused.duckdb",
    ).coverage_receipts(
        published_from=date(2024, 8, 23), as_of_date=date(2026, 8, 23),
        filter_policy_version="market-intel-high-value-filter-v1",
        required_sources=("nse_api",),
    )

    assert coverage["coverage_proven"] is True
    assert coverage["required_sources"] == ["nse_api"]
    assert coverage["missing_sources"] == []


def test_jcurve_projection_excludes_generic_high_value_signals() -> None:
    assert "CAPEX" in JCURVE_FILTER_SIGNALS
    assert "COMMERCIALISATION" in JCURVE_FILTER_SIGNALS
    assert "CORPORATE_TRANSACTION" not in JCURVE_FILTER_SIGNALS
    assert "MATERIAL_FINANCING" not in JCURVE_FILTER_SIGNALS

def _claim(claim_type: str, *, numeric=None, unit=None, status="HUMAN_VERIFIED"):
    return {
        "claim_id": f"claim:{claim_type}", "claim_type": claim_type,
        "evidence_state": "PRESENT", "numeric_value": numeric,
        "boolean_value": None, "text_value": None, "unit": unit,
        "project_name": "Plant II", "project_location": "Pune",
        "exact_excerpt": "The company commissioned Plant II at Pune.", "page": 2,
        "effective_at": "2026-07-01T00:00:00+05:30", "confidence": 0.95,
        "status": status,
    }


def test_capex_policy_fails_closed_and_requires_verified_evidence():
    policy = CapexStagePolicy.from_root(PROJECT_ROOT)
    claims = [
        _claim("CAPEX_ANNOUNCED", numeric=150.0, unit="INR_CRORE"),
        _claim("COMMERCIAL_PRODUCTION_STARTED"),
    ]
    missing = policy.evaluate(claims, inputs=MaterialityInputs())
    assert missing.stage == "CANDIDATE"
    assert "MATERIALITY_NOT_PROVEN" in missing.reason_codes

    passed = policy.evaluate(claims, inputs=MaterialityInputs(ttm_revenue_cr=1000.0))
    assert passed.stage == "J1"
    assert passed.materiality_metrics["capex_to_ttm_revenue"] == pytest.approx(0.15)

    claims[0]["status"] = "HUMAN_REVIEW_REQUIRED"
    pending = policy.evaluate(claims, inputs=MaterialityInputs(ttm_revenue_cr=1000.0))
    assert pending.stage == "CANDIDATE"
    assert "HUMAN_REVIEW_PENDING" in pending.reason_codes

    claims[0]["status"] = "HUMAN_VERIFIED"
    claims[1]["boolean_value"] = False
    denied = policy.evaluate(claims, inputs=MaterialityInputs(ttm_revenue_cr=1000.0))
    assert denied.stage == "CANDIDATE"
    assert "VERIFIED_DEMAND_PATH_MISSING" in denied.reason_codes


def test_curated_capex_baseline_has_requested_anchors_and_resolves_exactly(tmp_path):
    cohort = BaselineCohort.load(
        PROJECT_ROOT / "configs/research_screener/jcurve/capex_baseline_v1.json"
    )
    assert cohort.raw["required_company_count"] == 25
    symbols = {row["nse_symbol"] for row in cohort.raw["members"]}
    assert {"WELCORP", "HSCL", "DEEDEV"} <= symbols

    store_path = tmp_path / "screener.duckdb"
    conn = duckdb.connect(str(store_path))
    conn.execute("""CREATE TABLE company_master (
        company_id VARCHAR, legal_name VARCHAR, valid_from DATE, valid_to DATE)""")
    conn.execute("""CREATE TABLE security_master (
        security_id VARCHAR, company_id VARCHAR, isin VARCHAR, valid_from DATE, valid_to DATE)""")
    conn.execute("""CREATE TABLE listing_master (
        listing_id VARCHAR, security_id VARCHAR, exchange VARCHAR, symbol VARCHAR,
        bse_code VARCHAR, valid_from DATE, valid_to DATE)""")
    for index, member in enumerate(cohort.raw["members"]):
        company_id = f"company:{index}"
        security_id = f"security:{index}"
        conn.execute(
            "INSERT INTO company_master VALUES (?, ?, DATE '2020-01-01', NULL)",
            [company_id, member["legal_name"]],
        )
        conn.execute(
            "INSERT INTO security_master VALUES (?, ?, ?, DATE '2020-01-01', NULL)",
            [security_id, company_id, member["isin"]],
        )
        conn.execute(
            "INSERT INTO listing_master VALUES (?, ?, 'NSE', ?, NULL, DATE '2020-01-01', NULL)",
            [f"listing:nse:{index}", security_id, member["nse_symbol"]],
        )
        conn.execute(
            "INSERT INTO listing_master VALUES (?, ?, 'BSE', NULL, ?, DATE '2020-01-01', NULL)",
            [f"listing:bse:{index}", security_id, member["bse_code"]],
        )
    conn.close()

    resolved = cohort.resolve(store_path=store_path, as_of_date=date(2026, 8, 21))
    assert len(resolved.company_ids) == 25
    assert len(resolved.members) == 25
    with pytest.raises(ValueError, match="identity snapshot is later"):
        cohort.resolve(store_path=store_path, as_of_date=date(2026, 8, 20))


def test_v2_capex_baseline_has_100_unlabeled_stratified_cases() -> None:
    cohort = BaselineCohort.load(
        PROJECT_ROOT / "configs/research_screener/jcurve/capex_baseline_v2.json"
    )

    assert cohort.raw["required_company_count"] == 100
    assert len({row["isin"] for row in cohort.raw["members"]}) == 100
    roles: dict[str, int] = {}
    for member in cohort.raw["members"]:
        roles[member["case_role"]] = roles.get(member["case_role"], 0) + 1
        assert member["label_state"] == "UNLABELED"
    assert roles == {
        "known_anchor": 25,
        "screen1_only_build": 15,
        "screen2_only_older_capex": 15,
        "screen3_led_commissioning": 15,
        "screen4_led_ramp": 15,
        "multi_screen_commissioning_ramp": 15,
    }


def test_model_router_uses_strict_schema_and_different_verifier_family():
    policy = RouterPolicy.load(PROJECT_ROOT / "configs/research_screener/jcurve/model_policy.json")
    schemas = PROJECT_ROOT / "configs/research_screener/schemas"
    requests_seen = []
    extracted_claim = {
        "claim_type": "CAPEX_ANNOUNCED", "evidence_state": "PRESENT",
        "numeric_value": 150.0, "boolean_value": None, "text_value": None,
        "unit": "INR_CRORE", "project_name": "Plant II", "project_location": "Pune",
        "exact_excerpt": "The company announced capex of INR 150 crore.", "page": 2,
        "effective_at": "2026-07-01T00:00:00+05:30", "confidence": 0.94,
    }

    def post(url, **kwargs):
        requests_seen.append(kwargs["json"])
        if len(requests_seen) == 1:
            content = {"claims": [extracted_claim]}
            return _Response({"id": "req-extract", "model": "deepseek/deepseek-chat", "provider": "test-a", "choices": [{"message": {"content": json.dumps(content)}}], "usage": {"prompt_tokens": 100, "completion_tokens": 20, "cost": 0.001}})
        review = {"reviews": [{"claim_index": 0, "decision": "ACCEPT", "supported_excerpt": extracted_claim["exact_excerpt"], "supported_page": 2, "issue_codes": []}]}
        return _Response({"id": "req-verify", "model": "google/gemini-2.5-flash-lite", "provider": "test-b", "choices": [{"message": {"content": json.dumps(review)}}], "usage": {"prompt_tokens": 80, "completion_tokens": 10}})

    router = OpenRouterModelRouter(
        api_key="secret", policy=policy,
        extraction_schema=json.loads((schemas / "jcurve_claim_batch_v1.schema.json").read_text()),
        verification_schema=json.loads((schemas / "jcurve_review_batch_v1.schema.json").read_text()),
        post=post,
    )
    packet = EvidencePacket(
        announcement_id="announcement:1", source_artifact_id="artifact:1",
        source_content_hash="a" * 64, company_id="company:1", security_id="security:1",
        isin="INE001B01026", published_at=datetime(2026, 7, 2, tzinfo=UTC),
        title="Capex announcement", pages=(EvidencePage(2, extracted_claim["exact_excerpt"]),),
    )
    extraction = router.extract(packet, route=ModelRoute.TEXT)
    verification = router.verify(packet, claims=extraction.payload["claims"], extraction_route=ModelRoute.TEXT)
    assert extraction.call.model_id.startswith("deepseek/")
    assert verification.call.model_id.startswith("google/")
    assert all(request["response_format"]["type"] == "json_schema" for request in requests_seen)
    assert requests_seen[0]["response_format"]["json_schema"]["strict"] is True


def test_model_router_rejects_unbounded_packets():
    policy = RouterPolicy.load(PROJECT_ROOT / "configs/research_screener/jcurve/model_policy.json")
    schemas = PROJECT_ROOT / "configs/research_screener/schemas"
    router = OpenRouterModelRouter(
        api_key="secret", policy=policy,
        extraction_schema=json.loads((schemas / "jcurve_claim_batch_v1.schema.json").read_text()),
        verification_schema=json.loads((schemas / "jcurve_review_batch_v1.schema.json").read_text()),
        post=lambda *args, **kwargs: None,
    )
    packet = EvidencePacket(
        announcement_id="a", source_artifact_id="s", source_content_hash="b" * 64,
        company_id="c", security_id="s", isin="INE001B01026",
        published_at=datetime.now(UTC), title="x",
        pages=tuple(EvidencePage(page=i + 1, text="capex") for i in range(9)),
    )
    with pytest.raises(ValueError, match="page limit"):
        router.extract(packet, route=ModelRoute.TEXT)


def test_market_intel_adapter_enforces_publication_and_ingestion_cutoff(tmp_path):
    mi_path = tmp_path / "market.duckdb"
    mi = duckdb.connect(str(mi_path))
    mi.execute("""CREATE TABLE raw_event (
        raw_event_id BIGINT, event_hash VARCHAR, source VARCHAR, external_id VARCHAR,
        symbol VARCHAR, isin VARCHAR, company_name VARCHAR, title VARCHAR,
        description VARCHAR, published_at TIMESTAMP, event_date TIMESTAMP,
        ingested_at TIMESTAMP, raw_payload_json VARCHAR, link VARCHAR, attachment_url VARCHAR)""")
    mi.execute("CREATE TABLE resolved_event (raw_event_id BIGINT, primary_category VARCHAR, is_official BOOLEAN)")
    mi.execute("CREATE TABLE filing_document (raw_event_id BIGINT, local_path VARCHAR, content_hash VARCHAR, pdf_status VARCHAR)")
    base = ["nse_rss", "ext", "ABC", "INE001B01026", "ABC Ltd", "Capex expansion", "Plant II", datetime(2026, 7, 1), datetime(2026, 7, 1)]
    mi.execute("INSERT INTO raw_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [1, "h1", *base, datetime(2026, 7, 2), "{}", "https://nse/1", None])
    mi.execute("INSERT INTO raw_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [2, "h2", *base, datetime(2026, 7, 5), "{}", "https://nse/2", None])
    mi.execute("INSERT INTO resolved_event VALUES (1, 'capex_expansion', TRUE), (2, 'capex_expansion', TRUE)")
    mi.close()

    screener_path = tmp_path / "screener.duckdb"
    screener = duckdb.connect(str(screener_path))
    screener.execute("CREATE TABLE security_master (security_id VARCHAR, company_id VARCHAR, isin VARCHAR, valid_from DATE, valid_to DATE)")
    screener.execute("CREATE TABLE listing_master (listing_id VARCHAR, security_id VARCHAR, exchange VARCHAR, symbol VARCHAR, bse_code VARCHAR, exchange_security_id VARCHAR, valid_from DATE, valid_to DATE)")
    screener.execute("INSERT INTO security_master VALUES ('security:1', 'company:1', 'INE001B01026', DATE '2020-01-01', NULL)")
    screener.execute("INSERT INTO listing_master VALUES ('listing:1', 'security:1', 'NSE', 'ABC', NULL, NULL, DATE '2020-01-01', NULL)")
    screener.close()

    rows = MarketIntelAnnouncementAdapter(market_intel_db=mi_path, screener_db=screener_path).read(
        published_from=date(2026, 7, 1), as_of_date=date(2026, 7, 3),
    )
    assert [row.upstream_event_hash for row in rows] == ["h1"]
    assert rows[0].identity_status == "RESOLVED"
    assert rows[0].company_id == "company:1"


def test_market_intel_adapter_uses_exchange_specific_latest_identity_fallback(tmp_path):
    path = tmp_path / "screener.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE security_master (security_id VARCHAR, company_id VARCHAR, isin VARCHAR, valid_from DATE, valid_to DATE)")
    conn.execute("CREATE TABLE listing_master (listing_id VARCHAR, security_id VARCHAR, exchange VARCHAR, symbol VARCHAR, bse_code VARCHAR, exchange_security_id VARCHAR, valid_from DATE, valid_to DATE)")
    conn.execute("INSERT INTO security_master VALUES ('security:1', 'company:1', 'INE001B01026', DATE '2026-08-22', NULL)")
    conn.execute("""INSERT INTO listing_master VALUES
        ('listing:nse', 'security:1', 'NSE', 'ABC', NULL, NULL, DATE '2026-08-22', NULL),
        ('listing:bse', 'security:1', 'BSE', NULL, '500001', NULL, DATE '2026-08-22', NULL)""")

    matches = MarketIntelAnnouncementAdapter._resolve_identity(
        conn, source="nse_api", symbol="ABC", isin="INE001B01026",
        company_name="ABC Ltd", as_of=date(2025, 1, 1),
    )
    conn.close()

    assert matches == [("company:1", "security:1", "listing:nse")]


def test_market_intel_adapter_can_opt_into_high_value_filter_v1(tmp_path):
    mi_path = tmp_path / "market.duckdb"
    mi = duckdb.connect(str(mi_path))
    mi.execute("""CREATE TABLE raw_event (
        raw_event_id BIGINT, event_hash VARCHAR, source VARCHAR, external_id VARCHAR,
        symbol VARCHAR, isin VARCHAR, company_name VARCHAR, title VARCHAR,
        description VARCHAR, published_at TIMESTAMP, event_date TIMESTAMP,
        ingested_at TIMESTAMP, raw_payload_json VARCHAR, link VARCHAR, attachment_url VARCHAR)""")
    mi.execute("CREATE TABLE resolved_event (raw_event_id BIGINT, primary_category VARCHAR, is_official BOOLEAN)")
    mi.execute("CREATE TABLE filing_document (raw_event_id BIGINT, local_path VARCHAR, content_hash VARCHAR, pdf_status VARCHAR)")
    mi.execute("""CREATE TABLE announcement_filter_decision (
        raw_event_id BIGINT, policy_version VARCHAR, decision VARCHAR,
        matched_signals_json VARCHAR)""")
    mi.execute("""CREATE TABLE announcement_collection_run (
        collection_run_id VARCHAR, source VARCHAR, requested_from TIMESTAMP,
        requested_to TIMESTAMP, status VARCHAR, pages_complete BOOLEAN,
        item_count INTEGER, failure_count INTEGER, policy_version VARCHAR,
        policy_hash VARCHAR)""")
    now = datetime(2026, 7, 1)
    base = ["nse_api", "ext", "ABC", "INE001B01026", "ABC Ltd", "Capex", "Plant", now, now, now]
    mi.execute("INSERT INTO raw_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [1, "h1", *base, "{}", "https://nse/1", None])
    mi.execute("INSERT INTO raw_event VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [2, "h2", *base, "{}", "https://nse/2", None])
    mi.execute("INSERT INTO resolved_event VALUES (1, 'clarification', TRUE), (2, 'capex_expansion', TRUE)")
    mi.execute("""INSERT INTO announcement_filter_decision VALUES
        (1, 'market-intel-high-value-filter-v1', 'KEEP', '[\"CAPEX\"]'),
        (2, 'market-intel-high-value-filter-v1', 'DROP_METADATA_ONLY', '[]')""")
    mi.close()

    screener_path = tmp_path / "screener.duckdb"
    screener = duckdb.connect(str(screener_path))
    screener.execute("CREATE TABLE security_master (security_id VARCHAR, company_id VARCHAR, isin VARCHAR, valid_from DATE, valid_to DATE)")
    screener.execute("CREATE TABLE listing_master (listing_id VARCHAR, security_id VARCHAR, exchange VARCHAR, symbol VARCHAR, bse_code VARCHAR, exchange_security_id VARCHAR, valid_from DATE, valid_to DATE)")
    screener.execute("INSERT INTO security_master VALUES ('security:1', 'company:1', 'INE001B01026', DATE '2020-01-01', NULL)")
    screener.execute("INSERT INTO listing_master VALUES ('listing:1', 'security:1', 'NSE', 'ABC', NULL, NULL, DATE '2020-01-01', NULL)")
    screener.close()

    rows = MarketIntelAnnouncementAdapter(market_intel_db=mi_path, screener_db=screener_path).read(
        published_from=date(2026, 7, 1), as_of_date=date(2026, 7, 1),
        filter_policy_version="market-intel-high-value-filter-v1",
    )
    assert [row.upstream_event_hash for row in rows] == ["h1"]


def test_market_intel_coverage_accepts_contiguous_completed_chunks():
    lower = datetime(2024, 8, 23)
    upper = datetime(2024, 10, 23, 23, 59, 59, 999999)
    chunks = [
        (datetime(2024, 8, 23), datetime(2024, 9, 22, 23, 59, 59, 999999)),
        (datetime(2024, 9, 23), datetime(2024, 10, 23, 23, 59, 59, 999999)),
    ]

    assert MarketIntelAnnouncementAdapter._intervals_cover(
        chunks, lower=lower, upper=upper,
    )
    assert not MarketIntelAnnouncementAdapter._intervals_cover(
        [chunks[0], (datetime(2024, 9, 24), chunks[1][1])],
        lower=lower, upper=upper,
    )


def test_jcurve_migration_and_import_persistence(tmp_path):
    store = JCurveStore(tmp_path / "screener.duckdb")
    now = datetime(2026, 7, 2, tzinfo=UTC)
    raw = b"{}"
    digest = hashlib.sha256(raw).hexdigest()
    artifact = {
        "artifact_id": "artifact:raw", "ingestion_run_id": "ingest:raw",
        "source_key": "market_intel_announcement", "provider": "NSE",
        "retrieved_at": now, "content_hash": digest, "byte_count": len(raw),
        "parser_version": "v1", "schema_version": "v1", "validation_status": "VALID",
    }
    payload = {
        "run_id": "jcurve-import-test", "run_type": "BOOTSTRAP", "parent_run_id": None,
        "as_of_date": date(2026, 7, 2), "policy_version": "v1", "policy_hash": "a" * 64,
        "snapshot_hash": "b" * 64, "announcement_count": 1, "claim_count": 0,
        "episode_count": 0, "degraded_reason": None, "started_at": now,
        "artifacts": [artifact], "announcements": [{
            "announcement_id": "announcement:1", "upstream_raw_event_id": 1,
            "upstream_event_hash": "h1", "company_id": None, "security_id": None,
            "listing_id": None, "identity_status": "UNRESOLVED", "source": "nse_rss",
            "external_id": "ext", "category": "capex_expansion", "title": "Capex",
            "description": None, "published_at": now, "event_at": now, "retrieved_at": now,
            "source_artifact_id": "artifact:raw", "attachment_artifact_id": None,
        }],
    }
    store.persist_import(payload)
    assert store.completed_run("jcurve-import-test")

    evaluation = {
        "run_id": "jcurve-evaluate-test", "run_type": "EVALUATION",
        "parent_run_id": "jcurve-import-test", "as_of_date": date(2026, 7, 2),
        "policy_version": "agent-v1", "policy_hash": "c" * 64,
        "snapshot_hash": "d" * 64, "announcement_count": 1, "claim_count": 1,
        "episode_count": 1, "degraded_reason": None, "started_at": now,
        "calls": [{
            "request_id": "request:1", "announcement_id": "announcement:1",
            "role": "EXTRACTION", "route": "text", "model_id": "deepseek/deepseek-chat",
            "provider": "test", "prompt_version": "v1", "prompt_hash": "e" * 64,
            "source_page_hashes": ["f" * 64], "input_tokens": 10, "output_tokens": 5,
            "cost_usd": 0.001, "schema_valid": True, "attempt_count": 1,
            "retry_history": [], "response_hash": "1" * 64, "status": "COMPLETED",
            "error_code": None,
        }],
        "claims": [{
            "claim_id": "claim:1", "announcement_id": "announcement:1",
            "company_id": "company:1", "security_id": "security:1",
            "schema_version": "jcurve-claim-v1", "claim_type": "CAPEX_ANNOUNCED",
            "evidence_state": "PRESENT", "numeric_value": 150.0, "boolean_value": None,
            "text_value": None, "unit": "INR_CRORE", "project_name": "Plant II",
            "project_location": "Pune", "exact_excerpt": "Capex is INR 150 crore.",
            "page": 1, "source_artifact_id": "artifact:raw", "source_content_hash": digest,
            "published_at": now, "effective_at": now, "confidence": 0.9,
            "status": "HUMAN_REVIEW_REQUIRED", "claim_json": {"claim_type": "CAPEX_ANNOUNCED"},
        }],
        "reviews": [{
            "review_id": "review:1", "claim_id": "claim:1", "request_id": "request:1",
            "decision": "ACCEPT", "reviewer_model": "google/gemini-2.5-flash-lite",
            "provider": "test", "prompt_version": "v1", "independent_context": True,
            "normalized_claim_hash": "2" * 64, "issue_codes": [], "review_json": {"decision": "ACCEPT"},
        }],
        "episodes": [{
            "episode_id": "episode:1", "company_id": "company:1", "archetype": "CAPEX",
            "project_key": "plant-ii:pune", "project_name": "Plant II", "project_location": "Pune",
            "opened_at": now, "closed_at": None, "status": "REVIEW_REQUIRED",
            "first_trigger_artifact_id": "artifact:raw", "clustering_policy_version": "v1",
        }],
        "episode_evidence": [{"episode_id": "episode:1", "claim_id": "claim:1", "relation": "EVIDENCE"}],
        "observations": [{
            "observation_id": "observation:1", "episode_id": "episode:1",
            "as_of_date": date(2026, 7, 2), "stage": "CANDIDATE", "score": 60.0,
            "confidence": 0.9, "materiality_passed": True,
            "materiality_metrics": {"capex_to_ttm_revenue": 0.15},
            "reason_codes": ["HUMAN_REVIEW_PENDING"], "evidence_ids": [],
            "policy_version": "v1", "policy_hash": "3" * 64,
        }],
    }
    store.persist_evaluation(evaluation)
    assert store.completed_run("jcurve-evaluate-test")
