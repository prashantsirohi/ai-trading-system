import hashlib
from datetime import date, datetime, timezone
from types import SimpleNamespace

from ai_trading_system.domains.research_screener.issuer_filings import IssuerFilingRepairClient


class _Exchange:
    def __init__(self, raw: bytes):
        self.raw = raw

    def _get(self, url, *, referer):
        return SimpleNamespace(
            status_code=200, headers={"content-type": "application/pdf"}, content=self.raw,
        )

    @staticmethod
    def _validate(response, *, expected, source):
        return response.content


def _snapshot(scope="standalone"):
    existing = {
        "period_type": "annual", "period_end": date(2022, 3, 31),
        "published_at": datetime(2022, 5, 1, tzinfo=timezone.utc), "scope": scope,
        "source_provider": "NSE_LEGACY_XBRL", "source_row_hash": "exchange-row",
        "metrics": {
            "sales": 10.0, "operating_profit": 2.0, "net_profit": 1.0, "eps": 1.0,
            "reserves": 3.0, "borrowings": 0.5, "cash_and_bank": 1.0,
            "cash_from_operations": 1.5, "capex": 0.75,
        },
    }
    return {
        "scope": scope, "annual_statements": [existing], "quarterly_statements": [],
        "annual_completeness": 0.1667, "quarterly_completeness": 0.0,
        "target_periods": {
            "annual": [date(year, 3, 31) for year in range(2026, 2020, -1)],
            "quarterly": [],
        },
        "missing_target_periods": {"annual": [], "quarterly": []},
        "latest_disclosed_periods": {"annual": date(2022, 3, 31), "quarterly": None},
        "latest_parsed_periods": {"annual": date(2022, 3, 31), "quarterly": None},
    }


def _contract(raw: bytes, *, overlap_sales=10.0):
    return {
        "contract_version": "test-v1",
        "symbols": {
            "ABC": {
                "expected_isin": "INEABC", "scope": "standalone",
                "document": {
                    "provider": "COMPANY_IR", "document_kind": "ANNUAL_RESULTS",
                    "url": "https://issuer.test/results.pdf", "referer": "https://issuer.test/",
                    "published_at": "2022-05-27T00:00:00+05:30",
                    "sha256": hashlib.sha256(raw).hexdigest(), "identity_pages": [1],
                    "required_text_markers": ["ABC Limited", "CIN123"],
                },
                "statements": [
                    {
                        "period_type": "annual", "period_end": "2022-03-31",
                        "evidence_pages": [1],
                        "reconcile_metrics": ["sales", "operating_profit", "net_profit", "reserves"],
                        "metrics": {
                            "sales": overlap_sales, "operating_profit": 2.0, "net_profit": 1.0,
                            "eps": 1.0, "reserves": 3.0, "borrowings": 0.5,
                            "cash_and_bank": 1.0, "cash_from_operations": 1.5, "capex": 0.75,
                        },
                    },
                    {
                        "period_type": "annual", "period_end": "2021-03-31",
                        "evidence_pages": [1],
                        "metrics": {
                            "sales": 8.0, "operating_profit": 1.5, "net_profit": 0.8,
                            "eps": 0.8, "reserves": 2.5, "borrowings": 0.4,
                            "cash_and_bank": 0.9, "cash_from_operations": 1.2, "capex": 0.6,
                        },
                    },
                ],
            }
        },
    }


def test_issuer_pdf_reconciles_overlap_and_adds_missing_period_only(monkeypatch):
    raw = b"%PDF-test-fixture"
    client = IssuerFilingRepairClient(_Exchange(raw), _contract(raw))
    monkeypatch.setattr(client, "_extract_pages", lambda raw, pages: {1: "ABC Limited CIN123"})

    result, artifacts = client.augment(
        "ABC", "INEABC", _snapshot(), date(2026, 8, 11), company_type="CORPORATE",
    )

    assert artifacts[0]["validation_status"] == "VALID"
    assert [row["period_end"] for row in result["annual_statements"]] == [
        date(2022, 3, 31), date(2021, 3, 31),
    ]
    assert result["annual_statements"][0]["source_provider"] == "NSE_LEGACY_XBRL"
    assert result["annual_statements"][1]["formula_version"] == "issuer-pdf-curated-v1"
    assert result["issuer_repair"]["reconciled_periods"]["annual"] == [date(2022, 3, 31)]
    assert result["issuer_repair"]["added_periods"]["annual"] == [date(2021, 3, 31)]


def test_issuer_pdf_fails_closed_on_hash_or_overlap_mismatch(monkeypatch):
    raw = b"%PDF-test-fixture"
    bad_hash = _contract(raw)
    bad_hash["symbols"]["ABC"]["document"]["sha256"] = "0" * 64
    hash_client = IssuerFilingRepairClient(_Exchange(raw), bad_hash)
    monkeypatch.setattr(hash_client, "_extract_pages", lambda raw, pages: {1: "ABC Limited CIN123"})
    result, artifacts = hash_client.augment(
        "ABC", "INEABC", _snapshot(), date(2026, 8, 11), company_type="CORPORATE",
    )
    assert result["issuer_repair"]["status"] == "FAILED"
    assert artifacts[0]["validation_status"] == "FAILED"
    assert artifacts[0]["content_hash"] == hashlib.sha256(raw).hexdigest()

    mismatch_client = IssuerFilingRepairClient(_Exchange(raw), _contract(raw, overlap_sales=11.0))
    monkeypatch.setattr(mismatch_client, "_extract_pages", lambda raw, pages: {1: "ABC Limited CIN123"})
    result, artifacts = mismatch_client.augment(
        "ABC", "INEABC", _snapshot(), date(2026, 8, 11), company_type="CORPORATE",
    )
    assert result["issuer_repair"]["status"] == "FAILED"
    assert "OVERLAP_MISMATCH" in result["issuer_repair"]["reason"]
    assert artifacts[-1]["validation_status"] == "FAILED"


def test_issuer_pdf_replaces_an_exchange_period_with_no_mandatory_facts(monkeypatch):
    raw = b"%PDF-test-fixture"
    snapshot = _snapshot()
    snapshot["annual_statements"][0]["metrics"] = {
        metric: None for metric in snapshot["annual_statements"][0]["metrics"]
    }
    client = IssuerFilingRepairClient(_Exchange(raw), _contract(raw))
    monkeypatch.setattr(client, "_extract_pages", lambda raw, pages: {1: "ABC Limited CIN123"})

    result, _ = client.augment(
        "ABC", "INEABC", snapshot, date(2026, 8, 11), company_type="CORPORATE",
    )

    repaired = next(row for row in result["annual_statements"] if row["period_end"] == date(2022, 3, 31))
    assert repaired["formula_version"] == "issuer-pdf-curated-v1"
    assert result["issuer_repair"]["filled_empty_periods"]["annual"] == [date(2022, 3, 31)]
