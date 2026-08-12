from __future__ import annotations

import json
from datetime import date

from ai_trading_system.domains.research_screener.annual_reports import (
    AnnualReportClient,
    TOPIC_PATTERNS,
    extract_topic_evidence,
)


PDF_BYTES = b"%PDF-1.4 test fixture"


class _Response:
    def __init__(self, content: bytes, *, url: str, content_type: str):
        self.content = content
        self.url = url
        self.status_code = 200
        self.headers = {"content-type": content_type}

    def json(self):
        return json.loads(self.content)


class _Exchange:
    NSE_HOME = "https://www.nseindia.com/"

    def __init__(self, pdf: bytes):
        self.pdf = pdf

    def _get(self, url: str, *, referer: str):
        if "corporate-filings-annual-reports" in url:
            return _Response(b"<html></html>", url=url, content_type="text/html")
        return _Response(self.pdf, url=url, content_type="application/pdf")

    def _get_with_params(self, url: str, params: dict, *, referer: str):
        payload = {"data": [{
            "companyName": "Example Limited", "fromYr": "2024", "toYr": "2025",
            "submission_type": "New", "broadcast_dttm": "09-JUL-2025 18:50:53",
            "fileName": "https://nsearchives.nseindia.com/annual_reports/AR_EXAMPLE_2024_2025.pdf",
        }]}
        return _Response(json.dumps(payload).encode(), url=url, content_type="application/json")

    @staticmethod
    def _validate(response, *, expected: tuple[str, ...], source: str) -> bytes:
        assert response.status_code == 200
        assert any(token in response.headers["content-type"] for token in expected)
        return response.content


class _FallbackExchange(_Exchange):
    def _get_with_params(self, url: str, params: dict, *, referer: str):
        if "annual-reports" in url:
            payload = {"data": [{
                "companyName": "Example Limited", "fromYr": "2024", "toYr": "2025",
                "broadcast_dttm": "09-JUL-2025 18:50:53",
                "fileName": "https://nsearchives.nseindia.com/annual_reports/AR_EXAMPLE_2024_2025_A_100_09072025185053.pdf",
            }]}
        else:
            payload = {"Table": [{
                "SCRIP_CD": 500001, "NEWSSUB": "Annual Report 2024-25",
                "HEADLINE": "Annual Report", "SUBCATNAME": "Annual Report",
                "News_submission_dt": "2025-07-10T10:00:00",
                "ATTACHMENTNAME": "example.pdf", "Fld_Attachsize": len(PDF_BYTES),
                "SLONGNAME": "Example Limited",
            }]}
        return _Response(json.dumps(payload).encode(), url=url, content_type="application/json")


def test_extracts_attributable_page_anchors_and_not_disclosed_states():
    pages = [
        "Corporate governance and board of directors overview.",
        "Capital expenditure supports capacity expansion. The order book is strong.",
        "General financial statements.",
    ]
    rows = extract_topic_evidence(pages)
    by_topic = {}
    for row in rows:
        by_topic.setdefault(row["topic"], []).append(row)
    assert set(by_topic) == set(TOPIC_PATTERNS)
    assert by_topic["governance"][0]["page"] == 1
    assert by_topic["capex_capacity"][0]["page"] == 2
    assert by_topic["order_book"][0]["state"] == "DISCLOSED_TEXT_MATCH"
    assert by_topic["shareholding"][0]["state"] == "NOT_DISCLOSED"
    assert all(row["confidence"] in {"LOW", "NONE"} for row in rows)


def test_nse_discovery_rejects_post_cutoff_metadata():
    exchange = _Exchange(PDF_BYTES)
    member = {
        "symbol": "EXAMPLE", "company": "Example Limited", "isin": "INE000A01001",
        "listings": [{"exchange": "NSE", "symbol": "EXAMPLE"}],
    }
    document, artifacts = AnnualReportClient(exchange).discover(member, date(2025, 7, 8))
    assert document["state"] == "SOURCE_UNAVAILABLE"
    assert all(row["state"] == "NOT_DISCLOSED" for row in document["evidence"])
    assert artifacts[0]["source_key"] == "nse_annual_report_metadata"


def test_nse_discovery_preserves_document_hash_and_page_evidence(monkeypatch):
    exchange = _Exchange(PDF_BYTES)
    monkeypatch.setattr(
        "ai_trading_system.domains.research_screener.annual_reports.extract_pdf_pages",
        lambda raw: ["Corporate governance report.", "The order book is disclosed.", "Management outlook."],
    )
    member = {
        "symbol": "EXAMPLE", "company": "Example Limited", "isin": "INE000A01001",
        "listings": [{"exchange": "NSE", "symbol": "EXAMPLE"}],
    }
    document, artifacts = AnnualReportClient(exchange).discover(member, date(2025, 7, 10))
    assert document["state"] == "PRESENT"
    assert document["published_at"].date() == date(2025, 7, 9)
    assert document["page_count"] == 3
    assert document["source_artifact_id"] == artifacts[-1]["artifact_id"]
    assert len(artifacts[-1]["content_hash"]) == 64
    assert all(row["review_status"] != "AUTO_ACCEPTED" for row in document["evidence"])


def test_truncated_nse_archive_falls_back_to_official_bse(monkeypatch):
    exchange = _FallbackExchange(PDF_BYTES)
    monkeypatch.setattr(
        "ai_trading_system.domains.research_screener.annual_reports.extract_pdf_pages",
        lambda raw: ["Corporate governance report.", "The order book is disclosed.", "Management outlook."],
    )
    member = {
        "symbol": "EXAMPLE", "company": "Example Limited", "isin": "INE000A01001",
        "listings": [
            {"exchange": "NSE", "symbol": "EXAMPLE"},
            {"exchange": "BSE", "symbol": "EXAMPLE", "bse_code": "500001"},
        ],
    }
    document, artifacts = AnnualReportClient(exchange).discover(member, date(2025, 7, 10))
    assert document["state"] == "PRESENT"
    assert document["provider"] == "BSE"
    failed = [row for row in artifacts if row["validation_status"] == "FAILED"]
    assert len(failed) == 1
    assert failed[0]["metadata"]["expected_byte_count"] == 100
    assert failed[0]["metadata"]["observed_byte_count"] == len(PDF_BYTES)
