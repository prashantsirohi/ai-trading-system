from __future__ import annotations

import io
import re
import threading
from datetime import date, datetime, timedelta
from typing import Any

import requests
import pypdfium2 as pdfium
import pdfplumber

from .filings import parse_exchange_datetime
from .providers import OfficialExchangeClient, _artifact


TOPIC_PATTERNS: dict[str, tuple[str, ...]] = {
    "governance": ("corporate governance", "board of directors", "audit committee"),
    "shareholding": ("shareholding pattern", "promoter shareholding", "public shareholding"),
    "business_kpi": ("key performance indicator", "operating metric", "business performance"),
    "capex_capacity": ("capital expenditure", "capex", "installed capacity", "capacity expansion"),
    "order_book": ("order book", "order inflow", "book to bill"),
    "management_guidance": ("guidance", "outlook", "expect to", "we believe"),
}
EVIDENCE_EXTRACTOR_VERSION = "annual-report-topic-anchor-v2-serialized-pdfium-pdfplumber-fallback"
_PDFIUM_LOCK = threading.Lock()


class AnnualReportClient:
    """Acquire the latest point-in-time annual report from official exchanges."""

    NSE_PAGE = "https://www.nseindia.com/companies-listing/corporate-filings-annual-reports"
    NSE_API = "https://www.nseindia.com/api/annual-reports"
    BSE_API = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    BSE_PAGE = "https://www.bseindia.com/corporates/ann.html"
    BSE_ATTACHMENT = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/{name}"

    def __init__(self, exchange: OfficialExchangeClient):
        self.exchange = exchange
        self._nse_primed = False

    def discover(self, member: dict[str, Any], as_of_date: date) -> tuple[dict, list[dict]]:
        listings = member.get("listings", [])
        nse = next((row for row in listings if row.get("exchange") == "NSE" and row.get("symbol")), None)
        bse = next((row for row in listings if row.get("exchange") == "BSE" and row.get("bse_code")), None)
        artifacts: list[dict] = []
        errors: list[str] = []
        if nse:
            rows, artifact, error = self._nse_candidates(str(nse["symbol"]), as_of_date)
            artifacts.append(artifact)
            if error:
                errors.append(error)
            if rows:
                document, document_artifact, document_error = self._acquire_selected(
                    member, rows, as_of_date,
                )
                artifacts.append(document_artifact)
                if document is not None:
                    return document, artifacts
                errors.append(document_error or "NSE_ANNUAL_REPORT_ACQUISITION_FAILED")
        if bse:
            rows, bse_artifacts, error = self._bse_candidates(str(bse["bse_code"]), as_of_date)
            artifacts.extend(bse_artifacts)
            if error:
                errors.append(error)
            if rows:
                document, document_artifact, document_error = self._acquire_selected(
                    member, rows, as_of_date,
                )
                artifacts.append(document_artifact)
                if document is not None:
                    return document, artifacts
                errors.append(document_error or "BSE_ANNUAL_REPORT_ACQUISITION_FAILED")
        return self._unavailable(member, errors or ["NO_POINT_IN_TIME_ANNUAL_REPORT"]), artifacts

    def _acquire_selected(self, member: dict, candidates: list[dict],
                          as_of_date: date) -> tuple[dict | None, dict, str | None]:
        selected = max(candidates, key=lambda row: (row["to_year"], row["published_at"], row["url"]))
        raw: bytes | None = None
        try:
            response = self.exchange._get(selected["url"], referer=selected["referer"])
            raw = self.exchange._validate(
                response, expected=("application/pdf", "octet-stream"), source=f"ANNUAL_REPORT:{member['symbol']}",
            )
            if not raw.startswith(b"%PDF-"):
                raise RuntimeError("ANNUAL_REPORT_NOT_PDF")
            expected_bytes = selected.get("expected_byte_count")
            if expected_bytes and len(raw) != expected_bytes:
                raise RuntimeError(f"ANNUAL_REPORT_BYTE_COUNT_MISMATCH:{len(raw)}:{expected_bytes}")
            pages = extract_pdf_pages(raw)
            if not pages or sum(bool(text.strip()) for text in pages) < 3:
                raise RuntimeError("ANNUAL_REPORT_TEXT_UNAVAILABLE")
            artifact = _artifact(
                "annual_report_pdf", selected["provider"], raw, url=selected["url"],
                effective_date=date(selected["to_year"], 3, 31), row_count=len(pages),
                metadata={
                    "symbol": member["symbol"], "isin": member["isin"],
                    "company_name": selected["company_name"], "from_year": selected["from_year"],
                    "to_year": selected["to_year"], "published_at": selected["published_at"].isoformat(),
                    "identity_evidence": selected["identity_evidence"], "page_count": len(pages),
                    "expected_byte_count": expected_bytes,
                },
            )
            artifact["published_at"] = selected["published_at"]
            return {
                "state": "PRESENT", "provider": selected["provider"], "document_kind": "ANNUAL_REPORT",
                "fiscal_year": f"{selected['from_year']}-{selected['to_year']}",
                "published_at": selected["published_at"], "source_url": selected["url"],
                "source_artifact_id": artifact["artifact_id"], "page_count": len(pages),
                "identity_evidence": selected["identity_evidence"],
                "evidence_extractor_version": EVIDENCE_EXTRACTOR_VERSION,
                "evidence": extract_topic_evidence(pages),
            }, artifact, None
        except Exception as exc:
            error = f"{type(exc).__name__}:{exc}"
            artifact = _artifact(
                "annual_report_pdf", selected["provider"], raw or error.encode(), url=selected["url"],
                effective_date=as_of_date, row_count=0, status="FAILED",
                metadata={
                    "symbol": member["symbol"], "isin": member["isin"], "error": error,
                    "expected_byte_count": selected.get("expected_byte_count"),
                    "observed_byte_count": len(raw) if raw is not None else None,
                },
            )
            return None, artifact, error

    def _nse_candidates(self, symbol: str, as_of_date: date) -> tuple[list[dict], dict, str | None]:
        page = f"{self.NSE_PAGE}?symbol={symbol}&tabIndex=equity"
        try:
            if not self._nse_primed:
                response = self.exchange._get(page, referer=self.exchange.NSE_HOME)
                self.exchange._validate(response, expected=("text/html",), source="NSE_ANNUAL_REPORT_PAGE")
                self._nse_primed = True
            response = self.exchange._get_with_params(
                self.NSE_API, {"index": "equities", "symbol": symbol}, referer=page,
            )
            raw = self.exchange._validate(response, expected=("json",), source=f"NSE_ANNUAL_REPORTS:{symbol}")
            payload = response.json()
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise RuntimeError("NSE_ANNUAL_REPORTS_SCHEMA_CHANGED")
            artifact = _artifact(
                "nse_annual_report_metadata", "NSE", raw, url=response.url,
                effective_date=as_of_date, row_count=len(rows), metadata={"symbol": symbol},
            )
            candidates = []
            for row in rows:
                published = parse_exchange_datetime(row.get("broadcast_dttm"))
                url = str(row.get("fileName") or "")
                if not published or published.date() > as_of_date or not url.lower().endswith(".pdf"):
                    continue
                if symbol.upper() not in url.upper():
                    continue
                candidates.append({
                    "provider": "NSE", "company_name": str(row.get("companyName") or ""),
                    "from_year": int(row["fromYr"]), "to_year": int(row["toYr"]),
                    "published_at": published, "url": url, "referer": page,
                    "identity_evidence": "OFFICIAL_NSE_SYMBOL_QUERY_AND_ARCHIVE_FILENAME",
                    "expected_byte_count": self._nse_expected_bytes(url),
                })
            return candidates, artifact, None
        except (ValueError, TypeError, RuntimeError, requests.RequestException) as exc:
            return [], _artifact(
                "nse_annual_report_metadata", "NSE", str(exc).encode(), url=self.NSE_API,
                effective_date=as_of_date, row_count=0, status="FAILED",
                metadata={"symbol": symbol, "error": str(exc)},
            ), str(exc)

    def _bse_candidates(self, bse_code: str, as_of_date: date) -> tuple[list[dict], list[dict], str | None]:
        artifacts: list[dict] = []
        candidates: list[dict] = []
        start = as_of_date - timedelta(days=550)
        try:
            for page_number in range(1, 11):
                params = {
                    "pageno": page_number, "strCat": "-1", "strPrevDate": start.isoformat(),
                    "strScrip": bse_code, "strSearch": "P", "strToDate": as_of_date.isoformat(),
                    "strType": "C", "subcategory": "-1",
                }
                response = self.exchange._get_with_params(self.BSE_API, params, referer=self.BSE_PAGE)
                raw = self.exchange._validate(response, expected=("json",), source=f"BSE_ANNOUNCEMENTS:{bse_code}:{page_number}")
                payload = response.json()
                rows = payload.get("Table") if isinstance(payload, dict) else None
                if not isinstance(rows, list):
                    raise RuntimeError("BSE_ANNOUNCEMENTS_SCHEMA_CHANGED")
                artifacts.append(_artifact(
                    "bse_annual_report_metadata", "BSE", raw, url=response.url,
                    effective_date=as_of_date, row_count=len(rows),
                    metadata={"bse_code": bse_code, "page": page_number},
                ))
                for row in rows:
                    text = " ".join(str(row.get(key) or "") for key in ("NEWSSUB", "HEADLINE", "SUBCATNAME"))
                    if "annual report" not in text.lower() or str(row.get("SCRIP_CD")) != bse_code:
                        continue
                    published = datetime.fromisoformat(str(row.get("News_submission_dt") or row.get("DT_TM")))
                    name = str(row.get("ATTACHMENTNAME") or "")
                    if published.date() > as_of_date or not name.lower().endswith(".pdf"):
                        continue
                    year_match = re.search(r"20(\d{2})\s*[-–]\s*(\d{2,4})", text)
                    to_year = int("20" + year_match.group(2)[-2:]) if year_match else published.year
                    candidates.append({
                        "provider": "BSE", "company_name": str(row.get("SLONGNAME") or ""),
                        "from_year": to_year - 1, "to_year": to_year, "published_at": published,
                        "url": self.BSE_ATTACHMENT.format(name=name), "referer": self.BSE_PAGE,
                        "identity_evidence": "OFFICIAL_BSE_SCRIP_CODE_AND_ANNOUNCEMENT_ATTACHMENT",
                        "expected_byte_count": int(row["Fld_Attachsize"]) if row.get("Fld_Attachsize") else None,
                    })
                if not rows or candidates:
                    break
            return candidates, artifacts, None
        except (ValueError, TypeError, RuntimeError, requests.RequestException) as exc:
            artifacts.append(_artifact(
                "bse_annual_report_metadata", "BSE", str(exc).encode(), url=self.BSE_API,
                effective_date=as_of_date, row_count=0, status="FAILED",
                metadata={"bse_code": bse_code, "error": str(exc)},
            ))
            return [], artifacts, str(exc)

    @staticmethod
    def _nse_expected_bytes(url: str) -> int | None:
        match = re.search(r"_A_(\d+)_\d{14}\.pdf$", url, flags=re.IGNORECASE)
        return int(match.group(1)) if match else None

    @staticmethod
    def _unavailable(member: dict, errors: list[str]) -> dict:
        return {
            "state": "SOURCE_UNAVAILABLE", "provider": None, "document_kind": "ANNUAL_REPORT",
            "fiscal_year": None, "published_at": None, "source_url": None,
            "source_artifact_id": None, "page_count": 0, "identity_evidence": None,
            "evidence_extractor_version": EVIDENCE_EXTRACTOR_VERSION,
            "errors": errors,
            "evidence": [
                {"topic": topic, "state": "NOT_DISCLOSED", "page": None, "excerpt": None,
                 "confidence": "NONE", "review_status": "NOT_REVIEWABLE"}
                for topic in TOPIC_PATTERNS
            ],
        }


def extract_pdf_pages(raw: bytes) -> list[str]:
    try:
        with _PDFIUM_LOCK:
            document = pdfium.PdfDocument(raw)
            pages: list[str] = []
            try:
                for page in document:
                    text_page = page.get_textpage()
                    try:
                        pages.append(text_page.get_text_range() or "")
                    finally:
                        text_page.close()
                        page.close()
            finally:
                document.close()
            return pages
    except Exception:
        # PDFium is the fast path. Certain legacy producer variants are more
        # tolerant under pdfminer/pdfplumber, so preserve a deterministic
        # page-attributed fallback instead of declaring the filing absent.
        with pdfplumber.open(io.BytesIO(raw)) as pdf:
            return [page.extract_text() or "" for page in pdf.pages]


def extract_topic_evidence(pages: list[str]) -> list[dict]:
    observations: list[dict] = []
    for topic, patterns in TOPIC_PATTERNS.items():
        matches: list[dict] = []
        for page_number, text in enumerate(pages, start=1):
            flattened = re.sub(r"\s+", " ", text).strip()
            lowered = flattened.lower()
            for pattern in patterns:
                offset = lowered.find(pattern)
                if offset < 0:
                    continue
                start = max(0, offset - 120)
                end = min(len(flattened), offset + len(pattern) + 240)
                matches.append({
                    "topic": topic, "state": "DISCLOSED_TEXT_MATCH", "page": page_number,
                    "excerpt": flattened[start:end], "matched_term": pattern,
                    "confidence": "LOW", "review_status": "HUMAN_REVIEW_REQUIRED",
                })
                break
            if len(matches) >= 3:
                break
        observations.extend(matches or [{
            "topic": topic, "state": "NOT_DISCLOSED", "page": None, "excerpt": None,
            "confidence": "NONE", "review_status": "NOT_FOUND_IN_EXTRACTED_TEXT",
        }])
    return observations
