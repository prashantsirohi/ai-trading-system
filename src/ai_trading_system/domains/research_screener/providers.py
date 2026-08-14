from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import requests

from ai_trading_system.platform.db.paths import get_domain_paths

from .filings import completeness, parse_exchange_date, parse_exchange_datetime, parse_xbrl_statement

PARSER_VERSION = "research-screener-exchange-v4"
SCHEMA_VERSION = "phase0-v1"


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _artifact(source_key: str, provider: str, raw: bytes, *, url: str, effective_date: date,
              row_count: int | None, status: str = "VALID", metadata: dict | None = None) -> dict:
    digest = _hash(raw)
    locator_digest = _hash(f"{url}|{digest}".encode())
    return {
        "artifact_id": f"artifact:{source_key}:{effective_date}:{locator_digest[:20]}",
        "source_key": source_key,
        "provider": provider,
        "source_url": url,
        "effective_date": effective_date,
        "retrieved_at": datetime.now(UTC),
        "content_hash": digest,
        "byte_count": len(raw),
        "row_count": row_count,
        "parser_version": PARSER_VERSION,
        "schema_version": SCHEMA_VERSION,
        "validation_status": status,
        "metadata": metadata or {},
        "_raw": raw,
    }


class OfficialExchangeClient:
    """Fixed-source, session-aware exchange acquisition with no provider fallback."""

    NSE_MII = "https://nsearchives.nseindia.com/content/cm/NSE_CM_security_{date}.csv.gz"
    NSE_EQUITY = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    NSE_HOME = "https://www.nseindia.com/"
    NSE_QUOTE_PAGE = "https://www.nseindia.com/get-quotes/equity?series=EQ&symbol={symbol}"
    NSE_QUOTE_METADATA = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getMetaData&symbol={symbol}"
    NSE_QUOTE_SYMBOL_DATA = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolData&marketType={market_type}&series={series}&symbol={symbol}"
    BSE_ACTIVE = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active"
    NSE_FINANCIAL_PAGE = "https://www.nseindia.com/companies-listing/corporate-filings-financial-results"
    NSE_FINANCIAL_RESULTS = "https://www.nseindia.com/api/corporates-financial-results"
    NSE_INTEGRATED_RESULTS = "https://www.nseindia.com/api/integrated-filing-results"
    BSE_FINANCIAL_RESULTS = "https://api.bseindia.com/BseIndiaAPI/api/Corp_FinanceResult_ng_new/w"
    NSE_CORPORATE_ACTIONS = "https://www.nseindia.com/api/corporates-corporateActions"
    BSE_CORPORATE_ACTIONS = "https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w"

    def __init__(self, *, timeout: float = 45.0, min_interval: float = 0.35, session: requests.Session | None = None):
        self.timeout = timeout
        self.min_interval = min_interval
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept": "application/json,text/csv,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._last_request = 0.0
        self._quote_session_primed = False
        self._financial_session_primed = False
        self._corporate_action_session_primed = False

    def _get(self, url: str, *, referer: str) -> requests.Response:
        return self._request(url, referer=referer)

    def _request(self, url: str, *, referer: str, params: dict | None = None) -> requests.Response:
        last_error: requests.RequestException | None = None
        response: requests.Response | None = None
        for attempt in range(3):
            wait = self.min_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            try:
                response = self.session.get(
                    url, params=params, timeout=self.timeout, headers={"Referer": referer},
                )
                self._last_request = time.monotonic()
            except requests.RequestException as exc:
                self._last_request = time.monotonic()
                last_error = exc
                if attempt == 2:
                    raise
            else:
                if response.status_code not in {429, 500, 502, 503, 504} or attempt == 2:
                    return response
            time.sleep(attempt + 1)
        if response is not None:
            return response
        assert last_error is not None
        raise last_error

    @staticmethod
    def _validate(response: requests.Response, *, expected: tuple[str, ...], source: str) -> bytes:
        if response.status_code != 200:
            raise RuntimeError(f"{source}: HTTP_{response.status_code}")
        content_type = (response.headers.get("content-type") or "").lower()
        if not any(token in content_type for token in expected):
            raise RuntimeError(f"{source}: INVALID_CONTENT_TYPE:{content_type}")
        raw = response.content
        html_expected = any(token == "text/html" for token in expected)
        if not raw or (not html_expected and raw.lstrip().lower().startswith(b"<!doctype html")):
            raise RuntimeError(f"{source}: EMPTY_OR_HTML_RESPONSE")
        return raw

    def acquire_identity(self, as_of_date: date) -> dict:
        mii_response = None
        effective_date = as_of_date
        mii_url = ""
        errors: list[str] = []
        for offset in range(0, 11):
            candidate = as_of_date - timedelta(days=offset)
            url = self.NSE_MII.format(date=candidate.strftime("%d%m%Y"))
            try:
                response = self._get(url, referer="https://www.nseindia.com/all-reports")
            except requests.RequestException as exc:
                errors.append(f"{candidate}:{type(exc).__name__}")
                continue
            if response.status_code == 404:
                continue
            mii_response, effective_date, mii_url = response, candidate, url
            break
        if mii_response is None:
            raise RuntimeError("CM_MII_SECURITY_COMBINED unavailable: " + ";".join(errors[-3:]))
        mii_raw = self._validate(mii_response, expected=("gzip", "octet-stream"), source="CM_MII_SECURITY_COMBINED")
        try:
            mii_text = gzip.decompress(mii_raw).decode("utf-8-sig", errors="replace")
        except (OSError, EOFError) as exc:
            raise RuntimeError("CM_MII_SECURITY_COMBINED: INVALID_GZIP") from exc
        mii_rows = self._parse_csv(mii_text)
        if not mii_rows:
            raise RuntimeError("CM_MII_SECURITY_COMBINED: EMPTY_RECORDS")

        eq_url = self.NSE_EQUITY
        eq_response = self._get(eq_url, referer="https://www.nseindia.com/all-reports")
        eq_raw = self._validate(eq_response, expected=("csv", "text/plain", "octet-stream"), source="NSE_EQUITY_MASTER")
        eq_rows = self._parse_csv(eq_raw.decode("utf-8-sig", errors="replace"))
        if not eq_rows or not self._has_columns(eq_rows, ("SYMBOL", "ISIN NUMBER")):
            raise RuntimeError("NSE_EQUITY_MASTER: SCHEMA_CHANGED")

        bse_url = self.BSE_ACTIVE
        bse_response = self._get(bse_url, referer="https://www.bseindia.com/corporates/List_Scrips.html")
        bse_raw = self._validate(bse_response, expected=("json",), source="BSE_ACTIVE_EQUITY_MASTER")
        try:
            bse_rows = bse_response.json()
        except ValueError as exc:
            raise RuntimeError("BSE_ACTIVE_EQUITY_MASTER: INVALID_JSON") from exc
        if not isinstance(bse_rows, list) or not bse_rows or not self._has_columns(bse_rows, ("SCRIP_CD", "ISIN_NUMBER", "scrip_id")):
            raise RuntimeError("BSE_ACTIVE_EQUITY_MASTER: SCHEMA_CHANGED")

        return {
            "effective_date": effective_date,
            "mii_rows": mii_rows,
            "nse_rows": eq_rows,
            "bse_rows": bse_rows,
            "artifacts": [
                _artifact("combined_security_master", "NSE", mii_raw, url=mii_url, effective_date=effective_date, row_count=len(mii_rows)),
                _artifact("nse_equity_master", "NSE", eq_raw, url=eq_url, effective_date=effective_date, row_count=len(eq_rows)),
                _artifact("bse_active_equity_master", "BSE", bse_raw, url=bse_url, effective_date=effective_date, row_count=len(bse_rows)),
            ],
        }

    @staticmethod
    def _parse_csv(text: str) -> list[dict[str, str]]:
        sample = text[:8192]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",|;")
        except csv.Error:
            dialect = csv.excel
        return [{str(k or "").strip(): str(v or "").strip() for k, v in row.items()} for row in csv.DictReader(io.StringIO(text), dialect=dialect)]

    @staticmethod
    def _has_columns(rows: list[dict], required: tuple[str, ...]) -> bool:
        return bool(rows and all(column in rows[0] for column in required))

    @staticmethod
    def resolve_fixture(row: dict, identity: dict) -> dict:
        symbol = row["symbol"].upper()
        expected_isin = row["isin"].upper()
        nse = [x for x in identity["nse_rows"] if str(x.get("SYMBOL", "")).upper() == symbol]
        bse = [x for x in identity["bse_rows"] if str(x.get("scrip_id", "")).upper() == symbol]
        by_fixture_isin = [x for x in identity["bse_rows"] if str(x.get("ISIN_NUMBER", "")).upper() == expected_isin]
        observed_isins = {str(x.get("ISIN NUMBER", "")).upper() for x in nse}
        observed_isins.update(str(x.get("ISIN_NUMBER", "")).upper() for x in bse)
        observed_isins.discard("")
        listings: list[dict] = []
        for item in nse:
            listing_isin = str(item.get("ISIN NUMBER") or expected_isin).upper()
            listings.append({
                "listing_id": f"listing:NSE:{symbol}:{listing_isin}", "exchange": "NSE", "symbol": symbol,
                "series": item.get("SERIES"), "board": "MAIN" if item.get("SERIES") == "EQ" else "BOARD_UNKNOWN",
                "active_flag": True, "listing_date": None,
            })
        for item in bse:
            listings.append({
                "listing_id": f"listing:BSE:{item.get('SCRIP_CD')}:{item.get('ISIN_NUMBER')}", "exchange": "BSE",
                "symbol": item.get("scrip_id"), "bse_code": item.get("SCRIP_CD"),
                "exchange_security_id": item.get("SCRIP_CD"), "series": item.get("GROUP"),
                "board": "MAIN", "active_flag": item.get("Status") == "Active",
            })
        mii_matches = [
            item for item in identity["mii_rows"]
            if symbol in {str(value).strip().upper() for value in item.values()}
            or expected_isin in {str(value).strip().upper() for value in item.values()}
        ]
        status = "RESOLVED" if observed_isins == {expected_isin} and listings and mii_matches else "IDENTITY_CONFLICT"
        if not listings:
            status = "UNRESOLVED"
        return {
            "status": status,
            "observed_isins": sorted(observed_isins),
            "fixture_isin_matches_other_bse": [x.get("scrip_id") for x in by_fixture_isin if str(x.get("scrip_id", "")).upper() != symbol],
            "mii_match_count": len(mii_matches),
            "listings": listings,
            "face_value": next((float(x["FACE_VALUE"]) for x in bse if x.get("FACE_VALUE")), None),
        }

    def nse_market_cap(self, symbol: str, as_of_date: date, *, expected_isin: str) -> tuple[dict | None, dict]:
        page_url = self.NSE_QUOTE_PAGE.format(symbol=symbol)
        metadata_url = self.NSE_QUOTE_METADATA.format(symbol=symbol)
        url = self.NSE_QUOTE_SYMBOL_DATA.format(market_type="N", series="EQ", symbol=symbol)
        try:
            self._prime_nse_quote_session(page_url, symbol=symbol)
            metadata_response = self._get(metadata_url, referer=page_url)
            if metadata_response.status_code in {401, 403}:
                self._prime_nse_quote_session(page_url, symbol=symbol, force=True)
                metadata_response = self._get(metadata_url, referer=page_url)
            metadata_raw = self._validate(metadata_response, expected=("json",), source=f"NSE_QUOTE_METADATA:{symbol}")
            metadata = metadata_response.json()
            if str(metadata.get("symbol", "")).upper() != symbol.upper():
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: SYMBOL_MISMATCH")
            metadata_isin = str(metadata.get("isin", "")).upper()
            active_series = metadata.get("activeSeries") or []
            if not isinstance(active_series, list) or not active_series:
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: ACTIVE_SERIES_UNAVAILABLE")
            if "EQ" in active_series:
                selected_series = "EQ"
            elif len(active_series) == 1:
                selected_series = str(active_series[0])
            else:
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: AMBIGUOUS_ACTIVE_SERIES:{active_series}")
            market_type = str(metadata.get("marketType") or "N")
            url = self.NSE_QUOTE_SYMBOL_DATA.format(market_type=market_type, series=selected_series, symbol=symbol)
            response = self._get(url, referer=page_url)
            symbol_raw = self._validate(response, expected=("json",), source=f"NSE_MARKET_CAP:{symbol}")
            data = response.json()
            equity_rows = data.get("equityResponse") or []
            if not isinstance(equity_rows, list) or len(equity_rows) != 1:
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: INVALID_EQUITY_RESPONSE")
            quote = equity_rows[0]
            quote_metadata = quote.get("metaData") or {}
            if str(quote_metadata.get("symbol", "")).upper() != symbol.upper():
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: QUOTE_SYMBOL_MISMATCH")
            if str(quote_metadata.get("isinCode", "")).upper() != expected_isin.upper():
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: QUOTE_ISIN_MISMATCH")
            if quote_metadata.get("series") != selected_series:
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: QUOTE_SERIES_MISMATCH")
            trade = quote.get("tradeInfo") or {}
            full_rupees = self._float(trade.get("totalMarketCap"))
            if full_rupees is None or full_rupees <= 0:
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: MISSING_USABLE_FULL_MARKET_CAP")
            quote_timestamp = datetime.strptime(quote["lastUpdateTime"], "%d-%b-%Y %H:%M:%S")
            quote_date = quote_timestamp.date()
            if quote_date > as_of_date:
                raise RuntimeError(f"NSE_MARKET_CAP:{symbol}: FUTURE_DATED_QUOTE:{quote_date}")
            free_float_rupees = self._float(trade.get("ffmc"))
            raw = json.dumps({
                "metadata_response_raw": metadata_raw.decode("utf-8"),
                "symbol_response_raw": symbol_raw.decode("utf-8"),
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")
            artifact = _artifact(
                "nse_market_cap", "NSE", raw, url=url, effective_date=quote_date, row_count=1,
                metadata={
                    "quote_timestamp": quote["lastUpdateTime"], "raw_currency": "INR",
                    "raw_market_cap_unit": "RUPEES", "normalized_market_cap_unit": "INR_CRORE",
                    "metadata_url": metadata_url, "selected_series": selected_series,
                    "metadata_isin": metadata_isin,
                    "metadata_isin_matches_quote": metadata_isin == expected_isin.upper(),
                },
            )
            return {
                "full_market_cap_cr": full_rupees / 10_000_000,
                "free_float_market_cap_cr": free_float_rupees / 10_000_000 if free_float_rupees is not None else None,
                "shares_outstanding": self._float(trade.get("issuedSize")),
                "as_of_date": quote_date,
                "artifact_id": artifact["artifact_id"],
            }, artifact
        except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
            raw = str(exc).encode()
            artifact = _artifact("nse_market_cap", "NSE", raw, url=url, effective_date=as_of_date, row_count=0, status="FAILED", metadata={"error": str(exc)})
            return None, artifact

    def _prime_nse_quote_session(self, page_url: str, *, symbol: str,
                                 force: bool = False) -> None:
        if self._quote_session_primed and not force:
            return
        page_response = self._get(page_url, referer=self.NSE_HOME)
        self._validate(page_response, expected=("text/html",), source=f"NSE_QUOTE_PAGE:{symbol}")
        self._quote_session_primed = True

    def nse_fundamental_snapshot(self, symbol: str, expected_isin: str, as_of_date: date,
                                 *, company_type: str,
                                 identifier_history: list[dict] | None = None) -> tuple[dict, list[dict]]:
        artifacts: list[dict] = []
        candidates: list[dict] = []
        try:
            if not self._financial_session_primed:
                page = self._get(self.NSE_FINANCIAL_PAGE, referer=self.NSE_HOME)
                self._validate(page, expected=("text/html",), source=f"NSE_FINANCIAL_PAGE:{symbol}")
                self._financial_session_primed = True
            for period_type, period in (("annual", "Annual"), ("quarterly", "Quarterly")):
                response = self._get_with_params(
                    self.NSE_FINANCIAL_RESULTS, {"index": "equities", "symbol": symbol, "period": period},
                    referer=self.NSE_FINANCIAL_PAGE,
                )
                raw = self._validate(response, expected=("json",), source=f"NSE_FINANCIAL_RESULTS:{symbol}:{period}")
                rows = response.json()
                if not isinstance(rows, list):
                    raise RuntimeError(f"NSE_FINANCIAL_RESULTS:{symbol}:{period}:SCHEMA_CHANGED")
                artifacts.append(_artifact(
                    f"nse_financial_results_{period_type}", "NSE", raw, url=response.url,
                    effective_date=as_of_date, row_count=len(rows),
                ))
                for row in rows:
                    if str(row.get("symbol", "")).upper() != symbol.upper():
                        continue
                    published = parse_exchange_datetime(row.get("broadCastDate") or row.get("filingDate"))
                    period_end = parse_exchange_date(row.get("toDate"))
                    if not published or not period_end or published.date() > as_of_date:
                        continue
                    valid_isins = self._valid_filing_isins(expected_isin, period_end, identifier_history)
                    if str(row.get("isin", "")).upper() not in valid_isins:
                        continue
                    url = str(row.get("xbrl") or "")
                    if not url.lower().endswith(".xml"):
                        continue
                    candidates.append({
                        "period_type": period_type, "period_end": period_end,
                        "published_at": published, "scope": self._scope(row.get("consolidated")),
                        "source_document_url": url, "source_provider": "NSE_LEGACY_XBRL",
                        "identity_evidence": "METADATA_ISIN",
                    })

            integrated = self._get_with_params(
                self.NSE_INTEGRATED_RESULTS, {"index": "equities", "symbol": symbol, "page": 1, "size": 100},
                referer="https://www.nseindia.com/companies-listing/corporate-integrated-filing",
            )
            integrated_raw = self._validate(integrated, expected=("json",), source=f"NSE_INTEGRATED_RESULTS:{symbol}")
            payload = integrated.json()
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise RuntimeError(f"NSE_INTEGRATED_RESULTS:{symbol}:SCHEMA_CHANGED")
            artifacts.append(_artifact(
                "nse_integrated_financial_results", "NSE", integrated_raw, url=integrated.url,
                effective_date=as_of_date, row_count=len(rows),
            ))
            for row in rows:
                if str(row.get("symbol", "")).upper() != symbol.upper() or row.get("type") != "Integrated Filing- Financials":
                    continue
                published = parse_exchange_datetime(row.get("broadcast_Date") or row.get("creation_Date"))
                period_end = parse_exchange_date(row.get("qe_Date"))
                url = str(row.get("xbrl") or "")
                if not published or not period_end or published.date() > as_of_date or not url.lower().endswith(".xml"):
                    continue
                base = {
                    "period_end": period_end, "published_at": published,
                    "scope": self._scope(row.get("consolidated")), "source_document_url": url,
                    "source_provider": "NSE_INTEGRATED_XBRL", "identity_evidence": "XBRL_ISIN",
                }
                candidates.append(base | {"period_type": "quarterly"})
                if period_end.month == 3 and str(row.get("audited", "")).lower() == "audited":
                    candidates.append(base | {"period_type": "annual"})
        except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
            artifacts.append(_artifact(
                "nse_financial_results", "NSE", str(exc).encode(), url=self.NSE_FINANCIAL_RESULTS,
                effective_date=as_of_date, row_count=0, status="FAILED", metadata={"error": str(exc)},
            ))
        return self._normalize_filing_candidates(
            candidates, artifacts, as_of_date, company_type=company_type,
            expected_isin=expected_isin, expected_exchange_id=symbol,
            identifier_history=identifier_history,
        )

    def bse_fundamental_snapshot(self, bse_code: str, expected_isin: str, as_of_date: date,
                                 *, company_type: str,
                                 identifier_history: list[dict] | None = None) -> tuple[dict, list[dict]]:
        artifacts: list[dict] = []
        candidates: list[dict] = []
        try:
            response = self._get_with_params(
                self.BSE_FINANCIAL_RESULTS,
                {"SCRIP_CD": bse_code, "FlagDur": "7", "HFQ": "", "ISUBGROUP_CODE": "", "segment": "C"},
                referer=f"https://www.bseindia.com/corporates/comp_results.aspx?Code={bse_code}",
            )
            raw = self._validate(response, expected=("json",), source=f"BSE_FINANCIAL_RESULTS:{bse_code}")
            payload = response.json()
            rows = payload.get("Table") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise RuntimeError(f"BSE_FINANCIAL_RESULTS:{bse_code}:SCHEMA_CHANGED")
            artifacts.append(_artifact(
                "bse_financial_results", "BSE", raw, url=response.url, effective_date=as_of_date,
                row_count=len(rows), metadata={"expected_isin": expected_isin},
            ))
            for row in rows:
                if str(row.get("Scrip_cd")) != str(bse_code):
                    continue
                published = parse_exchange_datetime(row.get("Fld_CreateDate") or row.get("DT_TM"))
                code = str(row.get("quarter_code") or "").upper()
                period_end = self._bse_period_end(code)
                xml_name = str(row.get("XMLName") or "")
                if (not published or published.date() > as_of_date or not period_end
                        or not xml_name.lower().endswith((".xml", ".html"))):
                    continue
                period_type = "annual" if code.startswith("MC") else "quarterly" if code[:2] in {"JQ", "SQ", "DQ", "MQ"} else None
                if not period_type:
                    continue
                candidates.append({
                    "period_type": period_type, "period_end": period_end, "published_at": published,
                    "scope": "standalone", "source_document_url": f"https://www.bseindia.com/XBRLFILES/{xml_name}",
                    "source_provider": "BSE_IXBRL" if xml_name.lower().endswith(".html") else "BSE_XBRL",
                    "identity_evidence": "XBRL_ISIN_AND_SCRIP_CODE",
                    "document_revision_id": self._bse_document_revision_id(xml_name),
                })
        except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
            artifacts.append(_artifact(
                "bse_financial_results", "BSE", str(exc).encode(), url=self.BSE_FINANCIAL_RESULTS,
                effective_date=as_of_date, row_count=0, status="FAILED", metadata={"error": str(exc)},
            ))
        return self._normalize_filing_candidates(
            candidates, artifacts, as_of_date, company_type=company_type,
            expected_isin=expected_isin, expected_exchange_id=str(bse_code),
            identifier_history=identifier_history,
        )

    def nse_corporate_actions(self, symbol: str, expected_isin: str, start_date: date,
                              as_of_date: date) -> tuple[list[dict], dict]:
        url = self.NSE_CORPORATE_ACTIONS
        try:
            page_url = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
            if not self._corporate_action_session_primed:
                page = self._get(page_url, referer=self.NSE_HOME)
                self._validate(page, expected=("text/html",), source=f"NSE_CORPORATE_ACTION_PAGE:{symbol}")
                self._corporate_action_session_primed = True
            response = self._get_with_params(
                url, {"index": "equities", "symbol": symbol}, referer=page_url,
            )
            raw = self._validate(response, expected=("json",), source=f"NSE_CORPORATE_ACTIONS:{symbol}")
            rows = response.json()
            if not isinstance(rows, list):
                raise RuntimeError(f"NSE_CORPORATE_ACTIONS:{symbol}:SCHEMA_CHANGED")
            actions = []
            for row in rows:
                if str(row.get("symbol", "")).upper() != symbol.upper():
                    continue
                ex_date = parse_exchange_date(row.get("exDate"))
                if not ex_date or not start_date <= ex_date <= as_of_date:
                    continue
                row_isin = str(row.get("isin") or "").upper()
                # Old ISINs are legitimate on identifier-changing actions; retain both identities.
                actions.append(self._normalize_action(
                    row, ex_date=ex_date, subject=str(row.get("subject") or ""),
                    observed_isin=row_isin, expected_isin=expected_isin,
                ))
            artifact = _artifact(
                "nse_corporate_actions", "NSE", raw, url=response.url,
                effective_date=as_of_date, row_count=len(rows),
                metadata={"symbol": symbol, "expected_isin": expected_isin,
                          "window_start": start_date.isoformat()},
            )
            return actions, artifact
        except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
            return [], _artifact(
                "nse_corporate_actions", "NSE", str(exc).encode(), url=url,
                effective_date=as_of_date, row_count=0, status="FAILED",
                metadata={"error": str(exc), "symbol": symbol, "expected_isin": expected_isin},
            )

    def bse_corporate_actions(self, bse_code: str, symbol: str, expected_isin: str,
                              start_date: date, as_of_date: date) -> tuple[list[dict], dict]:
        url = self.BSE_CORPORATE_ACTIONS
        try:
            response = self._get_with_params(
                url, {"scripcode": bse_code},
                referer=f"https://www.bseindia.com/corporates/corporate_act?scrip_cd={bse_code}",
            )
            raw = self._validate(response, expected=("json",), source=f"BSE_CORPORATE_ACTIONS:{bse_code}")
            payload = response.json()
            rows = payload.get("Table2") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise RuntimeError(f"BSE_CORPORATE_ACTIONS:{bse_code}:SCHEMA_CHANGED")
            actions = []
            for row in rows:
                if str(row.get("scrip_code")) != str(bse_code):
                    continue
                if str(row.get("short_name") or "").upper() != symbol.upper():
                    continue
                ex_date = self._parse_bse_action_date(row.get("Ex_date"))
                if not ex_date or not start_date <= ex_date <= as_of_date:
                    continue
                actions.append(self._normalize_action(
                    row, ex_date=ex_date,
                    subject=" ".join(str(row.get(key) or "") for key in ("purpose", "Details")),
                    observed_isin=expected_isin, expected_isin=expected_isin,
                ))
            artifact = _artifact(
                "bse_corporate_actions", "BSE", raw, url=response.url,
                effective_date=as_of_date, row_count=len(rows),
                metadata={"bse_code": bse_code, "symbol": symbol, "expected_isin": expected_isin,
                          "window_start": start_date.isoformat()},
            )
            return actions, artifact
        except (requests.RequestException, RuntimeError, ValueError, TypeError) as exc:
            return [], _artifact(
                "bse_corporate_actions", "BSE", str(exc).encode(), url=url,
                effective_date=as_of_date, row_count=0, status="FAILED",
                metadata={"error": str(exc), "bse_code": bse_code, "expected_isin": expected_isin},
            )

    @staticmethod
    def _normalize_action(raw_row: dict, *, ex_date: date, subject: str,
                          observed_isin: str, expected_isin: str) -> dict:
        raw_payload = json.dumps(raw_row, sort_keys=True, separators=(",", ":"), default=str)
        return {
            "action_type": OfficialExchangeClient._classify_action(subject),
            "ex_date": ex_date, "raw_subject": subject.strip(),
            "observed_isin": observed_isin or None, "current_expected_isin": expected_isin,
            "source_row_hash": hashlib.sha256(raw_payload.encode()).hexdigest(),
            "raw_payload_json": raw_payload,
        }

    @staticmethod
    def _classify_action(subject: str) -> str:
        text = re.sub(r"\s+", " ", subject.strip().lower())
        checks = (
            ("demerger", ("demerger", "spin off", "spin-off")),
            ("merger", ("merger", "amalgamation", "scheme of arrangement")),
            ("consolidation", ("consolidation", "reverse split")),
            ("split", ("split", "sub-division", "sub division", "face value from")),
            ("bonus", ("bonus",)), ("rights", ("rights", "right issue")),
            ("symbol_name_change", ("symbol change", "name change", "change in name")),
            ("trading_series", ("series change", "trading series")),
            ("dividend", ("dividend",)),
        )
        return next((kind for kind, words in checks if any(word in text for word in words)), "other")

    @staticmethod
    def _parse_bse_action_date(value: Any) -> date | None:
        text = str(value or "").strip()
        for fmt in ("%d %b %Y", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _bse_document_revision_id(xml_name: str) -> str:
        name = Path(xml_name).name
        return re.sub(r"_IFIndAs\.html$|\.xml$", "", name, flags=re.IGNORECASE)

    def _normalize_filing_candidates(self, candidates: list[dict], artifacts: list[dict], as_of_date: date,
                                     *, company_type: str, expected_isin: str,
                                     expected_exchange_id: str,
                                     identifier_history: list[dict] | None = None) -> tuple[dict, list[dict]]:
        deduped: dict[tuple, dict] = {}
        for row in candidates:
            key = (row["scope"], row["period_type"], row["period_end"])
            current = deduped.get(key)
            same_bse_revision = bool(
                current and row.get("document_revision_id")
                and row.get("document_revision_id") == current.get("document_revision_id")
            )
            if current is None:
                deduped[key] = row
            elif (same_bse_revision and row["source_provider"] == "BSE_IXBRL"
                  and current["source_provider"] != "BSE_IXBRL"):
                # BSE publishes the same integrated revision through a broken
                # legacy XML locator and a working inline-XBRL locator.
                deduped[key] = row
            elif not same_bse_revision and row["published_at"] > current["published_at"]:
                deduped[key] = row
        candidates = list(deduped.values())
        scopes = sorted({row["scope"] for row in candidates if row["scope"] != "SCOPE_UNRESOLVED"})
        if not scopes:
            return self._empty_fundamentals("no filing-grade XBRL rows available on the fixed exchange source"), artifacts
        coverage = {
            scope: (
                len({r["period_end"] for r in candidates if r["scope"] == scope and r["period_type"] == "annual"}),
                len({r["period_end"] for r in candidates if r["scope"] == scope and r["period_type"] == "quarterly"}),
            ) for scope in scopes
        }
        consolidated = coverage.get("consolidated")
        if consolidated and consolidated[0] >= 5 and consolidated[1] >= 9:
            selected_scope = "consolidated"
            scope_reason = "consolidated_filing_history_usable"
        else:
            selected_scope = max(scopes, key=lambda scope: (min(coverage[scope][0] / 6, coverage[scope][1] / 12), scope == "consolidated"))
            scope_reason = "best_complete_scope_no_splicing"

        statements: list[dict] = []
        selected_candidates = [row for row in candidates if row["scope"] == selected_scope]
        latest_disclosed = {
            period_type: max(
                (row["period_end"] for row in selected_candidates if row["period_type"] == period_type),
                default=None,
            )
            for period_type in ("annual", "quarterly")
        }
        selected_candidates.sort(key=lambda row: (row["period_type"], row["period_end"]), reverse=True)
        counters = {"annual": 0, "quarterly": 0}
        for row in selected_candidates:
            limit = 6 if row["period_type"] == "annual" else 12
            if counters[row["period_type"]] >= limit:
                continue
            try:
                referer = (
                    "https://www.bseindia.com/corporates/comp_results.aspx"
                    if row["source_provider"].startswith("BSE_") else self.NSE_FINANCIAL_PAGE
                )
                response = self._get(row["source_document_url"], referer=referer)
                expected_content = (
                    ("text/html",) if row["source_document_url"].lower().endswith(".html")
                    else ("xml", "octet-stream", "text/plain")
                )
                raw = self._validate(response, expected=expected_content, source=row["source_document_url"])
                parsed = parse_xbrl_statement(raw, period_end=row["period_end"], period_type=row["period_type"])
                document_identity = parsed["document_identity"]
                document_isin = document_identity.get("ISIN")
                if not document_isin and row.get("identity_evidence") != "METADATA_ISIN":
                    raise RuntimeError(f"{row['source_document_url']}: XBRL_ISIN_MISSING")
                valid_isins = self._valid_filing_isins(
                    expected_isin, row["period_end"], identifier_history,
                )
                if document_isin and document_isin not in valid_isins:
                    raise RuntimeError(
                        f"{row['source_document_url']}: XBRL_ISIN_MISMATCH:{document_isin}"
                    )
                document_exchange_id = (
                    document_identity.get("ScripCode") if row["source_provider"].startswith("BSE_")
                    else document_identity.get("Symbol")
                )
                if document_exchange_id and document_exchange_id.upper() != expected_exchange_id.upper():
                    raise RuntimeError(
                        f"{row['source_document_url']}: XBRL_EXCHANGE_ID_MISMATCH:{document_exchange_id}"
                    )
                artifact = _artifact(
                    "filing_xbrl", row["source_provider"].split("_")[0], raw, url=row["source_document_url"],
                    effective_date=row["period_end"], row_count=len(parsed["raw_values"]),
                    metadata={"period_type": row["period_type"], "scope": selected_scope,
                              "published_at": row["published_at"].isoformat()},
                )
                artifact["published_at"] = row["published_at"]
                artifacts.append(artifact)
                statements.append(row | parsed | {"source_artifact_id": artifact["artifact_id"]})
                counters[row["period_type"]] += 1
            except (requests.RequestException, RuntimeError, ValueError, TypeError, ET.ParseError) as exc:
                artifacts.append(_artifact(
                    "filing_xbrl", row["source_provider"].split("_")[0], str(exc).encode(),
                    url=row["source_document_url"], effective_date=row["period_end"], row_count=0,
                    status="FAILED", metadata={"error": str(exc), "period_type": row["period_type"], "scope": selected_scope},
                ))

        annual = [row for row in statements if row["period_type"] == "annual"]
        quarterly = [row for row in statements if row["period_type"] == "quarterly"]
        target_periods = {
            "annual": self._target_period_ends(latest_disclosed["annual"], "annual", 6),
            "quarterly": self._target_period_ends(latest_disclosed["quarterly"], "quarterly", 12),
        }
        annual_completeness = completeness(
            annual, company_type=company_type, period_type="annual", periods=6,
            target_period_ends=target_periods["annual"],
        )
        quarterly_completeness = completeness(
            quarterly, company_type=company_type, period_type="quarterly", periods=12,
            target_period_ends=target_periods["quarterly"],
        )
        latest_parsed = {
            "annual": max((row["period_end"] for row in annual), default=None),
            "quarterly": max((row["period_end"] for row in quarterly), default=None),
        }
        latest_periods_parsed = all(
            latest_disclosed[period_type] is not None
            and latest_parsed[period_type] == latest_disclosed[period_type]
            for period_type in ("annual", "quarterly")
        )
        parsed_period_sets = {
            "annual": {row["period_end"] for row in annual},
            "quarterly": {row["period_end"] for row in quarterly},
        }
        missing_target_periods = {
            period_type: [period for period in target_periods[period_type]
                          if period not in parsed_period_sets[period_type]]
            for period_type in ("annual", "quarterly")
        }
        usable = min(annual_completeness, quarterly_completeness) >= 0.70 and latest_periods_parsed
        if not latest_periods_parsed:
            reason = (
                "latest disclosed filing XBRL could not be validated: "
                f"disclosed={latest_disclosed}, parsed={latest_parsed}"
            )
        elif not usable:
            reason = (
                "official filing XBRL completeness below 70%: "
                f"annual={annual_completeness}, quarterly={quarterly_completeness}"
            )
        else:
            reason = "official filing XBRL normalized with required period coverage"
        return {
            "scope": selected_scope, "scope_reason": scope_reason,
            "annual_completeness": annual_completeness, "quarterly_completeness": quarterly_completeness,
            "annual_period_count": len(annual), "quarterly_period_count": len(quarterly),
            "annual_statements": annual, "quarterly_statements": quarterly,
            "latest_disclosed_periods": latest_disclosed,
            "latest_parsed_periods": latest_parsed,
            "target_periods": target_periods,
            "missing_target_periods": missing_target_periods,
            "state": "PRESENT" if usable else "DATA_REPAIR_REQUIRED",
            "provenance_validation": {
                "provider": sorted({row["source_provider"] for row in statements}),
                "available_at": all(row["published_at"].date() <= as_of_date for row in statements),
                "source_row_hash": bool(statements) and all(row.get("source_row_hash") for row in statements),
                "filing_source": bool(statements), "reason": reason,
            },
        }, artifacts

    @staticmethod
    def _valid_filing_isins(expected_isin: str, period_end: date,
                            identifier_history: list[dict] | None) -> set[str]:
        """Return exact ISINs valid for a filing period, plus the current document identity.

        The current ISIN remains valid for a later restatement of an older period. A
        prior ISIN is accepted only inside its registered effective-date window.
        """
        valid = {expected_isin.upper()}
        for row in identifier_history or []:
            if str(row.get("identifier_type", "")).upper() != "ISIN":
                continue
            valid_from = row.get("valid_from")
            valid_to = row.get("valid_to")
            if isinstance(valid_from, str):
                valid_from = date.fromisoformat(valid_from)
            if isinstance(valid_to, str):
                valid_to = date.fromisoformat(valid_to)
            if valid_from and period_end < valid_from:
                continue
            if valid_to and period_end > valid_to:
                continue
            value = str(row.get("identifier_value") or "").strip().upper()
            if value:
                valid.add(value)
        return valid

    def _get_with_params(self, url: str, params: dict, *, referer: str) -> requests.Response:
        return self._request(url, referer=referer, params=params)

    @staticmethod
    def _scope(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text == "consolidated":
            return "consolidated"
        if text in {"standalone", "non-consolidated"}:
            return "standalone"
        return "SCOPE_UNRESOLVED"

    @staticmethod
    def _bse_period_end(code: str) -> date | None:
        match = re.fullmatch(r"(JQ|SQ|DQ|MQ|MC)(\d{4})-(\d{4})", code)
        if not match:
            return None
        prefix, first_year, second_year = match.groups()
        month_day = {"JQ": (6, 30), "SQ": (9, 30), "DQ": (12, 31), "MQ": (3, 31), "MC": (3, 31)}[prefix]
        year = int(second_year if prefix in {"MQ", "MC"} else first_year)
        return date(year, *month_day)

    @staticmethod
    def _target_period_ends(latest: date | None, period_type: str, count: int) -> list[date]:
        if latest is None:
            return []
        if period_type == "annual":
            return [date(latest.year - offset, latest.month, latest.day) for offset in range(count)]
        result = []
        start_month_index = latest.year * 12 + latest.month - 1
        quarter_days = {3: 31, 6: 30, 9: 30, 12: 31}
        for offset in range(count):
            month_index = start_month_index - 3 * offset
            year, zero_based_month = divmod(month_index, 12)
            month = zero_based_month + 1
            result.append(date(year, month, quarter_days[month]))
        return result

    @staticmethod
    def _empty_fundamentals(reason: str) -> dict:
        return {
            "scope": "SCOPE_UNRESOLVED", "scope_reason": "filing_source_unavailable",
            "annual_completeness": 0.0, "quarterly_completeness": 0.0,
            "annual_period_count": 0, "quarterly_period_count": 0,
            "annual_statements": [], "quarterly_statements": [], "state": "DATA_REPAIR_REQUIRED",
            "latest_disclosed_periods": {"annual": None, "quarterly": None},
            "latest_parsed_periods": {"annual": None, "quarterly": None},
            "target_periods": {"annual": [], "quarterly": []},
            "missing_target_periods": {"annual": [], "quarterly": []},
            "provenance_validation": {"provider": [], "available_at": False, "source_row_hash": False,
                                      "filing_source": False, "reason": reason},
        }

    @staticmethod
    def bse_market_cap(identity_row: dict, *, artifact_id: str) -> dict | None:
        value = OfficialExchangeClient._float(identity_row.get("Mktcap"))
        if not value or value <= 0:
            return None
        return {"full_market_cap_cr": value, "free_float_market_cap_cr": None, "artifact_id": artifact_id}

    @staticmethod
    def _float(value: Any) -> float | None:
        try:
            result = float(str(value).replace(",", ""))
            return result if pd.notna(result) else None
        except (TypeError, ValueError):
            return None


class ExistingRepositoryProvider:
    """Read-only adapter over existing OHLCV, feature, action, and fundamental stores."""

    def __init__(self, identity_by_isin: dict[str, dict], *, project_root: str | Path | None = None):
        self.paths = get_domain_paths(project_root=project_root, data_domain="operational")
        self.identity_by_isin = identity_by_isin

    def _symbol(self, isin: str) -> str:
        return str(self.identity_by_isin[isin]["symbol"]).upper()

    def market_snapshot(self, isin: str, as_of_date: date) -> dict:
        symbol = self._symbol(isin)
        conn = duckdb.connect(str(self.paths.ohlcv_db_path), read_only=True)
        try:
            row = conn.execute(
                """SELECT cast(timestamp AS DATE), close, adjusted_close, provider, ingestion_ts,
                          validation_status, adjustment_version, isin
                   FROM _catalog WHERE upper(symbol_id) = ? AND cast(timestamp AS DATE) <= ?
                   ORDER BY timestamp DESC LIMIT 1""",
                [symbol, as_of_date],
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return {"state": "SOURCE_UNAVAILABLE"}
        return {
            "state": "PRESENT", "price_date": row[0], "raw_close": row[1], "adjusted_close": row[2],
            "provider": row[3], "ingested_at": row[4], "validation_status": row[5],
            "adjustment_version": row[6], "stored_isin": row[7],
            "freshness_status": "FRESH" if (as_of_date - row[0]).days <= 4 else "STALE",
        }

    def get_ohlcv(self, isin: str, start_date: date, end_date: date, adjusted: bool = True) -> list[dict]:
        symbol = self._symbol(isin)
        conn = duckdb.connect(str(self.paths.ohlcv_db_path), read_only=True)
        try:
            rows = conn.execute(
                """SELECT cast(timestamp AS DATE), open, high, low, close, adjusted_open, adjusted_high,
                          adjusted_low, adjusted_close, volume, adjustment_factor, adjustment_version,
                          provider, ingestion_ts
                   FROM _catalog WHERE upper(symbol_id) = ? AND cast(timestamp AS DATE) BETWEEN ? AND ?
                   ORDER BY timestamp""",
                [symbol, start_date, end_date],
            ).fetchall()
        finally:
            conn.close()
        keys = ["trade_date", "raw_open", "raw_high", "raw_low", "raw_close", "adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close", "volume", "adjustment_factor", "corporate_action_version", "provider", "ingested_at"]
        return [dict(zip(keys, row)) for row in rows]

    def get_corporate_actions(self, isin: str, through_date: date) -> list[dict]:
        symbol = self._symbol(isin)
        conn = duckdb.connect(str(self.paths.ohlcv_db_path), read_only=True)
        try:
            rows = conn.execute(
                """SELECT isin, action_type, ex_date, parsed_ratio, price_factor, share_factor, source,
                          raw_payload_hash, status, normalizer_version, raw_subject, raw_payload_json
                   FROM _corporate_actions WHERE upper(symbol) = ? AND ex_date <= ? ORDER BY ex_date""",
                [symbol, through_date],
            ).fetchall()
        finally:
            conn.close()
        keys = ["stored_isin", "action_type", "ex_date", "ratio", "price_factor", "share_factor", "source", "source_row_hash", "status", "adjustment_version", "raw_subject", "raw_payload_json"]
        return [dict(zip(keys, row)) for row in rows]

    def technical_snapshot(self, isin: str, as_of_date: date) -> dict:
        rows = self.get_ohlcv(isin, as_of_date - timedelta(days=330), as_of_date)
        usable = [r for r in rows if r.get("adjusted_close") is not None]
        if len(usable) < 200:
            return {"status": "UNAVAILABLE", "reason": "INSUFFICIENT_ADJUSTED_HISTORY"}
        closes = pd.Series([r["adjusted_close"] for r in usable], dtype=float)
        latest = float(closes.iloc[-1])
        sma50 = float(closes.tail(50).mean())
        sma200 = float(closes.tail(200).mean())
        if latest > sma50 > sma200:
            status = "CONFIRMED"
        elif latest > sma200:
            status = "AWAIT_BREAKOUT"
        else:
            status = "WEAK_STRUCTURE"
        return {"status": status, "as_of_date": usable[-1]["trade_date"], "adjusted_close": latest, "sma50": sma50, "sma200": sma200, "formula_version": "technical-timing-v1"}

    def fundamental_snapshot(self, isin: str, through_date: date, *, company_type: str) -> dict:
        symbol = self._symbol(isin)
        db = self.paths.fundamentals_dir / "screener_financials.db"
        if not db.exists():
            return {"scope": "SCOPE_UNRESOLVED", "annual_completeness": 0.0, "quarterly_completeness": 0.0, "state": "DATA_REPAIR_REQUIRED"}
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                """SELECT period_type, report_date, statement_basis, metric_id, value, available_at, source, synced_at
                   FROM screener_financials WHERE upper(symbol) = ? AND date(available_at) <= date(?)
                   ORDER BY date(report_date) DESC""",
                [symbol, through_date.isoformat()],
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            return {"scope": "SCOPE_UNRESOLVED", "annual_completeness": 0.0, "quarterly_completeness": 0.0, "state": "DATA_REPAIR_REQUIRED"}
        frame = pd.DataFrame(rows, columns=["period_type", "report_date", "scope", "metric", "value", "available_at", "source", "synced_at"])
        mandatory = self._mandatory_metrics(company_type)
        scope_results: list[dict] = []
        for scope, scoped in frame.groupby("scope"):
            annual = self._completeness(scoped[scoped.period_type == "annual"], mandatory["annual"], 6)
            quarterly = self._completeness(scoped[scoped.period_type == "quarterly"], mandatory["quarterly"], 12)
            scope_results.append({"scope": scope, "annual": annual, "quarterly": quarterly, "score": (annual + quarterly) / 2})
        usable_consolidated = next((x for x in scope_results if x["scope"] == "consolidated" and x["score"] >= 0.70), None)
        selected = usable_consolidated or max(scope_results, key=lambda x: (x["score"], x["scope"] == "standalone"))
        return {
            "scope": selected["scope"], "scope_reason": "consolidated_usable" if usable_consolidated else "best_usable_scope_no_splicing",
            "annual_completeness": selected["annual"], "quarterly_completeness": selected["quarterly"],
            "state": "DATA_REPAIR_REQUIRED",
            "provenance_validation": {
                "provider": sorted(set(frame.source.astype(str))), "available_at": True,
                "source_row_hash": False, "filing_source": False,
                "reason": "existing Screener-derived rows lack exchange filing URL and source-row hash",
            },
        }

    @staticmethod
    def _mandatory_metrics(company_type: str) -> dict[str, set[str]]:
        if company_type == "BANK":
            bank = {"nim", "gnpa", "nnpa", "slippages", "credit_cost", "roa", "roe", "cet1"}
            return {"annual": bank, "quarterly": bank}
        if company_type == "FINANCIAL_INSTITUTION":
            return {
                "annual": {"sales", "net_profit", "eps", "reserves", "borrowings", "cash_and_bank"},
                "quarterly": {"sales", "net_profit", "eps"},
            }
        if company_type not in {"CORPORATE", "INDUSTRIAL", "MARKET_INFRASTRUCTURE"}:
            return {"annual": set(), "quarterly": set()}
        return {
            "annual": {"sales", "operating_profit", "net_profit", "eps", "reserves", "borrowings", "cash_and_bank", "cash_from_operations", "capex"},
            "quarterly": {"sales", "operating_profit", "net_profit", "exceptional_items", "eps"},
        }

    @staticmethod
    def _completeness(frame: pd.DataFrame, mandatory: set[str], periods: int) -> float:
        if frame.empty or not mandatory:
            return 0.0
        dates = list(dict.fromkeys(frame.report_date.astype(str)))[:periods]
        selected = frame[frame.report_date.astype(str).isin(dates)]
        present = set(selected.loc[selected.value.notna(), "metric"].astype(str))
        return round(len(present & mandatory) / len(mandatory), 4)
