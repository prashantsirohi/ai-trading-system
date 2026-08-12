import json
from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.domains.research_screener.providers import OfficialExchangeClient, _artifact
from ai_trading_system.domains.research_screener.service import PersistentScreenerService


def _identity():
    return {
        "mii_rows": [
            {"symbol": "ABC", "isin": "INEABC"},
            {"symbol": "HAWKINCOOK", "isin": "INE979B01015"},
            {"symbol": "E2E", "isin": "INE255Z01027"},
        ],
        "nse_rows": [
            {"SYMBOL": "ABC", "ISIN NUMBER": "INEABC", "SERIES": "EQ"},
            {"SYMBOL": "E2E", "ISIN NUMBER": "INE255Z01027", "SERIES": "EQ"},
        ],
        "bse_rows": [
            {"scrip_id": "ABC", "ISIN_NUMBER": "INEABC", "SCRIP_CD": "500001", "GROUP": "A", "Status": "Active", "FACE_VALUE": "1", "Mktcap": "1234"},
            {"scrip_id": "HAWKINCOOK", "ISIN_NUMBER": "INE979B01015", "SCRIP_CD": "508486", "GROUP": "B", "Status": "Active", "FACE_VALUE": "10", "Mktcap": "4398.72"},
            {"scrip_id": "SAREGAMA", "ISIN_NUMBER": "INE979A01025", "SCRIP_CD": "532163", "GROUP": "A", "Status": "Active", "FACE_VALUE": "1", "Mktcap": "10037"},
            {"scrip_id": "E2E", "ISIN_NUMBER": "INE255Z01027", "SCRIP_CD": "543532", "GROUP": "B", "Status": "Active", "FACE_VALUE": "1", "Mktcap": "2500"},
        ],
    }


def test_dual_listing_identity_resolves_by_isin():
    result = OfficialExchangeClient.resolve_fixture({"symbol": "ABC", "isin": "INEABC"}, _identity())
    assert result["status"] == "RESOLVED"
    assert {x["exchange"] for x in result["listings"]} == {"NSE", "BSE"}


def test_bse_only_symbol_is_discovered_without_nse_dependency():
    identity = _identity()
    identity["mii_rows"].append({"symbol": "ONLYBSE", "isin": "INEONLY"})
    identity["bse_rows"].append({"scrip_id": "ONLYBSE", "ISIN_NUMBER": "INEONLY", "SCRIP_CD": "500999", "GROUP": "B", "Status": "Active", "FACE_VALUE": "10"})
    result = OfficialExchangeClient.resolve_fixture({"symbol": "ONLYBSE", "isin": "INEONLY"}, identity)
    assert result["status"] == "RESOLVED"
    assert [x["exchange"] for x in result["listings"]] == ["BSE"]


def test_hawkins_fixture_isin_conflict_is_not_fuzzy_joined():
    result = OfficialExchangeClient.resolve_fixture({"symbol": "HAWKINCOOK", "isin": "INE979A01025"}, _identity())
    assert result["status"] == "IDENTITY_CONFLICT"
    assert result["observed_isins"] == ["INE979B01015"]
    assert result["fixture_isin_matches_other_bse"] == ["SAREGAMA"]


def test_corrected_hawkins_fixture_resolves_exactly_without_fuzzy_join():
    result = OfficialExchangeClient.resolve_fixture({"symbol": "HAWKINCOOK", "isin": "INE979B01015"}, _identity())
    assert result["status"] == "RESOLVED"
    assert result["observed_isins"] == ["INE979B01015"]
    assert {listing["exchange"] for listing in result["listings"]} == {"BSE"}


def test_e2e_current_isin_resolves_but_prior_isin_remains_a_conflict():
    prior = OfficialExchangeClient.resolve_fixture({"symbol": "E2E", "isin": "INE255Z01019"}, _identity())
    current = OfficialExchangeClient.resolve_fixture({"symbol": "E2E", "isin": "INE255Z01027"}, _identity())
    assert prior["status"] == "IDENTITY_CONFLICT"
    assert current["status"] == "RESOLVED"
    assert current["observed_isins"] == ["INE255Z01027"]


class _Response:
    def __init__(self, payload, content_type, *, status_code=200):
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        self._payload = payload
        self.content = payload if isinstance(payload, bytes) else json.dumps(payload).encode()

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses):
        self.headers = {}
        self.responses = list(responses)
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return self.responses.pop(0)


def _quote_session(last_update="10-Aug-2026 16:00:00", series="EQ", metadata_isin="INE731H01025"):
    metadata = {
        "symbol": "ACE", "isin": metadata_isin, "activeSeries": [series] if series != "EQ" else ["EQ", "T0"],
        "marketType": "N",
    }
    symbol_data = {"equityResponse": [{
        "metaData": {"symbol": "ACE", "isinCode": "INE731H01025", "series": series},
        "tradeInfo": {"totalMarketCap": 132837305138, "ffmc": 45785432473.83, "issuedSize": 119083196},
        "lastUpdateTime": last_update,
    }]}
    return _Session([
        _Response(b"<!doctype html><html>quote</html>", "text/html"),
        _Response(metadata, "application/json"),
        _Response(symbol_data, "application/json"),
    ])


def test_current_nse_quote_contract_converts_rupees_to_crore():
    session = _quote_session()
    client = OfficialExchangeClient(session=session, min_interval=0)
    cap, artifact = client.nse_market_cap("ACE", date(2026, 8, 10), expected_isin="INE731H01025")

    assert cap["full_market_cap_cr"] == 13283.7305138
    assert cap["free_float_market_cap_cr"] == pytest.approx(4578.543247383)
    assert cap["shares_outstanding"] == 119083196
    assert cap["as_of_date"] == date(2026, 8, 10)
    assert artifact["effective_date"] == date(2026, 8, 10)
    assert artifact["metadata"]["raw_market_cap_unit"] == "RUPEES"
    assert "functionName=getSymbolData" in session.urls[-1]


def test_nse_quote_rejects_future_dated_market_cap():
    client = OfficialExchangeClient(session=_quote_session("11-Aug-2026 16:00:00"), min_interval=0)
    cap, artifact = client.nse_market_cap("ACE", date(2026, 8, 10), expected_isin="INE731H01025")
    assert cap is None
    assert artifact["validation_status"] == "FAILED"
    assert "FUTURE_DATED_QUOTE" in artifact["metadata"]["error"]


def test_nse_quote_uses_single_official_active_non_eq_series():
    session = _quote_session(series="BE")
    client = OfficialExchangeClient(session=session, min_interval=0)
    cap, artifact = client.nse_market_cap("ACE", date(2026, 8, 10), expected_isin="INE731H01025")
    assert cap is not None
    assert artifact["metadata"]["selected_series"] == "BE"
    assert "series=BE" in session.urls[-1]


def test_nse_quote_accepts_lagging_metadata_only_when_final_quote_isin_is_exact():
    session = _quote_session(series="BE", metadata_isin="INE731H01019")
    client = OfficialExchangeClient(session=session, min_interval=0)
    cap, artifact = client.nse_market_cap("ACE", date(2026, 8, 10), expected_isin="INE731H01025")

    assert cap is not None
    assert artifact["metadata"]["metadata_isin"] == "INE731H01019"
    assert artifact["metadata"]["metadata_isin_matches_quote"] is False
    assert artifact["metadata"]["selected_series"] == "BE"


def test_nse_quote_primes_browser_session_once_for_multiple_symbols():
    metadata = {
        "symbol": "ACE", "isin": "INE731H01025", "activeSeries": ["EQ"], "marketType": "N",
    }
    symbol_data = {"equityResponse": [{
        "metaData": {"symbol": "ACE", "isinCode": "INE731H01025", "series": "EQ"},
        "tradeInfo": {"totalMarketCap": 132837305138, "ffmc": 45785432473.83, "issuedSize": 119083196},
        "lastUpdateTime": "10-Aug-2026 16:00:00",
    }]}
    session = _Session([
        _Response(b"<!doctype html><html>quote</html>", "text/html"),
        _Response(metadata, "application/json"), _Response(symbol_data, "application/json"),
        _Response(metadata, "application/json"), _Response(symbol_data, "application/json"),
    ])
    client = OfficialExchangeClient(session=session, min_interval=0)

    first, _ = client.nse_market_cap("ACE", date(2026, 8, 10), expected_isin="INE731H01025")
    second, _ = client.nse_market_cap("ACE", date(2026, 8, 10), expected_isin="INE731H01025")

    assert first is not None and second is not None
    assert sum("get-quotes/equity" in url for url in session.urls) == 1


def test_official_request_retries_transient_server_response(monkeypatch):
    session = _Session([
        _Response(b"busy", "text/plain", status_code=503),
        _Response(b"ok", "text/plain"),
    ])
    monkeypatch.setattr("ai_trading_system.domains.research_screener.providers.time.sleep", lambda _: None)
    response = OfficialExchangeClient(session=session, min_interval=0)._get(
        "https://exchange.test/data", referer="https://exchange.test/",
    )

    assert response.status_code == 200
    assert session.urls == ["https://exchange.test/data", "https://exchange.test/data"]


@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        ("Face Value Split (Sub-Division)", "split"),
        ("Bonus Issue 1:1", "bonus"), ("Rights Issue", "rights"),
        ("Consolidation of shares", "consolidation"), ("Final Dividend", "dividend"),
        ("Demerger of undertaking", "demerger"), ("Scheme of Amalgamation", "merger"),
        ("Change in Name", "symbol_name_change"), ("Trading Series Change", "trading_series"),
    ],
)
def test_official_action_taxonomy(subject, expected):
    assert OfficialExchangeClient._classify_action(subject) == expected


def test_action_history_validates_no_adjusting_events_and_blocks_unmatched_split():
    artifact = {"validation_status": "VALID"}
    status, detail = PersistentScreenerService._validate_action_history(
        [{"action_type": "dividend", "ex_date": date(2026, 7, 1)}], [], artifact,
    )
    assert status == "VALIDATED"
    assert detail["unmatched_events"] == []

    status, detail = PersistentScreenerService._validate_action_history(
        [{"action_type": "split", "ex_date": date(2026, 7, 1)}], [], artifact,
    )
    assert status == "ADJUSTMENT_INCOMPLETE"
    assert detail["unmatched_events"][0]["action_type"] == "split"


def test_kpi_contract_routes_banks_and_market_infrastructure():
    contracts = json.loads(
        (Path(__file__).resolve().parents[3] / "configs/research_screener/kpi_contracts.json").read_text()
    )
    bank = PersistentScreenerService._select_kpi_contract("HDFCBANK", "BANK", contracts, "artifact:kpi")
    mcx = PersistentScreenerService._select_kpi_contract("MCX", "MARKET_INFRASTRUCTURE", contracts, "artifact:kpi")
    corporate = PersistentScreenerService._select_kpi_contract("ACE", "CORPORATE", contracts, "artifact:kpi")
    assert bank["contract_name"] == "bank" and "nim" in bank["definition"]["quarterly_kpis"]
    assert mcx["contract_name"] == "market_infrastructure" and "transaction_volume" in mcx["definition"]["quarterly_kpis"]
    assert corporate["contract_name"] == "industrial_corporate" and "order_book" in corporate["definition"]["quarterly_kpis"]


def test_source_artifact_identity_includes_request_locator():
    left = _artifact("results", "NSE", b"[]", url="https://example.test/?symbol=ACE", effective_date=date(2026, 8, 10), row_count=0)
    right = _artifact("results", "NSE", b"[]", url="https://example.test/?symbol=MCX", effective_date=date(2026, 8, 10), row_count=0)
    assert left["content_hash"] == right["content_hash"]
    assert left["artifact_id"] != right["artifact_id"]


def test_bse_inline_and_xml_locators_share_revision_identity():
    xml = "FourOneUploadDocument/Integrated_Finance_Ind_As_508486_2852026175355.xml"
    inline = "IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_508486_2852026175355_IFIndAs.html"
    assert OfficialExchangeClient._bse_document_revision_id(xml) == OfficialExchangeClient._bse_document_revision_id(inline)


def test_filing_identity_accepts_prior_isin_only_inside_effective_window():
    history = [
        {"identifier_type": "ISIN", "identifier_value": "INEOLD", "valid_from": date(2020, 1, 1), "valid_to": date(2026, 6, 4)},
        {"identifier_type": "ISIN", "identifier_value": "INENEW", "valid_from": date(2026, 6, 5), "valid_to": None},
    ]
    assert OfficialExchangeClient._valid_filing_isins("INENEW", date(2026, 3, 31), history) == {"INEOLD", "INENEW"}
    assert OfficialExchangeClient._valid_filing_isins("INENEW", date(2026, 6, 30), history) == {"INENEW"}


def test_official_fundamental_fallback_selects_one_snapshot_without_splicing():
    primary = {
        "state": "DATA_REPAIR_REQUIRED", "annual_completeness": 0.6667, "quarterly_completeness": 1.0,
        "latest_disclosed_periods": {"annual": date(2026, 3, 31), "quarterly": date(2026, 6, 30)},
        "latest_parsed_periods": {"annual": date(2026, 3, 31), "quarterly": date(2026, 6, 30)},
        "annual_statements": [{"source_provider": "NSE"}], "quarterly_statements": [{"source_provider": "NSE"}],
        "provenance_validation": {"provider": ["NSE_INTEGRATED_XBRL"]},
    }
    fallback = {
        "state": "PRESENT", "annual_completeness": 1.0, "quarterly_completeness": 0.9167,
        "latest_disclosed_periods": {"annual": date(2026, 3, 31), "quarterly": date(2026, 6, 30)},
        "latest_parsed_periods": {"annual": date(2026, 3, 31), "quarterly": date(2026, 6, 30)},
        "annual_statements": [{"source_provider": "BSE"}], "quarterly_statements": [{"source_provider": "BSE"}],
        "provenance_validation": {"provider": ["BSE_XBRL"]},
    }
    selected = PersistentScreenerService._select_fundamental_provider(primary, fallback)
    assert selected["state"] == "PRESENT"
    assert {row["source_provider"] for row in selected["annual_statements"] + selected["quarterly_statements"]} == {"BSE"}
    assert selected["provider_selection"]["selected"] == "fallback"
