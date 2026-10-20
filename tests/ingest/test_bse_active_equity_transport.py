import hashlib
from types import SimpleNamespace

import pytest

from ai_trading_system.domains.ingest.new_symbol_onboarding import (
    BSEActiveEquityClient,
    BSEProfileClient,
    BSEMasterAcquisitionError,
    BSE_LISTING_PAGE_URL,
    BSE_ACTIVE_EQUITIES_URL,
    BSE_PROFILE_URL,
)
from ai_trading_system.domains.ingest.universe_refresh import transient_failure

ROW = {
    "SCRIP_CD": "500999",
    "Scrip_Name": "Example",
    "Status": "Active",
    "ISIN_NUMBER": "INEONLY01010",
    "scrip_id": "ONLYBSE",
    "Mktcap": "1000",
}


def response(url, *, status=200, payload=None):
    return SimpleNamespace(
        url=url,
        status_code=status,
        headers={"Content-Type": "application/json", "Set-Cookie": "private-cookie"},
        content=b"private-response-body",
        json=lambda: payload,
    )


def session_for(responses):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return responses.pop(0)

    return SimpleNamespace(headers={}, get=get), calls


def test_default_client_warms_official_page_with_browser_transport(monkeypatch):
    from curl_cffi import requests

    session, calls = session_for(
        [
            response(BSE_LISTING_PAGE_URL),
            response(BSE_ACTIVE_EQUITIES_URL, payload=[ROW]),
        ]
    )
    factory_options = []
    monkeypatch.setattr(
        requests, "Session", lambda **kw: (factory_options.append(kw) or session)
    )
    rows, url = BSEActiveEquityClient(timeout_sec=8).fetch()
    assert rows == [ROW]
    assert url == BSE_ACTIVE_EQUITIES_URL
    assert factory_options == [{"impersonate": "chrome"}]
    assert [c[0] for c in calls] == [BSE_LISTING_PAGE_URL, BSE_ACTIVE_EQUITIES_URL]
    assert all(c[1]["timeout"] == 8 for c in calls)
    assert calls[1][1]["params"]["status"] == "Active"


def test_denial_is_diagnostic_without_body_cookies_or_retry():
    denied = response(BSE_ACTIVE_EQUITIES_URL, status=403)
    session, calls = session_for([denied])
    with pytest.raises(BSEMasterAcquisitionError) as error:
        BSEActiveEquityClient(session=session).fetch()
    assert len(calls) == 1
    assert error.value.status_code == 403
    assert error.value.response_sha256 == hashlib.sha256(denied.content).hexdigest()
    assert "HTTP 403" in str(error.value)
    assert "private-cookie" not in str(error.value)
    assert "private-response-body" not in str(error.value)
    assert transient_failure(error.value) is False


def test_profile_uses_same_transport_and_preserves_denial(monkeypatch):
    from curl_cffi import requests

    session, calls = session_for([response(BSE_PROFILE_URL, status=403)])
    monkeypatch.setattr(requests, "Session", lambda **kw: session)
    target = SimpleNamespace(security_id="500999")
    with pytest.raises(BSEMasterAcquisitionError):
        BSEProfileClient().fetch(target)
    assert calls[0][0] == BSE_PROFILE_URL


def test_browser_timeouts_and_server_failures_retain_bounded_retry_classification():
    from curl_cffi.requests.exceptions import Timeout

    assert transient_failure(Timeout("timeout"))
    assert transient_failure(
        BSEMasterAcquisitionError(response(BSE_PROFILE_URL, status=503))
    )


@pytest.mark.parametrize(
    "payload", [[], {}, [ROW, "bad"], [ROW, {"SCRIP_CD": "500998"}]]
)
def test_incomplete_or_malformed_master_cannot_shrink_discovery(payload):
    session, _ = session_for([response(BSE_ACTIVE_EQUITIES_URL, payload=payload)])
    with pytest.raises(RuntimeError):
        BSEActiveEquityClient(session=session).fetch()
