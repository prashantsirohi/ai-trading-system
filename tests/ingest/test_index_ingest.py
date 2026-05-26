from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ingest import index_ingest
from ai_trading_system.domains.ingest.index_ingest import IndexCollector, IndexIngestConfig


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self):
        self.headers = {}
        self.urls: list[str] = []

    def get(self, url: str, timeout: int = 0):
        self.urls.append(url)
        if url == "https://www.nseindia.com/":
            return _FakeResponse(200)
        if "equity-stockIndices" in url:
            return _FakeResponse(404)
        return _FakeResponse(
            200,
            {
                "data": [
                    {
                        "index": "NIFTY 50",
                        "open": "100",
                        "high": "110",
                        "low": "95",
                        "last": "108",
                        "volume": "12345",
                        "turnover": "98765",
                    }
                ]
            },
        )


def _collector() -> IndexCollector:
    collector = IndexCollector.__new__(IndexCollector)
    collector.config = IndexIngestConfig(
        ohlcv_db_path=":memory:",
        indices=[("NIFTY 50", "NIFTY_50", "Broad Market", None)],
    )
    return collector


def test_fetch_index_ohlc_falls_back_to_all_indices(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(index_ingest.requests, "Session", lambda: session)

    out = _collector().fetch_index_ohlc("NIFTY 50", "2026-05-25", "2026-05-25")

    assert out.iloc[0].to_dict() == {
        "index_code": "NIFTY_50",
        "date": "2026-05-25",
        "open": 100,
        "high": 110,
        "low": 95,
        "close": 108,
        "volume": 12345,
        "value": 98765,
    }
    assert any("equity-stockIndices" in url for url in session.urls)
    assert "https://www.nseindia.com/api/allIndices" in session.urls


def test_fetch_latest_queries_once_per_index(monkeypatch):
    collector = _collector()
    calls: list[tuple[str, str, str]] = []

    def fake_fetch(index_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        calls.append((index_name, start_date, end_date))
        return pd.DataFrame([{"index_code": "NIFTY_50", "date": end_date, "close": 108}])

    monkeypatch.setattr(collector, "fetch_index_ohlc", fake_fetch)

    out = collector.fetch_latest(["2026-05-23", "2026-05-24", "2026-05-25"])

    assert calls == [("NIFTY 50", "2026-05-25", "2026-05-25")]
    assert len(out) == 1
