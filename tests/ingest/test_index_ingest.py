from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.domains.ingest import index_ingest
from ai_trading_system.domains.ingest.index_ingest import IndexCollector, IndexIngestConfig


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

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


def test_fetch_latest_reads_every_requested_archive_session(monkeypatch):
    collector = _collector()
    calls: list[str] = []

    def fake_archive(trade_date: str) -> pd.DataFrame:
        calls.append(trade_date)
        return pd.DataFrame(
            [{"index_code": "NIFTY_50", "date": trade_date, "close": 108}]
        )

    monkeypatch.setattr(collector, "fetch_index_archive", fake_archive)

    out = collector.fetch_latest(["2026-05-23", "2026-05-24", "2026-05-25"])

    assert calls == ["2026-05-23", "2026-05-24", "2026-05-25"]
    assert len(out) == 3


def test_fetch_latest_does_not_relabel_live_tick_for_historical_archive_miss(monkeypatch):
    collector = _collector()
    monkeypatch.setattr(collector, "fetch_index_archive", lambda trade_date: pd.DataFrame())
    live_calls: list[str] = []

    def fake_live(index_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        live_calls.append(end_date)
        return pd.DataFrame([{"index_code": "NIFTY_50", "date": end_date, "close": 108}])

    monkeypatch.setattr(collector, "fetch_index_ohlc", fake_live)

    out = collector.fetch_latest(["2026-08-21", "2026-09-08"])

    assert out.empty
    assert live_calls == []


def test_fetch_latest_allows_live_tick_for_today(monkeypatch):
    collector = _collector()
    today = date.today().isoformat()
    monkeypatch.setattr(collector, "fetch_index_archive", lambda trade_date: pd.DataFrame())
    monkeypatch.setattr(
        collector,
        "fetch_index_ohlc",
        lambda index_name, start_date, end_date: pd.DataFrame(
            [{"index_code": "NIFTY_50", "date": end_date, "close": 108}]
        ),
    )

    out = collector.fetch_latest([today])

    assert out.iloc[0]["date"] == today


def test_archive_parser_requires_exact_requested_date(monkeypatch):
    csv_text = """Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Volume,Turnover (Rs. Cr.)
Nifty 50,21-08-2026,24284.05,24284.05,24206.8,24252.0,259287088,18607.89
"""
    monkeypatch.setattr(
        index_ingest.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(200, text=csv_text),
    )

    out = _collector().fetch_index_archive("2026-08-21")

    assert out.iloc[0].to_dict() == {
        "index_code": "NIFTY_50",
        "date": "2026-08-21",
        "open": 24284.05,
        "high": 24284.05,
        "low": 24206.8,
        "close": 24252.0,
        "volume": 259287088,
        "value": 18607.89,
        "provider": "nse_index_archive",
    }
    assert _collector().fetch_index_archive("2026-08-22").empty
