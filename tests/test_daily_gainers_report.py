from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import date, datetime
from importlib import resources
from pathlib import Path

import duckdb

from ai_trading_system.domains.publish.channels.daily_gainers.events import attach_events
from ai_trading_system.domains.publish.channels.daily_gainers.gainers import compute_gainers
from ai_trading_system.domains.publish.channels.daily_gainers.llm import generate_insight
from ai_trading_system.domains.publish.channels.daily_gainers.renderer import render_html


def test_compute_gainers_strict_threshold_and_equity_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _write_ohlcv(
        db_path,
        [
            ("AAA", "NSE", "2026-05-01", 100.0, 1000, "equity", False),
            ("AAA", "NSE", "2026-05-02", 105.1, 1200, "equity", False),
            ("BBB", "NSE", "2026-05-01", 100.0, 1000, "equity", False),
            ("BBB", "NSE", "2026-05-02", 105.0, 1300, "equity", False),
            ("CCC", "NSE", "2026-05-01", 100.0, 1000, "equity", False),
            ("CCC", "NSE", "2026-05-02", 90.0, 1300, "equity", False),
            ("NIFTY", "NSE", "2026-05-01", 100.0, 1000, "index", True),
            ("NIFTY", "NSE", "2026-05-02", 120.0, 1300, "index", True),
        ],
    )

    out = compute_gainers(db_path, date(2026, 5, 2), threshold_pct=5.0)

    assert out["symbol_id"].tolist() == ["AAA"]
    assert round(float(out.iloc[0]["pct_change"]), 2) == 5.1


def test_compute_gainers_defaults_to_latest_trading_date_and_previous_available(tmp_path: Path) -> None:
    db_path = tmp_path / "ohlcv.duckdb"
    _write_ohlcv(
        db_path,
        [
            ("AAA", "NSE", "2026-05-01", 100.0, 1000, None, False),
            ("AAA", "NSE", "2026-05-04", 107.0, 1200, None, False),
            ("BBB", "NSE", "2026-05-02", 100.0, 1000, "equity", False),
            ("BBB", "NSE", "2026-05-04", 101.0, 1200, "equity", False),
        ],
    )

    out = compute_gainers(db_path, None, threshold_pct=5.0)

    assert out.attrs["as_of"] == date(2026, 5, 4)
    assert out["symbol_id"].tolist() == ["AAA"]
    assert out.iloc[0]["trade_date"] == date(2026, 5, 4)


def test_attach_events_returns_symbol_mapping(monkeypatch) -> None:
    service = _FakeEventService(
        {
            "AAA": [
                _EventRecord(
                    symbol="AAA",
                    primary_category="board_meeting",
                    one_line_summary="Board meeting announced",
                    event_date=datetime(2026, 5, 3, 9, 0),
                    published_at=None,
                    importance_score=8.0,
                    trust_score=91.0,
                    link="https://example.test/event",
                    source="nse",
                )
            ]
        }
    )
    monkeypatch.setattr(
        "ai_trading_system.domains.publish.channels.daily_gainers.events.get_event_query_service",
        lambda: service,
    )

    out = attach_events(["AAA", "BBB"], as_of=date(2026, 5, 4), lookback_days=7, min_trust=50.0)

    assert list(out) == ["AAA", "BBB"]
    assert out["AAA"][0].summary == "Board meeting announced"
    assert out["BBB"] == []
    assert service.calls[0]["since"] == datetime(2026, 4, 27, 0, 0)


def test_attach_events_tolerates_missing_market_intel(monkeypatch) -> None:
    def _missing():
        raise FileNotFoundError("missing")

    monkeypatch.setattr(
        "ai_trading_system.domains.publish.channels.daily_gainers.events.get_event_query_service",
        _missing,
    )

    assert attach_events(["AAA"], as_of=date(2026, 5, 4)) == {"AAA": []}


def test_generate_insight_without_key_returns_fallback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENROUTER_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    db_path = tmp_path / "ohlcv.duckdb"
    _write_ohlcv(
        db_path,
        [
            ("AAA", "NSE", "2026-05-01", 100.0, 1000, "equity", False),
            ("AAA", "NSE", "2026-05-02", 106.0, 1200, "equity", False),
        ],
    )
    gainers = compute_gainers(db_path, None)

    out = generate_insight(gainers, {"AAA": []})

    assert out["summary_md"] == "LLM unavailable - see table below."
    assert out["per_stock"] == {}
    assert out["status"] == "skipped_no_api_key"


def test_daily_gainers_renderer_includes_summary_rows_and_takeaways() -> None:
    html = render_html(
        {
            "report_date": date(2026, 5, 4),
            "threshold": 5.0,
            "lookback_days": 7,
            "gainers_count": 1,
            "summary_md": "**Momentum broadened** across one name.",
            "gainers": [
                {
                    "symbol_id": "AAA",
                    "pct_change": 7.25,
                    "close": 123.45,
                    "volume": 1000000,
                    "events_count": 1,
                    "top_event": "Board meeting announced",
                    "takeaway": "Move has a fresh board-meeting catalyst.",
                    "events": [
                        {
                            "event_date": datetime(2026, 5, 3, 9, 0),
                            "category": "board_meeting",
                            "summary": "Board meeting announced",
                            "trust_score": 91.0,
                            "link": None,
                        }
                    ],
                }
            ],
        }
    )

    assert "<strong>Momentum broadened</strong>" in html
    assert "AAA" in html
    assert "7.2%" in html
    assert "Board meeting announced" in html
    assert "Move has a fresh board-meeting catalyst." in html


def test_daily_gainers_assets_are_packaged() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    package_data = project["tool"]["setuptools"]["package-data"]["ai_trading_system"]

    assert "domains/publish/channels/daily_gainers/templates/*.html" in package_data
    assert "domains/publish/channels/daily_gainers/static/*.css" in package_data

    pkg = resources.files("ai_trading_system.domains.publish.channels.daily_gainers")
    assert pkg.joinpath("templates", "daily_report.html").is_file()
    assert pkg.joinpath("static", "report.css").is_file()


def _write_ohlcv(db_path: Path, rows: list[tuple[str, str, str, float, int, str | None, bool]]) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE _catalog(
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                close DOUBLE,
                volume BIGINT,
                instrument_type VARCHAR,
                is_benchmark BOOLEAN
            )
            """
        )
        conn.executemany(
            "INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


@dataclass
class _EventRecord:
    symbol: str
    primary_category: str
    one_line_summary: str
    event_date: datetime | None
    published_at: datetime | None
    importance_score: float
    trust_score: float
    link: str | None
    source: str


class _FakeEventService:
    def __init__(self, records_by_symbol: dict[str, list[_EventRecord]]) -> None:
        self.records_by_symbol = records_by_symbol
        self.calls: list[dict[str, object]] = []

    def get_events_for_symbol(self, symbol: str, **kwargs):
        self.calls.append({"symbol": symbol, **kwargs})
        return self.records_by_symbol.get(symbol, [])

