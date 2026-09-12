import json
import warnings

import pandas as pd
import pytest

from ai_trading_system.domains.ingest.onboarding_gate import (
    pending_identities,
    exclude_pending,
)
from ai_trading_system.domains.ingest.nse_delivery_scraper import (
    NseHistoricalDeliveryScraper,
)


def test_pending_gate_includes_legacy_pending_and_is_exchange_scoped(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"pending": {"NEWCO": {"exchange": "NSE"}}}))
    identities = pending_identities(state_path=path)
    frame = pd.DataFrame(
        {"symbol_id": ["NEWCO", "NEWCO", "HEALTHY"], "exchange": ["NSE", "BSE", "NSE"]}
    )
    assert exclude_pending(frame, identities=identities).index.tolist() == [1, 2]
    assert pending_identities(state_path=path, data_domain="research") == set()
    path.write_text("broken")
    with pytest.raises(ValueError):
        pending_identities(state_path=path)


def test_delivery_numeric_input_emits_no_future_warning():
    scraper = NseHistoricalDeliveryScraper.__new__(NseHistoricalDeliveryScraper)
    raw = pd.DataFrame(
        {
            "Symbol": ["ABC", "XYZ"],
            "Series": ["EQ", "BE"],
            "Date": ["10-Sep-2026", "10-Sep-2026"],
            "% Dly Qt to Traded Qty": [83.04, 63.23],
            "Total Traded Quantity": ["1,000", "2,000"],
            "Deliverable Qty": [830, 1265],
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        result = scraper.normalize_frame(raw)
    assert result["delivery_pct"].tolist() == [83.04, 63.23]
    assert pd.api.types.is_numeric_dtype(result["delivery_pct"])


def test_ranker_filters_pending_before_scoring(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from ai_trading_system.domains.ranking import ranker as module

    path = tmp_path / "state.json"
    path.write_text(json.dumps({"pending": {"NEWCO": {"exchange": "NSE"}}}))
    ranker = module.StockRanker.__new__(module.StockRanker)
    ranker.data_domain = "operational"
    ranker.onboarding_state_path = path
    ranker.input_loader = object()
    frame = pd.DataFrame(
        {"symbol_id": ["NEWCO", "HEALTHY"], "exchange": ["NSE", "NSE"]}
    )
    monkeypatch.setattr(
        module, "RankInputSnapshot", lambda *a: SimpleNamespace(market=lambda: frame)
    )

    class ReachedScoring(Exception):
        pass

    def score(scores, *a, **kw):
        assert scores["symbol_id"].tolist() == ["HEALTHY"]
        raise ReachedScoring()

    ranker._compute_relative_strength = score
    with pytest.raises(ReachedScoring):
        ranker.rank_all(date="2026-09-11", weights={})
